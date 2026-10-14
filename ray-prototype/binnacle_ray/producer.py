from __future__ import annotations

import hashlib
from dataclasses import dataclass

import ray
from ray.actor import ActorHandle

from binnacle_ray.record import Record
from binnacle_ray.registry import get_or_create_registry


@dataclass(frozen=True)
class SendResult:
    topic: str
    partition: int
    offset: int


class Producer:
    """Client-side partitioner on top of the partition actors.

    Non-empty keys use SHA-256 rather than Python's process-randomized
    ``hash()``, so every producer process maps the same key to the same
    partition. Empty keys use this producer instance's round-robin cursor.
    """

    def __init__(self, topic: str, registry: ActorHandle | None = None) -> None:
        if not topic:
            raise ValueError("topic must not be empty")
        self.topic = topic
        self._registry = registry or get_or_create_registry()
        self._partitions = ray.get(self._registry.get_partitions.remote(topic))
        self._next_empty_key_partition = 0

    @property
    def num_partitions(self) -> int:
        return len(self._partitions)

    def choose_partition(self, key: bytes) -> int:
        if not isinstance(key, bytes):
            raise TypeError("key must be bytes")
        if not key:
            partition = self._next_empty_key_partition
            self._next_empty_key_partition = (partition + 1) % self.num_partitions
            return partition
        digest = hashlib.sha256(key).digest()
        return int.from_bytes(digest[:8], "big") % self.num_partitions

    def send(self, key: bytes, value: bytes, timestamp: int = 0) -> SendResult:
        partition = self.choose_partition(key)
        record = Record(key=key, value=value, timestamp=timestamp)
        offset = ray.get(self._partitions[partition].append.remote([record]))
        return SendResult(topic=self.topic, partition=partition, offset=offset)
