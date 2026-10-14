from __future__ import annotations

import ray
from ray.actor import ActorHandle

from binnacle_ray.errors import PartitionOutOfRange, TopicAlreadyExists, UnknownTopic
from binnacle_ray.partition import PartitionActor

NAMESPACE = "binnacle"
REGISTRY_NAME = "topic-registry"


@ray.remote
class TopicRegistry:
    """Maps each topic to its partition actors.

    The registry creates (and therefore owns) the partition actors: if the
    registry dies, Ray kills them too (fate-sharing).
    """

    def __init__(self) -> None:
        self._topics: dict[str, list[ActorHandle]] = {}

    def create_topic(self, name: str, num_partitions: int) -> int:
        if not name:
            raise ValueError("topic name must not be empty")
        if num_partitions < 1:
            raise ValueError("num_partitions must be >= 1")
        if name in self._topics:
            raise TopicAlreadyExists(name)
        self._topics[name] = [PartitionActor.remote(name, p) for p in range(num_partitions)]
        return num_partitions

    def get_partition(self, topic: str, partition: int) -> ActorHandle:
        partitions = self._partitions_of(topic)
        if not 0 <= partition < len(partitions):
            raise PartitionOutOfRange(f"{topic} has {len(partitions)} partitions, got {partition}")
        return partitions[partition]

    def get_partitions(self, topic: str) -> list[ActorHandle]:
        return list(self._partitions_of(topic))

    def list_topics(self) -> dict[str, int]:
        return {name: len(parts) for name, parts in self._topics.items()}

    def _partitions_of(self, topic: str) -> list[ActorHandle]:
        if topic not in self._topics:
            raise UnknownTopic(topic)
        return self._topics[topic]


def get_or_create_registry() -> ActorHandle:
    """Find the cluster-wide registry by name (via Ray's GCS) or create it.

    Detached so it outlives the driver that created it: producer and consumer
    run as separate drivers and must see the same topics.
    """
    return TopicRegistry.options(
        name=REGISTRY_NAME,
        namespace=NAMESPACE,
        lifetime="detached",
        get_if_exists=True,
    ).remote()
