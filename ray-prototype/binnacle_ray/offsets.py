from __future__ import annotations

import ray
from ray.actor import ActorHandle

from binnacle_ray.errors import InvalidOffset
from binnacle_ray.registry import NAMESPACE

OFFSET_STORE_NAME = "offset-store"


@ray.remote
class OffsetStore:
    """Committed offsets for consumer groups.

    A committed offset is the next record the group should consume, matching
    Kafka's convention. The key is (group, topic, partition); an entry that
    has never been committed starts at offset 0.
    """

    def __init__(self) -> None:
        self._offsets: dict[tuple[str, str, int], int] = {}

    def commit_offset(self, group: str, topic: str, partition: int, offset: int) -> None:
        _validate_coordinates(group, topic, partition)
        if offset < 0:
            raise InvalidOffset(f"offset {offset} is negative")
        self._offsets[(group, topic, partition)] = offset

    def committed_offset(self, group: str, topic: str, partition: int) -> int:
        _validate_coordinates(group, topic, partition)
        return self._offsets.get((group, topic, partition), 0)

    def committed_offsets(self, group: str, topic: str) -> dict[int, int]:
        if not group:
            raise ValueError("group must not be empty")
        if not topic:
            raise ValueError("topic must not be empty")
        return {
            partition: offset
            for (stored_group, stored_topic, partition), offset in self._offsets.items()
            if stored_group == group and stored_topic == topic
        }

    def all_offsets(self) -> dict[tuple[str, str, int], int]:
        return dict(self._offsets)


def _validate_coordinates(group: str, topic: str, partition: int) -> None:
    if not group:
        raise ValueError("group must not be empty")
    if not topic:
        raise ValueError("topic must not be empty")
    if partition < 0:
        raise ValueError(f"partition {partition} is negative")


def get_or_create_offset_store() -> ActorHandle:
    """Find the cluster-wide committed-offset actor or create it.

    Like the topic registry, it is detached so independent producer and
    consumer drivers can connect to the same state, and so a consumer restart
    does not erase the group's committed position.
    """
    return OffsetStore.options(
        name=OFFSET_STORE_NAME,
        namespace=NAMESPACE,
        lifetime="detached",
        get_if_exists=True,
    ).remote()


def commit_offset(group: str, topic: str, partition: int, offset: int) -> None:
    """Store ``offset`` as the group's next position for one partition."""
    ray.get(get_or_create_offset_store().commit_offset.remote(group, topic, partition, offset))
