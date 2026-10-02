from __future__ import annotations

import os

import ray

from binnacle_ray.errors import InvalidOffset
from binnacle_ray.record import Record


@ray.remote
class PartitionActor:
    """One partition: an ordered, append-only, in-memory log.

    Invariant: offsets are 0, 1, 2, ... with no gaps or duplicates, and each
    appended batch occupies a contiguous range. Our code does not lock anything:
    the guarantee comes from Ray running one actor method at a time
    (max_concurrency=1 by default).
    """

    def __init__(self, topic: str, partition: int) -> None:
        self._topic = topic
        self._partition = partition
        self._log: list[Record] = []

    def append(self, records: list[Record]) -> int:
        if not records:
            raise ValueError("append needs at least one record")
        base_offset = len(self._log)
        self._log.extend(r.stamped() for r in records)
        return base_offset

    def read(self, offset: int, max_records: int = 100) -> list[tuple[int, Record]]:
        if offset < 0:
            raise InvalidOffset(f"offset {offset} is negative")
        if max_records < 1:
            raise ValueError("max_records must be >= 1")
        batch = self._log[offset : offset + max_records]
        return [(offset + i, r) for i, r in enumerate(batch)]

    def next_offset(self) -> int:
        return len(self._log)

    def whereami(self) -> dict:
        """Placement evidence: which process and Ray node host this partition."""
        return {
            "topic": self._topic,
            "partition": self._partition,
            "pid": os.getpid(),
            "node_id": ray.get_runtime_context().get_node_id(),
        }
