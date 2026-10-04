from __future__ import annotations

import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass

import ray
from ray.actor import ActorHandle

from binnacle_ray.errors import PartitionOutOfRange
from binnacle_ray.offsets import get_or_create_offset_store
from binnacle_ray.record import Record
from binnacle_ray.registry import get_or_create_registry


@dataclass(frozen=True)
class ConsumedRecord:
    topic: str
    partition: int
    offset: int
    record: Record

    @property
    def key(self) -> bytes:
        return self.record.key

    @property
    def value(self) -> bytes:
        return self.record.value


class Consumer:
    """Polling consumer with a static partition assignment.

    The initial fetch position for every assigned partition is the group's
    committed offset (the next record to consume), defaulting to 0. A new
    Consumer instance for the same group therefore resumes where the previous
    instance committed.
    """

    def __init__(
        self,
        topic: str,
        group: str,
        partitions: Iterable[int] | None = None,
        registry: ActorHandle | None = None,
        offset_store: ActorHandle | None = None,
    ) -> None:
        if not topic:
            raise ValueError("topic must not be empty")
        if not group:
            raise ValueError("group must not be empty")
        self.topic = topic
        self.group = group
        self._registry = registry or get_or_create_registry()
        self._offset_store = offset_store or get_or_create_offset_store()
        all_partitions = ray.get(self._registry.get_partitions.remote(topic))
        assignment = list(range(len(all_partitions))) if partitions is None else list(partitions)
        if not assignment:
            raise ValueError("consumer assignment must not be empty")
        if len(set(assignment)) != len(assignment):
            raise ValueError(f"duplicate partitions in assignment {assignment}")
        for partition in assignment:
            if not 0 <= partition < len(all_partitions):
                raise PartitionOutOfRange(
                    f"{topic} has {len(all_partitions)} partitions, got {partition}"
                )
        self._assignment = tuple(assignment)
        self._partitions = {p: all_partitions[p] for p in self._assignment}
        self._positions = self._load_committed_positions()

    @property
    def assignment(self) -> tuple[int, ...]:
        return self._assignment

    @property
    def positions(self) -> dict[int, int]:
        """Local next-fetch positions; they may be ahead of commits."""
        return dict(self._positions)

    def committed_offsets(self) -> dict[int, int]:
        return self._load_committed_positions()

    def seek_to_committed(self) -> None:
        """Reset local fetch positions to the latest committed offsets."""
        self._positions = self._load_committed_positions()

    def poll(self, max_records: int = 100) -> list[ConsumedRecord]:
        if max_records < 1:
            raise ValueError("max_records must be >= 1")
        records: list[ConsumedRecord] = []
        for partition in self._assignment:
            offset = self._positions[partition]
            batch = ray.get(
                self._partitions[partition].read.remote(offset, max_records=max_records)
            )
            if not batch:
                continue
            self._positions[partition] = batch[-1][0] + 1
            records.extend(
                ConsumedRecord(
                    topic=self.topic,
                    partition=partition,
                    offset=record_offset,
                    record=record,
                )
                for record_offset, record in batch
            )
        return records

    def commit_offset(self, partition: int, offset: int) -> None:
        if partition not in self._partitions:
            raise PartitionOutOfRange(f"partition {partition} is not assigned to this consumer")
        ray.get(self._offset_store.commit_offset.remote(self.group, self.topic, partition, offset))

    def commit_current_positions(self) -> None:
        """Commit the next-fetch position reached by previous polls.

        This mirrors Kafka's position commit API. ``run()`` uses the narrower
        per-record commit after the application handler succeeds, so records
        that were polled but not yet handled are not acknowledged.

        Calling this after ``poll()`` but before handling the records gives
        at-most-once: a crash then loses them.
        """
        ray.get(
            [
                self._offset_store.commit_offset.remote(
                    self.group, self.topic, partition, offset
                )
                for partition, offset in self._positions.items()
            ]
        )

    def run(
        self,
        handler: Callable[[ConsumedRecord], None],
        *,
        max_messages: int | None = None,
        max_records: int = 100,
        poll_interval_s: float = 0.1,
        idle_timeout_s: float | None = None,
    ) -> int:
        """Poll, handle, and commit each record after successful handling.

        Committing after ``handler`` returns gives at-least-once processing: a
        process crash after handling but before commit can redeliver that
        record, while a crash before successful handling cannot acknowledge it.
        """
        if max_messages is not None and max_messages < 1:
            raise ValueError("max_messages must be >= 1")
        if max_records < 1:
            raise ValueError("max_records must be >= 1")
        if poll_interval_s < 0:
            raise ValueError("poll_interval_s must be >= 0")
        if idle_timeout_s is not None and idle_timeout_s < 0:
            raise ValueError("idle_timeout_s must be >= 0")

        processed = 0
        idle_since = time.monotonic()
        try:
            while max_messages is None or processed < max_messages:
                remaining = None if max_messages is None else max_messages - processed
                batch = self.poll(max_records=max_records if remaining is None else min(max_records, remaining))
                if not batch:
                    if idle_timeout_s is not None and time.monotonic() - idle_since >= idle_timeout_s:
                        break
                    time.sleep(poll_interval_s)
                    continue
                idle_since = time.monotonic()
                for consumed in batch:
                    if max_messages is not None and processed >= max_messages:
                        break
                    handler(consumed)
                    self.commit_offset(consumed.partition, consumed.offset + 1)
                    processed += 1
        finally:
            # poll() advances positions past records that may never reach the
            # handler (early stop or handler error). Every handled record is
            # committed, so committed == handled: rewind to it so this instance
            # does not skip the polled-but-unhandled records on its next run().
            self.seek_to_committed()
        return processed

    def _load_committed_positions(self) -> dict[int, int]:
        return {
            partition: ray.get(
                self._offset_store.committed_offset.remote(self.group, self.topic, partition)
            )
            for partition in self._assignment
        }
