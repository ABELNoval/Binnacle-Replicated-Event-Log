"""Create a topic, write to every partition, read back, and show where each partition runs."""

import os

import ray

from binnacle_ray import Record, get_or_create_registry

TOPIC = "demo"
PARTITIONS = 3


def main() -> None:
    ray.init(include_dashboard=False, logging_level="ERROR")
    registry = get_or_create_registry()

    topics = ray.get(registry.list_topics.remote())
    if TOPIC not in topics:
        ray.get(registry.create_topic.remote(TOPIC, PARTITIONS))
    partitions = ray.get(registry.get_partitions.remote(TOPIC))

    for p, handle in enumerate(partitions):
        batch = [Record(key=f"p{p}".encode(), value=f"msg-{i}".encode()) for i in range(3)]
        base = ray.get(handle.append.remote(batch))
        print(f"partition {p}: appended 3 records at base offset {base}")

    print(f"\ndriver pid {os.getpid()}")
    for handle in partitions:
        where = ray.get(handle.whereami.remote())
        records = ray.get(handle.read.remote(0))
        values = [r.value.decode() for _, r in records]
        print(
            f"partition {where['partition']} -> pid {where['pid']}, "
            f"node {where['node_id'][:8]}, log {values}"
        )

    ray.shutdown()


if __name__ == "__main__":
    main()
