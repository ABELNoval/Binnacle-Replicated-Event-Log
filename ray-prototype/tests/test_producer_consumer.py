import uuid

import pytest
import ray

from binnacle_ray import Consumer, OffsetStore, PartitionOutOfRange, Producer, Record, TopicRegistry


@pytest.fixture
def registry():
    actor = TopicRegistry.remote()
    yield actor
    ray.kill(actor)


@pytest.fixture
def offset_store():
    actor = OffsetStore.remote()
    yield actor
    ray.kill(actor)


def topic_name(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def test_same_key_always_goes_to_same_partition(registry):
    topic = topic_name("keyed")
    ray.get(registry.create_topic.remote(topic, 4))
    producer = Producer(topic, registry=registry)

    results = [producer.send(b"same-key", f"value-{i}".encode()) for i in range(5)]
    assert {result.partition for result in results} == {results[0].partition}
    assert [result.offset for result in results] == list(range(5))

    partition = ray.get(registry.get_partition.remote(topic, results[0].partition))
    stored = ray.get(partition.read.remote(0, max_records=5))
    assert [record.value for _, record in stored] == [f"value-{i}".encode() for i in range(5)]


def test_key_hash_is_stable_across_producer_instances(registry):
    topic = topic_name("stable")
    ray.get(registry.create_topic.remote(topic, 8))
    first = Producer(topic, registry=registry)
    second = Producer(topic, registry=registry)
    assert first.choose_partition(b"stable-key") == second.choose_partition(b"stable-key")


def test_empty_keys_round_robin_across_partitions(registry):
    topic = topic_name("round-robin")
    ray.get(registry.create_topic.remote(topic, 3))
    producer = Producer(topic, registry=registry)

    results = [producer.send(b"", f"empty-{i}".encode()) for i in range(7)]
    assert [result.partition for result in results] == [0, 1, 2, 0, 1, 2, 0]


def test_consumer_resumes_from_last_committed_offset(registry, offset_store):
    topic = topic_name("resume")
    group = f"group-{uuid.uuid4().hex[:12]}"
    ray.get(registry.create_topic.remote(topic, 1))
    producer = Producer(topic, registry=registry)
    for i in range(5):
        producer.send(b"key", f"value-{i}".encode())

    first = Consumer(topic, group, registry=registry, offset_store=offset_store)
    first_batch = first.poll(max_records=2)
    assert [record.offset for record in first_batch] == [0, 1]
    first.commit_current_positions()

    restarted = Consumer(topic, group, registry=registry, offset_store=offset_store)
    second_batch = restarted.poll(max_records=10)
    assert [record.offset for record in second_batch] == [2, 3, 4]
    assert [record.value for record in second_batch] == [b"value-2", b"value-3", b"value-4"]


def test_static_assignment_reads_only_assigned_partitions(registry, offset_store):
    topic = topic_name("assignment")
    group = f"group-{uuid.uuid4().hex[:12]}"
    ray.get(registry.create_topic.remote(topic, 3))
    producer = Producer(topic, registry=registry)
    for partition in range(3):
        partition_actor = ray.get(registry.get_partition.remote(topic, partition))
        ray.get(
            partition_actor.append.remote(
                [Record(key=f"k{partition}".encode(), value=f"p{partition}".encode())]
            )
        )

    consumer = Consumer(
        topic,
        group,
        partitions=[1],
        registry=registry,
        offset_store=offset_store,
    )
    assert consumer.assignment == (1,)
    batch = consumer.poll(max_records=10)
    assert [(record.partition, record.offset, record.value) for record in batch] == [
        (1, 0, b"p1")
    ]


def test_consumer_rejects_unassigned_partition_commit(registry, offset_store):
    topic = topic_name("bad-assignment")
    ray.get(registry.create_topic.remote(topic, 2))
    consumer = Consumer(
        topic,
        f"group-{uuid.uuid4().hex[:12]}",
        partitions=[0],
        registry=registry,
        offset_store=offset_store,
    )
    with pytest.raises(PartitionOutOfRange):
        consumer.commit_offset(1, 0)


def test_run_commits_after_handling_and_new_consumer_continues(registry, offset_store):
    topic = topic_name("run")
    group = f"group-{uuid.uuid4().hex[:12]}"
    ray.get(registry.create_topic.remote(topic, 2))
    partitions = ray.get(registry.get_partitions.remote(topic))
    for partition, handle in enumerate(partitions):
        for i in range(3):
            ray.get(
                handle.append.remote(
                    [Record(key=f"k{partition}".encode(), value=f"{partition}-{i}".encode())]
                )
            )

    seen_first = []
    first = Consumer(topic, group, registry=registry, offset_store=offset_store)
    assert first.run(seen_first.append, max_messages=2, max_records=1) == 2
    committed = ray.get(offset_store.committed_offsets.remote(group, topic))
    assert sum(committed.values()) == 2

    seen_second = []
    restarted = Consumer(topic, group, registry=registry, offset_store=offset_store)
    assert restarted.run(seen_second.append, max_messages=4, max_records=2) == 4
    all_seen = seen_first + seen_second
    assert {(record.partition, record.offset) for record in all_seen} == {
        (partition, offset) for partition in range(2) for offset in range(3)
    }
    assert ray.get(offset_store.committed_offsets.remote(group, topic)) == {0: 3, 1: 3}
