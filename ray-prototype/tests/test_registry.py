import os

import pytest
import ray

from binnacle_ray import (
    PartitionOutOfRange,
    Record,
    TopicAlreadyExists,
    TopicRegistry,
    UnknownTopic,
)


@pytest.fixture
def registry():
    reg = TopicRegistry.remote()
    yield reg
    ray.kill(reg)


def test_create_topic_and_list(registry):
    assert ray.get(registry.create_topic.remote("orders", 3)) == 3
    assert ray.get(registry.list_topics.remote()) == {"orders": 3}
    assert len(ray.get(registry.get_partitions.remote("orders"))) == 3


def test_duplicate_topic_is_rejected(registry):
    ray.get(registry.create_topic.remote("orders", 1))
    with pytest.raises(TopicAlreadyExists):
        ray.get(registry.create_topic.remote("orders", 1))


@pytest.mark.parametrize("name, n", [("", 1), ("orders", 0)])
def test_invalid_topic_arguments(registry, name, n):
    with pytest.raises(ValueError):
        ray.get(registry.create_topic.remote(name, n))


def test_unknown_topic(registry):
    with pytest.raises(UnknownTopic):
        ray.get(registry.get_partition.remote("nope", 0))


def test_partition_out_of_range(registry):
    ray.get(registry.create_topic.remote("orders", 2))
    with pytest.raises(PartitionOutOfRange):
        ray.get(registry.get_partition.remote("orders", 2))


def test_partitions_are_independent_logs(registry):
    ray.get(registry.create_topic.remote("orders", 2))
    p0 = ray.get(registry.get_partition.remote("orders", 0))
    p1 = ray.get(registry.get_partition.remote("orders", 1))
    ray.get(p0.append.remote([Record(key=b"a", value=b"x", timestamp=1)] * 3))
    assert ray.get(p0.next_offset.remote()) == 3
    assert ray.get(p1.next_offset.remote()) == 0


def test_partitions_run_in_separate_processes(registry):
    ray.get(registry.create_topic.remote("orders", 3))
    partitions = ray.get(registry.get_partitions.remote("orders"))
    pids = {ray.get(p.whereami.remote())["pid"] for p in partitions}
    assert len(pids) == 3
    assert os.getpid() not in pids
