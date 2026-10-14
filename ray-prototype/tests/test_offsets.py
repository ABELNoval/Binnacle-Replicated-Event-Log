import uuid

import pytest
import ray

from binnacle_ray import (
    InvalidOffset,
    OffsetStore,
    commit_offset,
    get_or_create_offset_store,
)


@pytest.fixture
def store():
    actor = OffsetStore.remote()
    yield actor
    ray.kill(actor)


def test_uncommitted_offset_defaults_to_zero(store):
    assert ray.get(store.committed_offset.remote("group", "orders", 0)) == 0


def test_commit_and_read_offsets_by_group_topic_and_partition(store):
    ray.get(store.commit_offset.remote("group-a", "orders", 0, 10))
    ray.get(store.commit_offset.remote("group-a", "orders", 1, 20))
    ray.get(store.commit_offset.remote("group-b", "orders", 0, 30))
    ray.get(store.commit_offset.remote("group-a", "payments", 0, 40))

    assert ray.get(store.committed_offset.remote("group-a", "orders", 0)) == 10
    assert ray.get(store.committed_offset.remote("group-a", "orders", 1)) == 20
    assert ray.get(store.committed_offset.remote("group-b", "orders", 0)) == 30
    assert ray.get(store.committed_offset.remote("group-a", "payments", 0)) == 40
    assert ray.get(store.committed_offsets.remote("group-a", "orders")) == {0: 10, 1: 20}


def test_commit_overwrites_previous_offset(store):
    ray.get(store.commit_offset.remote("group", "orders", 0, 10))
    ray.get(store.commit_offset.remote("group", "orders", 0, 4))
    assert ray.get(store.committed_offset.remote("group", "orders", 0)) == 4


def test_module_level_commit_offset_uses_the_shared_detached_actor():
    suffix = uuid.uuid4().hex[:12]
    group = f"group-{suffix}"
    topic = f"topic-{suffix}"
    commit_offset(group, topic, 2, 7)
    shared_store = get_or_create_offset_store()
    assert ray.get(shared_store.committed_offset.remote(group, topic, 2)) == 7


def test_negative_committed_offset_is_rejected(store):
    with pytest.raises(InvalidOffset):
        ray.get(store.commit_offset.remote("group", "orders", 0, -1))


@pytest.mark.parametrize(
    "group, topic, partition",
    [("", "orders", 0), ("group", "", 0), ("group", "orders", -1)],
)
def test_invalid_offset_coordinates_are_rejected(store, group, topic, partition):
    with pytest.raises(ValueError):
        ray.get(store.commit_offset.remote(group, topic, partition, 0))
