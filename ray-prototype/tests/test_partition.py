import pytest
import ray

from binnacle_ray import InvalidOffset, PartitionActor, Record


def rec(i: int) -> Record:
    return Record(key=f"k{i}".encode(), value=f"v{i}".encode(), timestamp=1)


def test_append_returns_base_offset_of_each_batch():
    p = PartitionActor.remote("t", 0)
    assert ray.get(p.append.remote([rec(0), rec(1), rec(2)])) == 0
    assert ray.get(p.append.remote([rec(3)])) == 3
    assert ray.get(p.next_offset.remote()) == 4


def test_read_returns_records_in_order_with_offsets():
    p = PartitionActor.remote("t", 0)
    ray.get(p.append.remote([rec(i) for i in range(5)]))
    got = ray.get(p.read.remote(1, max_records=3))
    assert [off for off, _ in got] == [1, 2, 3]
    assert [r.value for _, r in got] == [b"v1", b"v2", b"v3"]


def test_read_past_the_end_returns_nothing():
    p = PartitionActor.remote("t", 0)
    ray.get(p.append.remote([rec(0)]))
    assert ray.get(p.read.remote(1)) == []
    assert ray.get(p.read.remote(50)) == []


def test_negative_offset_is_a_business_error():
    p = PartitionActor.remote("t", 0)
    with pytest.raises(InvalidOffset):
        ray.get(p.read.remote(-1))


def test_empty_batch_is_rejected():
    p = PartitionActor.remote("t", 0)
    with pytest.raises(ValueError):
        ray.get(p.append.remote([]))


def test_unset_timestamp_is_stamped_by_the_partition():
    p = PartitionActor.remote("t", 0)
    ray.get(p.append.remote([Record(key=b"k", value=b"v")]))
    [(_, stored)] = ray.get(p.read.remote(0))
    assert stored.timestamp > 0


@ray.remote
def produce_batch(partition, writer: int, size: int) -> int:
    batch = [Record(key=b"w%d" % writer, value=b"%d-%d" % (writer, i), timestamp=1) for i in range(size)]
    return ray.get(partition.append.remote(batch))


def test_concurrent_writers_get_contiguous_gap_free_offsets():
    writers, size = 20, 5
    p = PartitionActor.remote("t", 0)
    ray.get([produce_batch.remote(p, w, size) for w in range(writers)])

    log = ray.get(p.read.remote(0, max_records=writers * size))
    assert [off for off, _ in log] == list(range(writers * size))

    # each batch must occupy a contiguous range: no interleaving between writers
    for start in range(0, writers * size, size):
        keys = {r.key for _, r in log[start : start + size]}
        assert len(keys) == 1
