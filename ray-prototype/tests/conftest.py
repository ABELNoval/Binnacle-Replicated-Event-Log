import pytest
import ray


@pytest.fixture(scope="session", autouse=True)
def ray_cluster():
    ray.init(num_cpus=4, include_dashboard=False, logging_level="ERROR")
    yield
    ray.shutdown()
