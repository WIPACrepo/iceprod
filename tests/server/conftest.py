from prometheus_client import REGISTRY, GC_COLLECTOR, PLATFORM_COLLECTOR, PROCESS_COLLECTOR
import pytest


# disable these for testing
REGISTRY.unregister(GC_COLLECTOR)
REGISTRY.unregister(PLATFORM_COLLECTOR)
REGISTRY.unregister(PROCESS_COLLECTOR)


@pytest.fixture(autouse=True)
def clear_registry():
    collectors = tuple(REGISTRY._collector_to_names.keys())
    for collector in collectors:
        REGISTRY.unregister(collector)
    yield


@pytest.fixture(autouse=True)
def i3prod_path(monkeypatch, tmp_path):
    (tmp_path / 'etc').mkdir(mode=0o700)
    monkeypatch.setenv('I3PROD', str(tmp_path))
    yield tmp_path
