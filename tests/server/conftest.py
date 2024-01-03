import pytest

@pytest.fixture(autouse=True)
def i3prod_path(monkeypatch, tmp_path):
    (tmp_path / 'etc').mkdir(mode=0o700)
    monkeypatch.setenv('I3PROD', str(tmp_path))
    yield tmp_path
