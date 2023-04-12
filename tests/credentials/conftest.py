import os
import pytest


@pytest.fixture(scope='module')
def mongo_url(monkeymodule):
    if 'DB_URL' not in os.environ:
        monkeymodule.setenv('DB_URL', 'mongodb://localhost/creds')


@pytest.fixture(autouse=True)
def rest_dbs(monkeypatch):
    monkeypatch.setenv('DATABASES', 'creds')
