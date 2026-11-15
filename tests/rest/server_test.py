import asyncio
import logging

import pymongo
import pytest
import pytest_asyncio
from pymongo import _csot

from iceprod.common.mongo import Mongo
from iceprod.rest.server import Server


@pytest_asyncio.fixture
async def make_server(monkeypatch, port, mongo_url, rest_dbs, mongo_clear, s3conn):
    """Create (but don't start) a REST Server; stop it on teardown."""
    monkeypatch.setenv('CI_TESTING', '1')
    monkeypatch.setenv('PORT', str(port))
    monkeypatch.setenv('S3_ACCESS_KEY', 'XXXX')
    monkeypatch.setenv('S3_SECRET_KEY', 'XXXX')
    servers = []

    def _make():
        s = Server(s3_override=s3conn)
        servers.append(s)
        return s

    try:
        yield _make
    finally:
        for s in servers:
            await s.stop()


async def test_rest_server_indexes_created(make_server):
    s = make_server()
    await s.start(wait_for_indexes=True)

    for db_name in ('tasks', 'jobs'):
        info = await s.db_client.client[db_name][db_name].index_information()
        assert 'dataset_id_job_index_index' in info
        assert info['dataset_id_job_index_index']['key'] == [('dataset_id', pymongo.ASCENDING), ('job_index', pymongo.ASCENDING)]


async def test_rest_server_indexes_no_timeout(make_server, monkeypatch):
    """Index creation must not be bound by the client-wide DB_TIMEOUT"""
    timeouts = []

    async def fake_create_indexes(self, *, db_name=None, indexes):
        # 0 = no timeout; None would fall back to the client timeoutMS
        timeouts.append((_csot.get_timeout(), _csot.remaining()))
    monkeypatch.setattr(Mongo, 'create_indexes', fake_create_indexes)

    s = make_server()
    await s.start(wait_for_indexes=True)
    assert timeouts
    assert all(t == (0, None) for t in timeouts)


async def test_rest_server_indexes_failure_logged(make_server, monkeypatch, caplog):
    async def fail_create_indexes(self, *, db_name=None, indexes):
        raise pymongo.errors.OperationFailure('index build failed')
    monkeypatch.setattr(Mongo, 'create_indexes', fail_create_indexes)

    s = make_server()
    with caplog.at_level(logging.ERROR):
        await s.start(wait_for_indexes=True)  # must not raise
    assert 'index creation failed' in caplog.text


async def test_rest_server_indexes_background(make_server, monkeypatch):
    """start() returns before index creation finishes; stop() cancels it"""
    started = asyncio.Event()

    async def slow_create_indexes(self, *, db_name=None, indexes):
        started.set()
        await asyncio.sleep(1000)
    monkeypatch.setattr(Mongo, 'create_indexes', slow_create_indexes)

    s = make_server()
    await asyncio.wait_for(s.start(), timeout=5)
    await asyncio.wait_for(started.wait(), timeout=5)
    task = s._index_task
    assert task is not None and not task.done()

    await s.stop()
    assert task.cancelled()


@pytest.mark.parametrize('wait', [True, False])
async def test_rest_server_start_wait_flag(make_server, wait):
    s = make_server()
    await s.start(wait_for_indexes=wait)
    if wait:
        assert s._index_task is not None and s._index_task.done()
    else:
        assert s._index_task is not None
