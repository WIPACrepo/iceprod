import os
import socket

import boto3
from moto import mock_aws
from pymongo import AsyncMongoClient
import pytest
import pytest_asyncio
from wipac_dev_tools import from_environment


@pytest.fixture
def port():
    """Get an ephemeral port number."""
    # https://unix.stackexchange.com/a/132524
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))
    addr = s.getsockname()
    ephemeral_port = addr[1]
    s.close()
    return ephemeral_port


@pytest_asyncio.fixture
async def mongo_clear():
    default_config = {
        'DB_URL': 'mongodb://localhost/iceprod',
        'DATABASES': '',
    }
    config = from_environment(default_config)
    db_url, db_name = config['DB_URL'].rsplit('/', 1)  # type: ignore
    client = AsyncMongoClient(db_url, serverSelectionTimeoutMS=10)

    async def _clean(db_name):
        db = client[db_name]
        if db_name == 'config':
            # exclude replica set config
            await db['config'].drop()
        else:
            cols = await db.list_collection_names()
            for c in cols:
                await db[c].drop()

    if not config['DATABASES']:
        await _clean(db_name)
    else:
        for db_name in config['DATABASES'].split('|'):  # type: ignore
            await _clean(db_name)


@pytest.fixture(scope='module')
def monkeymodule():
    with pytest.MonkeyPatch.context() as mp:
        yield mp


@pytest.fixture(scope='module')
def mongo_url(monkeymodule):
    if 'DB_URL' not in os.environ:
        monkeymodule.setenv('DB_URL', 'mongodb://localhost/iceprod')


@pytest.fixture
def s3conn(monkeypatch):
    monkeypatch.setenv('S3_ADDRESS', 'http://localhost:5000')
    monkeypatch.setenv('S3_ACCESS_KEY', 'XXXX')
    monkeypatch.setenv('S3_SECRET_KEY', 'XXXX')

    with mock_aws():
        conn = boto3.client('s3', region_name='us-east-1')
        conn.create_bucket(Bucket='iceprod2-logs')
        yield conn