import os
import socket

import motor.motor_asyncio
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
        'DB_URL': 'mongodb://localhost/datasets',
        'DATABASES': '',
    }
    config = from_environment(default_config)
    db_url, db_name = config['DB_URL'].rsplit('/', 1)
    client = motor.motor_asyncio.AsyncIOMotorClient(db_url, serverSelectionTimeoutMS=10)

    async def _clean(db_name):
        db = client[db_name]
        cols = await db.list_collection_names()
        try:
            for c in cols:
                await db[c].drop()
            yield
        finally:
            for c in cols:
                await db[c].drop()

    if not config['DATABASES']:
        await _clean(db_name)
    else:
        for db_name in config['DATABASES'].split('|'):
            await _clean(db_name)


@pytest.fixture(scope='module')
def monkeymodule():
    with pytest.MonkeyPatch.context() as mp:
        yield mp


@pytest.fixture(scope='module')
def mongo_url(monkeymodule):
    if 'DB_URL' not in os.environ:
        monkeymodule.setenv('DB_URL', 'mongodb://localhost/iceprod')

