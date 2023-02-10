import logging

import boto3
from moto import mock_s3
import pytest
import pytest_asyncio
from rest_tools.client import RestClient
from rest_tools.utils import Auth

from iceprod.rest.auth import ROLES, GROUPS
from iceprod.rest.server import Server

@pytest.fixture
def s3conn(monkeypatch):
    monkeypatch.setenv('S3_ADDRESS', 'htpp://localhost:5000')
    monkeypatch.setenv('S3_ACCESS_KEY', 'XXXX')
    monkeypatch.setenv('S3_SECRET_KEY', 'XXXX')

    with mock_s3():
        conn = boto3.client('s3', region_name='us-east-1')
        conn.create_bucket(Bucket='iceprod2-logs')
        yield conn

@pytest.fixture
def rest_dbs(monkeypatch):
    monkeypatch.setenv('DATABASES', 'auth|config|datasets|grids|jobs|logs|pilots|task_stats|tasks')

@pytest_asyncio.fixture
async def server(monkeypatch, port, mongo_url, rest_dbs, mongo_clear, s3conn):
    monkeypatch.setenv('CI_TESTING', '1')
    monkeypatch.setenv('PORT', str(port))
    monkeypatch.setenv('S3_ACCESS_KEY', 'XXXX')
    monkeypatch.setenv('S3_SECRET_KEY', 'XXXX')

    s = Server(s3_override=s3conn)
    await s.start()

    auth = Auth('secret')

    def _add_to_data(data, attrs):
        logging.debug('attrs: %r', attrs)
        for item in attrs:
            key,value = item.split('=',1)
            d = data
            while '.' in key:
                k,key = key.split('.',1)
                if k not in d:
                    d[k] = {}
                d = d[k]
            if key not in d:
                d[key] = []
            d[key].append(value)

    def client(timeout=1, username='user', roles=[], groups=[], exp=10):
        data = {'preferred_username': username}
        for r in roles:
            _add_to_data(data, ROLES[r])
        for g in groups:
            _add_to_data(data, GROUPS[g])
        token = auth.create_token('username', expiration=exp, payload=data)
        return RestClient(f'http://localhost:{port}', token, timeout=timeout, retries=0)

    try:
        yield client
    finally:
        await s.stop()