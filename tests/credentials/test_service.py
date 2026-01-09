import time
import json
from unittest.mock import AsyncMock, MagicMock
import jwt
from pymongo import AsyncMongoClient
import pytest

import iceprod.credentials.service
import iceprod.credentials.util


GROUP = 'simprod'
USER = 'username'
DATASETS = ['dataset1', 'dataset2']
TASK_NAMES = ['alpha', 'bravo']


@pytest.mark.respx(using="httpx")
async def test_credentials_service_refresh_cred(respx_mock, monkeypatch):
    clients = json.dumps({
        'http://iceprod.test': {'client_id': 'id', 'client_secret': 'secret'}
    })
    rs = iceprod.credentials.service.RefreshService(MagicMock(), clients, 1, 1, 60)

    now = time.time()

    cred = {
        'url': 'http://iceprod.test',
        'type': 'oauth',
        'expiration': now,
        'last_use': now,
        'refresh_token': jwt.encode({'exp': now, 'iss': 'http://iceprod.test'}, 'secret'),
    }

    ref = jwt.encode({'exp': now+1000, 'iss': 'http://iceprod.test', 'refresh': 'refresh'}, 'secret')
    mock = respx_mock.post("http://iceprod.test/").respond(200, json={
        'access_token': jwt.encode({'exp': now+1000, 'iss': 'http://iceprod.test'}, 'secret'),
        'refresh_token': ref
    })

    authmock = MagicMock()
    authmock.return_value.token_url = 'http://iceprod.test'
    monkeypatch.setattr(iceprod.credentials.util, '_get_auth', authmock)

    new_cred = await rs.refresh_cred(cred)

    assert mock.called

    assert new_cred['refresh_token'] == ref
    assert new_cred['expiration'] == now+1000


async def test_credentials_service_refresh_empty(mongo_url, mongo_clear, respx_mock):
    db = AsyncMongoClient(mongo_url)['creds']
    clients = '{}'
    rs = iceprod.credentials.service.RefreshService(db, clients, 1, 1, 60)

    await rs._run_once()


@pytest.mark.respx(using="httpx", assert_all_called=False)
async def test_credentials_service_refresh_not_exp(mongo_url, mongo_clear, respx_mock):
    db = AsyncMongoClient(mongo_url)['creds']
    clients = '{}'
    rs = iceprod.credentials.service.RefreshService(db, clients, 1, 1, 60)

    now = time.time()

    await db.group_creds.insert_one({
        'groupname': GROUP,
        'url': 'http://iceprod.test',
        'type': 'oauth',
        'expiration': now+10000,
        'last_use': now,
        'refresh_token': jwt.encode({'exp': now, 'iss': 'http://iceprod.test'}, 'secret'),
    })

    mock = respx_mock.post("http://iceprod.test/").respond(200, json={})

    await rs._run_once()

    assert not mock.called
    
    assert rs.last_run_time is not None
    assert rs.last_success_time is not None


@pytest.mark.respx(using="httpx")
async def test_credentials_service_refresh_group(mongo_url, mongo_clear, respx_mock, monkeypatch):
    db = AsyncMongoClient(mongo_url)['creds']
    clients = json.dumps({
        'http://iceprod.test': {'client_id': 'id', 'client_secret': 'secret'}
    })
    rs = iceprod.credentials.service.RefreshService(db, clients, 1, 1, 60)

    now = time.time()

    await db.group_creds.insert_one({
        'groupname': GROUP,
        'url': 'http://iceprod.test',
        'type': 'oauth',
        'expiration': now,
        'last_use': now,
        'refresh_token': jwt.encode({'exp': now, 'iss': 'http://iceprod.test'}, 'secret'),
        'scope': '',
    })

    ref = jwt.encode({'exp': now+1000, 'iss': 'http://iceprod.test', 'refresh': 'refresh'}, 'secret')
    mock = respx_mock.post("http://iceprod.test/").respond(200, json={
        'access_token': jwt.encode({'exp': now+1000, 'iss': 'http://iceprod.test'}, 'secret'),
        'refresh_token': ref
    })

    authmock = MagicMock()
    authmock.return_value.token_url = 'http://iceprod.test'
    monkeypatch.setattr(iceprod.credentials.util, '_get_auth', authmock)

    await rs._run_once()

    assert mock.called

    ret = await db.group_creds.find_one({'url': 'http://iceprod.test'})
    assert ret
    assert ret['refresh_token'] == ref
    assert ret['expiration'] == now+1000


@pytest.mark.respx(using="httpx")
async def test_credentials_service_refresh_user(mongo_url, mongo_clear, respx_mock, monkeypatch):
    db = AsyncMongoClient(mongo_url)['creds']
    clients = json.dumps({
        'http://iceprod.test': {'client_id': 'id', 'client_secret': 'secret'}
    })
    rs = iceprod.credentials.service.RefreshService(db, clients, 1, 1, 60)

    now = time.time()

    await db.user_creds.insert_one({
        'username': USER,
        'url': 'http://iceprod.test',
        'type': 'oauth',
        'expiration': now,
        'last_use': now,
        'refresh_token': jwt.encode({'exp': now, 'iss': 'http://iceprod.test'}, 'secret'),
        'scope': '',
    })

    ref = jwt.encode({'exp': now+1000, 'iss': 'http://iceprod.test', 'refresh': 'refresh'}, 'secret')
    mock = respx_mock.post("http://iceprod.test/").respond(200, json={
        'access_token': jwt.encode({'exp': now+1000, 'iss': 'http://iceprod.test'}, 'secret'),
        'refresh_token': ref
    })

    authmock = MagicMock()
    authmock.return_value.token_url = 'http://iceprod.test'
    monkeypatch.setattr(iceprod.credentials.util, '_get_auth', authmock)

    await rs._run_once()

    assert authmock.called
    assert mock.called

    ret = await db.user_creds.find_one({'url': 'http://iceprod.test'})
    assert ret
    assert ret['refresh_token'] == ref
    assert ret['expiration'] == now+1000


@pytest.mark.respx(using="httpx")
async def test_credentials_service_refresh_dataset(mongo_url, mongo_clear, respx_mock, monkeypatch):
    db = AsyncMongoClient(mongo_url)['creds']
    clients = json.dumps({
        'http://iceprod.test': {'client_id': 'id', 'client_secret': 'secret'}
    })
    rs = iceprod.credentials.service.RefreshService(db, clients, 1, 1, 60)

    now = time.time()

    await db.dataset_creds.insert_one({
        'dataset_id': DATASETS[0],
        'task_name': TASK_NAMES[0],
        'url': 'http://iceprod.test',
        'type': 'oauth',
        'expiration': now,
        'last_use': now,
        'refresh_token': jwt.encode({'exp': now, 'iss': 'http://iceprod.test'}, 'secret'),
        'scope': '',
    })

    ref = jwt.encode({'exp': now+1000, 'iss': 'http://iceprod.test', 'refresh': 'refresh'}, 'secret')
    mock = respx_mock.post("http://iceprod.test/").respond(200, json={
        'access_token': jwt.encode({'exp': now+1000, 'iss': 'http://iceprod.test'}, 'secret'),
        'refresh_token': ref
    })

    authmock = MagicMock()
    authmock.return_value.token_url = 'http://iceprod.test'
    monkeypatch.setattr(iceprod.credentials.util, '_get_auth', authmock)

    await rs._run_once()

    assert authmock.called
    assert mock.called

    ret = await db.dataset_creds.find_one({'url': 'http://iceprod.test'})
    assert ret
    assert ret['refresh_token'] == ref
    assert ret['expiration'] == now+1000


@pytest.mark.respx(using="httpx")
async def test_credentials_service_exchange(respx_mock, monkeypatch):
    clients = json.dumps({
        'http://iceprod.test': {'client_id': 'id', 'client_secret': 'secret'}
    })
    rs = iceprod.credentials.service.RefreshService(AsyncMock(), clients, 1, 1, 60)

    now = time.time()

    cred = {
        'dataset_id': DATASETS[0],
        'task_name': TASK_NAMES[0],
        'url': 'http://iceprod.test',
        'type': 'oauth',
        'expiration': now,
        'last_use': now,
        'access_token': jwt.encode({'exp': now, 'iss': 'http://iceprod.test'}, 'secret'),
        'refresh_token': jwt.encode({'exp': now, 'iss': 'http://iceprod.test'}, 'secret'),
        'scope': '',
    }

    ref = jwt.encode({'exp': now+1000, 'iss': 'http://iceprod.test', 'refresh': 'refresh'}, 'secret')
    mock = respx_mock.post("http://iceprod.test/").respond(200, json={
        'access_token': jwt.encode({'exp': now+1000, 'iss': 'http://iceprod.test'}, 'secret'),
        'refresh_token': ref
    })

    authmock = MagicMock()
    authmock.return_value.token_url = 'http://iceprod.test'
    monkeypatch.setattr(iceprod.credentials.util, '_get_auth', authmock)

    ret = await rs.exchange_cred(cred, client_id='client_id')

    assert authmock.called
    assert mock.called

    assert ret
    assert ret['refresh_token'] == ref
    assert ret['expiration'] == now+1000


@pytest.mark.respx(using="httpx")
async def test_credentials_service_create(respx_mock, monkeypatch):
    clients = json.dumps({
        'http://iceprod.test': {'client_id': 'id', 'client_secret': 'secret', 'transfer_prefix': ['foo://']}
    })
    rs = iceprod.credentials.service.RefreshService(AsyncMock(), clients, 1, 1, 60)

    now = time.time()

    cred = {
        'url': 'http://iceprod.test',
        'transfer_prefix': 'foo://',
        'username': 'user',
        'scope': 'storage.read:/',
    }

    ref = jwt.encode({'exp': now+1000, 'iss': 'http://iceprod.test', 'refresh': 'refresh'}, 'secret')
    mock = respx_mock.post("http://iceprod.test/").respond(200, json={
        'access_token': jwt.encode({'exp': now+1000, 'iss': 'http://iceprod.test'}, 'secret'),
        'refresh_token': ref
    })

    authmock = MagicMock()
    authmock.return_value.token_url = 'http://iceprod.test'
    monkeypatch.setattr(iceprod.credentials.util, '_get_auth', authmock)

    ret = await rs.create_cred(**cred)

    assert authmock.called
    assert mock.called

    assert ret
    assert ret['refresh_token'] == ref
    assert ret['expiration'] == now+1000
