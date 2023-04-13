import time
import json
from unittest.mock import MagicMock
import motor.motor_asyncio
import jwt

import iceprod.credentials.service


GROUP = 'simprod'
USER = 'username'


def test_credentials_service_get_expiration():
    t = time.time()
    tok = jwt.encode({'exp': t}, 'secret')
    e = iceprod.credentials.service.get_expiration(tok)
    assert t == e


def test_credentials_service_is_expired():
    cred = {
        'type': 'oauth',
        'expiration': time.time()
    }
    assert iceprod.credentials.service.is_expired(cred)

    cred = {
        'type': 'oauth',
        'expiration': time.time()+100
    }
    assert not iceprod.credentials.service.is_expired(cred)


async def test_credentials_service_refresh_empty(mongo_url, mongo_clear, respx_mock):
    db = motor.motor_asyncio.AsyncIOMotorClient(mongo_url)['creds']
    clients = '{}'
    rs = iceprod.credentials.service.RefreshService(db, clients, 1, 1, 60)

    await rs._run_once()


async def test_credentials_service_refresh_not_exp(mongo_url, mongo_clear, respx_mock):
    db = motor.motor_asyncio.AsyncIOMotorClient(mongo_url)['creds']
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


async def test_credentials_service_refresh_group(mongo_url, mongo_clear, respx_mock, monkeypatch):
    db = motor.motor_asyncio.AsyncIOMotorClient(mongo_url)['creds']
    clients = json.dumps({
        'http://iceprod.test': ['client-id', 'client-secret']
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
    })

    ref = jwt.encode({'exp': now+1000, 'iss': 'http://iceprod.test', 'refresh': 'refresh'}, 'secret')
    mock = respx_mock.post("http://iceprod.test/").respond(200, json={
        'access_token': jwt.encode({'exp': now+1000, 'iss': 'http://iceprod.test'}, 'secret'),
        'refresh_token': ref
    })

    authmock = MagicMock()
    authmock.return_value.token_url = 'http://iceprod.test'
    monkeypatch.setattr(iceprod.credentials.service, 'get_auth', authmock)

    await rs._run_once()

    assert mock.called

    ret = await db.group_creds.find_one({'url': 'http://iceprod.test'})
    assert ret['refresh_token'] == ref
    assert ret['expiration'] == now+1000


async def test_credentials_service_refresh_user(mongo_url, mongo_clear, respx_mock, monkeypatch):
    db = motor.motor_asyncio.AsyncIOMotorClient(mongo_url)['creds']
    clients = json.dumps({
        'http://iceprod.test': ['client-id', 'client-secret']
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
    })

    ref = jwt.encode({'exp': now+1000, 'iss': 'http://iceprod.test', 'refresh': 'refresh'}, 'secret')
    mock = respx_mock.post("http://iceprod.test/").respond(200, json={
        'access_token': jwt.encode({'exp': now+1000, 'iss': 'http://iceprod.test'}, 'secret'),
        'refresh_token': ref
    })

    authmock = MagicMock()
    authmock.return_value.token_url = 'http://iceprod.test'
    monkeypatch.setattr(iceprod.credentials.service, 'get_auth', authmock)

    await rs._run_once()

    assert mock.called

    ret = await db.user_creds.find_one({'url': 'http://iceprod.test'})
    assert ret['refresh_token'] == ref
    assert ret['expiration'] == now+1000
