import json
import time
from unittest.mock import MagicMock

import jwt
import pytest

import iceprod.credentials.util


def test_credentials_util_get_expiration():
    t = time.time()
    tok = jwt.encode({'exp': t}, 'secret')
    e = iceprod.credentials.util.get_expiration(tok)
    assert t == e


def test_credentials_util_is_expired():
    cred = {
        'type': 'oauth',
        'expiration': time.time()
    }
    assert iceprod.credentials.util.is_expired(cred)

    cred = {
        'type': 'oauth',
        'expiration': time.time()+100
    }
    assert not iceprod.credentials.util.is_expired(cred)


def test_client_validate(monkeypatch):
    authmock = MagicMock()
    authmock.return_value.token_url = 'http://iceprod.test'
    monkeypatch.setattr(iceprod.credentials.util, '_get_auth', authmock)

    invalid = json.dumps({
        'http://iceprod.test': []
    })

    with pytest.raises(Exception):
        iceprod.credentials.util.ClientCreds(invalid).validate()

    valid =  json.dumps({
        'http://iceprod.test': {'client_id': 'id', 'client_secret': 'secret'}
    })
    cc = iceprod.credentials.util.ClientCreds(valid)
    cc.validate()
    assert cc.get_client('http://iceprod.test').client_id == 'id'
    assert cc.get_client('http://iceprod.test').client_secret == 'secret'

    valid =  json.dumps({
        'http://iceprod.test': {'client_id': 'id'}
    })
    cc = iceprod.credentials.util.ClientCreds(valid)
    cc.validate()
    assert cc.get_client('http://iceprod.test').client_id == 'id'
    assert cc.get_client('http://iceprod.test').client_secret == ''
