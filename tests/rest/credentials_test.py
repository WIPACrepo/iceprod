from datetime import datetime, timezone

import jwt
import pytest
import requests.exceptions

from iceprod.rest.handlers.credentials import get_expiration

GROUP = 'simprod'


def test_get_expiration():
    exp = datetime.now(timezone.utc)
    t = exp.timestamp()
    tok = jwt.encode({'exp': t}, 'secret')
    e = get_expiration(tok)
    assert exp.strftime('%Y-%m-%dT%H:%M:%S') == e


async def test_rest_credentials_groups_empty(server):
    client = server(roles=['system'])

    ret = await client.request('GET', f'/groups/{GROUP}/credentials')
    assert ret == {}


async def test_rest_credentials_groups_s3(server):
    client = server(roles=['system'])

    data = {
        'url': 'http://foo',
        'type': 's3',
        'access_key': 'XXXX',
        'secret_key': 'YYYY',
        'buckets': ['bar'],
    }
    await client.request('POST', f'/groups/{GROUP}/credentials', data)

    ret = await client.request('GET', f'/groups/{GROUP}/credentials')
    data['groupname'] = GROUP
    assert ret == {data['url']: data}
