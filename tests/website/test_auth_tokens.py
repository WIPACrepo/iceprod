"""
Tests for website token handling (Phase 5: no Keycloak refresh per request).

See `PublicHandler.get_current_user_async` / `refresh_access_token`.
"""
import pytest
from rest_tools.utils import Auth

from iceprod.website.handlers import base

TOKEN_URL = 'http://idp.test/token'


@pytest.fixture
def token_endpoint(monkeypatch):
    """Skip OpenID discovery; point refreshes at a fixed token url."""
    monkeypatch.setattr(base, 'get_token_endpoint', lambda auth_url: TOKEN_URL)


def set_tokens(client, access_exp: int, refresh: bool = True):
    """Put an access token (expiring in `access_exp` s) and a refresh token in the session."""
    data = {'access_token': client.auth.create_token(client.username, expiration=access_exp, payload=client.token_data)}
    if refresh:
        data['refresh_token'] = client.auth.create_token(client.username, expiration=3600, payload={'scope': 'offline_access posix'})
    client.session.set(client.username, data)


def token_posts(requests_mock):
    return [r for r in requests_mock.request_history if r.method == 'POST' and r.url == TOKEN_URL]


async def get_submit(client):
    """GET an @authenticated page; 200 if logged in, else a login redirect."""
    async with client.get_http_client() as http_client:
        return await client.request_raw(http_client, 'GET', '/submit')


async def test_valid_cached_token_no_refresh(server, token_endpoint, requests_mock):
    client = server(username='user', roles=['user'], groups=['users'])
    set_tokens(client, access_exp=3600)
    requests_mock.post(TOKEN_URL, json={'access_token': 'should-not-be-used'})

    ret = await get_submit(client)
    assert ret.status_code == 200
    assert token_posts(requests_mock) == []

    # a second page load also makes no refresh
    ret = await get_submit(client)
    assert ret.status_code == 200
    assert token_posts(requests_mock) == []


async def test_near_expiry_token_refreshes_once(server, token_endpoint, requests_mock):
    client = server(username='user', roles=['user'], groups=['users'])
    set_tokens(client, access_exp=base.REFRESH_MARGIN // 2)

    new_access = client.auth.create_token(client.username, expiration=3600, payload=client.token_data)
    requests_mock.post(TOKEN_URL, json={'access_token': new_access, 'refresh_token': 'new-refresh'})

    ret = await get_submit(client)
    assert ret.status_code == 200
    posts = token_posts(requests_mock)
    assert len(posts) == 1
    assert 'grant_type=refresh_token' in posts[0].text

    # session updated with the new tokens
    session = client.session.get_session(client.username)
    assert session['access_token'] == new_access
    assert session['refresh_token'] == 'new-refresh'

    # next request uses the fresh cached token: still only one refresh
    ret = await get_submit(client)
    assert ret.status_code == 200
    assert len(token_posts(requests_mock)) == 1


async def test_expired_token_refreshes(server, token_endpoint, requests_mock):
    client = server(username='user', roles=['user'], groups=['users'])
    set_tokens(client, access_exp=-10)

    new_access = client.auth.create_token(client.username, expiration=3600, payload=client.token_data)
    requests_mock.post(TOKEN_URL, json={'access_token': new_access, 'refresh_token': 'new-refresh'})

    ret = await get_submit(client)
    assert ret.status_code == 200
    assert len(token_posts(requests_mock)) == 1


async def test_failed_refresh_unauthenticated(server, token_endpoint, requests_mock):
    client = server(username='user', roles=['user'], groups=['users'])
    set_tokens(client, access_exp=-10)
    requests_mock.post(TOKEN_URL, status_code=400, json={'error': 'invalid_grant'})

    ret = await get_submit(client)
    # GET on an @authenticated page redirects to login
    assert ret.status_code == 302
    assert '/login' in ret.headers['location']
    assert len(token_posts(requests_mock)) == 1


@pytest.mark.parametrize('kind', ['bad_signature', 'expired', 'other_user', 'garbage'])
async def test_refresh_rejects_invalid_token(kind, server, token_endpoint, requests_mock):
    """A refreshed access token is validated before being stored; failure => not logged in."""
    client = server(username='user', roles=['user'], groups=['users'])
    set_tokens(client, access_exp=-10)
    old = client.session.get_session(client.username)['access_token']

    if kind == 'bad_signature':
        bad = Auth('not-the-secret').create_token(client.username, expiration=3600, payload=client.token_data)
    elif kind == 'expired':
        # well past the validator's clock-skew leeway
        bad = client.auth.create_token(client.username, expiration=-3600, payload=client.token_data)
    elif kind == 'other_user':
        bad = client.auth.create_token('mallory', expiration=3600, payload={'preferred_username': 'mallory'})
    else:
        bad = 'not-a-jwt'
    requests_mock.post(TOKEN_URL, json={'access_token': bad, 'refresh_token': 'new-refresh'})

    ret = await get_submit(client)
    assert ret.status_code == 302
    assert '/login' in ret.headers['location']
    assert len(token_posts(requests_mock)) == 1

    # nothing stored from the rejected response
    session = client.session.get_session(client.username)
    assert session['access_token'] == old
    assert session['refresh_token'] != 'new-refresh'


async def test_failed_refresh_post_forbidden(server, token_endpoint, requests_mock):
    client = server(username='user', roles=['user'], groups=['users'])
    set_tokens(client, access_exp=-10)
    requests_mock.post(TOKEN_URL, status_code=400, json={'error': 'invalid_grant'})

    async with client.get_http_client() as http_client:
        ret = await client.request_raw(http_client, 'POST', '/dataset/d1/actions', form_data={'action': 'reset'})
    assert ret.status_code in (400, 403)  # xsrf (400) or auth (403); never 2xx/3xx


async def test_no_refresh_token_expired_access(server, token_endpoint, requests_mock):
    """Unchanged path: access token only, expired => unauthenticated, no refresh attempt."""
    client = server(username='user', roles=['user'], groups=['users'])
    set_tokens(client, access_exp=-10, refresh=False)

    ret = await get_submit(client)
    assert ret.status_code == 302
    assert token_posts(requests_mock) == []


async def test_rest_client_refreshes_mid_request(server, token_endpoint, requests_mock, monkeypatch):
    """The user rest client is seeded with the cached token and refreshes via the handler on expiry."""
    client = server(username='user', roles=['user'], groups=['users'])
    set_tokens(client, access_exp=3600)

    captured = {}
    orig = base.PublicHandler.get_current_user_async

    async def spy(self):
        ret = await orig(self)
        captured['rest_client'] = self.rest_client
        captured['handler'] = self
        return ret

    monkeypatch.setattr(base.PublicHandler, 'get_current_user_async', spy)

    ret = await get_submit(client)
    assert ret.status_code == 200
    rc = captured['rest_client']
    assert rc.access_token == client.session.get_session(client.username)['access_token']
    assert rc.token_func == captured['handler'].refresh_access_token
    assert token_posts(requests_mock) == []


def test_get_token_endpoint_cached(monkeypatch, requests_mock):
    monkeypatch.setattr(base, '_TOKEN_ENDPOINTS', {})
    requests_mock.get('http://idp.test/.well-known/openid-configuration', json={'token_endpoint': TOKEN_URL})

    assert base.get_token_endpoint('http://idp.test') == TOKEN_URL
    assert base.get_token_endpoint('http://idp.test') == TOKEN_URL
    assert requests_mock.call_count == 1


def test_get_token_endpoint_no_url():
    with pytest.raises(RuntimeError):
        base.get_token_endpoint('')
