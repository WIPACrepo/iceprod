"""Tests for website action handlers (iceprod/website/handlers/actions.py)."""
import json
from unittest.mock import MagicMock
from urllib.parse import parse_qs, urlparse

import pytest
import requests
from bs4 import BeautifulSoup

from iceprod.website.handlers.actions import (
    build_return_url,
    sanitize_return_query,
    status_url,
)

# ---- pure helpers ----

@pytest.mark.parametrize('query,expected', [
    ('', ''),
    (None, ''),
    ('status=failed', 'status=failed'),
    ('status=failed&name=generate', 'status=failed&name=generate'),
    ('status=failed&evil=1&next=http://x', 'status=failed'),
    ('status=<script>', 'status=%3Cscript%3E'),
    ('&&&', ''),
])
def test_sanitize_return_query(query, expected):
    assert sanitize_return_query(query) == expected


@pytest.mark.parametrize('action_type,payload,query,page,expected', [
    ('dataset_status', {'dataset_id': 'd1'}, '', None, '/dataset/d1'),
    ('dataset_status', {}, '', None, '/dataset'),
    ('delete_logs', {'dataset_id': 'd1', 'task_id': None}, '', None, '/dataset/d1'),
    ('delete_logs', {'dataset_id': 'd1', 'task_id': 't1'}, '', None, '/dataset/d1/task/t1'),
    ('task_status', {'dataset_id': 'd1', 'task_ids': ['t1']}, '', None, '/dataset/d1/task/t1'),
    ('task_status', {'dataset_id': 'd1', 'task_ids': ['t1']}, 'status=failed', 'browse', '/dataset/d1/task?status=failed'),
    ('task_status', {'dataset_id': 'd1', 'task_ids': ['t1', 't2']}, 'status=failed', 'detail', '/dataset/d1/task?status=failed'),
    ('task_status', {'dataset_id': 'd1', 'task_ids': None}, '', None, '/dataset/d1/task'),
    ('job_status', {'dataset_id': 'd1', 'job_ids': ['j1']}, '', None, '/dataset/d1/job/j1'),
    ('job_status', {'dataset_id': 'd1', 'job_ids': ['j1']}, 'status=x&foo=bar', 'browse', '/dataset/d1/job?status=x'),
    ('job_status', {'dataset_id': 'd1', 'job_ids': ['j1', 'j2']}, '', None, '/dataset/d1/job'),
    # path segments are quoted, so payload values can't escape the path
    ('dataset_status', {'dataset_id': '../../evil'}, '', None, '/dataset/..%2F..%2Fevil'),
    ('task_status', {'dataset_id': 'd1', 'task_ids': ['//evil.com']}, '', None, '/dataset/d1/task/%2F%2Fevil.com'),
])
def test_build_return_url(action_type, payload, query, page, expected):
    assert build_return_url(action_type, payload, query, page) == expected


def test_status_url():
    assert status_url('task_status', 'abc') == '/actions/status/task_status/abc'
    url = status_url('task_status', 'abc', 'status=failed&evil=1', 'browse')
    q = parse_qs(urlparse(url).query)
    assert q == {'return_query': ['status=failed'], 'return_page': ['browse']}
    assert status_url('task_status', 'abc', '', 'bogus') == '/actions/status/task_status/abc'


# ---- handler helpers ----

def MockError(status_code=400, body=None):
    def _inner(*args, **kwargs):
        response = MagicMock()
        response.status_code = status_code
        response.text = json.dumps(body) if body else ''
        response.json = MagicMock(return_value=body)
        raise requests.exceptions.HTTPError(response=response)
    return _inner


async def get_xsrf(client, http_client) -> str:
    """Load a page with a form to get the xsrf cookie + token."""
    ret = await client.request_raw(http_client, 'GET', '/submit')
    ret.raise_for_status()
    doc = BeautifulSoup(ret.text, 'html.parser')
    return doc.find('input', {'name': '_xsrf'}).get('value')  # type: ignore


def user(server):
    return server(username='username', roles=['user'], groups=['users'])


# ---- DatasetAction ----

@pytest.mark.parametrize('action', ['reset', 'hard_reset', 'suspend'])
async def test_dataset_action_status(server, action):
    client = user(server)
    m = client.req_mock.add_mock('/actions/dataset_status', {'result': 'm1'})
    async with client.get_http_client() as http_client:
        xsrf = await get_xsrf(client, http_client)
        ret = await client.request_raw(http_client, 'POST', '/dataset/d1/actions', form_data={
            '_xsrf': xsrf, 'action': action, 'return_query': 'status=x&evil=1',
        })
    assert ret.status_code == 303
    loc = urlparse(ret.headers['location'])
    assert loc.path == '/actions/status/dataset_status/m1'
    assert parse_qs(loc.query) == {'return_query': ['status=x']}
    assert m.call_args[0][0] == 'POST'
    assert m.call_args[0][2] == {'dataset_id': 'd1', 'action': action}


async def test_dataset_action_delete_logs(server):
    client = user(server)
    m = client.req_mock.add_mock('/actions/delete_logs', {'result': 'm1'})
    async with client.get_http_client() as http_client:
        xsrf = await get_xsrf(client, http_client)
        ret = await client.request_raw(http_client, 'POST', '/dataset/d1/actions', form_data={
            '_xsrf': xsrf, 'action': 'delete_logs',
        })
    assert ret.status_code == 303
    assert ret.headers['location'] == '/actions/status/delete_logs/m1'
    assert m.call_args[0][2] == {'dataset_id': 'd1'}


async def test_dataset_action_priority(server):
    client = user(server)
    m = client.req_mock.add_mock('/datasets/d1/priority', {})
    async with client.get_http_client() as http_client:
        xsrf = await get_xsrf(client, http_client)
        ret = await client.request_raw(http_client, 'POST', '/dataset/d1/actions', form_data={
            '_xsrf': xsrf, 'action': 'priority', 'priority': '2.5',
        })
    assert ret.status_code == 303
    assert ret.headers['location'] == '/dataset/d1'
    assert m.call_args[0][0] == 'PUT'
    assert m.call_args[0][2] == {'priority': 2.5}


async def test_dataset_action_priority_invalid(server):
    client = user(server)
    m = client.req_mock.add_mock('/datasets/d1/priority', {})
    async with client.get_http_client() as http_client:
        xsrf = await get_xsrf(client, http_client)
        ret = await client.request_raw(http_client, 'POST', '/dataset/d1/actions', form_data={
            '_xsrf': xsrf, 'action': 'priority', 'priority': 'abc',
        })
    assert ret.status_code == 400
    doc = BeautifulSoup(ret.text, 'html.parser')
    assert 'not a number' in doc.find('div', {'id': 'error'}).text  # type: ignore
    assert doc.find('a', {'id': 'return'}).get('href') == '/dataset/d1'  # type: ignore
    assert not m.called


async def test_dataset_action_truncate(server):
    client = user(server)
    m = client.req_mock.add_mock('/datasets/d1/dataset_actions/truncate', {})
    async with client.get_http_client() as http_client:
        xsrf = await get_xsrf(client, http_client)
        ret = await client.request_raw(http_client, 'POST', '/dataset/d1/actions', form_data={
            '_xsrf': xsrf, 'action': 'truncate',
        })
    assert ret.status_code == 303
    assert ret.headers['location'] == '/dataset/d1'
    assert m.call_args[0][0] == 'POST'


async def test_dataset_action_rest_error(server):
    client = user(server)
    client.req_mock.add_mock('/datasets/d1/dataset_actions/truncate', MockError(403, {'error': 'auth failed'}))
    async with client.get_http_client() as http_client:
        xsrf = await get_xsrf(client, http_client)
        ret = await client.request_raw(http_client, 'POST', '/dataset/d1/actions', form_data={
            '_xsrf': xsrf, 'action': 'truncate',
        })
    assert ret.status_code == 400
    doc = BeautifulSoup(ret.text, 'html.parser')
    assert doc.find('div', {'id': 'error'}).text == 'auth failed'  # type: ignore


async def test_dataset_action_queue_error(server):
    client = user(server)
    client.req_mock.add_mock('/actions/dataset_status', MockError(400, {'error': 'bad thing'}))
    async with client.get_http_client() as http_client:
        xsrf = await get_xsrf(client, http_client)
        ret = await client.request_raw(http_client, 'POST', '/dataset/d1/actions', form_data={
            '_xsrf': xsrf, 'action': 'reset',
        })
    assert ret.status_code == 400
    doc = BeautifulSoup(ret.text, 'html.parser')
    assert doc.find('div', {'id': 'error'}).text == 'bad thing'  # type: ignore


async def test_dataset_action_invalid(server):
    client = user(server)
    async with client.get_http_client() as http_client:
        xsrf = await get_xsrf(client, http_client)
        ret = await client.request_raw(http_client, 'POST', '/dataset/d1/actions', form_data={
            '_xsrf': xsrf, 'action': 'foo',
        })
    assert ret.status_code == 400


async def test_dataset_action_no_xsrf(server):
    client = user(server)
    m = client.req_mock.add_mock('/actions/dataset_status', {'result': 'm1'})
    async with client.get_http_client() as http_client:
        ret = await client.request_raw(http_client, 'POST', '/dataset/d1/actions', form_data={
            'action': 'reset',
        })
    # missing _xsrf: rest_tools' get_argument raises 400 before tornado's 403
    assert ret.status_code in (400, 403)
    assert not m.called


async def test_dataset_action_bad_xsrf(server):
    client = user(server)
    m = client.req_mock.add_mock('/actions/dataset_status', {'result': 'm1'})
    async with client.get_http_client() as http_client:
        await get_xsrf(client, http_client)  # sets the xsrf cookie
        ret = await client.request_raw(http_client, 'POST', '/dataset/d1/actions', form_data={
            '_xsrf': '2|deadbeef|deadbeef|1', 'action': 'reset',
        })
    assert ret.status_code == 403
    assert not m.called


async def test_dataset_action_no_user(server):
    client = user(server)
    async with client.get_http_client() as http_client:
        xsrf = await get_xsrf(client, http_client)
        http_client.cookies.delete('iceprod_username')
        ret = await client.request_raw(http_client, 'POST', '/dataset/d1/actions', form_data={
            '_xsrf': xsrf, 'action': 'reset',
        })
    assert ret.status_code == 403


# ---- TaskAction ----

@pytest.mark.parametrize('action', ['reset', 'hard_reset', 'suspend'])
async def test_task_action_status(server, action):
    client = user(server)
    m = client.req_mock.add_mock('/actions/task_status', {'result': 'm1'})
    async with client.get_http_client() as http_client:
        xsrf = await get_xsrf(client, http_client)
        ret = await client.request_raw(http_client, 'POST', '/dataset/d1/task/actions', form_data={
            '_xsrf': xsrf, 'action': action, 'task_ids': ['t1', 't2'],
            'return_query': 'status=failed', 'return_page': 'browse',
        })
    assert ret.status_code == 303
    loc = urlparse(ret.headers['location'])
    assert loc.path == '/actions/status/task_status/m1'
    assert parse_qs(loc.query) == {'return_query': ['status=failed'], 'return_page': ['browse']}
    assert m.call_args[0][2] == {'dataset_id': 'd1', 'task_ids': ['t1', 't2'], 'action': action}


async def test_task_action_delete_logs(server):
    client = user(server)
    m = client.req_mock.add_mock('/actions/delete_logs', {'result': 'm1'})
    async with client.get_http_client() as http_client:
        xsrf = await get_xsrf(client, http_client)
        ret = await client.request_raw(http_client, 'POST', '/dataset/d1/task/actions', form_data={
            '_xsrf': xsrf, 'action': 'delete_logs', 'task_ids': 't1',
        })
    assert ret.status_code == 303
    assert ret.headers['location'] == '/actions/status/delete_logs/m1'
    assert m.call_args[0][2] == {'dataset_id': 'd1', 'task_id': 't1'}


@pytest.mark.parametrize('form', [
    {'action': 'reset'},                                   # no ids => would hit whole dataset
    {'action': 'reset', 'task_ids': ''},
    {'action': 'foo', 'task_ids': 't1'},
    {'action': 'delete_logs', 'task_ids': ['t1', 't2']},
])
async def test_task_action_invalid(server, form):
    client = user(server)
    m = client.req_mock.add_mock('/actions/task_status', {'result': 'm1'})
    m2 = client.req_mock.add_mock('/actions/delete_logs', {'result': 'm1'})
    async with client.get_http_client() as http_client:
        xsrf = await get_xsrf(client, http_client)
        ret = await client.request_raw(http_client, 'POST', '/dataset/d1/task/actions', form_data={
            '_xsrf': xsrf, **form,
        })
    assert ret.status_code == 400
    assert not m.called
    assert not m2.called


async def test_task_action_no_xsrf(server):
    client = user(server)
    m = client.req_mock.add_mock('/actions/task_status', {'result': 'm1'})
    async with client.get_http_client() as http_client:
        ret = await client.request_raw(http_client, 'POST', '/dataset/d1/task/actions', form_data={
            'action': 'reset', 'task_ids': 't1',
        })
    # missing _xsrf: rest_tools' get_argument raises 400 before tornado's 403
    assert ret.status_code in (400, 403)
    assert not m.called


# ---- JobAction ----

@pytest.mark.parametrize('action', ['reset', 'hard_reset', 'suspend'])
async def test_job_action_status(server, action):
    client = user(server)
    m = client.req_mock.add_mock('/actions/job_status', {'result': 'm1'})
    async with client.get_http_client() as http_client:
        xsrf = await get_xsrf(client, http_client)
        ret = await client.request_raw(http_client, 'POST', '/dataset/d1/job/actions', form_data={
            '_xsrf': xsrf, 'action': action, 'job_ids': 'j1',
        })
    assert ret.status_code == 303
    assert ret.headers['location'] == '/actions/status/job_status/m1'
    assert m.call_args[0][2] == {'dataset_id': 'd1', 'job_ids': ['j1'], 'action': action}


@pytest.mark.parametrize('form', [
    {'action': 'reset'},
    {'action': 'delete_logs', 'job_ids': 'j1'},
    {'action': 'foo', 'job_ids': 'j1'},
])
async def test_job_action_invalid(server, form):
    client = user(server)
    m = client.req_mock.add_mock('/actions/job_status', {'result': 'm1'})
    async with client.get_http_client() as http_client:
        xsrf = await get_xsrf(client, http_client)
        ret = await client.request_raw(http_client, 'POST', '/dataset/d1/job/actions', form_data={
            '_xsrf': xsrf, **form,
        })
    assert ret.status_code == 400
    assert not m.called


async def test_job_action_no_xsrf(server):
    client = user(server)
    m = client.req_mock.add_mock('/actions/job_status', {'result': 'm1'})
    async with client.get_http_client() as http_client:
        ret = await client.request_raw(http_client, 'POST', '/dataset/d1/job/actions', form_data={
            'action': 'reset', 'job_ids': 'j1',
        })
    # missing _xsrf: rest_tools' get_argument raises 400 before tornado's 403
    assert ret.status_code in (400, 403)
    assert not m.called


# ---- ActionStatus ----

async def test_action_status_invalid_type(server):
    client = user(server)
    m = client.req_mock.add_mock('/actions/submit/m1', {'status': 'queued', 'payload': {}})
    async with client.get_http_client() as http_client:
        ret = await client.request_raw(http_client, 'GET', '/actions/status/submit/m1')
    assert ret.status_code == 400
    assert not m.called


async def test_action_status_queued(server):
    client = user(server)
    client.req_mock.add_mock('/actions/task_status/m1', {
        'status': 'processing',
        'payload': {'dataset_id': 'd1', 'task_ids': ['t1'], 'action': 'reset', 'progress': 42},
    })
    async with client.get_http_client() as http_client:
        ret = await client.request_raw(http_client, 'GET', '/actions/status/task_status/m1', args={
            'return_query': 'status=failed', 'return_page': 'browse',
        })
    assert ret.status_code == 200
    doc = BeautifulSoup(ret.text, 'html.parser')
    assert doc.find('span', {'id': 'status'}).text == 'processing'  # type: ignore
    assert doc.find('span', {'id': 'progress'}).text == '42'  # type: ignore
    assert doc.find('a', {'id': 'return'}).get('href') == '/dataset/d1/task?status=failed'  # type: ignore
    # reload script targets the sanitized status url
    assert '/actions/status/task_status/m1?return_query=status%3Dfailed&return_page=browse' in ret.text


async def test_action_status_error(server):
    client = user(server)
    client.req_mock.add_mock('/actions/job_status/m1', {
        'status': 'error',
        'error_message': 'something broke',
        'payload': {'dataset_id': 'd1', 'job_ids': ['j1'], 'action': 'reset', 'progress': 0},
    })
    async with client.get_http_client() as http_client:
        ret = await client.request_raw(http_client, 'GET', '/actions/status/job_status/m1')
    assert ret.status_code == 200
    doc = BeautifulSoup(ret.text, 'html.parser')
    assert doc.find('div', {'id': 'error'}).text == 'something broke'  # type: ignore
    assert doc.find('a', {'id': 'return'}).get('href') == '/dataset/d1/job/j1'  # type: ignore
    assert 'window.location.replace' not in ret.text


async def test_action_status_not_found(server):
    client = user(server)
    client.req_mock.add_mock('/actions/job_status/m1', MockError(404, {'error': 'not found'}))
    async with client.get_http_client() as http_client:
        ret = await client.request_raw(http_client, 'GET', '/actions/status/job_status/m1')
    assert ret.status_code == 400
    doc = BeautifulSoup(ret.text, 'html.parser')
    assert doc.find('div', {'id': 'error'}).text == 'not found'  # type: ignore


@pytest.mark.parametrize('action_type,payload,args,expected', [
    ('dataset_status', {'dataset_id': 'd1', 'action': 'reset'}, {}, '/dataset/d1'),
    ('delete_logs', {'dataset_id': 'd1', 'task_id': None}, {}, '/dataset/d1'),
    ('delete_logs', {'dataset_id': 'd1', 'task_id': 't1'}, {}, '/dataset/d1/task/t1'),
    ('task_status', {'dataset_id': 'd1', 'task_ids': ['t1']}, {}, '/dataset/d1/task/t1'),
    ('task_status', {'dataset_id': 'd1', 'task_ids': ['t1']}, {'return_page': 'browse', 'return_query': 'status=failed&x=1'}, '/dataset/d1/task?status=failed'),
    ('task_status', {'dataset_id': 'd1', 'task_ids': ['t1', 't2']}, {'return_query': 'name=gen'}, '/dataset/d1/task?name=gen'),
    ('job_status', {'dataset_id': 'd1', 'job_ids': ['j1']}, {}, '/dataset/d1/job/j1'),
    ('job_status', {'dataset_id': 'd1', 'job_ids': ['j1', 'j2']}, {'return_query': 'status=processing'}, '/dataset/d1/job?status=processing'),
])
async def test_action_status_complete(server, action_type, payload, args, expected):
    client = user(server)
    client.req_mock.add_mock(f'/actions/{action_type}/m1', {
        'status': 'complete',
        'payload': {**payload, 'progress': 100},
    })
    async with client.get_http_client() as http_client:
        ret = await client.request_raw(http_client, 'GET', f'/actions/status/{action_type}/m1', args=args)
    assert ret.status_code == 303
    assert ret.headers['location'] == expected


# ---- rendered pages: forms present, no embedded token ----

def assert_no_token(html: str):
    """The page must not embed the user's access token (JWTs start with 'eyJ')."""
    assert 'passkey' not in html
    assert 'eyJ' not in html
    assert '/static/rest.js' not in html


def get_forms(html: str) -> dict[tuple[str, str], dict]:
    """Map (form action url, action name) -> {fields, onsubmit}."""
    doc = BeautifulSoup(html, 'html.parser')
    ret = {}
    for form in doc.find_all('form', {'method': 'post'}):
        fields: dict[str, list[str]] = {}
        for inp in form.find_all('input'):
            fields.setdefault(inp.get('name'), []).append(inp.get('value'))
        action = fields.get('action', [''])[0]
        ret[(form.get('action'), action)] = {'fields': fields, 'onsubmit': form.get('onsubmit')}
    return ret


def mock_dataset_page(client):
    client.req_mock.add_mock('/datasets/d1', {'dataset_id': 'd1', 'dataset': 123, 'priority': 1.0, 'status': 'processing', 'description': 'desc'})
    client.req_mock.add_mock('/datasets/d1/job_counts/status', {'processing': 1})
    client.req_mock.add_mock('/datasets/d1/task_counts/status', {'idle': 1})
    client.req_mock.add_mock('/datasets/d1/task_counts/name_status', {})
    client.req_mock.add_mock('/datasets/d1/task_stats', {})
    client.req_mock.add_mock('/config/d1', {})


async def test_page_dataset_detail(server):
    client = user(server)
    mock_dataset_page(client)
    async with client.get_http_client() as http_client:
        ret = await client.request_raw(http_client, 'GET', '/dataset/d1')
        assert ret.status_code == 200
        assert_no_token(ret.text)
        forms = get_forms(ret.text)

        url = '/dataset/d1/actions'
        for action in ('priority', 'reset', 'hard_reset', 'suspend', 'truncate', 'delete_logs'):
            assert (url, action) in forms, action
            assert forms[(url, action)]['fields']['_xsrf'][0]
        assert forms[(url, 'reset')]['onsubmit'] is None
        for action in ('hard_reset', 'suspend', 'truncate', 'delete_logs'):
            assert 'confirm(' in forms[(url, action)]['onsubmit']

        # submit a form straight from the page => end-to-end
        m = client.req_mock.add_mock('/datasets/d1/priority', {})
        fields = forms[(url, 'priority')]['fields']
        form_data = {k: v[0] for k, v in fields.items()}
        form_data['priority'] = '5'
        ret = await client.request_raw(http_client, 'POST', url, form_data=form_data)
        assert ret.status_code == 303
        assert m.call_args[0][2] == {'priority': 5.0}


async def test_page_task_browse(server):
    client = user(server)
    client.req_mock.add_mock('/datasets/d1/tasks', {
        't1': {'task_id': 't1', 'dataset_id': 'd1', 'job_index': 0, 'task_index': 0, 'name': 'a', 'failures': 0, 'status': 'failed'},
        't2': {'task_id': 't2', 'dataset_id': 'd1', 'job_index': 0, 'task_index': 1, 'name': 'b', 'failures': 0, 'status': 'failed'},
    })
    async with client.get_http_client() as http_client:
        ret = await client.request_raw(http_client, 'GET', '/dataset/d1/task', args={'status': 'failed'})
        assert ret.status_code == 200
        assert_no_token(ret.text)

        doc = BeautifulSoup(ret.text, 'html.parser')
        forms = doc.find_all('form', {'action': '/dataset/d1/task/actions'})
        # 3 "All" forms + per-row reset/hard_reset for 2 failed tasks
        # (suspend is not allowed from 'failed')
        assert len(forms) == 3 + 2*2
        all_reset = forms[0]
        ids = [i.get('value') for i in all_reset.find_all('input', {'name': 'task_ids'})]
        assert sorted(ids) == ['t1', 't2']
        assert all_reset.find('input', {'name': 'return_query'}).get('value') == 'status=failed'
        assert all_reset.find('input', {'name': 'return_page'}).get('value') == 'browse'

        # end-to-end: post the first per-row form
        m = client.req_mock.add_mock('/actions/task_status', {'result': 'm1'})
        row_form = forms[3]
        form_data = {i.get('name'): i.get('value') for i in row_form.find_all('input')}
        ret = await client.request_raw(http_client, 'POST', '/dataset/d1/task/actions', form_data=form_data)
        assert ret.status_code == 303
        loc = urlparse(ret.headers['location'])
        assert parse_qs(loc.query) == {'return_query': ['status=failed'], 'return_page': ['browse']}
        assert m.call_args[0][2]['task_ids'] == ['t1']


async def test_page_task_detail(server):
    client = user(server)
    client.req_mock.add_mock('/datasets/d1', {'dataset_id': 'd1', 'dataset': 123})
    client.req_mock.add_mock('/datasets/d1/tasks/t1?status=None', {'task_id': 't1', 'dataset_id': 'd1', 'job_id': 'j1', 'task_index': 0, 'status': 'failed'})
    client.req_mock.add_mock('/datasets/d1/tasks/t1/task_stats?last=true', {})
    async with client.get_http_client() as http_client:
        ret = await client.request_raw(http_client, 'GET', '/dataset/d1/task/t1')
        assert ret.status_code == 200
        assert_no_token(ret.text)
        forms = get_forms(ret.text)
        url = '/dataset/d1/task/actions'
        for action in ('reset', 'hard_reset', 'suspend', 'delete_logs'):
            assert forms[(url, action)]['fields']['task_ids'] == ['t1']
            assert forms[(url, action)]['fields']['return_page'] == ['detail']


async def test_page_job_browse(server):
    client = user(server)
    client.req_mock.add_mock('/datasets/d1/jobs', {
        'j1': {'job_id': 'j1', 'dataset_id': 'd1', 'job_index': 0, 'status': 'errors', 'status_changed': ''},
    })
    async with client.get_http_client() as http_client:
        ret = await client.request_raw(http_client, 'GET', '/dataset/d1/job')
        assert ret.status_code == 200
        assert_no_token(ret.text)
        doc = BeautifulSoup(ret.text, 'html.parser')
        forms = doc.find_all('form', {'action': '/dataset/d1/job/actions'})
        # 3 "All" forms + reset/hard_reset (suspend is not allowed from 'errors')
        assert len(forms) == 3 + 2


async def test_page_job_browse_empty(server):
    client = user(server)
    client.req_mock.add_mock('/datasets/d1/jobs', {})
    async with client.get_http_client() as http_client:
        ret = await client.request_raw(http_client, 'GET', '/dataset/d1/job')
        assert ret.status_code == 200
        assert '<form' not in ret.text.split('<table', 1)[1]


async def test_page_job_detail(server):
    client = user(server)
    client.req_mock.add_mock('/datasets/d1', {'dataset_id': 'd1', 'dataset': 123})
    client.req_mock.add_mock('/datasets/d1/jobs/j1', {'job_id': 'j1', 'dataset_id': 'd1', 'job_index': 0, 'status': 'errors'})
    client.req_mock.add_mock('/datasets/d1/tasks', {})
    async with client.get_http_client() as http_client:
        ret = await client.request_raw(http_client, 'GET', '/dataset/d1/job/j1')
        assert ret.status_code == 200
        assert_no_token(ret.text)
        forms = get_forms(ret.text)
        url = '/dataset/d1/job/actions'
        for action in ('reset', 'hard_reset'):
            assert forms[(url, action)]['fields']['job_ids'] == ['j1']
        assert (url, 'suspend') not in forms  # not allowed from 'errors'


async def test_csp_no_rest_api(server):
    """Browser JS no longer calls the REST API, so CSP only allows same-origin connects."""
    client = user(server)
    mock_dataset_page(client)
    async with client.get_http_client() as http_client:
        ret = await client.request_raw(http_client, 'GET', '/dataset/d1')
    csp = ret.headers['content-security-policy']
    assert "connect-src 'self';" in csp
    assert 'iceprod.test' not in csp
