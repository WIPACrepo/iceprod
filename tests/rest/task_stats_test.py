import asyncio
import logging

import pytest
import requests.exceptions
from rest_tools.utils.json_util import json_decode


async def test_rest_task_stats_post(server):
    client = server(roles=['system'])

    task_id = 'bar'
    data = {
        'dataset_id': 'foo',
        'bar': 1.23456,
        'baz': [1,2,3,4],
    }
    ret = await client.request('POST', f'/tasks/{task_id}/task_stats', data)
    task_stat_id = ret['result']


async def test_rest_task_stats_post_bad_role(server):
    client = server(roles=['user'])

    task_id = 'bar'
    data = {
        'dataset_id': 'foo',
        'bar': 1.23456,
        'baz': [1,2,3,4],
    }
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', f'/tasks/{task_id}/task_stats', data)
    assert exc_info.value.response.status_code == 403


async def test_rest_task_stats_bulk(server):
    client = server(roles=['system'])

    task_id = 'bar'
    data = {
        'dataset_id': 'foo',
        'bar': 1.23456,
        'baz': [1,2,3,4],
    }
    ret = await client.request('POST', f'/tasks/{task_id}/task_stats', data)
    task_stat_id = ret['result']
    ret = await client.request('POST', f'/tasks/{task_id}/task_stats', data)
    task_stat_id2 = ret['result']
    ret = await client.request('POST', f'/tasks/{task_id}/task_stats', data)
    task_stat_id3 = ret['result']

    url, kwargs = client._prepare('GET', f'/datasets/{data["dataset_id"]}/bulk/task_stats', {'buffer_size': 2})
    ret = await asyncio.wrap_future(client.session.request('GET', url, **kwargs))
    ret.raise_for_status()
    logging.info('ret.content: %r', ret.content)
    task_stats = [json_decode(r) for r in ret.content.split(b'\n') if r.strip()]
    ret_task_ids = [t['task_stat_id'] for t in task_stats]
    assert ret_task_ids == [task_stat_id, task_stat_id2, task_stat_id3]


async def test_rest_task_stats_get(server):
    client = server(roles=['system'])

    task_id = 'bar'
    data = {
        'dataset_id': 'foo',
        'bar': 1.23456,
        'baz': [1,2,3,4],
    }
    data_stat = data.copy()
    del data_stat['dataset_id']
    ret = await client.request('POST', f'/tasks/{task_id}/task_stats', data)
    task_stat_id = ret['result']
    ret = await client.request('POST', f'/tasks/{task_id}/task_stats', data)
    task_stat_id2 = ret['result']

    ret = await client.request('GET', f'/datasets/{data["dataset_id"]}/tasks/{task_id}/task_stats')
    assert len(ret) == 2
    assert task_stat_id in ret
    assert task_stat_id2 in ret
    assert 'task_id' in ret[task_stat_id]
    assert task_id == ret[task_stat_id]['task_id']
    assert data_stat == ret[task_stat_id]['stats']

    ret = await client.request('GET', f'/datasets/{data["dataset_id"]}/tasks/{task_id}/task_stats', {'last': 'true'})
    assert len(ret) == 1
    assert task_stat_id2 in ret
    assert 'task_id' in ret[task_stat_id2]
    assert task_id == ret[task_stat_id2]['task_id']
    assert data_stat == ret[task_stat_id2]['stats']

    ret = await client.request('GET', f'/datasets/{data["dataset_id"]}/tasks/{task_id}/task_stats', {'last': 'true', 'keys': 'task_id'})
    assert len(ret) == 1
    assert task_stat_id2 in ret
    assert 'task_id' in ret[task_stat_id2]
    assert task_id == ret[task_stat_id2]['task_id']
    assert 'stats' not in ret[task_stat_id2]


async def test_rest_task_stats_get_details(server):
    client = server(roles=['system'])

    task_id = 'bar'
    data = {
        'dataset_id': 'foo',
        'bar': 1.23456,
        'baz': [1,2,3,4],
    }
    data_stat = data.copy()
    del data_stat['dataset_id']
    ret = await client.request('POST', f'/tasks/{task_id}/task_stats', data)
    task_stat_id = ret['result']

    ret = await client.request('GET', f'/datasets/{data["dataset_id"]}/tasks/{task_id}/task_stats/{task_stat_id}')
    assert task_stat_id == ret['task_stat_id']
    assert task_id == ret['task_id']
    assert data_stat == ret['stats']
