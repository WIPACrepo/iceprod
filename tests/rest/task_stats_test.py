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


async def _bulk_task_stats(client, dataset_id, args):
    """Call the streaming bulk task_stats endpoint and decode each line"""
    url, kwargs = client._prepare('GET', f'/datasets/{dataset_id}/bulk/task_stats', args)
    ret = await asyncio.wrap_future(client.session.request('GET', url, **kwargs))
    ret.raise_for_status()
    return [json_decode(r) for r in ret.content.split(b'\n') if r.strip()]


async def test_rest_task_stats_bulk_task_ids(server):
    client = server(roles=['system'])

    stat_ids = {}
    for task_id in ('t1', 't2', 't3'):
        stat_ids[task_id] = []
        for _ in range(2):
            ret = await client.request('POST', f'/tasks/{task_id}/task_stats', {'dataset_id': 'foo', 'bar': 1})
            stat_ids[task_id].append(ret['result'])

    # all stats for a subset of tasks, sorted by create_date within each task
    task_stats = await _bulk_task_stats(client, 'foo', {'task_ids': 't1|t3'})
    assert [t['task_stat_id'] for t in task_stats] == stat_ids['t1'] + stat_ids['t3']

    # last stat only
    task_stats = await _bulk_task_stats(client, 'foo', {'task_ids': 't2', 'last': 'true'})
    assert [t['task_stat_id'] for t in task_stats] == stat_ids['t2'][-1:]

    # too many ids
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await _bulk_task_stats(client, 'foo', {'task_ids': '|'.join(f't{i}' for i in range(101))})
    assert exc_info.value.response.status_code == 400


async def test_rest_task_stats_bulk_keys(server):
    client = server(roles=['system'])

    data = {'dataset_id': 'foo', 'bar': 1, 'baz': 2}
    ids = []
    for _ in range(2):
        ret = await client.request('POST', '/tasks/t1/task_stats', data)
        ids.append(ret['result'])

    # last=false honors keys and always returns the ids
    task_stats = await _bulk_task_stats(client, 'foo', {'keys': 'stats'})
    assert [t['task_stat_id'] for t in task_stats] == ids
    for t in task_stats:
        assert set(t) == {'task_stat_id', 'task_id', 'stats'}

    task_stats = await _bulk_task_stats(client, 'foo', {'keys': 'stats', 'last': 'true'})
    assert len(task_stats) == 1
    assert set(task_stats[0]) == {'task_stat_id', 'task_id', 'stats'}
    assert task_stats[0]['task_stat_id'] == ids[-1]


async def test_rest_task_stats_bulk_user_auth(server):
    client = server(roles=['user'], groups=['users'])

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await _bulk_task_stats(client, 'notmine', {'task_ids': 't1'})
    assert exc_info.value.response.status_code == 403
