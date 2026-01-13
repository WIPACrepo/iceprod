import random
import string

import pytest



async def test_rest_logs_post(server):
    client = server(roles=['system'])

    data = {'data':'foo bar baz'}
    ret = await client.request('POST', '/logs', data)
    log_id = ret['result']


async def test_rest_logs_get(server):
    client = server(roles=['system'])

    data = {'name': 'stdlog', 'data': 'foo bar baz'}
    ret = await client.request('POST', '/logs', data)
    log_id = ret['result']

    ret = await client.request('GET', '/logs')
    assert log_id in ret
    assert len(ret) == 1
    for k in data:
        assert k in ret[log_id]
        assert data[k] == ret[log_id][k]

    args = {'name': 'stdlog', 'keys': 'log_id|name|data'}
    ret = await client.request('GET', '/logs', args)
    assert log_id in ret


async def test_rest_logs_get_details(server):
    client = server(roles=['system'])

    data = {'data': 'foo bar baz'}
    ret = await client.request('POST', '/logs', data)
    log_id = ret['result']

    ret = await client.request('GET', f'/logs/{log_id}')
    for k in data:
        assert k in ret
        assert data[k] == ret[k]


async def test_rest_logs_dataset_post(server):
    client = server(roles=['user'], groups=['users'])

    data = {
        'description': 'blah',
        'tasks_per_job': 4,
        'jobs_submitted': 1,
        'tasks_submitted': 4,
        'group': 'users',
    }
    ret = await client.request('POST', '/datasets', data)
    dataset_id = ret['result']

    data = {'data':'foo bar baz'}
    ret = await client.request('POST', f'/datasets/{dataset_id}/logs', data)
    log_id = ret['result']


async def test_rest_logs_dataset_get(server):
    client = server(roles=['user'], groups=['users'])

    data = {
        'description': 'blah',
        'tasks_per_job': 4,
        'jobs_submitted': 1,
        'tasks_submitted': 4,
        'group': 'users',
    }
    ret = await client.request('POST', '/datasets', data)
    dataset_id = ret['result']

    data = {'data':'foo bar baz', 'dataset_id': dataset_id}
    ret = await client.request('POST', f'/datasets/{dataset_id}/logs', data)
    log_id = ret['result']

    ret = await client.request('GET', f'/datasets/{dataset_id}/logs/{log_id}')
    for k in data:
        assert k in ret
        assert data[k] == ret[k]


async def test_rest_logs_dataset_task_get(server):
    client = server(roles=['user'], groups=['users'])

    data = {
        'description': 'blah',
        'tasks_per_job': 4,
        'jobs_submitted': 1,
        'tasks_submitted': 4,
        'group': 'users',
    }
    ret = await client.request('POST', '/datasets', data)
    dataset_id = ret['result']

    data = {'data':'foo', 'dataset_id': dataset_id, 'task_id': 'bar', 'name': 'stdout'}
    ret = await client.request('POST', f'/datasets/{dataset_id}/logs', data)
    log_id = ret['result']

    ret = await client.request('GET', f'/datasets/{dataset_id}/tasks/bar/logs')
    assert 'logs' in ret
    assert len(ret['logs']) == 1
    assert ret['logs'][0]['log_id'] == log_id
    assert data['data'] == ret['logs'][0]['data']

    # now try for groupings (only last of named type)
    data = {'data':'bar', 'dataset_id': dataset_id, 'task_id': 'bar', 'name': 'stderr'}
    await client.request('POST', f'/datasets/{dataset_id}/logs', data)
    
    data = {'data':'baz', 'dataset_id': dataset_id, 'task_id': 'bar', 'name': 'stdout'}
    await client.request('POST', f'/datasets/{dataset_id}/logs', data)
    
    ret = await client.request('GET', f'/datasets/{dataset_id}/tasks/bar/logs', {'group': 'true'})
    assert 'logs' in ret
    assert len(ret['logs']) == 2
    assert ret['logs'][0]['data'] == 'baz'
    assert ret['logs'][1]['data'] == 'bar'

    # now check order, num, and keys
    ret = await client.request('GET', f'/datasets/{dataset_id}/tasks/bar/logs', {'order': 'asc', 'num': 1, 'keys': 'log_id|data'})
    assert 'logs' in ret
    assert len(ret['logs']) == 1
    assert ret['logs'][0]['log_id'] == log_id
    assert ret['logs'][0]['data'] == 'foo'


def fake_data(N):
    return ''.join(random.choices(string.printable, k=N))


async def test_rest_logs_s3_post(s3conn, server):
    client = server(roles=['system'])

    data = {'data': fake_data(2000000)}
    ret = await client.request('POST', '/logs', data)
    log_id = ret['result']

    body = s3conn.get_object(Bucket='iceprod2-logs', Key=log_id)['Body'].read().decode('utf-8')
    assert body == data['data']

    data = {'data': fake_data(200000)}
    ret = await client.request('POST', '/logs', data)
    log_id = ret['result']

    with pytest.raises(Exception):
        s3conn.get_object(Bucket='iceprod2-logs', Key=log_id)


async def test_rest_logs_s3_get(s3conn, server):
    client = server(roles=['system'])

    data = {'data': fake_data(2000000)}
    ret = await client.request('POST', '/logs', data)
    log_id = ret['result']

    body = s3conn.get_object(Bucket='iceprod2-logs', Key=log_id)['Body'].read().decode('utf-8')
    assert body == data['data']

    ret = await client.request('GET', f'/logs/{log_id}')
    assert ret['data'] == data['data']

    data = {'data': fake_data(200000)}
    ret = await client.request('POST', '/logs', data)
    log_id = ret['result']

    with pytest.raises(Exception):
        s3conn.get_object(Bucket='iceprod2-logs', Key=log_id)

    ret = await client.request('GET', f'/logs/{log_id}')
    assert ret['data'] == data['data']


async def test_rest_logs_s3_dataset_post(s3conn, server):
    client = server(roles=['user'], groups=['users'])

    data = {
        'description': 'blah',
        'tasks_per_job': 4,
        'jobs_submitted': 1,
        'tasks_submitted': 4,
        'group': 'users',
    }
    ret = await client.request('POST', '/datasets', data)
    dataset_id = ret['result']

    data = {'data': fake_data(2000000)}
    ret = await client.request('POST', f'/datasets/{dataset_id}/logs', data)
    log_id = ret['result']

    body = s3conn.get_object(Bucket='iceprod2-logs', Key=log_id)['Body'].read().decode('utf-8')
    assert body == data['data']

    ret = await client.request('GET', f'/datasets/{dataset_id}/logs/{log_id}')
    assert ret['data'] == data['data']

    data = {'data': fake_data(200000)}
    ret = await client.request('POST', f'/datasets/{dataset_id}/logs', data)
    log_id = ret['result']

    with pytest.raises(Exception):
        s3conn.get_object(Bucket='iceprod2-logs', Key=log_id)

    ret = await client.request('GET', f'/datasets/{dataset_id}/logs/{log_id}')
    assert ret['data'] == data['data']


async def test_rest_logs_s3_dataset_task_get(server):
    client = server(roles=['user'], groups=['users'])

    data = {
        'description': 'blah',
        'tasks_per_job': 4,
        'jobs_submitted': 1,
        'tasks_submitted': 4,
        'group': 'users',
    }
    ret = await client.request('POST', '/datasets', data)
    dataset_id = ret['result']

    data = {'data': fake_data(2000000), 'dataset_id': dataset_id, 'task_id': 'bar', 'name': 'stdout'}
    ret = await client.request('POST', f'/datasets/{dataset_id}/logs', data)
    log_id = ret['result']

    ret = await client.request('GET', f'/datasets/{dataset_id}/tasks/bar/logs')
    assert 'logs' in ret
    assert len(ret['logs']) == 1
    assert ret['logs'][0]['log_id'] == log_id
    assert data['data'] == ret['logs'][0]['data']

    data = {'data': fake_data(200000), 'dataset_id': dataset_id, 'task_id': 'bar', 'name': 'stdout'}
    ret = await client.request('POST', f'/datasets/{dataset_id}/logs', data)
    log_id = ret['result']

    with pytest.raises(Exception):
        s3conn.get_object(Bucket='iceprod2-logs', Key=log_id)

    ret = await client.request('GET', f'/datasets/{dataset_id}/tasks/bar/logs', {'order': 'asc'})
    assert 'logs' in ret
    assert len(ret['logs']) == 2
    assert ret['logs'][1]['log_id'] == log_id
    assert data['data'] == ret['logs'][1]['data']

