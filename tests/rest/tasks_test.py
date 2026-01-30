from collections import Counter
import pytest
import requests.exceptions
from iceprod.server import states


async def test_rest_tasks_post(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 0,
        'job_index': 0,
        'name': 'bar',
        'depends': [],
        'requirements': {},
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']


async def test_rest_tasks_post_bad_role(server):
    client = server(roles=['user'])

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 0,
        'job_index': 0,
        'name': 'bar',
        'depends': [],
        'requirements': {},
    }
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', '/tasks', data)
    assert exc_info.value.response.status_code == 403


async def test_rest_tasks_get(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 0,
        'job_index': 0,
        'name': 'bar',
        'depends': [],
        'requirements': {},
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    ret = await client.request('GET', '/tasks')
    assert 'tasks' in ret
    assert len(ret['tasks']) == 1
    assert ret['tasks'][0]['task_id'] == task_id

    ret = await client.request('GET', '/tasks', {'keys': 'dataset_id|name'})
    assert 'tasks' in ret
    assert len(ret['tasks']) == 1
    assert 'task_id' not in ret['tasks'][0]
    assert 'dataset_id' in ret['tasks'][0]
    assert 'name' in ret['tasks'][0]

    data = {'status': 'waiting'}
    await client.request('PUT', f'/tasks/{task_id}/status', data)

    ret = await client.request('GET', '/tasks', {'status': states.TASK_STATUS_START})
    assert 'tasks' in ret
    assert len(ret['tasks']) == 0

    ret = await client.request('GET', '/tasks', {'status': 'waiting'})
    assert 'tasks' in ret
    assert len(ret['tasks']) == 1
    assert ret['tasks'][0]['task_id'] == task_id


async def test_rest_tasks_get_by_site(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 0,
        'job_index': 0,
        'name': 'bar',
        'depends': [],
        'requirements': {},
    }
    ret = await client.request('POST', '/tasks', data)
    task_id1 = ret['result']
    await client.request('PATCH', f'/tasks/{task_id1}', {'site': 'foo'})

    ret = await client.request('POST', '/tasks', data)
    task_id2 = ret['result']
    await client.request('PATCH', f'/tasks/{task_id2}', {'site': 'bar'})

    ret = await client.request('POST', '/tasks', data)
    task_id3 = ret['result']
    await client.request('PATCH', f'/tasks/{task_id3}', {'site': 'foo.bar'})

    ret = await client.request('GET', '/tasks', {'site': 'foo'})
    assert 'tasks' in ret
    assert len(ret['tasks']) == 2
    assert [t['task_id'] for t in ret['tasks']] == [task_id1, task_id3]

    ret = await client.request('GET', '/tasks', {'site': 'bar'})
    assert 'tasks' in ret
    assert len(ret['tasks']) == 1
    assert [t['task_id'] for t in ret['tasks']] == [task_id2]

    ret = await client.request('GET', '/tasks', {'site': 'foo.bar'})
    assert 'tasks' in ret
    assert len(ret['tasks']) == 1
    assert [t['task_id'] for t in ret['tasks']] == [task_id3]


async def test_rest_tasks_get_details(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 0,
        'job_index': 0,
        'name': 'bar',
        'depends': [],
        'requirements': {},
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    ret = await client.request('GET', f'/tasks/{task_id}')
    assert ret['task_id'] == task_id
    for k in data:
        assert k in ret
        assert data[k] == ret[k]
    for k in ('status','status_changed','failures','evictions','walltime',
              'walltime_err','walltime_err_n'):
        assert k in ret
    assert ret['status'] == states.TASK_STATUS_START


async def test_rest_tasks_patch(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 0,
        'job_index': 0,
        'name': 'bar',
        'depends': [],
        'requirements': {},
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    new_data = {
        'status': 'processing',
        'failures': 1,
    }
    ret = await client.request('PATCH', f'/tasks/{task_id}', new_data)
    for k in new_data:
        assert k in ret
        assert new_data[k] == ret[k]


async def test_rest_tasks_put_status(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 0,
        'job_index': 0,
        'name': 'bar',
        'depends': [],
        'requirements': {},
        'status': 'queued',
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    new_data = {
        'status': 'failed',
    }
    await client.request('PUT', f'/tasks/{task_id}/status', new_data)

    ret = await client.request('GET', f'/tasks/{task_id}')
    assert ret['status'] == new_data['status']


async def test_rest_tasks_dataset_get(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 0,
        'job_index': 0,
        'name': 'bar',
        'depends': [],
        'requirements': {},
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    ret = await client.request('GET', f'/datasets/{data["dataset_id"]}/tasks')
    assert task_id in ret
    for k in data:
        assert k in ret[task_id]
        assert ret[task_id][k] == data[k]


async def test_rest_tasks_dataset_get_details(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 0,
        'job_index': 0,
        'name': 'bar',
        'depends': [],
        'requirements': {},
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    ret = await client.request('GET', f'/datasets/{data["dataset_id"]}/tasks/{task_id}')
    for k in data:
        assert k in ret
        assert ret[k] == data[k]
    for k in ('status','status_changed','failures','evictions','walltime',
              'walltime_err','walltime_err_n'):
        assert k in ret
    assert ret['status'] == states.TASK_STATUS_START


async def test_rest_tasks_dataset_put_status(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 0,
        'job_index': 0,
        'name': 'bar',
        'depends': [],
        'requirements': {},
        'status': 'queued',
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    new_data = {
        'status': 'failed',
    }
    await client.request('PUT', f'/datasets/{data["dataset_id"]}/tasks/{task_id}/status', new_data)

    ret = await client.request('GET', f'/datasets/{data["dataset_id"]}/tasks/{task_id}')
    assert ret['status'] == new_data['status']


async def test_rest_tasks_dataset_summaries_status(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 0,
        'job_index': 0,
        'name': 'bar',
        'depends': [],
        'requirements': {},
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    ret = await client.request('GET', f'/datasets/{data["dataset_id"]}/task_summaries/status')
    assert ret == {states.TASK_STATUS_START: [task_id]}


async def test_rest_tasks_dataset_counts_status(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 0,
        'job_index': 0,
        'name': 'bar',
        'depends': [],
        'requirements': {},
    }
    ret = await client.request('POST', '/tasks', data)

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 1,
        'job_index': 0,
        'name': 'baz',
        'depends': [],
        'requirements': {'gpu': 1},
        'status': 'processing'
    }
    ret = await client.request('POST', '/tasks', data)

    ret = await client.request('GET', f'/datasets/{data["dataset_id"]}/task_counts/status')
    assert ret == {states.TASK_STATUS_START: 1, 'processing': 1}

    ret = await client.request('GET', f'/datasets/{data["dataset_id"]}/task_counts/status?status=complete')
    assert ret == {}
    
    ret = await client.request('GET', f'/datasets/{data["dataset_id"]}/task_counts/status?gpu=false')
    assert ret == {states.TASK_STATUS_START: 1}
    
    ret = await client.request('GET', f'/datasets/{data["dataset_id"]}/task_counts/status?gpu=true')
    assert ret == {'processing': 1}


async def test_rest_tasks_dataset_counts_name_status(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 0,
        'job_index': 0,
        'name': 'bar',
        'depends': [],
        'requirements': {},
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    ret = await client.request('GET', f'/datasets/{data["dataset_id"]}/task_counts/name_status')
    assert ret == {'bar': {states.TASK_STATUS_START: 1}}


async def test_rest_tasks_dataset_stats(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 0,
        'job_index': 0,
        'name': 'bar',
        'depends': [],
        'requirements': {},
        'status': 'processing',
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    ret = await client.request('GET', f'/datasets/{data["dataset_id"]}/task_stats')
    assert ret == {}

    # mark complete to get a stat
    await client.request('PUT', f'/tasks/{task_id}/status', {'status': 'complete'})

    ret = await client.request('GET', f'/datasets/{data["dataset_id"]}/task_stats')
    assert 'bar' in ret
    for s in ('count','total_hrs','total_err_hrs','avg_hrs','stddev_hrs','min_hrs','max_hrs','efficiency'):
        assert s in ret['bar']


async def test_rest_tasks_actions_waiting(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 0,
        'job_index': 0,
        'priority': .5,
        'name': 'bar',
        'depends': [],
        'requirements': {},
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    data = {
        'dataset_id': 'bar',
        'job_id': 'bar1',
        'task_index': 0,
        'job_index': 0,
        'priority': 10.,
        'name': 'bar',
        'depends': [],
        'requirements': {},
    }
    ret = await client.request('POST', '/tasks', data)
    task_id2 = ret['result']

    ret = await client.request('POST', '/task_actions/waiting', {'task_ids': [task_id2]})
    assert 'waiting' in ret
    assert ret['waiting'] == 1

    ret = await client.request('GET', f'/tasks/{task_id}')
    assert ret['status'] == states.TASK_STATUS_START

    ret = await client.request('GET', f'/tasks/{task_id2}')
    assert ret['status'] == 'waiting'


async def test_rest_tasks_actions_queue(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 0,
        'job_index': 0,
        'status': 'waiting',
        'priority': .5,
        'name': 'bar',
        'depends': [],
        'requirements': {},
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    ret = await client.request('POST', '/task_actions/queue', {})
    assert ret['task_id'] == task_id
    assert ret['status'] == 'queued'

    ret = await client.request('GET', f'/tasks/{task_id}')
    assert ret['status'] == 'queued'


async def test_rest_tasks_actions_queue_reqs(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 0,
        'job_index': 0,
        'status': 'waiting',
        'priority': .5,
        'name': 'bar',
        'depends': [],
        'requirements': {'memory': 4.5, 'disk': 100},
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    # not enough reqs to queue task
    args = {'requirements': {'memory': 2.0, 'disk': 120}}
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', '/task_actions/queue', args)
    assert exc_info.value.response.status_code == 404

    ret = await client.request('GET', f'/tasks/{task_id}')
    assert ret['status'] == 'waiting'

    # now should queue
    args = {'requirements': {'memory': 6.0, 'disk': 120}}
    ret = await client.request('POST', '/task_actions/queue', args)
    assert task_id == ret['task_id']

    ret = await client.request('GET', f'/tasks/{task_id}')
    assert ret['status'] == 'queued'


@pytest.mark.parametrize('num_tasks,deprio,avail,out_tasks', [
    (10, [], {}, {}),
    (10, [], {'d1': 50}, {'d1': 10}),
    (10, ['d1'], {'d1': 5}, {'d1': 5}),
    (10, [], {'d1': 2, 'd2': 3}, {'d1': 2, 'd2': 3}),
    (10, ['d1', 'd2'], {'d1': 10, 'd2': 10, 'd3': 2}, {'d2': 8, 'd3': 2}),
    (10, ['d1', 'd2'], {'d1': 10, 'd2': 10, 'd3': 20}, {'d3': 10}),
    (10, ['d2', 'd1'], {'d1': 20, 'd2': 20}, {'d1': 10}),
    (10, ['d1', 'd2'], {'d1': 20, 'd2': 20}, {'d2': 10}),
])
async def test_rest_tasks_actions_queue_many(num_tasks, deprio, avail, out_tasks, server):
    client = server(roles=['system'])

    for d in avail:
        for n in range(avail[d]):
            data = {
                'dataset_id': d,
                'job_id': 'foo1',
                'task_index': 0,
                'job_index': n,
                'status': 'waiting',
                'priority': .5,
                'name': 'bar',
                'depends': [],
                'requirements': {},
            }
            task_id = await client.request('POST', '/tasks', data)
            ret = await client.request('GET', f'/tasks/{task_id["result"]}')
            assert ret['status'] == 'waiting'

    args = {
        'num': num_tasks,
        'dataset_deprio': deprio,
    }

    if not out_tasks:
        with pytest.raises(requests.exceptions.HTTPError) as exc_info:
            await client.request('POST', '/task_actions/queue_many', args)
        assert exc_info.value.response.status_code == 404
    else:
        ret = await client.request('POST', '/task_actions/queue_many', args)
        c = Counter(r['dataset_id'] for r in ret)
        assert c == out_tasks


async def test_rest_tasks_actions_process(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 0,
        'job_index': 0,
        'status': 'queued',
        'priority': .5,
        'name': 'bar',
        'depends': [],
        'requirements': {},
        'instance_id': '12345',
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    args = {
        'instance_id': '12345'
    }
    ret = await client.request('POST', f'/tasks/{task_id}/task_actions/processing', args)
    assert task_id == ret['task_id']

    ret = await client.request('GET', f'/tasks/{task_id}')
    assert ret['status'] == 'processing'
    assert ret['instance_id'] == args['instance_id']


async def test_rest_tasks_actions_reset(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 0,
        'job_index': 0,
        'status': 'queued',
        'priority': .5,
        'name': 'bar',
        'depends': [],
        'requirements': {'memory':5.6, 'gpu':1},
        'instance_id': '12345',
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    # try without instance id
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', f'/tasks/{task_id}/task_actions/reset', {})
    assert exc_info.value.response.status_code == 400
    assert 'Missing instance_id' in exc_info.value.response.text

    # now with instance id
    args = {
        'instance_id': '12345',
    }
    await client.request('POST', f'/tasks/{task_id}/task_actions/reset', args)

    ret = await client.request('GET', f'/tasks/{task_id}')
    assert ret['status'] == 'waiting'
    assert ret['instance_id'] == ''

    # now with bad instance id
    args = {
        'status': 'queued',
        'instance_id': '12345',
    }
    await client.request('PATCH', f'/tasks/{task_id}', args)

    args = {
        'instance_id': '666',
    }
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', f'/tasks/{task_id}/task_actions/reset', args)
    assert exc_info.value.response.status_code == 404

    # now try with time_used
    args = {
        'status': 'queued',
        'instance_id': '12345',
    }
    await client.request('PATCH', f'/tasks/{task_id}', args)

    # now try with time_used
    args = {
        'status': 'queued',
        'instance_id': '12345',
    }
    await client.request('PATCH', f'/tasks/{task_id}', args)

    args = {
        'instance_id': '12345',
        'time_used': 7200,
    }
    await client.request('POST', f'/tasks/{task_id}/task_actions/reset', args)

    ret = await client.request('GET', f'/tasks/{task_id}')
    assert ret['status'] == 'waiting'
    assert ret['walltime_err_n'] == 1
    assert ret['walltime_err'] == 2.0

    # now try with resources
    args = {
        'status': 'queued',
        'instance_id': '12345',
    }
    await client.request('PATCH', f'/tasks/{task_id}', args)

    args = {
        'instance_id': '12345',
        'resources': {'time':2.5, 'memory':3.5, 'disk': 20.3, 'gpu': 23},
    }
    await client.request('POST', f'/tasks/{task_id}/task_actions/reset', args)

    ret = await client.request('GET', f'/tasks/{task_id}')
    assert ret['status'] == 'waiting'
    assert ret['walltime_err_n'] == 2
    assert ret['walltime_err'] == 4.5
    assert ret['requirements']['memory'] == data['requirements']['memory']
    assert ret['requirements']['time'] >= args['resources']['time']
    assert ret['requirements']['disk'] >= args['resources']['disk']
    assert ret['requirements']['gpu'] != args['resources']['gpu']  # gpu doesn't change

    # now try with a bad status
    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 1,
        'job_index': 0,
        'status': 'complete',
        'priority': .5,
        'name': 'bar',
        'depends': [],
        'requirements': {'memory':5.6, 'gpu':1},
        'instance_id': '12345',
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    args = {
        'instance_id': '12345',
    }
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', f'/tasks/{task_id}/task_actions/reset', args)
    assert exc_info.value.response.status_code == 400


async def test_rest_tasks_actions_failed(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 0,
        'job_index': 0,
        'status': 'queued',
        'priority': .5,
        'name': 'bar',
        'depends': [],
        'requirements': {'memory':5.6, 'gpu':1},
        'instance_id': '12345',
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    # try without instance id
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', f'/tasks/{task_id}/task_actions/failed', {})
    assert exc_info.value.response.status_code == 400
    assert 'Missing instance_id' in exc_info.value.response.text

    # now with instance id
    args = {
        'instance_id': '12345',
    }
    await client.request('POST', f'/tasks/{task_id}/task_actions/failed', args)

    ret = await client.request('GET', f'/tasks/{task_id}')
    assert ret['status'] == 'failed'

    # now with bad instance id
    args = {
        'status': 'queued',
        'instance_id': '12345',
    }
    await client.request('PATCH', f'/tasks/{task_id}', args)

    args = {
        'instance_id': '666',
    }
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', f'/tasks/{task_id}/task_actions/failed', args)
    assert exc_info.value.response.status_code == 404

    # now try with time_used
    args = {
        'status': 'queued',
        'instance_id': '12345',
    }
    await client.request('PATCH', f'/tasks/{task_id}', args)

    args = {
        'instance_id': '12345',
        'time_used': 7200
    }
    await client.request('POST', f'/tasks/{task_id}/task_actions/failed', args)

    ret = await client.request('GET', f'/tasks/{task_id}')
    assert ret['status'] == 'failed'
    assert ret['walltime_err_n'] == 1
    assert ret['walltime_err'] == 2.0

    # now try with resources
    args = {
        'status': 'queued',
        'instance_id': '12345',
    }
    await client.request('PATCH', f'/tasks/{task_id}', args)

    args = {
        'instance_id': '12345',
        'resources': {'time':2.5, 'memory':3.5, 'disk': 20.3, 'gpu': 23}
    }
    await client.request('POST', f'/tasks/{task_id}/task_actions/failed', args)

    ret = await client.request('GET', f'/tasks/{task_id}')
    assert ret['status'] == 'failed'
    assert ret['walltime_err_n'] == 2
    assert ret['walltime_err'] == 4.5
    assert ret['requirements']['memory'] == data['requirements']['memory']
    assert ret['requirements']['time'] >= args['resources']['time']
    assert ret['requirements']['disk'] >= args['resources']['disk']
    assert ret['requirements']['gpu'] != args['resources']['gpu']  # gpu doesn't change

    # now try with a bad status
    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 1,
        'job_index': 0,
        'status': 'complete',
        'priority': .5,
        'name': 'bar',
        'depends': [],
        'requirements': {'memory':5.6, 'gpu':1},
        'instance_id': '12345',
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    args = {
        'instance_id': '12345',
    }
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', f'/tasks/{task_id}/task_actions/failed', args)
    assert exc_info.value.response.status_code == 400


async def test_rest_tasks_actions_complete(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 0,
        'job_index': 0,
        'status': 'processing',
        'priority': .5,
        'name': 'bar',
        'depends': [],
        'requirements': {'memory':5.6, 'gpu':1},
        'instance_id': '12345',
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    # try without instance id
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', f'/tasks/{task_id}/task_actions/complete', {})
    assert exc_info.value.response.status_code == 400
    assert 'Missing instance_id' in exc_info.value.response.text

    # now with instance id
    args = {
        'instance_id': '12345',
    }
    await client.request('POST', f'/tasks/{task_id}/task_actions/complete', args)

    ret = await client.request('GET', f'/tasks/{task_id}')
    assert ret['status'] == 'complete'

    # now with bad instance id
    args = {
        'status': 'processing',
        'instance_id': '12345',
    }
    await client.request('PATCH', f'/tasks/{task_id}', args)

    args = {
        'instance_id': '666',
    }
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', f'/tasks/{task_id}/task_actions/complete', args)
    assert exc_info.value.response.status_code == 404

    # now try with time_used
    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 1,
        'job_index': 0,
        'status': 'processing',
        'priority': .5,
        'name': 'bar',
        'depends': [],
        'requirements': {'memory':5.6, 'gpu':1},
        'instance_id': '12345',
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    args = {
        'instance_id': '12345',
        'time_used': 7200
    }
    await client.request('POST', f'/tasks/{task_id}/task_actions/complete', args)

    ret = await client.request('GET', f'/tasks/{task_id}')
    assert ret['status'] == 'complete'
    assert ret['walltime'] == 2.0

    # now try with a bad status
    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 2,
        'job_index': 0,
        'status': 'idle',
        'priority': .5,
        'name': 'bar',
        'depends': [],
        'requirements': {'memory':5.6, 'gpu':1},
        'instance_id': '12345',
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    args = {
        'instance_id': '12345',
    }
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', f'/tasks/{task_id}/task_actions/complete', args)
    assert exc_info.value.response.status_code == 400


async def test_rest_tasks_actions_bulk_status(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 0,
        'job_index': 0,
        'name': 'bar',
        'depends': [],
        'requirements': {},
        'status': 'queued',
        'instance_id': '12345',
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    data2 = {'tasks': [task_id]}
    await client.request('POST', f'/datasets/{data["dataset_id"]}/task_actions/bulk_status/failed', data2)

    ret = await client.request('GET', f'/tasks/{task_id}')
    assert ret['status'] == 'failed'
    assert ret['instance_id'] == ''

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo2',
        'task_index': 0,
        'job_index': 0,
        'name': 'bar',
        'depends': [],
        'requirements': {},
    }
    ret = await client.request('POST', '/tasks', data)
    task_id2 = ret['result']

    data2 = {'tasks': [task_id, task_id2]}
    await client.request('POST', f'/datasets/{data["dataset_id"]}/task_actions/bulk_status/waiting', data2)

    ret = await client.request('GET', f'/tasks/{task_id}')
    assert ret['status'] == 'waiting'
    ret = await client.request('GET', f'/tasks/{task_id2}')
    assert ret['status'] == 'waiting'

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', f'/datasets/{data["dataset_id"]}/task_actions/bulk_status/blah', data2)
    assert exc_info.value.response.status_code == 400

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('POST', f'/datasets/{data["dataset_id"]}/task_actions/bulk_status/processing', {})
    assert exc_info.value.response.status_code == 400


async def test_rest_tasks_actions_bulk_suspend(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 0,
        'job_index': 0,
        'name': 'bar',
        'depends': [],
        'requirements': {},
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    data2 = {}
    await client.request('POST', f'/datasets/{data["dataset_id"]}/task_actions/bulk_suspend', data2)

    ret = await client.request('GET', f'/tasks/{task_id}')
    assert ret['status'] == 'suspended'


async def test_rest_tasks_actions_bulk_suspend_by_job(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 0,
        'job_index': 0,
        'name': 'bar',
        'depends': [],
        'requirements': {},
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo2',
        'task_index': 0,
        'job_index': 1,
        'name': 'bar',
        'depends': [],
        'requirements': {},
    }
    ret = await client.request('POST', '/tasks', data)
    task_id2 = ret['result']

    data2 = {'jobs': ['foo1']}
    await client.request('POST', f'/datasets/{data["dataset_id"]}/task_actions/bulk_suspend', data2)

    ret = await client.request('GET', f'/tasks/{task_id}')
    assert ret['status'] == 'suspended'
    ret = await client.request('GET', f'/tasks/{task_id2}')
    assert ret['status'] == states.TASK_STATUS_START


async def test_rest_tasks_actions_bulk_suspend_by_task(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 0,
        'job_index': 0,
        'name': 'bar',
        'depends': [],
        'requirements': {},
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo2',
        'task_index': 0,
        'job_index': 1,
        'name': 'bar',
        'depends': [],
        'requirements': {},
    }
    ret = await client.request('POST', '/tasks', data)
    task_id2 = ret['result']

    data2 = {'tasks': [task_id]}
    await client.request('POST', f'/datasets/{data["dataset_id"]}/task_actions/bulk_suspend', data2)

    ret = await client.request('GET', f'/tasks/{task_id}')
    assert ret['status'] == 'suspended'
    ret = await client.request('GET', f'/tasks/{task_id2}')
    assert ret['status'] == states.TASK_STATUS_START


async def test_rest_tasks_actions_bulk_reset(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 0,
        'job_index': 0,
        'name': 'bar',
        'depends': [],
        'requirements': {},
        'status': 'waiting',
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    data2 = {}
    await client.request('POST', f'/datasets/{data["dataset_id"]}/task_actions/bulk_reset', data2)

    ret = await client.request('GET', f'/tasks/{task_id}')
    assert ret['status'] == states.TASK_STATUS_START


async def test_rest_tasks_actions_bulk_reset_by_job(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 0,
        'job_index': 0,
        'name': 'bar',
        'depends': [],
        'requirements': {},
        'status': 'waiting',
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo2',
        'task_index': 0,
        'job_index': 1,
        'name': 'bar',
        'depends': [],
        'requirements': {},
        'status': 'waiting',
    }
    ret = await client.request('POST', '/tasks', data)
    task_id2 = ret['result']

    data2 = {'jobs': ['foo1']}
    await client.request('POST', f'/datasets/{data["dataset_id"]}/task_actions/bulk_reset', data2)

    ret = await client.request('GET', f'/tasks/{task_id}')
    assert ret['status'] == states.TASK_STATUS_START
    ret = await client.request('GET', f'/tasks/{task_id2}')
    assert ret['status'] == 'waiting'


async def test_rest_tasks_actions_bulk_reset_by_task(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 0,
        'job_index': 0,
        'name': 'bar',
        'depends': [],
        'requirements': {},
        'status': 'waiting',
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo2',
        'task_index': 0,
        'job_index': 1,
        'name': 'bar',
        'depends': [],
        'requirements': {},
        'status': 'waiting',
    }
    ret = await client.request('POST', '/tasks', data)
    task_id2 = ret['result']

    data2 = {'tasks': [task_id]}
    await client.request('POST', f'/datasets/{data["dataset_id"]}/task_actions/bulk_reset', data2)

    ret = await client.request('GET', f'/tasks/{task_id}')
    assert ret['status'] == states.TASK_STATUS_START
    ret = await client.request('GET', f'/tasks/{task_id2}')
    assert ret['status'] == 'waiting'


async def test_rest_tasks_actions_bulk_hard_reset(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 0,
        'job_index': 0,
        'name': 'bar',
        'depends': [],
        'requirements': {},
        'status': 'complete',
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    data2 = {}
    await client.request('POST', f'/datasets/{data["dataset_id"]}/task_actions/bulk_hard_reset', data2)

    ret = await client.request('GET', f'/tasks/{task_id}')
    assert ret['status'] == states.TASK_STATUS_START


async def test_rest_tasks_actions_bulk_hard_reset_by_job(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 0,
        'job_index': 0,
        'name': 'bar',
        'depends': [],
        'requirements': {},
        'status': 'complete',
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo2',
        'task_index': 0,
        'job_index': 1,
        'name': 'bar',
        'depends': [],
        'requirements': {},
        'status': 'complete',
    }
    ret = await client.request('POST', '/tasks', data)
    task_id2 = ret['result']

    data2 = {'jobs': ['foo1']}
    await client.request('POST', f'/datasets/{data["dataset_id"]}/task_actions/bulk_hard_reset', data2)

    ret = await client.request('GET', f'/tasks/{task_id}')
    assert ret['status'] == states.TASK_STATUS_START
    ret = await client.request('GET', f'/tasks/{task_id2}')
    assert ret['status'] == 'complete'


async def test_rest_tasks_actions_bulk_hard_reset_by_task(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 0,
        'job_index': 0,
        'name': 'bar',
        'depends': [],
        'requirements': {},
        'status': 'complete',
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo2',
        'task_index': 0,
        'job_index': 1,
        'name': 'bar',
        'depends': [],
        'requirements': {},
        'status': 'complete',
    }
    ret = await client.request('POST', '/tasks', data)
    task_id2 = ret['result']

    data2 = {'tasks': [task_id]}
    await client.request('POST', f'/datasets/{data["dataset_id"]}/task_actions/bulk_hard_reset', data2)

    ret = await client.request('GET', f'/tasks/{task_id}')
    assert ret['status'] == states.TASK_STATUS_START
    ret = await client.request('GET', f'/tasks/{task_id2}')
    assert ret['status'] == 'complete'


async def test_rest_tasks_actions_bulk_requirements(server):
    client = server(roles=['system'])

    data = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 0,
        'job_index': 0,
        'name': 'bar',
        'depends': [],
        'requirements': {},
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    data2 = {
        'dataset_id': 'foo',
        'job_id': 'foo1',
        'task_index': 1,
        'job_index': 0,
        'name': 'baz',
        'depends': [],
        'requirements': {},
    }
    ret = await client.request('POST', '/tasks', data2)
    task_id2 = ret['result']

    args = {'cpu': 2}
    await client.request('PATCH', f'/datasets/{data["dataset_id"]}/task_actions/bulk_requirements/{data["name"]}', args)

    ret = await client.request('GET', f'/tasks/{task_id}')
    assert ret['requirements']['cpu'] == 2
    ret = await client.request('GET', f'/tasks/{task_id2}')
    assert 'cpu' not in ret['requirements']


    args = {'gpu': 4}
    await client.request('PATCH', f'/datasets/{data["dataset_id"]}/task_actions/bulk_requirements/{data2["name"]}', args)

    ret = await client.request('GET', f'/tasks/{task_id}')
    assert 'gpu' not in ret['requirements']
    ret = await client.request('GET', f'/tasks/{task_id2}')
    assert ret['requirements']['gpu'] == 4

    args = {'os': ['foo', 'bar']}
    await client.request('PATCH', f'/datasets/{data["dataset_id"]}/task_actions/bulk_requirements/{data2["name"]}', args)

    ret = await client.request('GET', f'/tasks/{task_id}')
    assert 'os' not in ret['requirements']
    ret = await client.request('GET', f'/tasks/{task_id2}')
    assert ret['requirements']['os'] == args['os']

    # bad task name
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('PATCH', f'/datasets/{data["dataset_id"]}/task_actions/bulk_requirements/blah', args)
    assert exc_info.value.response.status_code == 404

    # bad req
    args = {'blah': 4}
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('PATCH', f'/datasets/{data["dataset_id"]}/task_actions/bulk_requirements/{data2["name"]}', args)
    assert exc_info.value.response.status_code == 400

    # bad req value
    args = {'memory': 'ten'}
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('PATCH', f'/datasets/{data["dataset_id"]}/task_actions/bulk_requirements/{data2["name"]}', args)
    assert exc_info.value.response.status_code == 400

    # bad req value
    args = {'gpu': 3.5}
    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        await client.request('PATCH', f'/datasets/{data["dataset_id"]}/task_actions/bulk_requirements/{data2["name"]}', args)
    assert exc_info.value.response.status_code == 400


async def test_rest_tasks_files_get_empty(server):
    client = server(roles=['system'])

    dataset_id = 'foo'
    ret = await client.request('GET', f'/datasets/{dataset_id}/files')
    assert ret == {'files': []}


async def test_rest_tasks_files_post(server):
    client = server(roles=['system'])
    
    dataset_id = 'foo'
    data = {
        'dataset_id': dataset_id,
        'job_id': 'foo1',
        'job_index': 0,
        'task_index': 0,
        'name': 'bar',
        'depends': [],
        'requirements': {},
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    file_data = {
        'job_index': data['job_index'],
        'task_name': data['name'],
        'filename': 'blah',
        'movement': 'input',
    }
    await client.request('POST', f'/datasets/{dataset_id}/files', file_data)

    ret = await client.request('GET', f'/datasets/{dataset_id}/files')
    assert len(ret['files']) == 1
    assert ret['files'][0]['remote'] == file_data['filename']


async def test_rest_tasks_files_task_get(server):
    client = server(roles=['system'])
    
    dataset_id = 'foo'
    data = {
        'dataset_id': dataset_id,
        'job_id': 'foo1',
        'job_index': 0,
        'task_index': 0,
        'name': 'bar',
        'depends': [],
        'requirements': {},
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    file_data = {
        'job_index': data['job_index'],
        'task_name': data['name'],
        'filename': 'blah',
        'movement': 'input',
    }
    await client.request('POST', f'/datasets/{dataset_id}/files', file_data)

    ret = await client.request('GET', f'/datasets/{dataset_id}/files/{task_id}')
    assert len(ret['files']) == 1
    assert ret['files'][0]['remote'] == file_data['filename']


async def test_rest_tasks_files_task_post(server):
    client = server(roles=['system'])
    
    dataset_id = 'foo'
    data = {
        'dataset_id': dataset_id,
        'job_id': 'foo1',
        'job_index': 0,
        'task_index': 0,
        'name': 'bar',
        'depends': [],
        'requirements': {},
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    file_data = {
        'filename': 'blah',
        'movement': 'input',
    }
    await client.request('POST', f'/datasets/{dataset_id}/files/{task_id}', file_data)

    ret = await client.request('GET', f'/datasets/{dataset_id}/files/{task_id}')
    assert len(ret['files']) == 1
    assert ret['files'][0]['remote'] == file_data['filename']


async def test_rest_tasks_files_task_delete(server):
    client = server(roles=['system'])
    
    dataset_id = 'foo'
    data = {
        'dataset_id': dataset_id,
        'job_id': 'foo1',
        'job_index': 0,
        'task_index': 0,
        'name': 'bar',
        'depends': [],
        'requirements': {},
    }
    ret = await client.request('POST', '/tasks', data)
    task_id = ret['result']

    file_data = {
        'filename': 'blah',
        'movement': 'input',
    }
    await client.request('POST', f'/datasets/{dataset_id}/files/{task_id}', file_data)
    await client.request('DELETE', f'/datasets/{dataset_id}/files/{task_id}')

    ret = await client.request('GET', f'/datasets/{dataset_id}/files/{task_id}')
    assert len(ret['files']) == 0
