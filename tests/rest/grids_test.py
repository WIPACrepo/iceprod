

async def test_rest_grids_empty(server):
    client = server(roles=['system'])

    ret = await client.request('GET', '/grids')
    assert ret == {}


async def test_rest_grids_add(server):
    client = server(roles=['system'])

    data = {
        'host': 'foo.bar.baz',
        'queues': {'foo': 'HTCondor'},
        'version': '1.2.3',
    }
    ret = await client.request('POST', '/grids', data)
    grid_id = ret['result']

    ret = await client.request('GET', '/grids')
    assert grid_id in ret
    for k in data:
        assert k in ret[grid_id]
        assert data[k] == ret[grid_id][k]


async def test_rest_grids_details(server):
    client = server(roles=['system'])

    data = {
        'host': 'foo.bar.baz',
        'queues': {'foo': 'HTCondor'},
        'version': '1.2.3',
    }
    ret = await client.request('POST', '/grids', data)
    grid_id = ret['result']

    ret = await client.request('GET', f'/grids/{grid_id}')
    for k in data:
        assert k in ret
        assert data[k] == ret[k]

async def test_rest_grids_patch(server):
    client = server(roles=['system'])

    data = {
        'host': 'foo.bar.baz',
        'queues': {'foo': 'HTCondor'},
        'version': '1.2.3',
    }
    ret = await client.request('POST', '/grids', data)
    grid_id = ret['result']

    new_data = {
        'queues': {'foo': 'HTCondor', 'bar': 'HTCondor'},
        'version': '1.2.8',
    }
    ret = await client.request('PATCH', f'/grids/{grid_id}', new_data)
    
    ret2 = await client.request('GET', f'/grids/{grid_id}')
    assert ret == ret2
    for k in new_data:
        assert k in ret
        assert new_data[k] == ret[k]
