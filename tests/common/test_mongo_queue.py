import asyncio
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import os
import uuid

from iceprod.common.mongo_queue import AsyncMongoQueue, Message

async def test_message():
    now = datetime.fromisoformat('2025-12-12T12:14:01Z')
    m = Message(
        payload={'1': 2},
        uuid=str(uuid.uuid4()),
        status='queued',
        priority=0,
        created_at=now,
    )
    data = {
        'payload': {'1': 2},
        'uuid': m.uuid,
        'status': 'queued',
        'priority': 0,
        'attempts': 0,
        'created_at': now,
    }

    m_dict = asdict(m)
    print(f'{m_dict}')
    for k in data:
        assert m_dict[k] == data[k]


async def test_queue_basics(mongo_url, mongo_clear):
    url = os.environ['DB_URL']
    queue = AsyncMongoQueue(url=url, collection_name='bar')
    await queue.setup()

    # check queue is empty
    ret = await queue.pop()
    assert not ret

    ret = await queue.count()
    assert ret == 0

    # put data on the queue
    data = {'1': 2, '3': 4}
    id_ = await queue.push(data)

    ret = await queue.count()
    assert ret == 1

    ret2 = await queue.get_status(id_)
    assert ret2 == 'queued'

    # get it from the queue
    ret = await queue.pop()
    assert ret
    assert ret.payload == data

    ret2 = await queue.get_status(id_)
    assert ret2 == 'processing'

    # check that there's only the one message
    ret2 = await queue.pop()
    assert not ret2

    # put it back
    await queue.release(ret.uuid)

    ret2 = await queue.get_status(id_)
    assert ret2 == 'queued'

    # now get it again
    ret = await queue.pop()
    assert ret
    assert ret.payload == data

    # check that it's complete
    await queue.complete(id_)
    ret = await queue.get_status(id_)
    assert ret == 'complete'

    ret = await queue.pop()
    assert ret == None

    # check that it's failed
    await queue.fail(id_, error_message='test')
    ret2 = await queue.get_status(id_)
    assert ret2 == 'error'

    ret = await queue.pop()
    assert ret == None

    ret2 = await queue.get_error(id_)
    assert ret2 == 'test'

    await queue.close()


async def test_queue_dedup(mongo_url, mongo_clear):
    url = os.environ['DB_URL']
    queue = AsyncMongoQueue(url=url, collection_name='bar')
    await queue.setup()

    id_ = await queue.push_if_not_exists({'1': 1, '2': 2})

    id2_ = await queue.push_if_not_exists({'1': 1, '2': 2})
    assert id_ == id2_

    id2_ = await queue.push_if_not_exists({'1': 1, '2': 2}, {'1': 1})
    assert id_ == id2_

    id2_ = await queue.push_if_not_exists({'2': 2})
    assert id_ == id2_

    ret = await queue.pop()
    assert ret
    assert ret.uuid == id_

    ret = await queue.pop()
    assert not ret

    await queue.close()



async def test_queue_retries(mongo_url, mongo_clear):
    url = os.environ['DB_URL']
    queue = AsyncMongoQueue(url=url, collection_name='bar')
    await queue.setup()

    id_ = await queue.push({'1': 1})

    ret = await queue.pop(timeout_seconds=.01)
    assert ret
    assert ret.uuid == id_
    
    ret = await queue.pop(timeout_seconds=.01)
    assert not ret

    await asyncio.sleep(.01)
    
    # check queue retry
    ret = await queue.pop(timeout_seconds=.01)
    assert ret
    assert ret.uuid == id_
    assert ret.attempts == 2

    await queue.close()


async def test_queue_context(mongo_url, mongo_clear):
    url = os.environ['DB_URL']
    queue = AsyncMongoQueue(url=url, collection_name='bar')
    await queue.setup()

    data = {'1': 2, '3': 4}
    id_ = await queue.push(data)

    try:
        async with queue.process_next() as ret:
            assert ret
            assert ret == data

            # check that there's only the one message
            ret2 = await queue.pop()
            assert not ret2
    except RuntimeError:
        pass

    # check that it's not poppable anymore
    ret2 = await queue.pop()
    assert not ret2

    ret2 = await queue.get_status(id_)
    assert ret2 == 'complete'

    await queue.close()


async def test_queue_context_error(mongo_url, mongo_clear):
    url = os.environ['DB_URL']
    queue = AsyncMongoQueue(url=url, collection_name='bar')
    await queue.setup()

    data = {'1': 2, '3': 4}
    id_ = await queue.push(data)

    try:
        async with queue.process_next() as ret:
            assert ret
            assert ret == data

            # check that there's only the one message
            ret2 = await queue.pop()
            assert not ret2

            # put it back
            raise RuntimeError()
    except RuntimeError:
        pass

    # check that it's not poppable anymore
    ret2 = await queue.pop()
    assert not ret2

    ret2 = await queue.get_status(id_)
    assert ret2 == 'error'

    await queue.close()


async def test_queue_extras(mongo_url, mongo_clear):
    url = os.environ['DB_URL']
    queue = AsyncMongoQueue(url=url, collection_name='bar', extra_indexes={
        'dataset_id': {
            'keys': [('dataset_id', 1)]
        }
    })
    await queue.setup()

    data = {'dataset_id': 2, '3': 4}
    id_ = await queue.push(data)

    ret = await queue.lookup_by_payload({'dataset_id': 2})
    assert ret
    assert ret.uuid == id_



async def test_queue_sort(mongo_url, mongo_clear):
    url = os.environ['DB_URL']
    queue = AsyncMongoQueue(url=url, collection_name='bar')
    await queue.setup()

    u1 = await queue.push({'1': 1})
    u2 = await queue.push({'2': 1})
    u3 = await queue.push({'3': 1})
    u4 = await queue.push({'4': 1})

    ret = await queue.pop()
    assert ret
    assert ret.uuid == u1

    ret = await queue.pop()
    assert ret
    assert ret.uuid == u2

    await queue.release(u1)

    ret = await queue.pop()
    assert ret
    assert ret.uuid == u3

    ret = await queue.pop()
    assert ret
    assert ret.uuid == u4
    await queue.release(u4)

    ret = await queue.pop()
    assert ret
    assert ret.uuid == u1
    await queue.release(u1)

    await queue.release(u1)
    await queue.release(u1)
    await queue.release(u2)

    ret = await queue.pop()
    assert ret
    assert ret.uuid == u2

    await queue.close()