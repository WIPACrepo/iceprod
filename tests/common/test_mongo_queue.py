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

    m_dict = asdict(m)
    print(f'{m_dict}')
    assert m_dict == {
        'payload': {'1': 2},
        'uuid': m.uuid,
        'status': 'queued',
        'priority': 0,
        'created_at': now,
        'locked_at': None,
        'worker_id': None,
    }


async def test_queue_basics(mongo_url, mongo_clear):
    url = os.environ['DB_URL']
    queue = AsyncMongoQueue(url=url, collection_name='bar')
    await queue.setup()

    # check queue is empty
    ret = await queue.pop()
    assert not ret

    # put data on the queue
    data = {'1': 2, '3': 4}
    id_ = await queue.push(data)

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
    await queue.fail(ret.uuid)

    ret2 = await queue.get_status(id_)
    assert ret2 == 'queued'

    # now get it again
    ret = await queue.pop()
    assert ret
    assert ret.payload == data

    await queue.complete(ret.uuid)

    # check that it's not on the queue anymore
    ret2 = await queue.get_status(id_)
    assert not ret2

    await queue.close()


async def test_queue_context(mongo_url, mongo_clear):
    url = os.environ['DB_URL']
    queue = AsyncMongoQueue(url=url, collection_name='bar')
    await queue.setup()

    data = {'1': 2, '3': 4}
    await queue.push(data)

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

    # now get it again
    async with queue.process_next() as ret:
        assert ret
        assert ret == data

        # and complete it

    # check that it's not on the queue anymore
    ret2 = await queue.pop()
    assert not ret2

    await queue.close()
