import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, asdict, KW_ONLY
from datetime import datetime, timedelta, timezone
import logging
from typing import Any, AsyncIterator, Literal
import uuid

from pymongo import ReturnDocument, ASCENDING, DESCENDING

from .mongo import Mongo, CollectionIndexes


type Payload = dict[str, Any]


class FailMessage(Exception):
    pass


@dataclass
class Message:
    """Mongo Queue Message"""
    payload: Payload
    _: KW_ONLY
    uuid: str
    status: Literal['queued', 'processing', 'error', 'complete']
    priority: int
    created_at: datetime
    attempts: int = 0
    locked_at: None | datetime = None
    worker_id: None | str = None
    error_message: None | str = None


class AsyncMongoQueue:
    """
    A basic queue system using MongoDB.

    Will retry after a timeout, and can rank messages by priority (lower is better).

    Args:
        url: mongo url
        collection_name: mongo collection
        worker_id: unique id for consumer / worker
        deadline: number of days messages should remain on the queue in any state
        extra_indexes: payload indexes to apply
        **mongo_args: any extra args to Mongo, such as write concern or timeout
    """
    def __init__(
        self,
        *,
        url: str,
        collection_name: str,
        worker_id: str | None = None,
        deadline: float = 1.0,
        extra_indexes: CollectionIndexes | None = None,
        **mongo_args
    ):
        self.client = Mongo(url=url, **mongo_args)
        self.collection_name = collection_name
        self.collection = self.client.db[collection_name]
        self.worker_id = worker_id or uuid.uuid4().hex
        self.deadline = deadline
        self.extra_indexes = extra_indexes

    async def close(self):
        await self.client.close()

    async def setup(self):
        """Initializes indexes for performance and priority fetching."""
        await self.client.ping()
        indexes = {
            self.collection_name: {
                'uuid_index_1': {
                    'keys': [
                        ('uuid', ASCENDING)
                    ],
                    'unique': True,
                },
                'queue_index_1': {
                    'keys': [
                        ("status", ASCENDING),
                        ("priority", DESCENDING),
                        ("attempts", ASCENDING),
                        ("created_at", ASCENDING)
                    ],
                }
            }
        }
        if self.extra_indexes:
            indexes[self.collection_name].update(self.extra_indexes)
        await self.client.create_indexes(indexes=indexes)
        asyncio.create_task(self.clean_task())

    async def push(self, payload: Payload, priority: int = 0) -> str:
        """
        Adds a new message to the queue.

        Args:
            payload: message data
            priority: priority of the message (higher is better)

        Returns:
            Message id
        """
        message = Message(
            uuid=uuid.uuid4().hex,
            payload=payload,
            status='queued',
            priority=priority,
            created_at=datetime.now(timezone.utc),
        )
        await self.collection.insert_one(asdict(message))
        return message.uuid

    async def push_if_not_exists(self, payload: Payload, filter_payload: Payload | None = None, priority: int = 0) -> str:
        """
        Adds a new message to the queue, deduplicating on payload.

        Args:
            payload: message data
            filter_payload: different payload to filter on for deduplication (if None, uses payload)
            priority: priority of the message (higher is better)

        Returns:
            Message id
        """
        message = Message(
            uuid=uuid.uuid4().hex,
            payload=payload,
            status='queued',
            priority=priority,
            created_at=datetime.now(timezone.utc),
        )
        payload_lookup = filter_payload if filter_payload is not None else payload
        query = {
            f'payload.{name}': value for name,value in payload_lookup.items()
        }
        query['status'] = {'$in': ['queued', 'processing']}
        update = {
            '$setOnInsert': asdict(message),
        }
        ret = await self.collection.find_one_and_update(
            filter=query,
            update=update,
            projection={'_id': False},
            return_document=ReturnDocument.AFTER,
            upsert=True
        )
        if not ret:
            raise Exception('failed to push')
        return ret['uuid']

    async def get_status(self, message_id: str) -> None | str:
        """Get the status if a message exists"""
        ret = await self.collection.find_one(
            {'uuid': message_id},
            projection={'_id': False, 'status': True}
        )
        if ret:
            return ret['status']
        else:
            return None

    async def get_error(self, message_id: str) -> None | str:
        """Get the error message if a message exists"""
        ret = await self.collection.find_one(
            {'uuid': message_id},
            projection={'_id': False, 'status': True, 'error_message': True}
        )
        if ret and ret['status'] == 'error':
            return ret.get('error_message', None)
        else:
            return None

    async def get_payload(self, message_id: str) -> None | Payload:
        ret = await self.collection.find_one(
            {'uuid': message_id},
            projection={'_id': False}
        )
        if ret:
            return ret['payload']
        else:
            return None

    async def lookup_by_payload(self, payload_lookup: Payload, **extra_args) -> Message | None:
        """
        Lookup a message by payload details.

        Must use mongo `.` notation for nested keys.
        """
        query = {
            f'payload.{name}': value for name,value in payload_lookup.items()
        }
        logging.info('payload_lookup query = %r', query)
        ret = await self.collection.find_one(
            query,
            projection={'_id': False},
            **extra_args
        )
        if ret:
            ret = Message(**ret)
        return ret

    async def count(self, query: dict[str, Any] | None = None) -> int:
        """Count how many requests are in the queue."""
        if not query:
            query = {}
        ret = await self.collection.count_documents(query, maxTimeMS=1000)
        return ret

    async def pop(self, timeout_seconds: float = 300.) -> Message | None:
        """Atomically grabs the next available message (or an abandoned timed-out one)."""
        timeout_cutoff = datetime.now(timezone.utc) - timedelta(seconds=timeout_seconds)

        query = {
            '$or': [
                {'status': 'queued'},
                {'status': 'processing', 'locked_at': {'$lt': timeout_cutoff}}
            ]
        }

        update = {
            '$set': {
                'status': 'processing',
                'locked_at': datetime.now(timezone.utc),
                'worker_id': self.worker_id
            },
            '$inc': {'attempts': 1},
        }

        # Atomically find and update
        ret = await self.collection.find_one_and_update(
            query,
            update,
            sort=[('priority', DESCENDING), ('attempts', ASCENDING), ('created_at', ASCENDING)],
            projection={'_id': False},
            return_document=ReturnDocument.AFTER
        )
        if ret:
            ret = Message(**ret)
        return ret

    async def update_payload(self, message_id: str, payload: Payload):
        """Update a payload"""
        update = {
            f'payload.{name}': value for name,value in payload.items()
        }
        await self.collection.update_one(
            {'uuid': message_id},
            {'$set': update}
        )

    async def complete(self, message_id: str):
        """Acknowledges and sets the state to 'completed' upon successful processing."""
        await self.collection.update_one(
            {'uuid': message_id},
            {'$set': {'status': 'complete'}}
        )

    async def release(self, message_id: str):
        """Re-releases the message back to 'queued' state."""
        await self.collection.update_one(
            {'uuid': message_id},
            {'$set': {'status': 'queued', 'locked_at': None, 'worker_id': None}}
        )

    async def fail(self, message_id: str, error_message: str | None = None):
        """Acknowledges and sets the state to 'error' upon a failed processing."""
        data = {'status': 'error'}
        if error_message:
            data['error_message'] = error_message
        await self.collection.update_one(
            {'uuid': message_id},
            {'$set': data}
        )

    @asynccontextmanager
    async def process_next(self, timeout_seconds=300) -> AsyncIterator[Payload | None]:
        """
        Context manager that pops a message and auto-handles completion/failure.
        """
        message = await self.pop(timeout_seconds=timeout_seconds)

        if not message:
            yield None
            return

        try:
            yield message.payload
            await self.complete(message.uuid)
        except Exception as e:
            await self.fail(message_id=message.uuid, error_message=str(e))
            raise

    async def clean(self):
        """Clean out old messages"""
        await self.collection.delete_many({
            'created_at': {'$lt': datetime.now(timezone.utc) - timedelta(days=self.deadline)},
        })

    async def clean_task(self):
        """Periodically run the clean function."""
        while True:
            try:
                await self.clean()
            except Exception:
                logging.info('mongo_queue.clean failed', exc_info=True)

            # make sure to run at least 10 times more often than the deadline interval
            await asyncio.sleep(self.deadline*8640)
