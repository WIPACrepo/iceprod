from contextlib import asynccontextmanager
from dataclasses import dataclass, asdict, KW_ONLY
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, Literal
import uuid

from pymongo import ReturnDocument, ASCENDING, DESCENDING

from .mongo import Mongo


type Payload = str | dict[str, Any]


@dataclass
class Message:
    payload: Payload
    _: KW_ONLY
    uuid: str
    status: Literal['queued', 'processing']
    priority: int
    created_at: datetime
    locked_at: None | datetime = None
    worker_id: None | str = None


class AsyncMongoQueue:
    """
    A basic queue system using MongoDB.

    Will retry after a timeout, and can rank messages by priority (lower is better).
    """
    def __init__(self, *, url: str, collection_name: str, worker_id: str | None = None, **mongo_args):
        self.client = Mongo(url=url, **mongo_args)
        self.collection_name = collection_name
        self.collection = self.client.db[collection_name]
        self.worker_id = worker_id or uuid.uuid4().hex

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
                        ("created_at", ASCENDING)
                    ],
                }
            }
        }
        await self.client.create_indexes(indexes=indexes)

    async def push(self, payload: Payload, priority: int = 0) -> str:
        """
        Adds a new message to the queue.

        Args:
            payload: message data

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

    async def pop(self, timeout_seconds: int = 300) -> Message | None:
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
            }
        }

        # Atomically find and update
        ret = await self.collection.find_one_and_update(
            query,
            update,
            sort=[('priority', -1), ('created_at', 1)],
            projection={'_id': False},
            return_document=ReturnDocument.AFTER
        )
        if ret:
            ret = Message(**ret)
        return ret

    async def complete(self, message_id: str):
        """Acknowledges and removes the message upon successful processing."""
        await self.collection.delete_one({'uuid': message_id})

    async def fail(self, message_id: str):
        """Re-releases the message back to 'queued' state."""
        await self.collection.update_one(
            {'uuid': message_id},
            {'$set': {'status': 'queued', 'locked_at': None, 'worker_id': None}}
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
        except Exception:
            await self.fail(message_id=message.uuid)
            raise
