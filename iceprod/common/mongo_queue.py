from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
import uuid

from pymongo import AsyncMongoClient, ReturnDocument


class AsyncMongoQueue:
    """
    A basic queue system using MongoDB.

    Will retry after a timeout, and can rank messages by priority (lower is better).
    """
    def __init__(self, *, uri: str, db_name: str, collection_name: str, worker_id: str | None = None):
        self.client = AsyncMongoClient(uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        self.worker_id = worker_id or str(uuid.uuid4())

    async def init_indexes(self):
        """Initializes indexes for performance and priority fetching."""
        await self.collection.create_index([
            ("status", 1), 
            ("priority", -1), 
            ("created_at", 1)
        ])

    async def push(self, payload: dict, priority: int = 0):
        """Adds a new message to the queue."""
        message = {
            "payload": payload,
            "status": "queued",
            "priority": priority,
            "created_at": datetime.now(timezone.utc),
            "locked_at": None,
            "worker_id": None
        }
        result = await self.collection.insert_one(message)
        return result.inserted_id

    async def pop(self, timeout_seconds: int = 300):
        """Atomically grabs the next available message (or an abandoned timed-out one)."""
        timeout_cutoff = datetime.now(timezone.utc) - timedelta(seconds=timeout_seconds)

        query = {
            "$or": [
                {"status": "queued"},
                {"status": "processing", "locked_at": {"$lt": timeout_cutoff}}
            ]
        }
        
        update = {
            "$set": {
                "status": "processing",
                "locked_at": datetime.now(timezone.utc),
                "worker_id": self.worker_id
            }
        }
        
        # Atomically find and update
        return await self.collection.find_one_and_update(
            query,
            update,
            sort=[("priority", -1), ("created_at", 1)],
            return_document=ReturnDocument.AFTER
        )

    async def complete(self, message_id):
        """Acknowledges and removes the message upon successful processing."""
        await self.collection.delete_one({"_id": message_id})

    async def fail(self, message_id):
        """Re-releases the message back to 'queued' state."""
        await self.collection.update_one(
            {"_id": message_id},
            {"$set": {"status": "queued", "locked_at": None, "worker_id": None}}
        )

    @asynccontextmanager
    async def process_next(self, timeout_seconds=300):
        """
        Context manager that pops a message and auto-handles completion/failure.
        """
        message = await self.pop(timeout_seconds=timeout_seconds)

        if not message:
            yield None
            return

        try:
            yield message['payload']
            await self.complete(message['_id'])
        except Exception:
            await self.fail(message_id=message['_id'])
            raise
