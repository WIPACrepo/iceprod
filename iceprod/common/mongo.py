import logging
from typing import Any

from pymongo import AsyncMongoClient
from pymongo.asynchronous.database import AsyncDatabase


type CollectionIndexes = dict[str, dict[str, Any]]
"""
Indexes for a collection.

Dict of index name : dict of keywords to create index.

Example:
    {
        "my_index": {
            "keys": [
                ("status": pymongo.ASCENDING),
                ("name": pymongo.DESCENDING)
            ]
        },
        "id_index": {
            "keys": [
                ("uuid": pymongo.ASCENDING)
            ],
            "unique": True
        }
    }
"""


class Mongo:
    def __init__(self, *, url: str, timeout: int = 60, write_concern: int = 1):
        logging_url = url.split('@')[-1] if '@' in url else url
        logging.info(f'DB: {logging_url}')
        if '/' in logging_url:
            db_url, db_name = url.rsplit('/', 1)
        else:
            db_url = url
            db_name = None

        self.client: AsyncMongoClient = AsyncMongoClient(
            db_url,
            timeoutMS=timeout*1000,
            w=write_concern,
        )
        self._db_name = db_name

    @property
    def db(self) -> AsyncDatabase:
        db_name = self._db_name
        logging.info(f'DB name: {db_name}')
        if not db_name:
            raise RuntimeError('must specify database name in __init__ url')
        return self.client[db_name]

    def __getitem__(self, name: str) -> AsyncDatabase:
        logging.info(f'DB name: {name}')
        return self.client[name]

    async def ping(self):
        await self.client.admin.command('ping')

    async def close(self):
        await self.client.close()

    async def create_indexes(self, *, db_name: str | None = None, indexes: dict[str, CollectionIndexes]):
        database: AsyncDatabase = self.client[db_name] if db_name else self.db
        for collection in indexes:
            existing = await database[collection].index_information()
            for name in indexes[collection]:
                if name not in existing:
                    logging.info('DB: creating index %s:%s', collection, name)
                    kwargs = indexes[collection][name]
                    await database[collection].create_index(name=name, **kwargs)
            for name in existing:
                if (not name.startswith('_')) and name not in indexes[collection]:
                    logging.info('DB: drop index %s:%s', collection, name)
                    await database[collection].drop_index(name)
