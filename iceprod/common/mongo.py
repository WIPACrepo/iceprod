import logging
from typing import Any

from pymongo import AsyncMongoClient
from pymongo.asynchronous.database import AsyncDatabase


class Mongo:
    def __init__(self, *, url: str, timeout: int = 60, write_concern: int = 1):
        logging_url = url.split('@')[-1] if '@' in url else url
        logging.info(f'DB: {logging_url}')
        if '/' in logging_url:
            db_url, db_name = url.rsplit('/', 1)
        else:
            db_url = url
            db_name = None

        self.client = AsyncMongoClient(
            db_url,
            timeoutMS=timeout*1000,
            w=write_concern,
        )
        self._db_name = db_name

    @property
    def db(self, name: str | None = None) -> AsyncDatabase:
        db_name = name if name else self._db_name
        logging.info(f'DB name: {db_name}')
        if not db_name:
            raise RuntimeError('must specify database name either in __init__ url or as an argument')
        return self.client[db_name]

    async def ping(self):
        await self.client.admin.command('ping')

    async def close(self):
        await self.client.close()

    async def create_indexes(self, *, db_name: str | None = None, indexes: dict[str, dict[str, dict[str, Any]]]):
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
