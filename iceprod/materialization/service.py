import asyncio
import logging
import time
from datetime import datetime, timedelta

import pymongo

from ..server.util import nowstr, datetime2str
from .materialize import Materialize

logger = logging.getLogger('materialization_service')


class MaterializationService:
    """Materialization service."""
    def __init__(self, database, rest_client, cleanup_hours=6):
        self.db = database
        self.materialize = Materialize(rest_client)
        self.cleanup_hours = cleanup_hours

        self.start_time = time.time()
        self.last_run_time = None
        self.last_cleanup_time = None
        self.last_success_time = None

    async def run(self):
        """
        Run loop.
        """
        while True:
            ret = await self._run_once()
            if ret == 'sleep':
                logger.info('materialization service has nothing to do. sleeping')
                await asyncio.sleep(60)

    async def _run_once(self):
        materialization_id = None
        try:
            self.last_run_time = time.time()
            now = nowstr()

            # periodically cleanup
            if (not self.last_cleanup_time) or self.last_run_time - self.last_cleanup_time > 3600*self.cleanup_hours:
                clean_time = datetime2str(datetime.utcfromtimestamp(self.last_run_time)-timedelta(hours=self.cleanup_hours))
                await self.db.materialization.delete_many({'status': {'$in': ['complete', 'error']}, 'modify_timestamp': {'$lt': clean_time}})
                await self.db.materialization.update_many({'status': 'processing', 'modify_timestamp': {'$lt': clean_time}}, {'$set': {'status': 'waiting', 'modify_timestamp': now}})
                self.last_cleanup_time = time.time()

            # get next materialization from DB
            ret = await self.db.materialization.find_one_and_update(
                {'status': 'waiting'},
                {'$set': {'status': 'processing', 'modify_timestamp': now}},
                projection={'_id':False},
                sort=[('modify_timestamp', 1)],
                return_document=pymongo.ReturnDocument.AFTER,
            )
            if not ret:
                return 'sleep'

            # run materialization
            materialization_id = ret["materialization_id"]
            logger.warning(f'running materialization request {materialization_id}')
            kwargs = {}
            if 'dataset_id' in ret and ret['dataset_id']:
                kwargs['only_dataset'] = ret['dataset_id']
            if 'num' in ret and ret['num']:
                kwargs['num'] = ret['num']
            if 'set_status' in ret and ret['set_status']:
                kwargs['set_status'] = ret['set_status']
            ret = await self.materialize.run_once(**kwargs)

            if ret:
                await self.db.materialization.update_one(
                    {'materialization_id': materialization_id},
                    {'$set': {'status': 'complete'}},
                )
            else:
                logger.warning(f'materialization request {materialization_id} took too long, bumping to end of queue for another run')
                await self.db.materialization.update_one(
                    {'materialization_id': materialization_id},
                    {'$set': {'modify_timestamp': nowstr()}},
                )
            self.last_success_time = time.time()
        except Exception:
            logger.error('error running materialization', exc_info=True)
            if materialization_id:
                await self.db.materialization.update_one(
                    {'materialization_id': materialization_id},
                    {'$set': {'status': 'error'}},
                )
