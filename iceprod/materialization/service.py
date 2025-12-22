import asyncio
import logging
import time
from typing import Any

from rest_tools.client import RestClient, ClientCredentialsAuth

from .config import get_config
from ..common.mongo_queue import AsyncMongoQueue
from ..core.logger import stderr_logger
from .materialize import Materialize, DATASET_CYCLE_TIMEOUT

logger = logging.getLogger('materialization_service')


class TimeoutException(Exception):
    pass


class MaterializationService:
    """Materialization service."""
    def __init__(self, message_queue: AsyncMongoQueue, rest_client: RestClient):
        self.message_queue = message_queue
        self.materialize = Materialize(rest_client)

    async def run(self, loop: bool = True):
        """
        Run loop.
        """
        while True:
            sleep_time = 0
            try:
                start_time = time.time()
                async with self.message_queue.process_next(timeout_seconds=DATASET_CYCLE_TIMEOUT+10) as data:
                    if data:
                        await self._run_once(data)
                    else:
                        logger.info('materialization service has nothing to do. sleeping')
                        sleep_time = time.time() - start_time + 60
            except TimeoutException:
                pass
            except Exception:
                logger.error('error running materialization', exc_info=True)

            if not loop:
                break

            if sleep_time:
                await asyncio.sleep(sleep_time)

    async def _run_once(self, data: dict[str, Any]):
        # run materialization
        logger.warning(f'running materialization request: {data}')
        kwargs = {}
        if 'dataset_id' in data and data['dataset_id']:
            kwargs['only_dataset'] = data['dataset_id']
        if 'num' in data and data['num']:
            kwargs['num'] = data['num']
        if 'set_status' in data and data['set_status']:
            kwargs['set_status'] = data['set_status']
        ret = await self.materialize.run_once(**kwargs)
        logger.info('ret: %r', ret)

        if not ret:
            logger.warning(f'materialization request took too long, bumping to end of queue for another run')
            raise TimeoutException()


async def main():
    config = get_config()

    message_queue = AsyncMongoQueue(
        url=config.DB_URL,
        collection_name='materialization_queue',
        extra_indexes={'dataset_id_index': {'keys': 'dataset_id', 'unique': False}},
        timeout=config.DB_TIMEOUT,
        write_concern=config.DB_WRITE_CONCERN
    )

    rest_client: RestClient
    if config.ICEPROD_API_CLIENT_ID and config.ICEPROD_API_CLIENT_SECRET:
        logging.info(f'enabling auth via {config.OPENID_URL} for aud "{config.OPENID_AUDIENCE}"')
        rest_client = ClientCredentialsAuth(
            address=config.ICEPROD_API_ADDRESS,
            token_url=config.OPENID_URL,
            client_id=config.ICEPROD_API_CLIENT_ID,
            client_secret=config.ICEPROD_API_CLIENT_SECRET,
        )
    elif config.CI_TESTING:
        rest_client = RestClient(config.ICEPROD_API_ADDRESS, timeout=1, retries=0)
    else:
        raise RuntimeError('ICEPROD_API_CLIENT_ID or ICEPROD_API_CLIENT_SECRET not specified, and CI_TESTING not enabled!')

    ms = MaterializationService(message_queue=message_queue, rest_client=rest_client)
    await ms.run()


if __name__ == '__main__':
    stderr_logger()
    asyncio.run(main())