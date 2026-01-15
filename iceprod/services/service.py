import asyncio
import logging
from pathlib import Path
import pkgutil
import time

from rest_tools.client import RestClient, ClientCredentialsAuth

from iceprod.common.mongo_queue import AsyncMongoQueue
from iceprod.services.base import BaseAction, TimeoutException

from .config import get_config
from ..core.logger import stderr_logger

logger = logging.getLogger('service')


async def main() -> None:
    config = get_config()

    message_queue = AsyncMongoQueue(
        url=config.DB_URL,
        collection_name='services_queue',
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

    cred_client: RestClient
    if config.ICEPROD_CRED_CLIENT_ID and config.ICEPROD_CRED_CLIENT_SECRET:
        logging.info(f'enabling auth via {config.OPENID_URL} for aud "{config.OPENID_AUDIENCE}"')
        cred_client = ClientCredentialsAuth(
            address=config.ICEPROD_CRED_ADDRESS,
            token_url=config.OPENID_URL,
            client_id=config.ICEPROD_CRED_CLIENT_ID,
            client_secret=config.ICEPROD_CRED_CLIENT_SECRET,
        )
    elif config.CI_TESTING:
        cred_client = RestClient(config.ICEPROD_CRED_ADDRESS, timeout=1, retries=0)
    else:
        raise RuntimeError('ICEPROD_CRED_CLIENT_ID or ICEPROD_CRED_CLIENT_SECRET not specified, and CI_TESTING not enabled!')

    # find all actions
    actions = {}
    plugin_path = str(Path(__file__).parent / 'actions')
    for _, name, _ in pkgutil.iter_modules([plugin_path]):
        action_class = pkgutil.resolve_name(f'iceprod.services.actions.{name}:Action')
        action : BaseAction = action_class(queue=message_queue, logger=logger, api_client=rest_client, cred_client=cred_client)
        actions[name] = action

    # process messages
    while True:
        sleep_time = 0.
        try:
            start_time = time.time()

            data = await message_queue.pop(timeout_seconds=config.SERVICE_TIMEOUT+10)

            if data:
                try:
                    type_ = data.payload.pop('type')
                    logging.info('running service for type %s', type_)
                    fut = actions[type_].run(data)
                    await asyncio.wait_for(fut, timeout=config.SERVICE_TIMEOUT)
                except TimeoutException:
                    await message_queue.release(data.uuid)
                except Exception as e:
                    await message_queue.fail(data.uuid, error_message=str(e))
                    raise
                else:
                    await message_queue.complete(data.uuid)
            else:
                logging.info('service has nothing to do. sleeping')
                sleep_time = config.SERVICE_SLEEP_SECS - (time.time() - start_time)
        except Exception:
            logging.error('error running services', exc_info=True)

        if config.CI_TESTING:
            break  # only process one message during testing

        if sleep_time > 0:
            await asyncio.sleep(sleep_time)

    await message_queue.close()

if __name__ == '__main__':
    stderr_logger()
    asyncio.run(main())
