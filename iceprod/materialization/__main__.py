import asyncio
import dataclasses
import logging

from wipac_dev_tools import from_environment_as_dataclass

from .server import Server
from ..core.logger import stderr_logger


stderr_logger()


async def main():
    s = Server()
    await s.start()
    try:
        await asyncio.Event().wait()
    finally:
        await s.stop()


asyncio.run(main())
