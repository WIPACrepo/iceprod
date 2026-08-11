import asyncio

from ..core.logger import stderr_logger
from .server import Server

stderr_logger()


async def main():
    s = Server()
    await s.start()
    try:
        await asyncio.Event().wait()
    finally:
        await s.stop()


asyncio.run(main())
