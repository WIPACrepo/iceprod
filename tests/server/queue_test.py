import asyncio
from unittest.mock import MagicMock, AsyncMock

import iceprod.server.config
import iceprod.server.queue


def test_queue_init():
    override = ['queue.type=test']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    iceprod.server.queue.Queue(cfg=cfg)


async def test_queue_check_proxy():
    proxy_mock = MagicMock(iceprod.server.queue.SiteGlobusProxy)

    override = ['queue.type=test']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    q = iceprod.server.queue.Queue(cfg=cfg)
    q.proxy = proxy_mock

    t = asyncio.create_task(q.check_proxy())
    await asyncio.sleep(0)
    t.cancel()
    await asyncio.sleep(0)

    assert proxy_mock.update_proxy.called
    assert cfg['queue']['x509proxy'] == proxy_mock.get_proxy.return_value


async def test_queue_check_proxy_duration():
    proxy_mock = MagicMock(iceprod.server.queue.SiteGlobusProxy)

    override = ['queue.type=test']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    q = iceprod.server.queue.Queue(cfg=cfg)
    q.proxy = proxy_mock

    t = asyncio.create_task(q.check_proxy(duration=10*3600))
    await asyncio.sleep(0)
    t.cancel()
    await asyncio.sleep(0)

    assert proxy_mock.set_duration.called
    assert proxy_mock.set_duration.call_args == ((10,),)
    assert proxy_mock.update_proxy.called
    assert cfg['queue']['x509proxy'] == proxy_mock.get_proxy.return_value


async def test_queue_run(monkeypatch):
    proxy_mock = MagicMock(iceprod.server.queue.SiteGlobusProxy)
    monkeypatch.setattr(iceprod.server.queue, 'SiteGlobusProxy', proxy_mock)

    override = ['queue.type=test']
    cfg = iceprod.server.config.IceProdConfig(save=False, override=override)

    q = iceprod.server.queue.Queue(cfg=cfg)

    async def fn():
        await asyncio.sleep(0)
    q.grid.run = AsyncMock(side_effect=fn)

    await q.run()

    assert q.grid.run.called
    assert proxy_mock.return_value.update_proxy.called
    assert cfg['queue']['x509proxy'] == proxy_mock.return_value.get_proxy.return_value
