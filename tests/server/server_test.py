"""
Test Server
"""

from __future__ import absolute_import, division, print_function


import logging
logger = logging.getLogger('iceprod_server_test')

import os
import glob

from unittest.mock import AsyncMock, MagicMock

import pytest
import iceprod.server.server
from iceprod.server.server import Server, roll_files


@pytest.fixture(autouse=True)
def log_level(monkeypatch):
    m = MagicMock()
    monkeypatch.setattr(iceprod.server.server, 'set_log_level', m)
    yield m


@pytest.fixture
def config(monkeypatch):
    m = MagicMock()
    m.return_value = {'logging':{'level':'debug'}}
    monkeypatch.setattr(iceprod.server.server, 'IceProdConfig', m)
    yield m


@pytest.fixture
def queue(monkeypatch):
    m = MagicMock()
    monkeypatch.setattr(iceprod.server.server, 'Queue', m)
    yield m


def test_01_init(config, queue):
    s = Server()
    assert config.called
    assert queue.called


async def test_10_run(config, queue):
    queue.return_value.run = AsyncMock()

    s = Server()
    await s.run()
    assert queue.return_value.run.called == True


def test_90_roll_files(tmp_path):
    filename = str(tmp_path / 'file')
    fd = open(filename, 'ba+')
    fd.write(b'foo')
    fd = roll_files(fd, filename)
    assert os.path.exists(filename)

    files = glob.glob(f'{filename}.*')
    assert len(files) == 1

    fd.write(b'bar')
    fd.close()

    with open(files[0], 'br') as f:
        assert f.read() == b'foo'

