"""
Test script for config
"""

import logging
logger = logging.getLogger('config_test')

import os, sys, time
import shutil
import tempfile
import random
import unittest
import json

import pytest

import iceprod.server.config


@pytest.fixture
def prod_path(monkeypatch, tmp_path):
    (tmp_path / 'etc').mkdir(mode=0o700)
    monkeypatch.setenv('I3PROD', str(tmp_path))
    yield tmp_path


def test_01_IceProdConfig(prod_path):
    """Test config.IceProdConfig()"""
    cfg = iceprod.server.config.IceProdConfig(defaults=False, validate=False)
    cfg.save()
    if not os.path.exists(cfg.filename):
        raise Exception('did not save configfile')

    with open(cfg.filename) as f:
        assert f.read() == '{}'

    cfg['testing'] = [1,2,3]
    assert cfg['testing'] == [1,2,3]

    expected = dict(cfg)
    with open(cfg.filename) as f:
        actual = json.load(f)
    assert actual == expected

    cfg.load()
    assert cfg['testing'] == [1,2,3]

    del cfg['testing']
    assert 'testing' not in cfg

    expected = {}
    with open(cfg.filename) as f:
        actual = json.load(f)
    assert actual == expected


def test_02_IceProdConfig(prod_path):
    """Test config.IceProdConfig()"""
    cfg = iceprod.server.config.IceProdConfig(filename='test.json')
    assert cfg.filename == 'test.json'


def test_10_config_override(prod_path):
    vals = ['test=foo']
    cfg = iceprod.server.config.IceProdConfig(override=vals, defaults=False, validate=False)
    assert cfg['test'] == 'foo'
    del cfg['test']

    vals = ['test.test2.test3=123', 'test.test4=456.5', 'test2={"foo":123}','test3=true']
    cfg = iceprod.server.config.IceProdConfig(override=vals, defaults=False, validate=False)
    assert cfg['test']['test2']['test3'] == 123
    assert cfg['test']['test4'] == 456.5
    assert cfg['test2'] == {'foo': 123}
    assert cfg['test3'] is True

def test_20_defaults(prod_path):
    cfg = iceprod.server.config.IceProdConfig(validate=False)
    assert cfg['logging']['level'] == 'INFO'

def test_30_validate(prod_path):
    cfg = iceprod.server.config.IceProdConfig()

    cfg['logging']['level'] = 'foo'
    with pytest.raises(Exception):
        cfg.do_validate()
