"""
Test script for config
"""

import os
import json

import pytest

import iceprod.server.config


def test_01_IceProdConfig():
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


def test_02_filename(i3prod_path):
    """Test config.IceProdConfig()"""
    name = str(i3prod_path / 'test.json')
    cfg = iceprod.server.config.IceProdConfig(filename=name)
    assert cfg.filename == name


def test_10_config_override():
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

    vals = ['queue.resources.cpu=1', 'queue.exclusive=true']
    cfg = iceprod.server.config.IceProdConfig(override=vals, defaults=False, validate=False)


def test_20_defaults():
    cfg = iceprod.server.config.IceProdConfig(validate=False)
    assert cfg['logging']['level'] == 'INFO'


def test_30_validate():
    cfg = iceprod.server.config.IceProdConfig()

    cfg['logging']['level'] = 'foo'
    with pytest.raises(Exception):
        cfg.do_validate()


def test_40_save():
    cfg = iceprod.server.config.IceProdConfig(validate=False, save=False)
    assert os.path.exists(cfg.filename) is False
