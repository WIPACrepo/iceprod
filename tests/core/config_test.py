from iceprod.core.config import Dataset
import pytest
from rest_tools.client import RestClient


def test_dataset_dataclasses():
    with pytest.raises(Exception):
        Dataset()


async def test_load_config(requests_mock):
    dataset_id = 'did123'
    dataset_data = {
        'dataset': 123,
        'dataset_id': 'did123',
        'group': 'g123',
        'username': 'u123',
    }
    requests_mock.get(f'http://test.iceprod/dataset/{dataset_id}', json=dataset_data)
    config_data = {
        'my': 'config'
    }
    requests_mock.get(f'http://test.iceprod/config/{dataset_id}', json=config_data)

    r = RestClient('http://test.iceprod')
    d = await Dataset.load_from_api(dataset_id, r)

    
    assert d.dataset_id == dataset_id
    assert d.dataset_num == dataset_data['dataset']
    assert d.group == dataset_data['group']
    assert d.user == dataset_data['username']
    assert config_data == d.config


async def test_defaults():
    dataset_data = {
        'dataset': 123,
        'dataset_id': 'did123',
        'group': 'g123',
        'username': 'u123',
    }
    config_data = {}
    d = Dataset(dataset_data['dataset_id'], dataset_data['dataset'], dataset_data['group'], dataset_data['username'], config_data)
    d.fill_defaults()
    assert d.config['version'] == 4


async def test_validate_error():
    dataset_data = {
        'dataset': 123,
        'dataset_id': 'did123',
        'group': 'g123',
        'username': 'u123',
    }
    config_data = {
        'my': 'config'
    }
    d = Dataset(dataset_data['dataset_id'], dataset_data['dataset'], dataset_data['group'], dataset_data['username'], config_data)
    with pytest.raises(Exception):
        d.validate()


async def test_validate_valid():
    dataset_data = {
        'dataset': 123,
        'dataset_id': 'did123',
        'group': 'g123',
        'username': 'u123',
    }
    config_data = {
        'tasks': [{
            'name': 'first',
            'trays': [{
                'modules': [{}]
            }]
        }]
    }
    d = Dataset(dataset_data['dataset_id'], dataset_data['dataset'], dataset_data['group'], dataset_data['username'], config_data)
    d.fill_defaults()
    d.validate()
