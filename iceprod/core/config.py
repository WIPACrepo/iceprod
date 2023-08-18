import asyncio
from dataclasses import dataclass
import importlib.resources
import json
try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

import jsonschema
from rest_tools.client import RestClient


CONFIG_SCHEMA = json.loads((importlib.resources.files('iceprod.core')/'data'/'dataset.schema.json').read_text())


@dataclass
class Dataset:
    """IceProd Dataset config and basic attributes"""
    dataset_id: str
    dataset_num: int
    group: str
    user: str
    config: dict

    @classmethod
    async def load_from_api(cls, dataset_id: str, rest_client: RestClient) -> Self:
        dataset = await rest_client.request('GET', f'/datasets/{dataset_id}')
        config = await rest_client.request('GET', f'/config/{dataset_id}')
        return cls(dataset_id, dataset['dataset'], dataset['group'], dataset['username'], config)

    def fill_defaults(self):
        def _fill_dict(user, schema):
            for prop in schema['properties']:
                v = schema['properties'][prop].get('default', None)
                if prop not in user and v is not None:
                    user[prop] = v
            for k in user:
                schema_value = schema['properties'].get(k, {})
                if isinstance(user[k], dict) and schema_value['type'] == 'object':
                    _fill_dict(user[k], schema_value)
                elif isinstance(user[k], list) and schema_value['type'] == 'array':
                    _fill_list(user[k], schema_value)

        def _fill_list(user, schema):
            for item in user:
                if isinstance(item, dict):
                    _fill_dict(item, schema['items'])

        _fill_dict(self.config, CONFIG_SCHEMA)

    def validate(self):
        jsonschema.validate(self.config, CONFIG_SCHEMA)


@dataclass
class Job:
    """IceProd Job instance"""
    dataset: Dataset
    job_id: str
    job_index: int
    status: str


@dataclass
class Task:
    """
    IceProd Task instance, ready for running.

    Old task stats are not loaded by default, but can be loaded on request.
    """
    dataset: Dataset
    job: Job
    task_id: str
    task_index: int
    name: str
    depends: list
    requirements: dict
    status: str
    site: str
    stats: dict

    @classmethod
    async def load_from_api(cls, dataset_id: str, task_id: str, rest_client: RestClient) -> Self:
        dataset, config, task = await asyncio.gather(
            rest_client.request('GET', f'/datasets/{dataset_id}'),
            rest_client.request('GET', f'/config/{dataset_id}'),
            rest_client.request('GET', f'/datasets/{dataset_id}/tasks/{task_id}')
        )
        d = Dataset(dataset_id, dataset['dataset'], dataset['group'], dataset['username'], config)
        job = await rest_client.request('GET', f'/datasets/{dataset_id}/jobs/{task["job_id"]}')
        j = Job(d, task['job_id'], job['job_index'], job['status'])
        return cls(d, j, task['task_id'], task['task_index'], task['name'], task['depends'], task['requirements'], task['status'], task['site'], {})

    async def load_stats_from_api(self, rest_client: RestClient):
        ret = await rest_client.request('GET', f'/datasets/{self.dataset.dataset_id}/tasks/{self.task_id}/task_stats', {'last': 'true'})
        if not ret:
            raise Exception('No stats to load!')
        # get first (only) result in ret
        self.stats = next(iter(ret.values()))

    def get_task_config(self):
        return self.dataset.config['tasks'][self.task_index]
