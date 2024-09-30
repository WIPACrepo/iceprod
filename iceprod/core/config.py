import asyncio
from dataclasses import dataclass
import importlib.resources
import json
import logging
try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

import jsonschema
from rest_tools.client import RestClient


CONFIG_SCHEMA = json.loads((importlib.resources.files('iceprod.core')/'data'/'dataset.schema.json').read_text())

DATA_DEFAULTS = {key: value.get('default', None) for key,value in CONFIG_SCHEMA['$defs']['data']['items']['properties'].items()}


@dataclass
class Dataset:
    """IceProd Dataset config and basic attributes"""
    dataset_id: str
    dataset_num: int
    jobs_submitted: int
    tasks_submitted: int
    tasks_per_job: int
    status: str
    priority: float
    group: str
    user: str
    debug: bool
    config: dict

    @classmethod
    async def load_from_api(cls, dataset_id: str, rest_client: RestClient) -> Self:
        dataset = await rest_client.request('GET', f'/datasets/{dataset_id}')
        config = await rest_client.request('GET', f'/config/{dataset_id}')
        return cls(
            dataset_id=dataset_id,
            dataset_num=dataset['dataset'],
            jobs_submitted=dataset['jobs_submitted'],
            tasks_submitted=dataset['tasks_submitted'],
            tasks_per_job=dataset['tasks_per_job'],
            status=dataset['status'],
            priority=dataset['priority'],
            group=dataset['group'],
            user=dataset['username'],
            debug=dataset['debug'],
            config=config,
        )

    def fill_defaults(self):
        def _load_ref(schema_value):
            if '$ref' in list(schema_value.keys()):
                # load from ref
                parts = schema_value['$ref'].split('/')[1:]
                schema_value = CONFIG_SCHEMA
                while parts:
                    schema_value = schema_value.get(parts.pop(0), {})
                logging.debug('loading from ref: %r', schema_value)
            return schema_value

        def _fill_dict(user, schema):
            for prop in schema['properties']:
                schema_value = _load_ref(schema['properties'][prop])
                v = schema_value.get('default', None)
                if prop not in user and v is not None:
                    user[prop] = v
            for k in user:
                schema_value = _load_ref(schema['properties'].get(k, {}))
                logging.debug('filling defaults for %s: %r', k, schema_value)
                try:
                    t = schema_value.get('type', 'str')
                    logging.debug('user[k] type == %r, schema_value[type] == %r', type(user[k]), t)
                    if isinstance(user[k], dict) and t == 'object':
                        _fill_dict(user[k], schema_value)
                    elif isinstance(user[k], list) and t == 'array':
                        _fill_list(user[k], schema_value)
                except KeyError:
                    logging.warning('error processing key %r with schema %r', k, schema_value)
                    raise

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
    task_files: list | None = None
    instance_id: str | None = None

    @classmethod
    async def load_from_api(cls, dataset_id: str, task_id: str, rest_client: RestClient) -> Self:
        d, task = await asyncio.gather(
            Dataset.load_from_api(dataset_id, rest_client),
            rest_client.request('GET', f'/datasets/{dataset_id}/tasks/{task_id}')
        )
        job = await rest_client.request('GET', f'/datasets/{dataset_id}/jobs/{task["job_id"]}')
        j = Job(d, task['job_id'], job['job_index'], job['status'])
        return cls(
            dataset=d,
            job=j,
            task_id=task['task_id'],
            task_index=task['task_index'],
            name=task['name'],
            depends=task['depends'],
            requirements=task['requirements'],
            status=task['status'],
            site=task['site'],
            stats={},
            task_files=[],
            instance_id=task.get('instance_id', '')
        )

    async def load_stats_from_api(self, rest_client: RestClient):
        ret = await rest_client.request('GET', f'/datasets/{self.dataset.dataset_id}/tasks/{self.task_id}/task_stats', {'last': 'true'})
        if not ret:
            raise Exception('No stats to load!')
        # get first (only) result in ret
        self.stats = next(iter(ret.values()))

    async def load_task_files_from_api(self, rest_client: RestClient):
        ret = await rest_client.request('GET', f'/datasets/{self.dataset.dataset_id}/files/{self.task_id}', {})
        data = []
        for r in ret['files']:
            d = DATA_DEFAULTS.copy()
            d.update(r)
            data.append(d)
        self.task_files = data

    def get_task_config(self):
        return self.dataset.config['tasks'][self.task_index]
