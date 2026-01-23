import asyncio
from collections.abc import Iterable
from copy import deepcopy
from dataclasses import dataclass
import importlib.resources
import json
import logging
from pathlib import Path
import re
from typing import Any
try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

from cachetools.func import ttl_cache
import jsonschema
from rest_tools.client import RestClient


class ValidationError(Exception):
    pass


class ConfigSchema:
    @ttl_cache
    @staticmethod
    def list_versions() -> list[float]:
        path = Path(str(importlib.resources.files('iceprod.core') / 'data'))
        ret = []
        for p in path.glob('dataset_v*.schema.json'):
            if ver := re.match(r'dataset_v(\d\.\d).schema.json', p.name):
                ret.append(float(ver.group(1)))
        return sorted(ret)

    @ttl_cache
    @staticmethod
    def schema(version: float = 3.1) -> dict[str, Any]:
        rounded_ver = round(version, 1)
        path = importlib.resources.files('iceprod.core') / 'data' / f'dataset_v{rounded_ver}.schema.json'
        return json.loads(path.read_text())

    @ttl_cache
    @staticmethod
    def data_defaults(version: float = 3.1) -> dict[str, Any]:
        schema = ConfigSchema.schema(version)
        return {key: value.get('default', None) for key,value in schema['$defs']['data']['items']['properties'].items()}


class _ConfigMixin:
    config: dict

    def fill_defaults(self):
        """Fill in config defaults"""
        ver = self.config.get('version', None)
        if isinstance(ver, str):
            ver = float(ver)
        config_schema = ConfigSchema.schema(ver) if ver else ConfigSchema.schema()

        def _load_ref(schema_value):
            if '$ref' in list(schema_value.keys()):
                # load from ref
                parts = schema_value['$ref'].split('/')[1:]
                schema_value = config_schema
                while parts:
                    schema_value = schema_value.get(parts.pop(0), {})
                logging.debug('loading from ref: %r', schema_value)
            return schema_value

        def _fill_dict(user, schema):
            for prop in schema['properties']:
                schema_value = _load_ref(schema['properties'][prop])
                v = schema_value.get('default', None)
                if isinstance(v, (dict, list)):
                    v = deepcopy(v)  # make a copy to not use the same instance multiple times
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

        _fill_dict(self.config, config_schema)

    def validate(self):
        """Validate config"""
        ver = self.config['version']
        if isinstance(ver, str):
            ver = float(ver)
            self.config['version'] = ver
        try:
            jsonschema.validate(self.config, ConfigSchema.schema(ver))
        except jsonschema.ValidationError as e:
            try:
                logging.warning("raising! %r", e.path)
                path = ''.join(f'[{p!r}]' for p in e.path)
                if isinstance(e.schema, Iterable) and 'error_msg' in e.schema:
                    msg = e.schema['error_msg']
                else:
                    msg = str(e).split('\n',1)[0]
                raise ValidationError(f'Validation error in config{path}: {msg}') from e
            except AttributeError:
                raise e


@dataclass
class Config(_ConfigMixin):
    """IceProd Dataset config"""
    config: dict


@dataclass
class Dataset(_ConfigMixin):
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
    task_files: list[dict[Any, Any]] | None = None
    instance_id: str | None = None
    oauth_tokens: list[Any] | None = None

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
        data_defaults = ConfigSchema.data_defaults(self.dataset.config['version'])
        for r in ret['files']:
            d = data_defaults.copy()
            d.update(r)
            data.append(d)
        self.task_files = data

    def get_task_config(self) -> dict[Any, Any]:
        return self.dataset.config['tasks'][self.task_index]
