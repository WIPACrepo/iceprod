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
    dataset_id: str
    dataset_num: int
    group: str
    user: str
    config: dict

    @classmethod
    async def load_from_api(cls, dataset_id: str, rest_client: RestClient) -> Self:
        dataset = await rest_client.request('GET', f'/dataset/{dataset_id}')
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

        