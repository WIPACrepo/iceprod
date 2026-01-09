import os
import pymongo
from iceprod.common.mongo import Mongo


async def test_mongo_init(mongo_url, mongo_clear):
    url = os.environ['DB_URL']
    m = Mongo(url=url)
    await m.ping()

    db_name = url.rsplit('/',1)[1]
    assert m.client[db_name] == m.db
    assert m.client['bar'] == m['bar']
    await m.close()

async def test_mongo_indexes(mongo_url, mongo_clear):
    url = os.environ['DB_URL']
    m = Mongo(url=url)

    indexes = {
        'bar': {
            'uuid_index': {
                'keys': [
                    ('uuid', pymongo.DESCENDING),
                ],
                'unique': True,
            }
        }
    }
    await m.create_indexes(indexes=indexes)
