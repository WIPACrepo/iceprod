import pymongo
from iceprod.common.mongo import Mongo


async def test_mongo_init(mongo_clear):
    m = Mongo(url='mongodb://localhost/foo')
    await m.ping()

    assert m.client['foo'] == m.db
    assert m.client['bar'] == m['bar']
    await m.close()

async def test_mongo_indexes(mongo_clear):
    m = Mongo(url='mongodb://localhost/foo')

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
