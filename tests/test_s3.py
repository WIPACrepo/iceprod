from math import isclose
import time
from urllib.parse import urlparse, parse_qs

from iceprod.s3 import S3


async def test_create_bucket(s3conn):
    s = S3(address='http://localhost:5000', access_key='XXXX', secret_key='XXXX', bucket='test', mock_s3=s3conn)
    await s.create_bucket()

    assert any(b['Name'] == 'test' for b in s3conn.list_buckets()['Buckets'])


async def test_s3_get(s3conn):
    s = S3(address='http://localhost:5000', access_key='XXXX', secret_key='XXXX', bucket='test', mock_s3=s3conn)
    await s.create_bucket()

    r = await s.get('foo')
    assert not r

async def test_s3_put(s3conn):
    s = S3(address='http://localhost:5000', access_key='XXXX', secret_key='XXXX', bucket='test', mock_s3=s3conn)
    await s.create_bucket()

    data = 'some data'
    await s.put('foo', data)
    r = await s.get('foo')
    assert r == data

async def test_s3_get_presigned(s3conn):
    s = S3(address='http://localhost:5000', access_key='XXXX', secret_key='XXXX', bucket='test', mock_s3=s3conn)
    await s.create_bucket()

    data = 'some data'
    await s.put('foo', data)
    exp = 1000
    r = s.get_presigned('foo', expiration=exp)
    o = urlparse(r)
    assert o.path.endswith('/foo')
    q = parse_qs(o.query)
    assert 'Expires' in q
    assert isclose(float(q['Expires'][0]), time.time()+exp, abs_tol=1.)

async def test_s3_put_presigned(s3conn):
    s = S3(address='http://localhost:5000', access_key='XXXX', secret_key='XXXX', bucket='test', mock_s3=s3conn)
    await s.create_bucket()

    exp = 1000
    r = s.put_presigned('foo', expiration=exp)
    o = urlparse(r)
    assert o.path.endswith('/foo')
    q = parse_qs(o.query)
    assert 'Expires' in q
    assert isclose(float(q['Expires'][0]), time.time()+exp, abs_tol=1.)

async def test_s3_exists(s3conn):
    s = S3(address='http://localhost:5000', access_key='XXXX', secret_key='XXXX', bucket='test', mock_s3=s3conn)
    await s.create_bucket()

    r = await s.exists('foo')
    assert r is False

    await s.put('foo', 'data')
    r = await s.exists('foo')
    assert r is True

async def test_s3_delete(s3conn):
    s = S3(address='http://localhost:5000', access_key='XXXX', secret_key='XXXX', bucket='test', mock_s3=s3conn)
    await s.create_bucket()

    await s.put('foo', 'data')
    r = await s.exists('foo')
    assert r is True

    await s.delete('foo')
    r = await s.exists('foo')
    assert r is False

async def test_s3_list(s3conn):
    s = S3(address='http://localhost:5000', access_key='XXXX', secret_key='XXXX', bucket='test', mock_s3=s3conn)
    await s.create_bucket()

    await s.put('foo', 'data')
    await s.put('bar/1', 'data')
    await s.put('bar/2', 'data2')

    r = await s.list()
    assert r == {
        'foo': 4,
        'bar': {
            '1': 4,
            '2': 5,
        }
    }

    r = await s.list(prefix='bar', recursive=False)
    assert r == {
        'bar/1': 4,
        'bar/2': 5,
    }

async def test_s3_rmtree(s3conn):
    s = S3(address='http://localhost:5000', access_key='XXXX', secret_key='XXXX', bucket='test', mock_s3=s3conn)
    await s.create_bucket()

    await s.put('foo', 'data')
    await s.put('bar/1', 'data')
    await s.put('bar/2', 'data2')

    await s.rmtree('bar')

    r = await s.list(recursive=False)
    assert r == {
        'foo': 4,
    }
