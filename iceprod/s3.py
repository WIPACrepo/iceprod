import asyncio
from concurrent.futures import ThreadPoolExecutor
import io
from functools import partial
import logging

try:
    import boto3
    import botocore.client
    import botocore.exceptions
except ImportError:
    boto3 = None

logger = logging.getLogger('s3')


class S3:
    """S3 wrapper for uploading and downloading objects"""
    def __init__(self, address, access_key, secret_key, bucket='iceprod2-logs'):
        self.s3 = None
        self.bucket = bucket
        try:
            self.s3 = boto3.client(
                's3',
                'us-east-1',
                endpoint_url=address,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                config=botocore.client.Config(max_pool_connections=101)
            )
        except Exception:
            logger.warning('failed to connect to s3: %r', address, exc_info=True)
            raise
        self.executor = ThreadPoolExecutor(max_workers=20)

    async def get(self, key):
        """Download object from S3"""
        ret = ''
        with io.BytesIO() as f:
            loop = asyncio.get_running_loop()
            try:
                await loop.run_in_executor(self.executor, partial(self.s3.download_fileobj, Bucket=self.bucket, Key=key, Fileobj=f))
                ret = f.getvalue()
            except botocore.exceptions.ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == '404':
                    return ''  # don't error on a 404
                raise
        return ret.decode('utf-8')

    def get_presigned(self, key, expiration=3600):
        """Make a presigned download url"""
        return self.s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': self.bucket, 'Key': key},
            ExpiresIn=expiration,
        )

    async def put(self, key, data):
        """Upload object to S3"""
        with io.BytesIO(data.encode('utf-8')) as f:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(self.executor, partial(self.s3.upload_fileobj, f, self.bucket, key))

    def put_presigned(self, key, expiration=3600):
        """Make a presigned upload url"""
        return self.s3.generate_presigned_url(
            'put_object',
            Params={'Bucket': self.bucket, 'Key': key},
            ExpiresIn=expiration,
        )

    async def exists(self, key):
        """Check existence in S3"""
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(self.executor, partial(self.s3.head_object, Bucket=self.bucket, Key=key))
        except Exception:
            return False
        return True

    async def delete(self, key):
        """Delete object in S3"""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self.executor, partial(self.s3.delete_object, Bucket=self.bucket, Key=key))


class FakeS3(S3):
    def __init__(self, s3override):
        self.s3 = s3override
        self.bucket = 'iceprod2-logs'
        self.executor = ThreadPoolExecutor(max_workers=20)
