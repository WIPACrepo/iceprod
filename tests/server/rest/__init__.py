import os
import shutil
import tempfile
import random
import time
import subprocess
import collections
import logging
import socket
from functools import partial
from unittest.mock import MagicMock

from pymongo import MongoClient
from tornado.testing import AsyncTestCase
from rest_tools.utils import Auth
from rest_tools.server import RestServer, RestHandler

from iceprod.server.modules.rest_api import setup_rest

logger = logging.getLogger('rest_tests')

class OK(RestHandler):
    def get(self, *args):
        self.write('{}')
    def put(self, *args):
        self.write('{}')

def port():
    """Get an ephemeral port number."""
    # https://unix.stackexchange.com/a/132524
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))
    addr = s.getsockname()
    ephemeral_port = addr[1]
    s.close()
    return ephemeral_port

class RestTestCase(AsyncTestCase):
    def setUp(self, config):
        super(RestTestCase,self).setUp()
        self.test_dir = tempfile.mkdtemp(dir=os.getcwd())
        def cleanup():
            shutil.rmtree(self.test_dir)
        self.addCleanup(cleanup)

        try:
            self.port = port()
            self.mongo_port = random.randint(10000,50000)

            db_names = list(config['rest'].keys())

            if 'TEST_DATABASE_URL' in os.environ:
                self.mongo_port = int(os.environ['TEST_DATABASE_URL'].split(':')[-1])
                for d in db_names:
                    clean_db(os.environ['TEST_DATABASE_URL'], d)
            else:
                dbpath = os.path.join(self.test_dir,'db')
                os.mkdir(dbpath)
                dblog = os.path.join(dbpath,'logfile')
                m = subprocess.Popen(['mongod', '--port', str(self.mongo_port),
                                      '--dbpath', dbpath, '--smallfiles',
                                      '--quiet', '--nounixsocket',
                                      '--logpath', dblog])
                time.sleep(0.05)
                self.addCleanup(partial(time.sleep, 0.05))
                self.addCleanup(m.terminate)

            host = f'http://localhost:{self.port}'
            if 'auth' not in config:
                config['auth'] = {'url': host}
            config['auth'].update({
                'secret': 'secret'
            })
            for d in db_names:
                config['rest'][d]['database'] = {
                    'port': self.mongo_port,
                }

            routes, args = setup_rest(config, module=MagicMock())
            self.server = RestServer(**args)
            for r in routes:
                self.server.add_route(*r)
            if 'auths' not in db_names:
                self.server.add_route(r'/auths/(.*)', OK)  # approve all auths requests
            self.server.startup(port=self.port)
            self.token = Auth('secret').create_token('foo', type='user', payload={'role':'admin','username':'admin'})
            if isinstance(self.token, bytes):
                self.token = self.token.decode('utf-8')

        except Exception:
            logger.error('failed setup', exc_info=True)
            raise


def clean_db(addr, dbname):
    db = MongoClient(addr)[dbname]
    colls = db.list_collection_names()
    for c in colls:
        db.drop_collection(c)
