from __future__ import absolute_import, division, print_function


import os
import sys
import time
import signal
import argparse
import subprocess
import logging
import importlib
import json
import sqlite3
import tempfile
import shutil
import glob

import requests
import psutil

# add iceprod to PYTHONPATH
sys.path.insert(0,os.getcwd())
sys.path.insert(0,os.path.abspath(os.path.basename(__file__)))
if 'PYTHONPATH' in os.environ:
    os.environ['PYTHONPATH'] = os.getcwd()+':'+os.environ['PYTHONPATH']
else:
    os.environ['PYTHONPATH'] = os.getcwd()

logging.basicConfig()
logger = logging.getLogger('integration_tests')

from iceprod.core.jsonRPCclient import JSONRPC


parser = argparse.ArgumentParser()
parser.add_argument('datasets', type=str, nargs='?', default='*')
args = parser.parse_args()

curdir = os.getcwd()
tmpdir = tempfile.mkdtemp(dir=curdir)
os.chdir(tmpdir)
os.environ['I3PROD'] = tmpdir
def cleanup():
    procs = psutil.Process().children()
    for p in procs:
        p.terminate()
    gone, alive = psutil.wait_procs(procs, timeout=1)
    for p in alive:
        p.kill()

    try:
        subprocess.call(['condor_rm','-all'])
    except Exception:
        pass
    os.chdir(curdir)
    shutil.rmtree(tmpdir)

# handle any signals
def handler1(signum, frame):
    logger.warn('Signal handler called with signal %s' % signum)
    logger.warn('Exiting...')
    cleanup()
    sys.exit(1)
signal.signal(signal.SIGQUIT, handler1)
signal.signal(signal.SIGINT, handler1)

# start testing
logger.warn('starting...')

# server config
port = 37284
cfg = {
    "queue":{
        "a":{
            "type":"condor",
            "tasks_on_queue":[10,10],
            "pilots_on_queue":[10,10],
        },
        "software_dir":curdir,
    },
    "system":{
        "ssl":False,
    },
    "webserver":{
        "port":port,
        "tornado_port":port+1,
    },
}
if not os.path.exists('etc'):
    os.mkdir('etc')
with open('etc/iceprod_config.json','w') as f:
    json.dump(cfg,f)

# start iceprod server instance
iceprod_server = subprocess.Popen([os.path.join(curdir,'bin/iceprod_server.py'),'-n'])
time.sleep(5)
if iceprod_server.poll() is not None:
    cleanup()
    raise Exception('server died unexpectedly')

try:
    # add passkey
    with sqlite3.connect('db') as conn:
        sql = 'insert into passkey (passkey_id,auth_key,expire,user_id) values '
        sql += '("blah","passkey","3000-01-01T00:00:00","")'
        conn.execute(sql)

    client = JSONRPC('http://localhost:%d/jsonrpc'%port,passkey='passkey')
    def submit_dataset(cfg):
        try:
            return client.submit_dataset(cfg, njobs=10)
        except Exception:
            raise Exception('JSONRPC failure')
    def wait_for_dataset(dataset_id):
        logger.info('waiting on dataset %s',dataset_id)
        while True:
            try:
                tasks = client.public_get_number_of_tasks_in_each_state(dataset_id)
            except Exception:
                raise Exception('JSONRPC failure')
            if tasks['complete'] == 10:
                return
            if tasks['failed'] | tasks['suspended'] > 1:
                raise Exception('dataset failed')
            time.sleep(60)
            print('.',end='')

    # submit datasets
    dataset_ids = []
    for dataset in glob.glob(args.datasets):
        if dataset.startswith('_') or not dataset.endswith('.py'):
            continue
        m = importlib.import_module(dataset)
        dataset_ids.append(submit_dataset(m.config))

    # wait for successful completion of datasets
    for d in dataset_ids:
        wait_for_dataset(d)
finally:
    cleanup()

logger.warn('success!')
