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
curdir = os.getcwd()
integration_dir = os.path.dirname(os.path.abspath(__file__))
iceprod_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0,curdir)
sys.path.insert(0,iceprod_dir)
sys.path.insert(0,integration_dir)
if 'PYTHONPATH' in os.environ:
    os.environ['PYTHONPATH'] = '{}:{}:{}:{}'.format(integration_dir,iceprod_dir,curdir,os.environ['PYTHONPATH'])
else:
    os.environ['PYTHONPATH'] = '{}:{}:{}'.format(integration_dir,iceprod_dir,curdir)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('integration_tests')

from iceprod.core.jsonRPCclient import JSONRPC


parser = argparse.ArgumentParser()
parser.add_argument('-p','--port', type=int, default=37284, help='iceprod port')
parser.add_argument('--pilots', action='store_true', help='submit pilot jobs')
parser.add_argument('--timeout', type=int, default=3600, help='test timeout')
parser.add_argument('datasets', action='append', nargs='?')
args = parser.parse_args()
if args.datasets and args.datasets[0] is None:
    args.datasets = []

tmpdir = tempfile.mkdtemp(dir=curdir)
os.chdir(tmpdir)
os.environ['I3PROD'] = tmpdir
def cleanup():
    procs = psutil.Process().children(recursive=True)
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
    if os.path.exists(tmpdir):
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
logger.info('starting...')

site_temp = os.path.join(tmpdir,'site_temp')
os.mkdir(site_temp)

# server config
port = args.port
cfg = {
    "modules":{
        "master_updater":False,
    },
    "logging":{
        "level":"DEBUG"
    },
    "schedule":{
        "buffer_jobs_tasks":"every 1 minutes",
    },
    "queue":{
        "a":{
            "type":"condor",
            "description":"test",
            "tasks_on_queue":[30,50,20],
            "pilots_on_queue":[30,50,20],
            "software_dir":os.environ['ICEPRODROOT'],
            "iceprod_dir":iceprod_dir
        },
        "queue_interval":30,
        "submit_pilots":args.pilots,
        "submit_dir":os.path.join(tmpdir,'submit'),
        "site_temp":site_temp,
        "debug":True,
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
start_time = time.time()
iceprod_server = subprocess.Popen([os.path.join(iceprod_dir,'bin/iceprod_server.py'),'-n'],cwd=tmpdir)
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
        desc = None
        if 'description' in cfg:
            desc = cfg['description']
        return client.submit_dataset(cfg, njobs=10, description=desc)
    def wait_for_dataset(dataset_id):
        logger.info('waiting on dataset %s',dataset_id)
        while True:
            tasks = {'complete':0,'failed':0,'suspended':0}
            tasks.update(client.public_get_number_of_tasks_in_each_state(dataset_id))
            if tasks['complete'] == sum(tasks.values()) and tasks['complete'] > 10:
                return
            if tasks['failed'] | tasks['suspended'] > 1:
                raise Exception('dataset failed')
            time.sleep(60)
            if time.time()-start_time > args.timeout:
                raise Exception('over timeout limit')

    # submit datasets
    dataset_ids = []
    files = glob.glob(os.path.join(os.path.dirname(os.path.abspath(__file__)),'*.json'))
    for dataset in files:
        if args.datasets and not any(x in dataset for x in args.datasets):
            continue
        logger.info('starting dataset %s', os.path.basename(dataset))
        cfg = json.load(open(dataset))
        dataset_ids.append(submit_dataset(cfg))

    # wait for successful completion of datasets
    for d in dataset_ids:
        wait_for_dataset(d)
finally:
    cleanup()

logger.info('success!')
