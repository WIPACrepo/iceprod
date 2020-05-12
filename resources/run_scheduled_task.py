#!/usr/bin/env python
import os
import sys
import argparse
import logging
import asyncio
import importlib

from rest_tools.client import RestClient

iceprod_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0,iceprod_path)

logger = logging.getLogger()

def main():
    parser = argparse.ArgumentParser(description='run a scheduled task once')
    parser.add_argument('-t', '--token', help='auth token')
    parser.add_argument('--debug', default=False, type=bool, help='debug enabled/disabled')
    parser.add_argument('--dataset_id', default=None, help='dataset_id')
    parser.add_argument('scheduled_task', help='name of the scheduled task to run')

    args = parser.parse_args()
    args = vars(args)

    logformat='%(asctime)s %(levelname)s %(name)s %(module)s:%(lineno)s - %(message)s'
    logging.basicConfig(format=logformat, level=logging.DEBUG if args['debug'] else logging.INFO)

    rpc = RestClient('https://iceprod2-api.icecube.wisc.edu', args['token'])

    mod = importlib.import_module('iceprod.server.scheduled_tasks.'+args['scheduled_task'])
    fn = getattr(mod,'run')
    kwargs = {'debug': args['debug']}
    if args['dataset_id']:
        kwargs['dataset_id'] = args['dataset_id']

    asyncio.run(fn(rpc, **kwargs))

if __name__ == '__main__':
    main()
