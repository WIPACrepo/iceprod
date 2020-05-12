#!/usr/bin/env python

import os
import subprocess
import argparse
import json
import glob
import logging
import asyncio

from rest_tools.client import RestClient
from iceprod.server.scheduled_tasks.buffer_jobs_tasks import run

logger = logging.getLogger()


def main():
    parser = argparse.ArgumentParser(description='clean up processing tasks not in pilots')
    parser.add_argument('-t', '--token', help='auth token')
    parser.add_argument('-d','--dataset', help='dataset id')
    parser.add_argument('-n','--num', type=int, default=20000,
                        help='max number of jobs to buffer')
    parser.add_argument('--debug', action='store_true', help='debug')
    
    args = parser.parse_args()
    args = vars(args)

    logging.basicConfig(level=logging.DEBUG if args['debug'] else logging.INFO)
    
    rpc = RestClient('https://materialization.iceprod.icecube.aq', args['token'])

    asyncio.run(run(rpc, only_dataset=args['dataset'], num=args['num'], run_once=True, debug=args['debug']))

if __name__ == '__main__':
    main()

