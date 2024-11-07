#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import json
import logging
import os
import platform

import watchtower
import yamlconf

from monitor.Monitor import Monitor
from monitor.QueueManager import QueueManager
from monitor.client.BackendClient import BackendClient

fmt = '%(levelname)-8s | %(asctime)s | %(threadName)10s | %(filename)-20s:(%(lineno)3s) %(funcName)-20s | %(message)s'
logging.basicConfig(format=fmt, level='INFO')
logger = logging.getLogger()

h = watchtower.CloudWatchLogHandler(
    log_group_name=f'/lcb/monitor/{platform.node()}',
    log_group_retention_days=3,
    send_interval=30)
logger.addHandler(h)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('-c', '--config', type=str, default='appconfig.yml',
                        help='path to the yaml configuration file to use (default: ./appconfig.yml)')
    parser.add_argument('-t', '--test', action='store_true',
                        help='run in test mode, no data will be converted or sent to aws. This '
                             'overrides the -c option to use \'appconfig-test.yml\'')
    parser.add_argument('--debug', action='store_true')

    args = parser.parse_args()

    if args.config:
        configFile = args.config
    else:
        configFile = 'appconfig.yml'

    stage = 'prod'
    if args.test:
        stage = 'test'
        configFile = 'appconfig-test.yml'
        logger.warning('\nRunning in TEST mode !!!\n')

    with open(configFile, 'r') as stream:
        config = yamlconf.load(stream)
        config['test'] = args.test

    if os.path.exists(config['monitor']['msconvert']):
        logger.info('Found ProteoWizard')
    else:
        logger.error(f"Can't find ProteoWizard at {config['monitor']['msconvert']}")
        exit(1)

    backend_cli = BackendClient(os.getenv(config['backend']['url_var'], "https://test-api.metabolomics.us/gostasis"),
                                os.getenv(config['backend']['api_key_var'], "s45LgmYFPv8NbzVUbcIfRQI6NWlF7W09TUUMavx5"))
    if backend_cli:
        logger.info(f'Backend client initialized. (url: {backend_cli._url})')

    if args.debug:
        logging.root.setLevel(level='DEBUG')
        logger.debug('Running in debug mode')
        logger.debug('Configuration: ' + json.dumps(config, indent=2))

        backend_cli.logger.level = 'DEBUG'

    queue_mgr = QueueManager(stage)

    Monitor(config, backend_cli, queue_mgr).run()
