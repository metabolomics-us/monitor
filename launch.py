#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import json
import logging
import os
import platform

import watchtower
import yamlconf
from cisclient.client import CISClient
from stasis_client.client import StasisClient

from monitor.Monitor import Monitor
from monitor.QueueManager import QueueManager

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

    stasis_cli = StasisClient(os.getenv(config['stasis']['url_var'], "https://test-api.metabolomics.us/stasis"),
                              os.getenv(config['stasis']['api_key_var'], "s45LgmYFPv8NbzVUbcIfRQI6NWlF7W09TUUMavx5"))
    if stasis_cli:
        logger.info(f'Stasis client initialized. (url: {stasis_cli._url})')

    cis_cli = CISClient(os.getenv(config['cis']['url_var'], "https://test-api.metabolomics.us/cis"),
                        os.getenv(config['cis']['api_key_var'], "s45LgmYFPv8NbzVUbcIfRQI6NWlF7W09TUUMavx5"))

    if cis_cli:
        logger.info(f'Cis client initialized. (url: {cis_cli._url})')

    if args.debug:
        logging.root.setLevel(level='DEBUG')
        logger.debug('Running in debug mode')
        logger.debug('Configuration: ' + json.dumps(config, indent=2))

        stasis_cli.logger.level = 'DEBUG'
        # cis_cli.logger.level = 'DEBUG'

    queue_mgr = QueueManager(stage)

    Monitor(config, stasis_cli, cis_cli, queue_mgr).run()
