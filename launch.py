#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import json
import os
import sys
from queue import Queue

import yamlconf
from cisclient.client import CISClient
from loguru import logger
from stasis_client.client import StasisClient

from monitor.Monitor import Monitor

logger.remove()
fmt = "<level>{level: <7}</level> | <g>{time:YYYY-MM-DD hh:mm:ss}</g> | <m>{thread.name: <10}</m> | " \
      "<c>{file: <20} [line:{line: ^3}] {function: <20}</c> | {message}"
logger.add(sys.stderr, format=fmt, level="INFO")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('-c', '--config', type=str, default='appconfig.yml',
                        help='path to the yaml configuration file to use (default: ./appconfig.yml)')
    parser.add_argument('-t', '--test', action='store_true',
                        help='run in test mode, no data will be converted or sent to aws. This '
                             'overrides the -c option to use \'appconfig-test.yml\'')
    parser.add_argument('--debug', action='store_true')

    args = parser.parse_args()

    if args.debug:
        logger.remove()
        logger.add(sys.stderr, format=fmt, level="DEBUG")
        logger.debug("Running in debug mode")

    if args.config:
        configFile = args.config
    else:
        configFile = 'appconfig.yml'

    if args.test:
        configFile = 'appconfig-test.yml'
        logger.warning('\nRunning in TEST mode !!!\n')

    with open(configFile, 'r') as stream:
        config = yamlconf.load(stream)
        config['test'] = args.test

        if args.debug:
            logger.debug('Configuration: ' + json.dumps(config, indent=2))

    if os.path.exists(config['monitor']['msconvert']):
        logger.info('Found ProteoWizard')
    else:
        logger.error(f"Can't find ProteoWizard at {config['monitor']['msconvert']}")
        exit(1)

    stasis_cli = StasisClient(os.getenv(config['stasis']['url_var'], "https://test-api.metabolomics.us/stasis"),
                              os.getenv(config['stasis']['api_key_var'], "9MjbJRbAtj8spCJJVTPbP3YWc4wjlW0c7AP47Pmi"))
    if stasis_cli:
        logger.info("Stasis client initialized")

    cis_cli = CISClient(os.getenv(config['cis']['url_var'], "https://test-api.metabolomics.us/cis"),
                        os.getenv(config['cis']['api_key_var'], "9MjbJRbAtj8spCJJVTPbP3YWc4wjlW0c7AP47Pmi"))

    if cis_cli:
        logger.info("Cis client initialized")

    conv_q = Queue()
    aws_q = Queue()
    sched_q = Queue()

    Monitor(config, stasis_cli, cis_cli, conv_q, aws_q, sched_q).run()

