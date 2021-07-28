#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import os
import sys
from collections import deque

import yamlconf
from loguru import logger
from stasis_client.client import StasisClient

from monitor.Monitor import Monitor

logger.remove()
fmt = "<level>{level: <8}</level> | <g>{time:YYYY-MM-DD hh:mm:ss}</g> | <m>{thread.name: <10}</m> | " \
      "<c>{file: <20} [line:{line: ^3}] {function: <20}</c> | {message}"
logger.add(sys.stderr, format=fmt, level="INFO")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('-c', '--config', type=str, default='appconfig.yml',
                        help='path to the yaml configuration file to use (default: ./appconfig.yml)')
    parser.add_argument('-t', '--test', action='store_true',
                        help='run in test mode, no data will be converted or sent to aws. This '
                             'overrides the -c option to use \'appconfig-test.yml\'')
    args = parser.parse_args()

    if args.config:
        configFile = args.config
    else:
        configFile = 'appconfig.yml'

    if args.test:
        configFile = 'appconfig-test.yml'
        logger.warning('\nRunning in TEST mode !!!\n')
        token = os.getenv("TEST_STASIS_API_TOKEN")
    else:
        token = os.getenv("PROD_STASIS_API_TOKEN")

    with open(configFile, 'r') as stream:
        config = yamlconf.load(stream)

    stasis_cli = StasisClient(config['stasis']['url'], token)

    conv_q = deque([])
    aws_q = deque([])

    Monitor(config, stasis_cli, conv_q, aws_q, test=args.test).run()
