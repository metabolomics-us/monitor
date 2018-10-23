#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
from queue import Queue

import yamlconf

from monitor.Monitor import Monitor
from rest.stasis.StasisClient import StasisClient


def start_watching(config, test):
    stasis_cli = StasisClient(config['stasis']['url'])

    conv_q = Queue()
    aws_q = Queue()

    monitor = Monitor(config, stasis_cli, conv_q, aws_q, test=test)

    try:
        monitor.start()
    except KeyboardInterrupt:
        print('Shutting down remaining threads')


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
        print('\nRunning in TEST mode !!!\n')

    with open(configFile, 'r') as stream:
        config = yamlconf.load(stream)

    try:
        start_watching(config, args.test)
    except Exception as ex:
        print(ex.args)
