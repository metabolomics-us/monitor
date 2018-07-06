#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
from queue import Queue

import yamlconf

from monitor.Monitor import Monitor
from rest.stasis.StasisClient import StasisClient


def start_watching(config):
    stasis_cli = StasisClient(config['stasis']['url'])

    conv_q = Queue()
    aws_q = Queue()

    monitor = Monitor(config, stasis_cli, conv_q, aws_q)

    try:
        monitor.start()
    except Exception as ex:
        print(ex.args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('-c', '--config', type=str, default='appconfig.yml',
                        help='path to the yaml configuration file to use (default: ./appconfig.yml)')
    args = parser.parse_args()

    if args.config:
        configFile = args.config
    else:
        configFile = 'appconfig.yml'

    with open(configFile, 'r') as stream:
        config = yamlconf.load(stream)

    start_watching(config)