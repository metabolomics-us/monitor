#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse

import yamlconf

from monitor.Monitor import Monitor
from rest.dataformer.DataformerClient import DataformerClient
from rest.stasis.StasisClient import StasisClient


def start_watching(config):
    stasis_cli = StasisClient(config['stasis']['url'])
    dataform_cli = DataformerClient(
        config['dataform']['url'],
        config['dataform']['port'],
        config['monitor']['storage']
    )

    monitor = Monitor(config, stasis_cli, dataform_cli)

    try:
        monitor.start()
    except Exception as ex:
        print(ex.args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('-c', '--config', type = str, default = 'appconfig.yaml', help = 'path to the yaml configuration file to use (default: ./appconfig.yaml)')
    args = parser.parse_args()

    if args.config:
        configFile = args.config
    else:
        configFile = 'appconfig.yaml'

    with open(configFile, 'r') as stream:
        config = yamlconf.load(stream)

    start_watching(config)