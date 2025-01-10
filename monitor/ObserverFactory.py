#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import platform
import sys

from watchdog.observers import Observer
from watchdog.observers.api import BaseObserver
from watchdog.observers.polling import PollingObserver

logger = logging.getLogger('ObserverFactory')
if not logger.handlers:
    logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)

class ObserverFactory:

    def __init__(self):
        self.platform = platform.node()

    def getObserver(self, mode) -> BaseObserver:
        if mode == 'local':
            logger.info(f'Creating regular Observer for local monitoring')
            return Observer()
        elif mode == 'remote':
            logger.info(f'Creating PollingObserver for remote folder monitoring')
            return PollingObserver()
        else:
            logger.info(f'Invalid mode, check your configuration')
            quit()
