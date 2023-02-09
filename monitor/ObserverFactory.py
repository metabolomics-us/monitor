#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import platform
import sys

from watchdog.observers import Observer
from watchdog.observers.api import BaseObserver
from watchdog.observers.polling import PollingObserver

logger = logging.getLogger('ObserverFactory')
logger.addHandler(logging.StreamHandler(sys.stdout))

class ObserverFactory:

    def __init__(self):
        self.platform = platform.node()
        logger.info(f'Runing on platform {self.platform}')

    def getObserver(self) -> BaseObserver:
        if self.platform == 'eclipse':
            return Observer()
        else:
            return PollingObserver()
