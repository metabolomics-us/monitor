#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import sys

from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver

logger = logging.getLogger('test_ObserverFactory')
logger.addHandler(logging.StreamHandler(sys.stdout))

def test_get_observer_eclipse(observer_factory):
    observer_factory.platform = 'eclipse'
    obs = observer_factory.getObserver()

    assert obs != None
    logger.info(type(obs))

    assert isinstance(obs, Observer)
    assert not isinstance(obs, PollingObserver)

def test_get_observer_non_eclipse(observer_factory):
    obs = observer_factory.getObserver()

    assert obs != None
    logger.info(type(obs))

    assert isinstance(obs, PollingObserver)
    assert not isinstance(obs, Observer)

