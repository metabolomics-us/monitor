#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import os
import platform
import time
from threading import Thread

import watchtower
from cisclient.client import CISClient
from stasis_client.client import StasisClient

from monitor.ObserverFactory import ObserverFactory
from monitor.QueueManager import QueueManager
from monitor.RawDataEventHandler import RawDataEventHandler
from monitor.workers.BucketWorker import BucketWorker
from monitor.workers.PwizWorker import PwizWorker

THREAD_TIMEOUT = 5

logger = logging.getLogger('Monitor')
h = watchtower.CloudWatchLogHandler(
    log_group_name=f'/lcb/monitor/{platform.node()}',
    log_group_retention_days=3,
    send_interval=10)
logger.addHandler(h)


class Monitor(Thread):
    """
    A file monitoring class
    """

    def __init__(self, config, stasis_cli: StasisClient, cis_cli: CISClient,
                 queue_mgr: QueueManager, daemon=False):
        """

        Args:
            config:
                An object containing config settings
            stasis_cli: StasisClient
                A stasis client instance
            cis_cli: CisClient
                A cis client instance
            queue_mgr: QueueManager
                A QueueManager object that handles setting up queues and sending/receiving messages
            daemon:
                Run the worker as daemon. (Optional. Default: True)
        """
        super().__init__(name=platform.node(), daemon=daemon)
        self.config = config
        self.stasis_cli = stasis_cli
        self.cis_cli = cis_cli
        self.running = True
        self.test = config['test']
        self.threads = []
        self.queue_mgr = queue_mgr

    def run(self):
        """Starts the monitoring of the selected folders"""
        observer = ObserverFactory().getObserver()

        try:
            # Setup the aws uploader worker
            aws_worker = BucketWorker(self,
                                      self.stasis_cli,
                                      self.config,
                                      self.queue_mgr)

            # scheduler = Scheduler(self,
            #                       self.stasis_cli,
            #                       self.cis_cli,
            #                       self.config,
            #                       self.queue_mgr)

            # threads = [aws_worker, scheduler]
            threads = [aws_worker]

            # Setup the pwiz workers
            [threads.append(
                PwizWorker(self,
                           self.stasis_cli,
                           self.queue_mgr,
                           self.config,
                           name=f'Converter{x}')
            ) for x in range(0, 5)]

            logger.info(f'Starting threads')
            for t in threads:
                t.start()

            event_handler = RawDataEventHandler(
                self.stasis_cli,
                self.queue_mgr,
                self.config['monitor']['extensions'],
                test=self.config['test'],
            )

            for p in self.config['monitor']['paths']:
                if os.path.isdir(p):
                    logger.info(f'Adding path {p} to monitor')
                    observer.schedule(event_handler, p, recursive=True)
                else:
                    logger.error(f'Cannot find raw data folder {p}. It will NOT be monitored.')

            if self.running:
                observer.start()
                logger.info(f'Monitor "{self.name}" started')

            while self.running:
                time.sleep(0.1)

        except KeyboardInterrupt:
            logger.info(f'Monitor "{self.name}" shutting down')
            self.running = False

        finally:
            logger.info(f'\tMonitor "{self.name}" closing queues and threads')
            observer.unschedule_all()
            observer.stop()
            observer.join(THREAD_TIMEOUT) if observer.is_alive() else None
            self.join_threads()
            self.join(THREAD_TIMEOUT) if self.is_alive() else None

    def join_threads(self):
        for t in self.threads:
            logger.warning(f'\tJoining thread {t.name}. Timeout in {THREAD_TIMEOUT} seconds')
            t.running = False
            t.join(THREAD_TIMEOUT)
