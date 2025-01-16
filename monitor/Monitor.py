#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import os
import platform
import time
import math
from threading import Thread

import watchtower

from monitor.ObserverFactory import ObserverFactory
from monitor.QueueManager import QueueManager
from monitor.RawDataEventHandler import RawDataEventHandler
from monitor.client.BackendClient import BackendClient
from monitor.workers.BucketWorker import BucketWorker
from monitor.workers.PwizWorker import PwizWorker

THREAD_TIMEOUT = 5

logger = logging.getLogger('Monitor')
# if not logger.handlers:
#     h = watchtower.CloudWatchLogHandler(
#         log_group_name=f'/lcb/monitor/{platform.node()}',
#         log_group_retention_days=3,
#         send_interval=10)
#     logger.addHandler(h)


class Monitor(Thread):
    """
    A file monitoring class
    """

    def __init__(self, config, backend_cli: BackendClient, queue_mgr: QueueManager, daemon=False):
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
        if config['debug']:
            logger.setLevel(level='DEBUG')

        self.config = config
        self.backend_cli = backend_cli
        self.running = True
        self.test = config['test']
        self.threads = []
        self.queue_mgr = queue_mgr

    def run(self):
        """Starts the monitoring of the selected folders"""
        observer = ObserverFactory().getObserver(self.config['monitor']['mode'])

        try:
            proc_cores = math.floor(os.cpu_count() / 3)
            upld_cores = math.ceil(proc_cores / 3)
            # proc_cores = 10
            # upld_cores = 2

            logger.info(f'Using {proc_cores} for processing')
            logger.info(f'Using {upld_cores} for uploading')

            threads = []
            # Setup the pwiz workers
            [threads.append(
                PwizWorker(self,
                           self.backend_cli,
                           self.queue_mgr,
                           self.config,
                           name=f'Converter{x}')
            ) for x in range(0, proc_cores)]

            # Setup the aws uploader worker
            [threads.append(
                BucketWorker(self,
                             self.backend_cli,
                             self.config,
                             self.queue_mgr,
                             name=f'Uploader{x}')
            ) for x in range(0, upld_cores)]

            logger.info(f'Starting threads')
            for t in threads:
                t.start()

            event_handler = RawDataEventHandler(
                self.backend_cli,
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
