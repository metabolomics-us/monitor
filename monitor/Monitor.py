#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import time
from queue import Queue
from threading import Thread

from cisclient.client import CISClient
from loguru import logger
from stasis_client.client import StasisClient
from watchdog.observers import Observer

from monitor.RawDataEventHandler import RawDataEventHandler
from monitor.workers.BucketWorker import BucketWorker
from monitor.workers.PwizWorker import PwizWorker
from monitor.workers.Scheduler import Scheduler

THREAD_TIMEOUT = 5


class Monitor(Thread):
    """
    A file monitoring class
    """

    def __init__(self, config, stasis_cli: StasisClient, cis_cli: CISClient,
                 conv_q: Queue, up_q: Queue, sched_q: Queue,
                 daemon=False):
        """

        Args:
            config:
                An object containing config settings
            stasis_cli: StasisClient
                A stasis client instance
            cis_cli: CisClient
                A cis client instance
            up_q: Queue
                A queue that contains the filenames to be uploaded
            sched_q: Queue
                A queue that contains the samples to be auto-scheduled
            daemon:
                Run the worker as daemon. (Optional. Default: True)
        """
        super().__init__(name='Monitor', daemon=daemon)
        self.config = config
        self.stasis_cli = stasis_cli
        self.cis_cli = cis_cli
        self.conversion_q = conv_q
        self.upload_q = up_q
        self.schedule_q = sched_q
        self.running = True
        self.test = config['test']
        self.threads = []

    def run(self):
        """Starts the monitoring of the selected folders"""

        observer = Observer()
        try:
            # Setup the aws uploader worker
            aws_worker = BucketWorker(self,
                                      self.stasis_cli,
                                      self.config,
                                      self.upload_q,
                                      self.schedule_q)

            scheduler = Scheduler(self,
                                  self.stasis_cli,
                                  self.cis_cli,
                                  self.config,
                                  self.schedule_q)

            threads = [aws_worker, scheduler]

            # Setup the pwiz workers
            [threads.append(
                PwizWorker(self,
                           self.stasis_cli,
                           self.conversion_q,
                           self.upload_q,
                           self.config,
                           name=f'Converter{x}')
            ) for x in range(0, 5)]

            logger.info(f'Starting threads')
            for t in threads:
                t.start()

            event_handler = RawDataEventHandler(
                self.stasis_cli,
                self.conversion_q,
                self.upload_q,
                self.config['monitor']['extensions'],
                test=self.config['test']
            )

            for p in self.config['monitor']['paths']:
                if os.path.exists(p):
                    logger.info(f'Adding path {p} to monitor')
                    observer.schedule(event_handler, p, recursive=True)
                else:
                    logger.error(f'Cannot find raw data folder {p}. '
                                 f'Please fix the configuration file and restart the application.')
                    self.running = False
                    exit(2)

            if self.running:
                observer.start()
                logger.info('Monitor started')

            while self.running:
                # pass
                time.sleep(0.1)

        except KeyboardInterrupt:
            logger.info('Monitor shutting down')
            self.running = False

        finally:
            logger.info('\tMonitor closing queues and threads')
            observer.unschedule_all()
            observer.stop()
            observer.join(THREAD_TIMEOUT) if observer.is_alive() else None
            self.conversion_q.queue.clear() if self.conversion_q.not_empty else None
            self.upload_q.queue.clear() if self.upload_q.not_empty else None
            self.schedule_q.queue.clear() if self.schedule_q.not_empty else None
            self.conversion_q.join()
            self.upload_q.join()
            self.schedule_q.join()
            self.join_threads()
            self.join(THREAD_TIMEOUT) if self.is_alive() else None

    def join_threads(self):
        for t in self.threads:
            logger.warning(f'\tJoining thread {t.name}. Timeout in {THREAD_TIMEOUT} seconds')
            t.running = False
            t.join(THREAD_TIMEOUT)
