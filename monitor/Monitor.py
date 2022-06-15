#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
from collections import deque
from threading import Thread

from cisclient.client import CISClient
from loguru import logger
from stasis_client.client import StasisClient
from watchdog.observers import Observer

from monitor.RawDataEventHandler import RawDataEventHandler
from monitor.workers.BucketWorker import BucketWorker
from monitor.workers.PwizWorker import PwizWorker
from monitor.workers.Scheduler import Scheduler

THREAD_TIMEOUT = 1


class Monitor(Thread):
    """A file monitoring class

    Parameters
    ----------
        config: dict
            A yaml application configuration file loaded with yamlconf
        stasis_cli: StasisClient
            A client class to the Stasis rest API
        dataform_cli: DataformerClient
            A client class to the DataFormer rest API
    """

    def __init__(self, config, stasis_cli: StasisClient, cis_cli: CISClient,
                 conv_q: deque, aws_q: deque, sched_q: deque,
                 test=False, daemon=False):
        super().__init__(name='Monitor', daemon=daemon)
        self.config = config
        self.stasis_cli = stasis_cli
        self.cis_cli = cis_cli
        self.conversion_q = conv_q
        self.upload_q = aws_q
        self.schedule_q = sched_q
        self.running = True
        self.test = test
        self.threads = []

    def run(self):
        """Starts the monitoring of the selected folders"""

        observer = Observer()
        try:
            # Setup the aws uploader worker
            aws_worker = BucketWorker(self,
                                      self.stasis_cli,
                                      self.config['aws']['bucketName'],
                                      self.upload_q,
                                      self.config['monitor']['storage'],
                                      self.schedule_q,
                                      self.config['test'])

            scheduler = Scheduler(self,
                                  self.stasis_cli,
                                  self.cis_cli,
                                  self.schedule_q,
                                  test=self.config['test'],
                                  schedule=self.config['schedule'])

            threads = [aws_worker, scheduler]

            # Setup the pwiz workers
            [threads.append(
                PwizWorker(
                    self,
                    self.stasis_cli,
                    self.conversion_q,
                    self.upload_q,
                    self.config['monitor'],
                    test=self.config['test'],
                    name=f'Converter{x}'
                )
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
                logger.info(f'Adding path {p} to monitor')
                observer.schedule(event_handler, p, recursive=True)

            observer.start()

            logger.info('Monitor started')

            while self.running:
                time.sleep(0.1)

        except KeyboardInterrupt:
            logger.info('Monitor shutting down')
            self.running = False

        finally:
            logger.info('\tMonitor closing queues and threads')
            self.conversion_q.clear()
            self.upload_q.clear()
            self.schedule_q.clear()
            self.join_threads()
            observer.stop()
            observer.join()

    def join_threads(self):
        for t in self.threads:
            logger.warning(f'\tJoining thread {t.name}. Timeout in {THREAD_TIMEOUT} seconds')
            t.running = False
            t.join(THREAD_TIMEOUT)
