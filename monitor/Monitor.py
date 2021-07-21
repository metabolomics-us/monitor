#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import time
from queue import Queue
from threading import Thread

from loguru import logger
from stasis_client.client import StasisClient
from watchdog.observers import Observer

from monitor.RawDataEventHandler import RawDataEventHandler
from monitor.workers.FillMyBucketWorker import FillMyBucketWorker
from monitor.workers.PwizWorker import PwizWorker

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
    logger.add(sys.stdout, format="{time} {level} {message}", filter="Monitor", level="INFO")

    def __init__(self, config, stasis_cli: StasisClient, conv_q: Queue, aws_q: Queue, test=False, daemon=False):
        super().__init__(name='Monitor', daemon=daemon)
        self.config = config
        self.stasis_cli = stasis_cli
        self.conversion_q = conv_q
        self.upload_q = aws_q
        self.running = True
        self.test = test

    def run(self):
        """Starts the monitoring of the selected folders"""

        observer = Observer()
        threads = []
        try:
            # Setup the aws uploader worker
            aws_worker = FillMyBucketWorker(self.stasis_cli, self.config['aws']['bucketName'], self.upload_q,
                                            self.config['monitor']['storage'], self.test)

            threads = [aws_worker]
            [threads.append(
                PwizWorker(
                    self.stasis_cli,
                    self.conversion_q,
                    self.upload_q,
                    self.config['monitor']['storage'],
                    self.config['monitor']['msconvert'],
                    self.test,
                    name='conversion_worker_%d' % x
                )
            ) for x in range(0, 5)]

            for t in threads:
                logger.info('[Monitor] - starting thread %s...' % t.name)
                t.start()

            event_handler = RawDataEventHandler(
                self.stasis_cli,
                self.conversion_q,
                self.upload_q,
                self.config['monitor']['extensions'],
                self.test
            )

            for p in self.config['monitor']['paths']:
                logger.info(f'Adding path {p} to observer')
                observer.schedule(event_handler, p, recursive=True)
            observer.start()

            logger.info('Monitor started')

            while self.running:
                time.sleep(1)

        except KeyboardInterrupt:
            logger.info('Monitor shutting down')
            self.running = False
            observer.stop()

        finally:
            logger.info('Monitor closing queues and threads')
            observer.join()
            self.conversion_q.join()
            self.upload_q.join()
            self.__join_threads__(threads)

    def __join_threads__(self, threads):
        for t in threads:
            logger.warning(f'Joining thread {t.name}. Timeout in {THREAD_TIMEOUT} seconds')
            t.running = False
            t.join(THREAD_TIMEOUT)
