#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
from threading import Thread

from watchdog.observers import Observer

from monitor.NewFileScanner import NewFileScanner
from monitor.workers.FillMyBucketWorker import FillMyBucketWorker
from monitor.workers.PwizWorker import PwizWorker


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

    def __init__(self, config, stasis_cli, conv_q, aws_q, daemon=False):
        super().__init__(name='Monitor', daemon=daemon)
        self.config = config
        self.stasis_cli = stasis_cli
        self.conversion_q = conv_q
        self.upload_q = aws_q
        self.running = True

    def run(self):
        """Starts the monitoring of the selected folders"""

        # Setup the aws uploader worker
        aws_worker = FillMyBucketWorker(self.stasis_cli, self.config['aws']['bucketName'], self.upload_q,
                                        self.config['monitor']['storage'])

        threads = [aws_worker] + [PwizWorker(
            self.stasis_cli,
            self.conversion_q,
            self.upload_q,
            self.config['monitor']['storage'],
            self.config['monitor']['msconvert'],
            name='conversion_worker_%d' % x
        ) for x in range(0, 5)
        ]

        for t in threads:
            print('[Monitor] - starting thread %s...' % t.name)
            t.start()

        event_handler = NewFileScanner(
            self.stasis_cli,
            self.conversion_q,
            self.upload_q,
            self.config['monitor']['extensions']
        )

        observer = Observer()
        for p in self.config['monitor']['paths']:
            print('[Monitor] - adding path %s to observer' % p)
            observer.schedule(event_handler, p, recursive=True)
        observer.start()

        print('[Monitor] - monitor started')

        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.running = False
            observer.stop()
        finally:
            print('[Monitor] - Monitor closing queues and threads')
            observer.join()
            self.conversion_q.join()
            self.upload_q.join()
            self.__join_threads(threads)

    def __join_threads(self, threads):
        for t in threads:
            print('[Monitor] - joining thread %s' % t.name)
            t.running = False
            t.join()
