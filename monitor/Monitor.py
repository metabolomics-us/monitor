#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
from threading import Thread

from watchdog.observers import Observer

from monitor.NewFileScanner import NewFileScanner
from monitor.workers.AgilentWorker import AgilentWorker
from monitor.workers.ConversionWorker import ConversionWorker
from monitor.workers.FillMyBucketWorker import FillMyBucketWorker


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

    def __init__(self, config, stasis_cli, dataform_cli, agi_q, conv_q, aws_q):
        super().__init__()
        self.config = config
        self.stasis_cli = stasis_cli
        self.dataform_cli = dataform_cli
        self.running = True
        self.zipping_q = agi_q
        self.conversion_q = conv_q
        self.upload_q = aws_q

    def start(self):
        """Starts the monitoring of the selected folders"""

        # Setup the zipping worker
        agilent_worker = AgilentWorker(
            self.stasis_cli,
            self.zipping_q,
            self.conversion_q,
            self.config['monitor']['storage'])
        agilent_worker.daemon = True

        # Setup the convertion worker
        conversion_worker = ConversionWorker(
            self.stasis_cli,
            self.dataform_cli,
            self.conversion_q,
            self.upload_q,
            self.config['monitor']['storage']
        )
        conversion_worker.daemon = True

        # Setup the aws uploader worker
        aws_worker = FillMyBucketWorker(self.config['aws']['bucketName'], self.upload_q)
        aws_worker.daemon = True

        threads = [agilent_worker, conversion_worker, aws_worker]

        for t in threads:
            print("starting thread %s..." % t.name)
            t.start()

        event_handler = NewFileScanner(
            self.stasis_cli,
            self.zipping_q,
            self.conversion_q,
            self.upload_q,
            self.config['monitor']['extensions']
        )

        observer = Observer()
        for p in self.config['monitor']['paths']:
            print("adding path %s to observer" % p)
            observer.schedule(event_handler, p, recursive=True)
        observer.start()

        print("monitor started")

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()

        observer.join()
