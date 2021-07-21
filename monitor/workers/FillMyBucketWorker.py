#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import shutil
import sys
import time
from queue import Queue
from threading import Thread

from loguru import logger
from stasis_client.client import StasisClient

from monitor.Bucket import Bucket


class FillMyBucketWorker(Thread):
    """Worker class that uploads each file in it's to an S3 bucket

    Parameters
    ----------
        bucket_name: str
            Name of the S3 bucket this worker is uploading to
        up_q: Queue
            A queue that contains the filenames to be uploaded
        storage: str
            A path for storing converted mzml files
    """

    logger.add(sys.stdout, format="{time} {level} {message}", filter=f"BucketWorker", level="INFO")

    def __init__(self, stasis: StasisClient, bucket_name, up_q: Queue, storage, test=False, name='aws_worker', daemon=True):
        super().__init__(name=name, daemon=daemon)
        self.bucket = Bucket(bucket_name)
        self.upload_q = up_q
        self.running = True
        self.stasis_cli = stasis
        self.storage = storage
        self.test = test

    def run(self):
        """Starts the AWS bucket filler Worker"""
        item = None

        while self.running:
            try:
                if self.upload_q.empty():
                    logger.info('Nothing to do, waiting for file to upload...')
                    time.sleep(5)
                    continue
                else:
                    logger.info(f'Approximate upload queue size: {self.upload_q.qsize()}')

                    item = self.upload_q.get()

                    logger.info(f'Sending ({item}) {os.path.getsize(item)} bytes to aws')
                    base_file, extension = os.path.splitext(item.split(os.sep)[-1])

                    if not self.test:
                        if self.bucket.save(item):
                            logger.info(f'File {item} saved to {self.bucket.bucket_name}')
                        else:
                            logger.info(f'Fail to upload {item} to {self.bucket.bucket_name}')
                            self.stasis_cli.sample_state_update(base_file, 'failed')
                    else:
                        logger.info("[BucketWorker] - file %s saved to %s" % (item, self.bucket.bucket_name))

                    base_file, extension = os.path.splitext(item.split(os.sep)[-1])
                    dest = os.path.join(self.storage, base_file + extension)
                    logger.info('[BucketWorker] - Moving %s to perm storage %s' % (item, dest))
                    if not self.test:
                        shutil.move(item, os.path.join(self.storage, base_file + extension))

                    self.upload_q.task_done()
            except KeyboardInterrupt:
                logger.warning("[BucketWorker] - stopping aws_worker")
                self.running = False
                self.upload_q.task_done()
                break
            except Exception as ex:
                logger.error("[BucketWorker] - Error uploading sample %s: %s" % (item, str(ex)))
                if not self.test:
                    self.stasis_cli.sample_state_update(str(item.split(os.sep)[-1]).split('.')[0], 'failed')
                self.upload_q.task_done()
                break

        self.upload_q.join()

    def exists(self, filename):
        return self.bucket.exists(filename)
