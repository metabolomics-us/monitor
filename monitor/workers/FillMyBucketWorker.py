#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import shutil
from queue import Queue
from threading import Thread

from loguru import logger
from stasis_client.client import StasisClient

from monitor.Bucket import Bucket


class FillMyBucketWorker(Thread):
    """Worker class that uploads each file in it's to an S3 bucket and to a local folder to avoid reconversion

    Parameters
    ----------
        bucket_name: str
            Name of the S3 bucket this worker is uploading to
        up_q: Queue
            A queue that contains the filenames to be uploaded
        storage: str
            A path for storing converted mzml files
    """

    def __init__(self, parent, stasis: StasisClient, bucket_name, up_q: Queue, storage, test=False,
                 name='Uploader0', daemon=True):
        super().__init__(name=name, daemon=daemon)
        self.parent = parent
        self.bucket = Bucket(bucket_name)
        self.upload_q = up_q
        self.running = False
        self.stasis_cli = stasis
        self.storage = storage
        self.test = test

    def run(self):
        """Starts the Uploader Worker"""
        item = None
        self.running = True

        while self.running:
            try:
                logger.info(f'Approximate upload queue size: {self.upload_q.qsize()}')

                item = self.upload_q.get(block=True)

                logger.info(f'Sending ({item}) {os.path.getsize(item)} bytes to aws')
                base_file, extension = os.path.splitext(item.split(os.sep)[-1])

                if not self.test:
                    if self.bucket.save(item):
                        logger.info(f'File {item} saved to {self.bucket.bucket_name}')
                    else:
                        logger.info(f'Fail to upload {item} to {self.bucket.bucket_name}')
                        self.stasis_cli.sample_state_update(base_file, 'failed')

                    base_file, extension = os.path.splitext(item.split(os.sep)[-1])
                    dest = os.path.join(self.storage, base_file + extension)
                    logger.info(f'base_file {base_file} || extension {extension}')
                    logger.info(f'Moving {item} to perm storage {dest}')
                    if not self.test:
                        shutil.move(item, os.path.join(self.storage, base_file + extension))
                else:
                    logger.info(f'Fake StasisUpdate: Fail to upload {item} to {self.bucket.bucket_name}')

                self.upload_q.task_done()
            except KeyboardInterrupt:
                logger.warning(f'Stopping UploaderWorker {self.name}')
                self.running = False
                self.upload_q.task_done()
                self.parent.join_threads()
                break
            except Exception as ex:
                logger.error(f'Error uploading sample {item}: {str(ex)}')
                if not self.test:
                    self.stasis_cli.sample_state_update(str(item.split(os.sep)[-1]).split('.')[0], 'failed')
                else:
                    logger.info(f'Fake StasisUpdate: Error uploading sample {item}: {str(ex)}')

                self.upload_q.task_done()

        self.upload_q.join()

    def exists(self, filename):
        return self.bucket.exists(filename)
