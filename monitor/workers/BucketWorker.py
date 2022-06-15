#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import time
from collections import deque
from threading import Thread

from loguru import logger
from stasis_client.client import StasisClient

from monitor.Bucket import Bucket


class BucketWorker(Thread):
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

    def __init__(self, parent, stasis: StasisClient, bucket_name,
                 up_q: deque, storage, sched_q: deque,
                 test: bool = False, name='Uploader0', daemon=True):
        super().__init__(name=name, daemon=daemon)
        self.parent = parent
        self.bucket = Bucket(bucket_name)
        self.upload_q = up_q
        self.schedule_q = sched_q
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
                item = self.upload_q.popleft()

                logger.info(f'Sending ({item}) {os.path.getsize(item)} bytes to aws')
                file_basename, extension = str(item.split(os.sep)[-1]).rsplit('.', 1)

                if not self.test:
                    if self.bucket.save(item):
                        logger.info(f'File {item} saved to {self.bucket.bucket_name}')
                    else:
                        self.fail_sample(file_basename, extension)
                else:
                    logger.info(f'Fake StasisUpdate: Uploaded {item} to {self.bucket.bucket_name}')

                self.schedule_q.append(file_basename)

            except KeyboardInterrupt:
                logger.warning(f'Stopping UploaderWorker {self.name}')
                self.running = False
                self.parent.join_threads()
                continue

            except IndexError:
                time.sleep(1)
                continue

            except Exception as ex:
                logger.error(f'Error uploading sample {item}: {str(ex)}')
                if not self.test:
                    fname, ext = str(item.split(os.sep)[-1]).rsplit('.', 1)
                    self.fail_sample(fname, ext)
                else:
                    logger.info(f'Fake StasisUpdate: Error uploading sample {item}: {str(ex)}')
                continue

            logger.info(f'Uploader queue size: {len(self.upload_q)}')
        logger.info(f'Stopping {self.name}')

    def fail_sample(self, file_basename, extension):
        try:
            logger.error(f'\tAdd "failed" upload status to stasis for sample "{file_basename}.{extension}"')
            self.stasis_cli.sample_state_update(file_basename, 'failed')
        except Exception as ex:
            logger.error(f'\tStasis client can\'t send "failed" status for sample {file_basename}\n'
                         f'\tResponse: {str(ex)}')

    def exists(self, filename):
        return self.bucket.exists(filename)
