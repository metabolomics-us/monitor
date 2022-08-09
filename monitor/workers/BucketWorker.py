#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import time
from queue import Queue
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
                 up_q: Queue, storage, sched_q: Queue,
                 test: bool = False, name='Uploader0', daemon=True, schedule=False):
        super().__init__(name=name, daemon=daemon)
        self.parent = parent
        self.bucket = Bucket(bucket_name)
        self.upload_q = up_q
        self.schedule_q = sched_q
        self.running = False
        self.stasis_cli = stasis
        self.storage = storage
        self.schedule = schedule
        self.test = test

    def run(self):
        """Starts the Uploader Worker"""
        item = None
        self.running = True

        while self.running:
            try:
                item = self.upload_q.get()

                logger.info(f'Sending ({item}) {os.path.getsize(item)} bytes to aws')
                file_basename, extension = str(item.split(os.sep)[-1]).rsplit('.', 1)

                if not self.test:
                    if self.bucket.save(item):
                        logger.info(f'File {item} saved to {self.bucket.bucket_name}')
                        if self.schedule:
                            logger.info('\tAdding to scheduling queue.')
                            self.schedule_q.put_nowait(file_basename)
                    else:
                        self.fail_sample(file_basename, extension)
                else:
                    logger.info(f'Fake StasisUpdate: Uploaded {item} to {self.bucket.bucket_name}')
                    if self.schedule:
                        logger.info('\tAdding to fake scheduling queue.')
                        self.schedule_q.put_nowait(file_basename)

            except KeyboardInterrupt:
                logger.warning(f'Stopping {self.name} due to Control+C')
                self.running = False
                self.schedule_q.queue.clear()
                self.upload_q.queue.clear()
                self.schedule_q.join()
                self.upload_q.join()
                self.parent.join_threads()

            except IndexError:
                time.sleep(1)

            except Exception as ex:
                logger.error(f'Error uploading sample {item}: {str(ex)}')
                if not self.test:
                    fname, ext = str(item.split(os.sep)[-1]).rsplit('.', 1)
                    self.fail_sample(fname, ext)
                else:
                    logger.info(f'Fake StasisUpdate: Error uploading sample {item}: {str(ex)}')

            finally:
                self.upload_q.task_done()
                logger.info(f'Uploader queue size: {self.upload_q.qsize()}')

        logger.info(f'\tStopping {self.name}')
        self.join()

    def fail_sample(self, file_basename, extension):
        try:
            logger.error(f'\tAdd "failed" upload status to stasis for sample "{file_basename}.{extension}"')
            self.stasis_cli.sample_state_update(file_basename, 'failed')
        except Exception as ex:
            logger.error(f'\tStasis client can\'t send "failed" status for sample {file_basename}\n'
                         f'\tResponse: {str(ex)}')

    def exists(self, filename):
        return self.bucket.exists(filename)
