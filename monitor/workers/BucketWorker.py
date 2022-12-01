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
    """
    Worker class that uploads each file in it's to an S3 bucket and to a local folder to avoid reconversion
    """

    def __init__(self, parent, stasis: StasisClient, config,
                 up_q: Queue, sched_q: Queue,
                 name='Uploader0', daemon=True):
        """

        Args:
            parent:
                Instance parent object
            stasis: StasisClient
                A stasis client instance
            config:
                An object containing config settings
            up_q: Queue
                A queue that contains the filenames to be uploaded
            sched_q: Queue
                A queue that contains the samples to be auto-scheduled
            name: str (Optional. Default: Uploader0)
                Name of the worker instance
            daemon:
                Run the worker as daemon. (Optional. Default: True)
        """
        super().__init__(name=name, daemon=daemon)
        self.parent = parent
        self.bucket = Bucket(config['aws']['bucket_name'])
        self.upload_q = up_q
        self.schedule_q = sched_q
        self.running = False
        self.stasis_cli = stasis
        self.storage = config['monitor']['storage']
        self.schedule = config['monitor']['schedule']
        self.test = config['test']

    def run(self):
        """Starts the Uploader Worker"""
        item = None
        file_basename = None
        extension = None

        self.running = True

        while self.running:
            try:
                item = self.upload_q.get()

                file_basename, extension = str(item.split(os.sep)[-1]).rsplit('.', 1)

                logger.info(f'Uploading {item} ({os.path.getsize(item)} bytes) to {self.bucket.bucket_name}')
                remote_name = self.bucket.save(item)

                if remote_name:
                    logger.info(f'\tFile {remote_name} saved to {self.bucket.bucket_name}')
                    self.pass_sample(file_basename, extension)

                    if self.schedule:
                        logger.info('\tAdding to scheduling queue.')
                        self.schedule_q.put_nowait(file_basename)
                else:
                    self.fail_sample(file_basename, 'mzml',
                                     reason='some unknown error happened while uploading the file')

            except ConnectionResetError as cre:
                logger.error(f'\tConnection Reset: {cre.strerror} uploading {cre.filename}')
                self.fail_sample(file_basename, 'mzml', reason=str(cre))

            except KeyboardInterrupt:
                logger.warning(f'\tStopping {self.name} due to Control+C')
                self.running = False
                self.schedule_q.queue.clear()
                self.upload_q.queue.clear()
                self.schedule_q.join()
                self.upload_q.join()
                self.parent.join_threads()

            except IndexError:
                time.sleep(1)

            except Exception as ex:
                logger.error(f'\tError uploading sample {item}: {str(ex)}')
                self.fail_sample(file_basename, 'mzml', reason=str(ex))

            finally:
                self.upload_q.task_done()
                logger.info(f'Uploader queue size: {self.upload_q.qsize()}')

        logger.info(f'\tStopping {self.name}')
        self.join()

    def pass_sample(self, file_basename, extension="mzml"):
        try:
            logger.info(f'\tAdd "uploaded_raw" status to stasis for sample {file_basename}.{extension}')
            self.stasis_cli.sample_state_update(file_basename, 'uploaded_raw', f'{file_basename}.mzml')
        except Exception as ex:
            logger.error(f'\tStasis client can\'t send "uploaded_raw" status for sample {file_basename}. '
                         f'\tResponse: {str(ex)}')

    def fail_sample(self, file_basename, extension, reason):
        try:
            logger.error(f'\tAdd "failed" upload status to stasis for sample {file_basename}.{extension}')
            self.stasis_cli.sample_state_update(file_basename, 'failed', reason=reason)
        except Exception as ex:
            logger.error(f'\tStasis client can\'t send "failed" status for sample {file_basename}.{extension}. '
                         f'\tResponse: {str(ex)}')

    def exists(self, filename):
        return self.bucket.exists(filename)
