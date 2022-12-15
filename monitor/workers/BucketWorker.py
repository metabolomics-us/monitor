#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os
import platform
import tempfile
import time
from threading import Thread

import boto3
import watchtower
from stasis_client.client import StasisClient

from monitor.Bucket import Bucket
from monitor.QueueManager import QueueManager

logger = logging.getLogger('BucketWorker')
h = watchtower.CloudWatchLogHandler(
    log_group_name=f'/lcb/monitor/{platform.node()}',
    log_group_retention_days=3,
    send_interval=30)
logger.addHandler(h)


class BucketWorker(Thread):
    """
    Worker class that uploads each file in it's to an S3 bucket and to a local folder to avoid reconversion
    """

    def __init__(self, parent, stasis: StasisClient, config,
                 queue_mgr: QueueManager,
                 name='Uploader0', daemon=True):
        """

        Args:
            parent:
                Instance parent object
            stasis: StasisClient
                A stasis client instance
            config:
                An object containing config settings
            queue_mgr: QueueManager
                A QueueManager object that handles setting up queues and sending/receiving messages
            name: str (Optional. Default: Uploader0)
                Name of the worker instance
            daemon:
                Run the worker as daemon. (Optional. Default: True)
        """
        super().__init__(name=name, daemon=daemon)
        self.sqs = boto3.client('sqs')

        self.parent = parent
        self.running = False
        self.queue_mgr = queue_mgr
        self.bucket = Bucket(config['aws']['bucket_name'])
        self.stasis_cli = stasis
        self.storage = tempfile.tempdir
        self.schedule = config['monitor']['schedule']
        self.test = config['test']

    def run(self):
        """Starts the Uploader Worker"""
        self.running = True

        item = None
        file_basename = None
        extension = None

        while self.running:
            try:
                item = self.queue_mgr.get_next_message(self.queue_mgr.upload_q())
                if not item:
                    logger.debug('\twaiting...')
                    time.sleep(1.7)
                    continue

                file_basename, extension = str(item.split(os.sep)[-1]).rsplit('.', 1)

                logger.info(f'Uploading {item} ({os.path.getsize(item)} bytes) to {self.bucket.bucket_name}')
                remote_name = self.bucket.save(item)

                if remote_name:
                    logger.info(f'\tFile {remote_name} saved to {self.bucket.bucket_name}')
                    self.pass_sample(file_basename, extension)

                    # auto preprocess causes too many issues
                    # if self.schedule:
                    #     logger.info('\tAdding to scheduling queue.')
                    #     self.queue_mgr.put_message(self.queue_mgr.preprocess_q(), file_basename)
                else:
                    self.fail_sample(file_basename, 'mzml',
                                     reason='some unknown error happened while uploading the file')

                logger.info(f'Uploader queue size: {self.queue_mgr.get_size(self.queue_mgr.upload_q())}')

            except ConnectionResetError as cre:
                logger.error(f'\tConnection Reset: {cre.strerror} uploading {cre.filename}')
                self.fail_sample(file_basename, 'mzml', reason=str(cre))

            except KeyboardInterrupt:
                logger.warning(f'\tStopping {self.name} due to Control+C')
                self.running = False
                self.parent.join_threads()

            except IndexError:
                time.sleep(1)

            except Exception as ex:
                logger.error(f'\tError uploading sample {item}: {str(ex)}')
                self.fail_sample(file_basename, 'mzml', reason=str(ex))

            finally:
                pass
                # logger.info(f'Uploader queue size: {self.queue_mgr.get_size(self.queue_mgr.upload_q())}')

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
