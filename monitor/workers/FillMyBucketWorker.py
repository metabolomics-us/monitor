#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import shutil
from threading import Thread

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

    def __init__(self, stasis, bucket_name, up_q, storage, test=False, name='aws_worker', daemon=True):
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
                    print("[BucketWorker] - nothing to do, waiting for file to upload...")

                item = self.upload_q.get()

                print("[BucketWorker] - sending (%s) %s bytes to aws" % (item, os.path.getsize(item)))
                base_file, extension = os.path.splitext(item.split(os.sep)[-1])

                if not self.test:
                    if (self.bucket.save(item)):
                        print("[BucketWorker] - file %s saved to %s" % (item, self.bucket.bucket_name))
                    else:
                        self.stasis_cli.set_tracking(base_file, 'failed')
                else:
                    print("[BucketWorker] - file %s saved to %s" % (item, self.bucket.bucket_name))

                base_file, extension = os.path.splitext(item.split(os.sep)[-1])
                dest = os.path.join(self.storage, base_file + extension)
                print('[BucketWorker] - Moving %s to perm storage %s' % (item, dest))
                if not self.test:
                    shutil.move(item, os.path.join(self.storage, base_file + extension))

                # self.upload_q.task_done()
            except KeyboardInterrupt:
                print("[BucketWorker] - stopping aws_worker")
                self.running = False
                # self.upload_q.join()
            except Exception as ex:
                print("[BucketWorker] - Error uploading sample %s: %s" % (item, str(ex)))
                if not self.test:
                    self.stasis_cli.set_tracking(str(item.split(os.sep)[-1]).split('.')[0], 'failed')
            finally:
                self.upload_q.task_done()

    def exists(self, filename):
        return self.bucket.exists(filename)
