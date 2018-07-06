#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from threading import Thread

from monitor.Bucket import Bucket


class FillMyBucketWorker(Thread):
    """Worker class that uploads each file in it's to an S3 bucket

    Parameters
    ----------
        bucket_name: str
            Name of the S3 bucket this worker is uploading to
        upload_q: Queue
            A queue that contains the filenames to be uploaded
    """

    def __init__(self, bucket_name, up_q, name='aws_worker', daemon=True):
        super().__init__(name=name, daemon=daemon)
        self.bucket = Bucket(bucket_name)
        self.upload_q = up_q
        self.running = True

    def run(self):
        """Starts the AWS bucket filler Worker"""
        item = None

        while self.running:
            try:
                print("[BucketWorker] - looking for something to do...")
                item = self.upload_q.get()

                print("[BucketWorker] - sending (%s) %s bytes to aws" % (item, os.path.getsize(item)))
                if (self.bucket.save(item)):
                    print("[BucketWorker] - file %s saved to %s" % (item, self.bucket.bucket_name))

                self.upload_q.task_done()
            except KeyboardInterrupt:
                print("[BucketWorker] - stopping aws_worker")
                self.upload_q.join()
                self.running = False
            except Exception as ex:
                print("[BucketWorker] - Error uploading sample %s: %s" % (item, str(ex)))
                self.upload_q.task_done()

    def exists(self, filename):
        return self.bucket.exists(filename)
