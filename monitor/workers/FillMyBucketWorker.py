#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

    def __init__(self, bucket_name, up_q, name='aws_worker'):
        super().__init__(name=name)
        self.bucket = Bucket(bucket_name)
        self.upload_q = up_q

    def run(self):
        """Starts the AWS bucket filler Worker"""
        running = True
        item = None

        while running:
            try:
                item = self.upload_q.get()
                # filename = item.split(os.sep)[-1]
                # print('uploading %s as \'%s\' to bucket' % (item, filename))
                self.bucket.save(item)
                self.upload_q.task_done()
            except KeyboardInterrupt:
                print('stopping aws_worker')
                self.upload_q.join()
                running = False
            except Exception as ex:
                print('Error: %s' % str(ex))
                self.upload_q.task_done()
                pass

    def exists(self, filename):
        return self.bucket.exists(filename)
