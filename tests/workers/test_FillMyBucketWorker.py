import os
import unittest
from queue import Queue

from monitor.Bucket import Bucket
from monitor.workers.FillMyBucketWorker import FillMyBucketWorker


class TestFillMyBucketWorker(unittest.TestCase):

    def test_upload(self):
        upload_q = Queue()
        mzml_file = os.path.join(os.path.dirname(__file__), '..', '..', 'resources', 'test.mzml')
        filename = mzml_file.split(os.sep)[-1]

        bucket = Bucket('data-carrot')
        worker = FillMyBucketWorker('data-carrot', upload_q)
        bucket.delete(filename)
        assert not bucket.exists(filename)

        worker.start()

        # process next valid item in queue
        upload_q.put(mzml_file)

        upload_q.join()

        assert bucket.exists(filename)
        bucket.delete(filename)
