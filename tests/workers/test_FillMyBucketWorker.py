import os
from queue import Queue

from monitor.Bucket import Bucket
from monitor.workers.FillMyBucketWorker import FillMyBucketWorker


class TestFillMyBucketWorker():

    def test_upload(self):
        upload_q = Queue()
        mzml_file = os.path.join('..', '..', 'resources', 'test.mzml')
        filename = mzml_file.split(os.sep)[-1]

        bucket = Bucket('data-carrot')
        worker = FillMyBucketWorker('data-carrot', upload_q)
        assert not bucket.exists(filename)

        worker.daemon = True
        worker.start()

        # process next valid item in queue
        upload_q.put(mzml_file)

        upload_q.join()

        assert bucket.exists(filename)
        bucket.delete(filename)
