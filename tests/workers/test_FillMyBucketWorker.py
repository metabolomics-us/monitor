import os
from multiprocessing import JoinableQueue

from monitor.workers.FillMyBucketWorker import FillMyBucketWorker


class TestFillMyBucketWorker():

    def test_upload(self, requireMocking):
        upload_q = JoinableQueue()
        mzml_file = os.path.join('..', '..', 'resources', 'test.mzml')

        worker = FillMyBucketWorker('test-data-carrot', upload_q)
        bucket = worker.bucket
        assert not bucket.exists(mzml_file)

        # process next valid item in queue
        upload_q.put(mzml_file)

        worker.daemon = True
        worker.start()
        worker.join(timeout=2)

        assert bucket.exists('test.mzml')
