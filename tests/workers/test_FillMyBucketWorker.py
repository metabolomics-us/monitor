import os
import shutil
import time
from pprint import pprint
from queue import Queue

from monitor.Bucket import Bucket
from monitor.workers.BucketWorker import BucketWorker


def test_upload(stasis_cli, wconfig):

    upload_q = Queue()
    mzml_file = os.path.join(os.path.dirname(__file__), '..', '..', 'resources', 'test.mzml')
    shutil.copy(mzml_file, os.path.join(wconfig['monitor']['storage'], 'test.mzml'))
    test_file = os.path.join(wconfig['monitor']['storage'], 'test.mzml')
    filename = test_file.split(os.sep)[-1]

    bucket = Bucket(wconfig['aws']['bucket_name'])
    worker = BucketWorker(None, stasis_cli, wconfig, upload_q, Queue())
    bucket.delete(filename)
    assert not bucket.exists(filename)

    worker.start()
    time.sleep(1)

    # process next valid item in queue
    upload_q.put(test_file)

    worker.join(timeout=10)
    time.sleep(1)

    response = bucket.exists(filename)
    pprint(f'{filename} exists: {response}')

    assert response is True
    bucket.delete(filename)
