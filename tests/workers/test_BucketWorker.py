import os
import shutil
import time

from loguru import logger

from monitor.Bucket import Bucket
from monitor.workers.BucketWorker import BucketWorker


def test_upload(mocks, stasis_cli, wconfig, test_qm):
    test_qm.clean(test_qm.upload_q())

    mzml_file = 'D:\\data\\mzml\\test.mzml'
    worker = BucketWorker(None, stasis_cli, wconfig, test_qm)

    shutil.copy(mzml_file, os.path.join(worker.storage, 'test.mzml'))
    test_file = os.path.join(worker.storage, 'test.mzml')
    filename = test_file.split(os.sep)[-1]

    bucket = Bucket(wconfig['aws']['bucket_name'])
    bucket.delete(filename)
    assert not bucket.exists(filename)

    worker.start()
    time.sleep(1)

    # process next valid item in queue
    # boto3.client('sqs').send_message(QueueUrl=test_qm.upload_q(), MessageBody=test_file, DelaySeconds=0)
    test_qm.put_message(test_qm.upload_q(), test_file)

    time.sleep(1)
    worker.join(timeout=10)

    response = bucket.exists(filename)
    logger.info(f'{filename} exists: {response}')

    assert response is True
    bucket.delete(filename)
