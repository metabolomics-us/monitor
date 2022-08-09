import os
import shutil
import unittest
from queue import Queue

import yamlconf
from mock import MagicMock

from monitor.Bucket import Bucket
from monitor.workers.BucketWorker import BucketWorker


class TestFillMyBucketWorker(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(os.path.join(os.path.dirname(__file__), '..', '..',
                               'appconfig.yml'), 'r') as conf:
            cls.config = yamlconf.load(conf)
        if not os.path.exists(cls.config['monitor']['storage']):
            os.makedirs(cls.config['monitor']['storage'])

    def test_upload(self):
        st_cli = MagicMock(name='stasis_cli_mock')
        st_cli.return_value.add_tracking.return_value = True

        upload_q = Queue()
        mzml_file = os.path.join(os.path.dirname(__file__), '..', '..', 'resources', 'test.mzml')
        shutil.copy(mzml_file, os.path.join(self.config['monitor']['storage'], 'test.mzml'))
        test_file = os.path.join(self.config['monitor']['storage'], 'test.mzml')
        filename = test_file.split(os.sep)[-1]

        bucket = Bucket('data-carrot')
        worker = BucketWorker(None, st_cli, 'data-carrot', upload_q, self.config['monitor']['storage'], Queue())
        bucket.delete(filename)
        assert not bucket.exists(filename)

        worker.start()

        # process next valid item in queue
        upload_q.put(test_file)

        worker.join(timeout=10)

        assert bucket.exists(filename)
        bucket.delete(filename)
