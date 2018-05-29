import os
import unittest
from multiprocessing import JoinableQueue

from mock import MagicMock

from monitor.workers.AgilentWorker import AgilentWorker


class TestAgilentWorker(unittest.TestCase):
    agi_file = '../../resources/monitored.d'

    # cleanup
    def tearDown(self):
        os.remove('%s.zip' % self.agi_file)

    def test_agilentWorker(self):
        st_cli = MagicMock(name='stasis_cli_mock')
        st_cli.return_value.add_tracking.return_value = True

        zip_q = JoinableQueue()
        cnv_q = JoinableQueue()

        worker = AgilentWorker(st_cli, zip_q, cnv_q, '~/.carrot_storage/tmp')

        # should be skipped and
        zip_q.put('bad_file.d')
        # process next valid item in queue
        zip_q.put(self.agi_file)

        worker.daemon = True
        worker.start()
        worker.join(timeout=5)
        assert (os.path.exists('%s.zip' % self.agi_file))
