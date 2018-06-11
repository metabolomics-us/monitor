import os
import unittest
from multiprocessing import JoinableQueue

import yamlconf
from mock import MagicMock

from monitor.workers.AgilentWorker import AgilentWorker


class TestAgilentWorker(unittest.TestCase):
    agi_file = os.path.join('..', '..', 'resources', 'monitored.d')

    def setUp(self):
        with open(os.path.join('..', '..', 'appconfig.yml'), 'r') as stream:
            self.config = yamlconf.load(stream)

    # cleanup
    def tearDown(self):
        os.remove(os.path.join(self.config['monitor']['storage'], '%s.zip' % self.agi_file.split(os.sep)[-1]))

    def test_agilentWorker(self):
        st_cli = MagicMock(name='stasis_cli_mock')
        st_cli.return_value.add_tracking.return_value = True

        zip_q = JoinableQueue()
        cnv_q = JoinableQueue()

        worker = AgilentWorker(st_cli, zip_q, cnv_q, self.config['monitor']['storage'])

        # should be skipped and
        zip_q.put('bad_file.d')
        # process next valid item in queue
        zip_q.put(self.agi_file)
        print('original: %s' % self.agi_file.split(os.sep)[-1])

        worker.daemon = True
        worker.start()
        worker.join(timeout=5)
        assert (
            os.path.exists(os.path.join(self.config['monitor']['storage'], '%s.zip' % self.agi_file.split(os.sep)[-1])))
