import os
import unittest
from queue import Queue

import yamlconf
from mock import MagicMock

from monitor.workers.PwizWorker import PwizWorker


class TestPwizWorker(unittest.TestCase):
    agi_file = os.path.join(os.path.dirname(__file__),
                            '..', '..', 'resources', 'monitored.d')

    # setup
    @classmethod
    def setUpClass(cls):
        with open(os.path.join(os.path.dirname(__file__), '..', '..',
                               'appconfig.yml'), 'r') as conf:
            cls.config = yamlconf.load(conf)

    # cleanup
    def tearDown(self):
        filename = str(self.agi_file.split(os.sep)[-1]).split('.')[0]
        os.remove(os.path.join(self.config['monitor']['storage'], '%s.mzml' % filename))

    def test_pwizWorker(self):
        st_cli = MagicMock(name='stasis_cli_mock')
        st_cli.return_value.add_tracking.return_value = True

        print('creating queues')
        cnv_q = Queue()
        aws_q = Queue()

        print('creating worker')
        worker = PwizWorker(st_cli, cnv_q, aws_q,
                            self.config['monitor']['storage'],
                            self.config['monitor']['msconvert'])

        worker.start()

        # should be skipped and
        print('adding bad file')
        cnv_q.put('bad_file.d')

        print('adding good file')
        # process next valid item in queue
        cnv_q.put(self.agi_file)

        print('closing queue')
        cnv_q.join()

        filename = str(self.agi_file.split(os.sep)[-1]).split('.')[0]
        assert os.path.exists(os.path.join(self.config['monitor']['storage'], '%s.mzml' % filename))
        assert 1 == aws_q.qsize()
