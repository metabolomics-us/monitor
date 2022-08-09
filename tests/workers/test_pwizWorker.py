import os
import unittest
from queue import Queue

import yamlconf
from mock import MagicMock

from monitor.workers.PwizWorker import PwizWorker


class TestPwizWorker(unittest.TestCase):
    config = None
    agi_file = os.path.join(os.path.dirname(__file__),
                            '..', '..', 'resources', 'monitored.d')

    @classmethod
    def setUpClass(cls):
        with open(os.path.join(os.path.dirname(__file__), '..', '..',
                               'appconfig-test.yml'), 'r') as conf:
            cls.config = yamlconf.load(conf)

    @classmethod
    def tearDownClass(cls):
        filename = str(cls.agi_file.split(os.sep)[-1]).split('.')[0]
        test_file = os.path.join(cls.config['monitor']['storage'], 'autoconv', f'{filename}.mzml')
        if os.path.exists(test_file):
            os.remove(test_file)

    def test_pwizWorker(self):
        st_cli = MagicMock(name='stasis_cli_mock')
        st_cli.return_value.add_tracking.return_value = True

        print('creating queues', flush=True)
        cnv_q = Queue()
        aws_q = Queue()

        print('creating worker', flush=True)
        worker = PwizWorker(None, st_cli, cnv_q, aws_q, self.config['monitor'], test=False)

        worker.start()

        # should be skipped and
        print('adding bad file', flush=True)
        cnv_q.put_nowait('d:\\data\\DNU\\bad_file.d')

        print('adding good file', flush=True)
        # process next valid item in queue
        cnv_q.put_nowait('d:\\data\\monitored.d')

        worker.join(timeout=10)

        filename = str(self.agi_file.split(os.sep)[-1]).split('.')[0]
        test_file = os.path.join(self.config['monitor']['storage'], 'autoconv', f'{filename}.mzml')

        assert os.path.exists(test_file)
        assert 1 == aws_q.qsize()


    def test_update_storage(self):

        print('creating worker', flush=True)
        worker = PwizWorker(None, MagicMock(name='stasis_cli_mock'), Queue(), Queue(), self.config['monitor'])

        good_names = [
            'SantanaSerum031_MX625118_posCSH_CMS-21-2-H2S-Serum-035.d',
            'MX625118_posCSH_CMS-21-2-H2S-Serum-035.d',
            'SantanaSerum031_mx625118_posCSH_CMS-21-2-H2S-Serum-035.d',
            'mx625118_posCSH_CMS-21-2-H2S-Serum-035.d']
        bad_names = [
            '625118_posCSH_CMS-21-2-H2S-Serum-035.d',
            'MX62518_posCSH_CMS-21-2-H2S-Serum-035.d',
            'blahMX625118_posCSH_CMS-21-2-H2S-Serum-035.d'
        ]

        for n in good_names:
            res = worker.update_output(n)
            assert 'd:\\mzml\\mx625118\\' == res.lower()

        for n in bad_names:
            res = worker.update_output(n)
            assert 'd:\\mzml\\autoconv\\' == res.lower()
