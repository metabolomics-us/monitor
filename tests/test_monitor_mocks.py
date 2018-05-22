import os
from multiprocessing import JoinableQueue
from threading import Thread
from unittest.mock import Mock, patch

import yamlconf

import monitor


class _TestMonitorWorkers(object):
    @classmethod
    def setup_class(cls):
        cls.mock_st_cli_patcher = patch('rest.stasis.StasisClient.StasisClient')
        cls.mock_df_cli_patcher = patch('rest.dataformer.DataformerClient.DataformerClient')
        cls.mock_file_scanner_patcher = patch('monitor.NewFileScanner')

        with open('../appconfig.yaml', 'r') as conf:
            cls.config = yamlconf.load(conf)

    @classmethod
    def teardown_class(cls):
        cls.mock_file_scanner_patcher.stop()
        cls.mock_st_cli_patcher.stop()
        cls.mock_df_cli_patcher.stop()

    def add_agilent_file(self, tmp):
        print('\n-----  creating agilent file  -----')
        agi_folder = '%s/test.d' % tmp
        os.makedirs('%s/AcqData' % agi_folder)

        with open('%s/desktop.ini' % agi_folder, 'w') as agi_file:
            agi_file.write("blah")
        with open('%s/AcqData/AcqMethod.xml' % agi_folder, 'w') as agi_file:
            agi_file.write('method')

    def test_start(self, tmpdir):
        print("config: %s" % self.config)

        self.mock_st_cli = Mock()
        self.mock_df_cli = Mock()

        self.config['monitor']['paths'] = [str(tmpdir)]

        zip_q = JoinableQueue()
        cnv_q = JoinableQueue()

        filemon = monitor.Monitor(self.config, self.mock_st_cli, self.mock_df_cli, zip_q, cnv_q)

        filemon.event_handler = self.mock_file_scanner

        monThread = Thread(target=filemon.start)
        monThread.setDaemon(True)
        monThread.start()

        self.add_agilent_file(tmpdir)

        monThread.join()
