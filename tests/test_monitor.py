import os
import shutil
import time
from multiprocessing import Process

import yamlconf

from monitor.Monitor import Monitor
from rest.dataformer.DataformerClient import DataformerClient
from rest.stasis.StasisClient import StasisClient


class TestMonitorApp(object):
    @classmethod
    def setup_class(cls):
        with open('../appconfig.yaml', 'r') as conf:
            cls.config = yamlconf.load(conf)

    def create_file_delayed(self, raw_filename, tmpdir, delay, count):
        print("\tfile creator sleeping for %d seconds..." % delay)
        time.sleep(delay)
        print("\twake up lazy thread!!!")

        for c in range(count):
            raw_fname, raw_ext = os.path.splitext(raw_filename)
            destination = "%s/extra/path/%s-%d%s" % (tmpdir, raw_fname.split('/')[-1], c, raw_ext)
            not os.path.exists(destination) or os.makedirs(destination)
            print("\tcopying %s to %s" % (raw_filename, destination))
            shutil.copytree(raw_filename, destination)
            time.sleep(1)

    def test_start(self, tmpdir):
        self.config['monitor']['paths'] = [str(tmpdir)]
        self.config['monitor']['storage'] = str(tmpdir)
        print("config: %s" % self.config)

        st_cli = StasisClient(self.config['stasis']['url'])
        df_cli = DataformerClient(self.config['dataform']['url'],
                                  self.config['dataform']['port'],
                                  self.config['monitor']['storage'])

        filemon = Monitor(self.config, st_cli, df_cli)

        delay = 3
        count = 5

        mon_thread = Process(target=filemon.start)
        # mon_thread.daemon = True # not allowed to have children

        raw_filename = "../resources/monitored.d"
        file_thread = Process(target=self.create_file_delayed, args=(raw_filename, tmpdir, delay, count))

        print("about to start monitor")
        mon_thread.start()

        print("about to start file creation")
        file_thread.start()

        file_thread.join()
        print("file creator joined")

        mon_thread.join(timeout=delay*count)
        print("monitor joined")
        mon_thread.terminate()

        raw_fname, raw_ewxt = os.path.splitext(raw_filename.split('/')[-1])

        conv_file = "%s/%s-0%s" % (tmpdir, raw_fname, ".mzml")
        assert os.path.exists(conv_file)
