#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import shutil
import time
import unittest
from os import path
from queue import Queue
from threading import Thread

import yamlconf
from cisclient.client import CISClient
from stasis_client.client import StasisClient

from monitor.Bucket import Bucket
from monitor.Monitor import Monitor


class TestMonitorApp(unittest.TestCase):
    tmpdir = None

    @classmethod
    def setUpClass(cls):
        with open('appconfig-test.yml', 'r') as conf:
            cls.config = yamlconf.load(conf)
            cls.tmpdir = './tmp/'
        os.mkdir(cls.tmpdir)

    @classmethod
    def tearDownClass(cls):
        print('removing %s' % cls.tmpdir)
        shutil.rmtree(cls.tmpdir)

    def __create_file_delayed(self, raw_filename, tmpdir, count):
        for c in range(count):
            raw_fname, raw_ext = path.splitext(raw_filename)
            destination = path.join(tmpdir, "extra", "path", "%s-%d%s" % (raw_fname.split(os.sep)[-1], c, raw_ext))
            not path.exists(destination) or os.makedirs(destination)
            print("\tcopying %s to %s" % (raw_filename, destination))
            shutil.copytree(raw_filename, destination)

    def test_start(self):
        conv_q = Queue()
        aws_q = Queue()
        sched_q = Queue()

        self.config['monitor']['paths'] = [str(self.tmpdir)]
        self.config['monitor']['storage'] = './tmp/mzml'

        st_cli = StasisClient(os.getenv(self.config['stasis']['url_var'], "https://test-api.metabolomics.us/stasis"),
                              os.getenv(self.config['stasis']['api_key_var'],
                                        "9MjbJRbAtj8spCJJVTPbP3YWc4wjlW0c7AP47Pmi"))
        cis_cli = CISClient(os.getenv(self.config['cis']['url_var'], "https://test-api.metabolomics.us/cis"),
                            os.getenv(self.config['cis']['api_key_var'], "9MjbJRbAtj8spCJJVTPbP3YWc4wjlW0c7AP47Pmi"))
        filemon = Monitor(self.config, st_cli, cis_cli, conv_q, aws_q, sched_q)
        filemon.daemon = True

        print('creating file-copy thread', flush=True)
        count = 5  # how many files to create
        raw_filename = path.join(os.path.dirname(__file__),
                                 "..", "resources", "monitored.d")
        file_thread = Thread(target=self.__create_file_delayed, args=(raw_filename, self.tmpdir, count))

        print("about to start monitor", flush=True)
        filemon.start()

        print("about to start file creation", flush=True)
        time.sleep(5)  # wait for monitor to start completely
        file_thread.start()

        print('joining file creator - %s' % time.ctime(), flush=True)
        file_thread.join()

        print('joining filemonitor - %s' % time.ctime(), flush=True)
        filemon.running = False
        filemon.join(timeout=4 * count)

        raw_fname, raw_ext = path.splitext(raw_filename.split(os.sep)[-1])
        conv_files = [path.join("%s-%d%s" % (raw_fname, x, ".mzml")) for x in range(count)]

        print('\n\nchecking bucket files', flush=True)
        bucket = Bucket(bucket_name='data-carrot')

        print('bucket should have files: ' + ', '.join(conv_files), flush=True)
        assert all([bucket.exists(conv_file) for conv_file in conv_files])

        # cleanup
        [bucket.delete(cf) for cf in conv_files]
