#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import shutil
import time
from os import path
from queue import Queue
from threading import Thread

import yamlconf

from monitor.Bucket import Bucket
from monitor.Monitor import Monitor
from rest.dataformer.DataformerClient import DataformerClient
from rest.stasis.StasisClient import StasisClient


class TestMonitorApp(object):
    @classmethod
    def setup_class(cls):
        with open('../appconfig.yml', 'r') as conf:
            cls.config = yamlconf.load(conf)

    def create_file_delayed(self, raw_filename, tmpdir, delay, count):
        # print("\tfile creator sleeping for %d seconds..." % delay)
        # time.sleep(delay)
        # print("\twake up lazy thread!!!")
        #
        for c in range(count):
            raw_fname, raw_ext = path.splitext(raw_filename)
            destination = path.join(tmpdir, "extra", "path", "%s-%d%s" % (raw_fname.split(os.sep)[-1], c, raw_ext))
            not path.exists(destination) or os.makedirs(destination)
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

        agi_q = Queue()
        conv_q = Queue()
        aws_q = Queue()

        filemon = Monitor(self.config, st_cli, df_cli, agi_q, conv_q, aws_q)

        delay = 5
        count = 2

        mon_thread = Thread(target=filemon.start)
        mon_thread.daemon = True  # not allowed to have children

        raw_filename = path.join("..", "resources", "monitored.d")
        file_thread = Thread(target=self.create_file_delayed, args=(raw_filename, tmpdir, delay, count))

        print("about to start monitor")
        mon_thread.start()

        print("about to start file creation")
        file_thread.start()

        file_thread.join()
        print("file creator joined")

        agi_q.join()
        conv_q.join()
        aws_q.join()

        bucket = Bucket(bucket_name='data-carrot')
        raw_fname, raw_ext = path.splitext(raw_filename.split(os.sep)[-1])
        conv_file = path.join(tmpdir, "%s-0%s" % (raw_fname, ".mzml"))

        while not bucket.exists(conv_file):
            time.sleep(2)
