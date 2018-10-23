#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import subprocess
from os import path
from threading import Thread


class PwizWorker(Thread):
    """Worker class that converts a raw data file to mzml

    Parameters
    ----------
        st_cli: StasisClient
            Rest client object to interact with the stasis api
        conversion_q: Queue
            A queue to hold the files to be converted to mzml
        upload_q: Queue
            A queue to hold the files to be uploaded to aws bucket
        storage: str
            A path for storing converted mzml files
        runner: str
            The path to the msconvert executable
    """

    def __init__(self, st_cli, conversion_q, upload_q, storage, runner,
                 test=False, name='conversion_worker', daemon=True):
        super().__init__(name=name, daemon=daemon)
        self.running = True
        self.stasis_cli = st_cli
        self.conversion_q = conversion_q
        self.upload_q = upload_q
        self.storage = storage
        self.test = test

        self.runner = runner
        self.args = ['--mzML', '-e', '.mzml', '--zlib',
                     '--filter', '"peakPicking true 1-"',
                     '--filter', '"zeroSamples removeExtra"',
                     '-o', os.path.join('.', 'tmp')]

    def run(self):
        """Starts the processing of elements in the conversion queue"""

        item = None
        while self.running:
            try:
                if self.conversion_q.empty():
                    print('[PwizWorker] - nothing to do, waiting for file to convert...')

                item = self.conversion_q.get()

                file_basename = str(item.split(os.sep)[-1]).split('.')[0]

                result = subprocess.run([self.runner, item] + self.args, stdout=subprocess.PIPE, check=True)

                if result.returncode == 0:
                    resout = result.stdout.decode('ascii').split('writing output file: ')[-1].strip()
                    # update tracking status and upload to aws
                    print('[PwizWorker] - added %s to upload queue' % resout)
                    if not self.test:
                        self.stasis_cli.set_tracking(file_basename, 'converted')

                    self.upload_q.put(resout)
                else:
                    # update tracking status
                    print('[PwizWorker] - setting %s as failed' % path)
                    if not self.test:
                        self.stasis_cli.set_tracking(file_basename, 'failed')

                # self.conversion_q.task_done()

            except subprocess.CalledProcessError as cpe:
                print('[PwizWorker] - %s' % {'command': cpe.cmd,
                                             'exit_code': cpe.returncode,
                                             'output': cpe.output,
                                             'stdout': cpe.stdout,
                                             'stderr': cpe.stderr})
                print('[PwizWorker] - skipping conversion of sample %s' % str(item))
                if not self.test:
                    self.stasis_cli.set_tracking(str(item.split(os.sep)[-1]).split('.')[0], 'failed')

                # self.conversion_q.task_done()
            except KeyboardInterrupt:
                print('[PwizWorker] - stopping conversion_worker')
                self.running = False
                # self.conversion_q.join()
            except Exception as ex:
                print('[PwizWorker] - skipping conversion of sample %s -- Error: %s' % (str(item), str(ex)))
                if not self.test:
                    self.stasis_cli.set_tracking(str(item.split(os.sep)[-1]).split('.')[0], 'failed')
            finally:
                self.conversion_q.task_done()
