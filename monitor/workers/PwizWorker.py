#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import subprocess
import time
from queue import Queue
from threading import Thread

import win32com.client as com
from loguru import logger
from stasis_client.client import StasisClient


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

    def __init__(self, parent, st_cli: StasisClient, conversion_q, upload_q, storage, runner,
                 test=False, name='Converter0', daemon=True):
        super().__init__(name=name, daemon=daemon)
        self.parent = parent
        self.running = False
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
        self.running = True

        while self.running:
            try:
                logger.info(f'Approximate conversion queue size: {self.upload_q.qsize()}')

                item = self.conversion_q.get()

                # file_basename, extension = str(item.split(os.sep)[-1]).split('.')

                logger.info(f'FILE: {item}')

                self.wait_for_item(item)

                # result = subprocess.run([self.runner, item] + self.args, stdout=subprocess.PIPE, check=True)
                #
                # if result.returncode == 0:
                #     resout = result.stdout.decode('ascii').split('writing output file: ')[-1].strip()
                #     # update tracking status and upload to aws
                #     logger.info(f'Added {resout} to upload queue')
                #     if not self.test:
                #         self.stasis_cli.sample_state_update(file_basename, 'converted', file_basename+'.'+extension')
                #     else:
                #         logger.info(f'fake converted {item}')
                #
                #     self.upload_q.put(resout)
                # else:
                #     # update tracking status
                #     logger.info(f'Setting {path} as failed')
                #     if not self.test:
                #         self.stasis_cli.sample_state_update(file_basename, 'failed')
                #     else:
                #         logger.info(f'fake conversion failed {evt_path}')

                self.conversion_q.task_done()

            except subprocess.CalledProcessError as cpe:
                logger.warning(f'Conversion of {item} failed.')
                logger.error({'command': cpe.cmd,
                              'exit_code': cpe.returncode,
                              'output': cpe.output,
                              'stdout': cpe.stdout,
                              'stderr': cpe.stderr})

                if not self.test:
                    self.stasis_cli.sample_state_update(str(item.split(os.sep)[-1]).split('.')[0], 'failed')
                else:
                    logger.warning(f'Fake StasisUpdate: Conversion of {item} failed')
                    logger.error({'command': cpe.cmd,
                                  'exit_code': cpe.returncode,
                                  'output': cpe.output,
                                  'stdout': cpe.stdout,
                                  'stderr': cpe.stderr})

                self.conversion_q.task_done()
            except KeyboardInterrupt:
                logger.warning(f'Stopping {self.name} due to Control+C')
                self.running = False
                self.conversion_q.task_done()
                self.parent.join_threads()
                break
            except Exception as ex:
                logger.error(f'Skipping conversion of sample {str(item)} -- Error: {str(ex)}')

                if not self.test:
                    self.stasis_cli.sample_state_update(str(item.split(os.sep)[-1]).split('.')[0], 'failed')
                else:
                    logger.error(f'Fake StasisUpdate: Skipping conversion of sample {str(item)} -- Error: {str(ex)}')

                self.conversion_q.task_done()

        self.conversion_q.join()

    def get_folder_size(self, path):
        return os.stat(path).st_size

    def get_folder_size2(self, path):
        folder_size = 0
        for (path, dirs, files) in os.walk(path):
            for file in files:
                filename = os.path.join(path, file)
                folder_size += os.path.getsize(filename)
        return folder_size

    def get_folder_size3(self, path):
        fso = com.Dispatch("Scripting.FileSystemObject")
        return fso.GetFolder(path).Size

    def wait_for_item(self, path):
        """Waits for a file or folder to be fully updated"""
        size = 0
        while size < self.get_folder_size3(path):
            logger.info('\t\t...waiting for file copy...\t\t')
            time.sleep(1)
            size = self.get_folder_size3(path)
