#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import subprocess
import time
from collections import deque
from os.path import getsize, join
from pathlib import Path
from threading import Thread, Lock, local

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

    def __init__(self, parent, st_cli: StasisClient, conversion_q: deque, upload_q: deque, storage, runner,
                 test=False, name='Converter0', daemon=True):
        super().__init__(name=name, daemon=daemon)
        self.parent = parent
        self.running = False
        self.stasis_cli = st_cli
        self.conversion_q = conversion_q
        self.upload_q = upload_q
        self.storage = storage if storage.endswith(os.path.sep) else storage + os.path.sep
        self.test = test
        self._lock = Lock()

        self.runner = runner
        self.args = ['--mzML', '-e', '.mzml', '--zlib',
                     '--filter', '"peakPicking true 1-"',
                     '--filter', '"zeroSamples removeExtra"',
                     '-o']

    def run(self):
        """Starts the processing of elements in the conversion queue"""
        item = local()
        self.running = True

        while self.running:
            try:
                item = self.conversion_q.popleft()
                mxid = re.search(r'_(mx\d{6,7})_', item, re.IGNORECASE)

                if mxid:
                    self.storage = self.storage + mxid.group(1).lower() + os.path.sep
                else:
                    self.storage = self.storage + 'autoconv' + os.path.sep
                self.args.append(self.storage)

                file_basename, extension = str(item.split(os.sep)[-1]).split('.')

                logger.info(f'FILE: {item}')
                self.wait_for_item(item)

                if any([x in item for x in [f'{os.sep}DNU{os.sep}', 'preinj']]):
                    logger.info('Skipping conversion of DNU sample {item}')
                    continue

                if item.endswith('.mzml'):
                    self.upload_q.append(item)
                else:
                    if not self.test:
                        logger.info(f'RUNNING: {[self.runner, item, *self.args]}')
                        result = subprocess.run([self.runner, item] + self.args, stdout=subprocess.PIPE, check=True)

                        if result.returncode == 0:
                            logger.info('RESULT: ' + result)
                            resout = result.stdout.decode('ascii').split('writing output file: ')[-1].strip()
                            # update tracking status and upload to aws
                            logger.info(f'Added {resout} to upload queue')
                            logger.info(
                                f'--- add "converted" status to AWS tracking table for sample "{file_basename}.{extension}"')
                            # self.pass_sample(extension, file_basename)
                            self.upload_q.append(resout)

                        else:
                            # update tracking status
                            logger.warning(f'Setting {item} as failed')
                            if not self.test:
                                logger.error(
                                    f'--- add "failed conversion" status to AWS tracking table for sample "{file_basename}.{extension}"')
                                # self.fail_sample(str(item.split(os.sep)[-1]).split('.')[0])
                            else:
                                logger.warning(f'Fake StasisUpdate: Conversion of {item} failed')
                    else:
                        logger.info(f'Fake StasisUpdate: Converted {item}')
                        logger.info(f'RUNNING: {[self.runner, item] + self.args}')
                        resout = self.storage + file_basename + '.mzml'
                        os.makedirs(os.path.dirname(resout), exist_ok=True)
                        with open(resout, 'w') as d:
                            d.flush()

                        time.sleep(1)

                        logger.info(f'Added {resout} to upload queue')
                        self.upload_q.append(resout)

            except subprocess.CalledProcessError as cpe:
                logger.warning(f'Conversion of {item} failed.')
                logger.error({'command': cpe.cmd,
                              'exit_code': cpe.returncode,
                              'output': cpe.output,
                              'stdout': cpe.stdout,
                              'stderr': cpe.stderr})

                if not self.test:
                    logger.error(
                        f'--- add "failed conversion" status to AWS tracking table for sample "{file_basename}.{extension}"')
                    # self.fail_sample(str(item.split(os.sep)[-1]).split('.')[0])
                else:
                    logger.warning(f'Fake StasisUpdate: Conversion of {item} failed')
                    logger.error({'command': cpe.cmd,
                                  'exit_code': cpe.returncode,
                                  'output': cpe.output,
                                  'stdout': cpe.stdout,
                                  'stderr': cpe.stderr})
                continue

            except KeyboardInterrupt:
                logger.warning(f'Stopping {self.name} due to Control+C')
                self.running = False
                self.parent.join_threads()

            except IndexError:
                time.sleep(1)
                continue

            except Exception as ex:
                logger.error(f'Skipping conversion of sample {item} -- Error: {str(ex)}')

                if not self.test:
                    logger.error(
                        f'--- add "failed conversion" status to AWS tracking table for sample "{file_basename}.{extension}"')
                    # self.fail_sample(str(item.split(os.sep)[-1]).split('.')[0])
                else:
                    logger.error(f'Fake StasisUpdate: Skipping conversion of sample {str(item)} -- Error: {str(ex)}')
                continue

            logger.info(f'next task, queue size? {len(self.conversion_q)}')
        logger.info(f'Stopping {self.name}')

    def get_file_size(self, path):
        return os.stat(path).st_size

    def get_folder_size2(self, path):
        folder_size = 0
        for (path, dirs, files) in os.walk(path):
            logger.info(f'\t\tscanning {path}')
            for file in files:
                filename = os.path.join(path, file)
                folder_size += os.path.getsize(filename)
        return folder_size

    def get_folder_size3(self, path):
        root_directory = Path(path)
        return sum(f.stat().st_size for f in root_directory.glob('**/*') if f.is_file())

    def get_folder_size4(self, path):
        dirs_dict = {}
        my_size = 0
        for (path, dirs, files) in os.walk(path, topdown=False):
            size = sum(getsize(join(path, name)) for name in files)
            subdir_size = sum(dirs_dict[join(path, d)] for d in dirs)
            my_size = dirs_dict[path] = size + subdir_size

        return my_size

    def wait_for_item(self, path):
        """Waits for a file or folder to be fully updated"""
        size = 0
        curr = 0
        c = 0
        while size <= curr:
            size = curr
            time.sleep(0.5)
            curr = self.get_folder_size4(path) if os.path.isdir(path) else self.get_file_size(path)
            if size == curr and c < 5:
                c += 1
            elif c == 5:
                break

    def pass_sample(self, extension, file_basename):
        self.stasis_cli.sample_state_update(file_basename, 'converted', f'{file_basename}.{extension}')

    def fail_sample(self, item):
        self.stasis_cli.sample_state_update(item, 'failed')
