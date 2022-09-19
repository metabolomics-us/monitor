#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import subprocess
import time
from os.path import getsize, join
from pathlib import Path
from queue import Queue
from threading import Thread, Lock, local

from loguru import logger
from stasis_client.client import StasisClient


class PwizWorker(Thread):
    """
    Worker class that converts a raw data file to mzml
    """

    def __init__(self, parent, st_cli: StasisClient,
                 conversion_q: Queue, upload_q: Queue,
                 config, name='Converter0', daemon=True):
        """

        Args:
            parent:
                Instance parent object
            st_cli: StasisClient
                A stasis client instance
            conversion_q: Queue
                A queue that contains the filenames to be converted
            upload_q: Queue
                A queue that contains the filenames to be uploaded
            config:
                An object containing config settings
            name: str (Optional. Default: Converter0)
                Name of the worker instance
            daemon:
                Run the worker as daemon. (Optional. Default: True)

        """
        super().__init__(name=name, daemon=daemon)
        self.parent = parent
        self.running = False
        self.stasis_cli = st_cli
        self.conversion_q = conversion_q
        self.upload_q = upload_q
        self.config = config
        self.storage = config['monitor']['storage'] if config['monitor']['storage'].endswith(os.path.sep) else \
            config['monitor']['storage'] + os.path.sep
        self.test = config['test']

        self._lock = Lock()

        self.runner = config['monitor']['msconvert']
        self.args = ['--mzML', '-e', '.mzml', '--zlib',
                     '--filter', '"peakPicking true 1-"',
                     '--filter', '"zeroSamples removeExtra"',
                     '-o']

    def run(self):
        """Starts the processing of elements in the conversion queue"""
        self.running = True

        item = local()

        while self.running:
            try:
                item = self.conversion_q.get()
                file_basename, extension = str(item.split(os.sep)[-1]).split('.')

                # check if sample exists in stasis first
                if self.config['monitor']['exists']:
                    if not self.stasis_cli.sample_acquisition_exists(file_basename):
                        logger.info("File not in stasis, skipping")
                        continue

                # replace with regex and list of skip values from config var
                if any([re.search(x, item) is not None for x in self.config['monitor']['skip']]):
                    logger.info(f'Skipping conversion of DNU sample {item}')
                    continue

                logger.info(f'FILE: {item}')
                self.wait_for_item(item)

                if item.endswith('.mzml'):
                    self.upload_q.put_nowait(item)
                else:
                    try:
                        if not self.test:
                            self.convert(file_basename, extension, item)
                        else:
                            self.fake_convert(file_basename, extension, item)

                    except subprocess.CalledProcessError as cpe:
                        logger.warning(f'Conversion of {item} failed.')
                        logger.error({'command': cpe.cmd,
                                      'exit_code': cpe.returncode,
                                      'output': cpe.output,
                                      'stdout': cpe.stdout,
                                      'stderr': cpe.stderr})

                        if not self.test:
                            self.fail_sample(file_basename, extension)
                        else:
                            logger.warning(f'Fake StasisUpdate: Conversion of {item} failed')
                            logger.error({'command': cpe.cmd,
                                          'exit_code': cpe.returncode,
                                          'output': cpe.output,
                                          'stdout': cpe.stdout,
                                          'stderr': cpe.stderr})

            except KeyboardInterrupt:
                logger.warning(f'Stopping {self.name} due to Control+C')
                self.running = False
                self.conversion_q.queue.clear()
                self.upload_q.queue.clear()
                self.conversion_q.join()
                self.upload_q.join()
                self.parent.join_threads()

            except IndexError:
                time.sleep(1)

            except Exception as ex:
                if not self.test:
                    logger.error(f'Skipping conversion of sample {item} -- Error: {ex.args}')
                    filename, ext = str(item.split(os.sep)[-1]).split('.')
                    self.fail_sample(filename, ext)
                else:
                    logger.error(f'Fake StasisUpdate: Skipping conversion of sample {str(item)} -- Error: {ex.args}')

            finally:
                self.conversion_q.task_done()
                logger.info(f'Conversion queue size: {self.conversion_q.qsize()}')

        logger.info(f'\tStopping {self.name}')
        self.join()

    def convert(self, file_basename, extension, item):
        args = local()
        storage = self.update_output(item)
        args = [self.runner, item, *self.args, storage.lower()]

        logger.info(f'RUNNING: {args}')
        result = subprocess.run(args, stdout=subprocess.PIPE, check=True)
        if result.returncode == 0:
            resout = re.search(r'writing output file: (.*?)\n', result.stdout.decode('ascii')).group(1).strip()

            self.pass_sample(file_basename, extension)

            # update tracking status and upload to aws
            logger.info(f'\tAdd {resout} to upload queue')
            self.upload_q.put_nowait(resout)

        else:
            # update tracking status
            logger.warning(f'\tSetting {item} as failed')
            if not self.test:
                self.fail_sample(file_basename, extension)
            else:
                logger.warning(f'Fake StasisUpdate: Conversion of {item} failed')

    def fake_convert(self, filename_base, extension, item):
        args = local()
        storage = self.update_output(item)
        args = [self.runner, item, *self.args, storage]

        logger.info(f'RUNNING: {args}')
        logger.info(f'Fake StasisUpdate: Converted {storage}')
        resout = storage + filename_base + '.mzml'

        os.makedirs(os.path.dirname(resout), exist_ok=True)
        with open(resout, 'w') as d:
            d.flush()

        time.sleep(1)

        logger.info(f'Added {resout} to upload queue')
        self.upload_q.put_nowait(resout)

    def get_file_size(self, path):
        return os.stat(path).st_size

    def get_folder_size2(self, path):
        folder_size = 0
        for (path, dirs, files) in os.walk(path):
            logger.debug(f'\t\tscanning {path}')
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
        """Waits for a file or folder to be fully updated checking it's size"""
        size = -1
        curr = self.get_folder_size4(path) if os.path.isdir(path) else self.get_file_size(path)

        c = 0
        while size < curr:
            if path.endswith('.d') or path.endswith('.wiff'):
                if self.test:
                    time.sleep(5)
                else:
                    time.sleep(3)
            else:
                time.sleep(1)
            size = curr
            curr = self.get_folder_size4(path) if os.path.isdir(path) else self.get_file_size(path)

            if size == curr and c < 5:
                c += 1
            elif c == 5:
                break

    def pass_sample(self, file_basename, extension):
        try:
            logger.info(f'\tAdd "converted" status to stasis for sample "{file_basename}.{extension}"')
            self.stasis_cli.sample_state_update(file_basename, 'converted', f'{file_basename}.{extension}')
        except Exception as ex:
            logger.error(f'\tStasis client can\'t send "converted" status for sample {file_basename}\n'
                         f'\tResponse: {str(ex)}')
            pass

    def fail_sample(self, file_basename, extension):
        try:
            logger.error(f'\tAdd "failed" conversion status to stasis for sample "{file_basename}.{extension}"')
            self.stasis_cli.sample_state_update(file_basename, 'failed')
        except Exception as ex:
            logger.error(f'\tStasis client can\'t send "failed" status for sample {file_basename}\n'
                         f'\tResponse: {str(ex)}')

    def update_output(self, item):
        mxid = re.search(r'^(mx\d{6,7})_|_(mx\d{6,7})_', item, re.IGNORECASE + re.DOTALL + re.MULTILINE)

        if mxid and mxid[1] is None:
            storage = self.storage + mxid[2].lower() + os.path.sep
        elif mxid and mxid[2] is None:
            storage = self.storage + mxid[1].lower() + os.path.sep
        else:
            storage = self.storage + 'autoconv' + os.path.sep

        logger.info(f'\tStorage: {storage}')
        os.makedirs(storage, exist_ok=True)
        return storage
