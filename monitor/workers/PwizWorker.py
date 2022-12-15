#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import os
import re
import subprocess
import tempfile
import time
from os.path import getsize, join
from pathlib import Path
from threading import Thread, Lock, local

from loguru import logger
from stasis_client.client import StasisClient

from monitor.QueueManager import QueueManager


class PwizWorker(Thread):
    """
    Worker class that converts a raw data file to mzml
    """

    def __init__(self, parent, st_cli: StasisClient, queue_mgr: QueueManager, config,
                 name='Converter0', daemon=True):
        """

        Args:
            parent:
                Instance parent object
            st_cli: StasisClient
                A stasis client instance
            queue_mgr: QueueManager
                A QueueManager object that handles setting up queues and sending/receiving messages
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
        self.queue_mgr = queue_mgr
        self.stasis_cli = st_cli
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
                item = self.queue_mgr.get_next_message(self.queue_mgr.conversion_q())
                if not item:
                    logger.debug('\twaiting...')
                    time.sleep(1.7)
                    continue

                splits = str(item.split(os.sep)[-1]).rsplit('.', 1)
                file_basename = splits[0]
                logging.debug(f'base: {file_basename}')
                extension = splits[1] if len(splits) == 2 else ''

                result = [re.search(x, item) is not None for x in self.config['monitor']['skip']]
                if any(result):
                    logger.info(f'\tSkipping conversion of invalid sample: {item}.')
                    continue

                # check if sample exists in stasis first
                if self.config['monitor']['exists']:
                    logging.debug(f"Skipping non-existent files")
                    if not self.stasis_cli.sample_acquisition_exists(file_basename):
                        logger.info('File not in stasis, skipping.')
                        continue

                logger.info(f'Starting conversion of {item}')

                self.wait_for_item(item)

                if item.endswith('.mzml'):
                    self.queue_mgr.put_message(self.queue_mgr.upload_q, item)
                else:
                    # add acquired status
                    self.pass_sample('acquired', file_basename, extension)

                    # try conversion and update status
                    try:
                        self.convert(file_basename, extension, item)

                    except subprocess.CalledProcessError as cpe:
                        logger.warning(f'Conversion of {item} failed.')
                        logger.error({'command': cpe.cmd,
                                      'exit_code': cpe.returncode,
                                      'output': cpe.output,
                                      'stdout': cpe.stdout,
                                      'stderr': cpe.stderr})

                        self.fail_sample(file_basename, extension, reason=str(cpe))

                size = self.queue_mgr.get_size(self.queue_mgr.conversion_q())
                logger.info(f'Conversion queue size: {size}')

            except KeyboardInterrupt:
                logger.warning(f'Stopping {self.name} due to Control+C')
                self.running = False
                self.parent.join_threads()

            except IndexError as ex:
                logger.error(str(ex))
                time.sleep(1)

            except Exception as ex:
                logger.error(f'Skipping conversion of sample {item} -- Error: {ex.args}')
                filename, ext = str(item.split(os.sep)[-1]).split('.')
                self.fail_sample(filename, ext, reason=str(ex))

            finally:
                pass
                # size = self.queue_mgr.get_size(self.queue_mgr.conversion_q())
                # logger.info(f'Conversion queue size: {size}')

        logger.info(f'\tStopping {self.name}')
        self.join()

    def convert(self, file_basename, extension, item):
        """
        Converts a sample
        Args:
            file_basename: sample's filename (no extension)
            extension: sample's extension
            item: full sample name

        Returns:

        """
        # pw_args = local()
        storage = tempfile.gettempdir()

        pw_args = [self.runner, item, *self.args, storage.lower()]

        logger.info(f'\tRunning ProteoWizard: {pw_args}')
        result = subprocess.run(pw_args, stdout=subprocess.PIPE, check=True)

        if result.returncode == 0:
            resout = re.search(r'writing output file: (.*?)\n', result.stdout.decode('ascii')).group(1).strip()

            # update tracking status and upload to aws
            self.pass_sample('converted', file_basename, extension)

            logger.info(f'\tAdd {resout} to upload queue')
            self.queue_mgr.put_message(self.queue_mgr.upload_q(), resout)

        else:
            # update tracking status
            logger.warning(f'\tSetting {item} as failed')
            self.fail_sample(file_basename, extension, reason=result.stdout.decode('ascii'))

    def get_file_size(self, path):
        return os.stat(path).st_size

    def get_folder_size2(self, path):
        folder_size = 0
        for (path, dirs, files) in os.walk(path):
            logger.debug(f'\tscanning {path}')
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
                time.sleep(3)
            else:
                time.sleep(1)
            size = curr
            curr = self.get_folder_size4(path) if os.path.isdir(path) else self.get_file_size(path)

            if size == curr and c < 5:
                c += 1
            elif c == 5:
                break

    def pass_sample(self, status, file_basename, extension):
        try:
            logger.info(f'\tAdd "{status}" status to stasis for sample "{file_basename}.{extension}"')
            self.stasis_cli.sample_state_update(file_basename, status, f'{file_basename}.{extension}')
        except Exception as ex:
            logger.error(f'\tStasis client can\'t send "{status}" status for sample {file_basename}. '
                         f'\tResponse: {str(ex)}')
            pass

    def fail_sample(self, file_basename, extension, reason: str):
        try:
            logger.error(f'\tAdd "failed" conversion status to stasis for sample "{file_basename}.{extension}"')
            self.stasis_cli.sample_state_update(file_basename, 'failed', reason=reason)
        except Exception as ex:
            logger.error(f'\tStasis client can\'t send "failed" status for sample {file_basename}.{extension}. '
                         f'\tResponse: {str(ex)}')

    def update_output(self, item):
        mxid = re.search(r'^(mx\d{6,7})_|_(mx\d{6,7})_', item, re.IGNORECASE + re.DOTALL + re.MULTILINE)

        if mxid and mxid[1] is None:
            storage = self.storage + mxid[2].lower() + os.path.sep
        elif mxid and mxid[2] is None:
            storage = self.storage + mxid[1].lower() + os.path.sep
        else:
            storage = self.storage + 'autoconv' + os.path.sep

        logger.info(f'\tSet conversion output folder to: {storage}')
        os.makedirs(storage, exist_ok=True)
        return storage
