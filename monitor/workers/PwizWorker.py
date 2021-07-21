#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import subprocess
import sys
import time
from queue import Queue
from threading import Thread

from loguru import logger
from stasis_client.client import StasisClient

logger.remove()
fmt = "<g>{time}</g> | <level>{level}</level> | <c>{name}:{function} ({line})</c> | <m>{thread.name}</m>\n{message}"
logger.add(sys.stderr, format=fmt, level="INFO")


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

    def __init__(self, st_cli: StasisClient, conversion_q, upload_q, storage, runner,
                 test=False, name='conversion_worker', daemon=True):
        super().__init__(name=name, daemon=daemon)
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
                if self.conversion_q.empty():
                    logger.info('Nothing to do, waiting for file to convert...')
                    time.sleep(5)
                    continue
                else:
                    logger.info(f'Approximate conversion queue size: {self.upload_q.qsize()}')

                    item = self.conversion_q.get_nowait()

                    file_basename = str(item.split(os.sep)[-1]).split('.')[0]

                    logger.info(f'FILE: {item} - {file_basename}')
                    curr = 0
                    time.sleep(1)
                    while os.path.getsize(item) > curr:
                        time.sleep(5)
                        curr = os.path.getsize(item)
                        logger.info(f'\t...file copying ({curr})...')

                    # result = subprocess.run([self.runner, item] + self.args, stdout=subprocess.PIPE, check=True)
                    #
                    # if result.returncode == 0:
                    #     resout = result.stdout.decode('ascii').split('writing output file: ')[-1].strip()
                    #     # update tracking status and upload to aws
                    #     logger.info(f'Added {resout} to upload queue')
                    #     if not self.test:
                    #         self.stasis_cli.sample_state_update(file_basename, 'converted', f'{file_basename}.mzml')
                    #     else:
                    #         logger.info(f'fake converted {evt_path}')
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
                logger.warning(f'skipping conversion of sample {str(item)}')
                logger.error({'command': cpe.cmd,
                             'exit_code': cpe.returncode,
                             'output': cpe.output,
                             'stdout': cpe.stdout,
                             'stderr': cpe.stderr})


                if not self.test:
                    self.stasis_cli.sample_state_update(str(item.split(os.sep)[-1]).split('.')[0], 'failed')
                else:
                    logger.info(f'Fake conversion failed {item}')

                self.conversion_q.task_done()
            except KeyboardInterrupt:
                logger.warning(f'Stopping {self.name} due to Control+C')

                self.running = False
                self.conversion_q.task_done()

                break
            except Exception as ex:
                logger.error(f'Skipping conversion of sample {str(item)} -- Error: {str(ex)}')

                if not self.test:
                    self.stasis_cli.sample_state_update(str(item.split(os.sep)[-1]).split('.')[0], 'failed')
                else:
                    logger.info(f'Fake conversion failed {item}')

                self.conversion_q.task_done()

        self.conversion_q.join()
