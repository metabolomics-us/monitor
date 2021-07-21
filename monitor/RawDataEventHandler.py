#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import sys
import time
from queue import Queue

from loguru import logger
from stasis_client.client import StasisClient
from watchdog.events import FileSystemEventHandler, FileSystemEvent

logger.remove()
fmt = "<g>{time}</g> | <level>{level}</level> | <c>{name}:{function} ({line})</c> | <m>{thread.name}</m>\n{message}"
logger.add(sys.stderr, format=fmt, level="INFO")

AGILENT_CONTENTS = r'.*?\.d/\w.*'
WIFF_COMPANIONS = r'.*?\.(:?scan|~idx2)'


class RawDataEventHandler(FileSystemEventHandler):
    """A custom file event handler for watchdog

    Parameters
    ----------
        st_cli: StasisClient
            Rest client object to interact with the stasis api
        conversion_q: Queue
            Queue of raw data files to be converted to mzml
        extensions: array
            An array of valid lowercased file extensions (['.d', '.raw', '.mzml])
        test: Boolean
            A boolean indicating test run when True
    """

    def __init__(self, st_cli: StasisClient, conversion_q: Queue, upload_q: Queue, extensions, test: bool = False):
        super().__init__()
        self.stasis_cli = st_cli
        self.conversion_q = conversion_q
        self.upload_q = upload_q
        self.extensions = extensions
        self.test = test

    def on_created(self, event):
        """Called when a file or directory is created.

        Parameters
        ----------
            event : DirCreatedEvent or FileCreatedEvent
                Event representing file/directory creation.
        """
        if not re.match(AGILENT_CONTENTS, event.key[2]) and not re.match(WIFF_COMPANIONS, event.key[2]):
            self.__process_event__(event)
        else:
            logger.info(f'skipping companion file {event.key[2]}')
            return

    def on_moved(self, event):
        """Called when a file or directory is moved or renamed.

        Parameters
        ----------
            event : DirCreatedEvent or FileCreatedEvent
                Event representing file/directory creation.
        """
        if not re.match(AGILENT_CONTENTS, event.key[2]) and not re.match(WIFF_COMPANIONS, event.key[2]):
            self.__process_event__(event)
        else:
            logger.info(f'skipping companion file {event.key[2]}')
            return

    def on_modified(self, event):
        logger.info(f'\tmodified {event.key[2]}')
        return

    def __process_event__(self, event: FileSystemEvent):
        """Handles the file or folder events

        Parameters
        ----------
            event : DirCreatedEvent or FileCreatedEvent
                Event representing file/directory creation.
        """

        evt_type, evt_path, evt_is_dir = event.key[0:3]
        if 'moved' == evt_type:
            evt_path = event.key[2]
            evt_is_dir = event.key[3]

        file_name, file_extension = os.path.splitext(evt_path)

        if evt_is_dir:
            logger.info(f'dir {evt_path}')
            if file_extension == '.d':
                logger.info(f'agilent {evt_path}')
            else:
                logger.info(f'Processing {evt_type} event for path: {evt_path}')
                #
                # dir_size = 0
                # while dir_size < self.get_folder_size2(evt_path):
                #     time.sleep(6)
                #     dir_size = self.get_folder_size2(evt_path)
                #
                # # 3. trigger status acquired
                # if not self.test:
                #     logger.info('updating stasis')
                #     self.stasis_cli.sample_state_update(evt_path, 'acquired', file_extension)
                # else:
                #     logger.info(f'fake acquired {evt_path}')
                #
                # # 3.5 add to conversion queue
                # logger.info(f'Adding to conversion {evt_path}')
                # self.conversion_q.put(evt_path)
        else:
            logger.info(f'file {evt_path}')
            if file_extension == '.mzml':
                logger.info(f'Processing {evt_type} event for file: {evt_path}')
                size = 0
                while size < os.stat(evt_path).st_size:
                    time.sleep(3)
                    size = os.stat(evt_path).st_size
                    logger.info(f'File size: {size}')

                # this should be set in aws already,
                # maybe i can check before setting tracking info, just in case
                # if not self.test:
                #     logger.info('updating stasis')
                #     self.stasis_cli.set_tracking(evt_path, "entered")
                #     self.stasis_cli.set_tracking(evt_path, "acquired")
                #     self.stasis_cli.set_tracking(evt_path, "converted")

                logger.info(f'Adding to upload {evt_path}')
                self.upload_q.put(evt_path)

            elif file_extension in self.extensions:
                logger.info(f'Processing {evt_type} event for file: {evt_path}')
                # dir_size = 0
                # while dir_size < self.get_folder_size(evt_path):
                #     time.sleep(5)
                #     dir_size = self.get_folder_size(evt_path)
                #
                # # 3. trigger status acquired
                # if not self.test:
                #     logger.info('updating stasis')
                #     self.stasis_cli.sample_state_update(evt_path, 'acquired', )
                #
                # # 3.5 add to conversion queue
                # logger.info(f'Adding to conversion {evt_path}')
                # self.conversion_q.put(evt_path)

            elif file_extension not in self.extensions:
                logger.info(f'Unknown file type, conversion not available for: {evt_path}')

    def get_folder_size(self, path):
        return os.stat(path).st_size

    def get_folder_size2(self, path):
        folder_size = 0
        for (path, dirs, files) in os.walk(path):
            for file in files:
                filename = os.path.join(path, file)
                folder_size += os.path.getsize(filename)
        return folder_size
