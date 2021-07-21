#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import time
from queue import Queue

from loguru import logger
from stasis_client.client import StasisClient
from watchdog.events import FileSystemEventHandler, FileSystemEvent

AGILENT_REGEX = r'^.*?\.d$'
WIFF_REGEX = r'^.*?\.wiff$'


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
        if re.match(AGILENT_REGEX, event.src_path) or re.match(WIFF_REGEX, event.src_path):
            logger.debug(f'{type(event)} created {event.src_path}')
            self.add_to_queue(event.src_path, event.is_directory)

    def on_moved(self, event):
        """Called when a file or directory is moved or renamed.

        Parameters
        ----------
            event : DirCreatedEvent or FileCreatedEvent
                Event representing file/directory creation.
        """
        if re.match(AGILENT_REGEX, event.src_path) or re.match(WIFF_REGEX, event.src_path):
            logger.debug(f'moved {event.dest_path}')
            self.add_to_queue(event.src_path, event.is_directory)

    def add_to_queue(self, path, is_directory):
        file, extension = os.path.splitext(path)
        if extension is '.mzml':
            self.upload_q.put(path)
        elif extension in self.extensions:
            self.wait_for_item(path)
            self.conversion_q.put(path)
        else:
            logger.error(f'Invalid file {path}')

    def wait_for_item(self, path):
        size = 0
        while size < self.get_folder_size2(path):
            logger.info('\t\t...waiting for file copy...\t\t')
            time.sleep(5)
            size = self.get_folder_size2(path)



    def process_event(self, event: FileSystemEvent, type, src, dest=None, is_dir=False):
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

        file_name_src, file_extension_src = os.path.splitext(src)
        file_name_dest, file_extension_dest = os.path.splitext(dest) if dest is not None else None, None

        if is_dir:
            if file_extension_src == '.d' or file_extension_dest == '.d':
                logger.info(f'agilent {evt_path}')
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
                logger.info(f'Processing {type} event for path: {src}')
        else:
            logger.info(f'file {src & dest}')
            if file_extension_src == '.mzml' or file_extension_dest == '.mzml':
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

            elif file_extension_src in self.extensions or file_extension_dest in self.extensions:
                logger.info(f'Processing {type} event for file: {src & dest}')
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

            else:
                logger.info(f'Unknown file type, conversion not available for: {src and dest}')

    def get_folder_size(self, path):
        return os.stat(path).st_size

    def get_folder_size2(self, path):
        folder_size = 0
        for (path, dirs, files) in os.walk(path):
            for file in files:
                filename = os.path.join(path, file)
                folder_size += os.path.getsize(filename)
        return folder_size
