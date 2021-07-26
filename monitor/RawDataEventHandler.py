#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
from queue import Queue

from loguru import logger
from stasis_client.client import StasisClient
from watchdog.events import FileSystemEventHandler, FileCreatedEvent, DirCreatedEvent, FileMovedEvent, DirMovedEvent

AGILENT_REGEX = r'^.*?\.d$'
WIFF_REGEX = r'^.*?\.wiff$'
OTHERS_REGEX = r'^.*?\.(:?raw|mzml)$'

FOLDERS_RX = r'^.*?\.d$'
FILES_RX = r'^.*?\.(:?raw|wiff|mzml)$'

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
        # if any([re.match(rex, event.src_path) for rex in [AGILENT_REGEX, WIFF_REGEX, OTHERS_REGEX]]):
        if self.valid_event(event):
            # logger.info(f'{type(event)} created {event.src_path}')
            self.add_to_queue(event.src_path)
        # else:
        #     logger.info(f'skipping {event.src_path}')

    def on_moved(self, event):
        """Called when a file or directory is moved or renamed.

        Parameters
        ----------
            event : DirMovedEvent or FileMovedEvent
                Event representing file/directory creation.
        """
        # if any([re.match(rex, event.src_path) for rex in [AGILENT_REGEX, WIFF_REGEX, OTHERS_REGEX]]):
        if self.valid_event(event):
            # logger.info(f'moved {event.dest_path}')
            self.add_to_queue(event.dest_path)
        # else:
            # logger.info(f'skipping {event.src_path}')

    def add_to_queue(self, path):
        file, extension = os.path.splitext(path)
        if path.endswith('.mzml'):
            logger.info(f'Adding {file} to upload queue')
            self.upload_q.put(path)
        elif extension in self.extensions:
            logger.info(f'Adding {file} to conversion queue')
            self.conversion_q.put(path)
        else:
            logger.error(f'Invalid file {path}')

    def valid_event(self, event) -> bool:
        if isinstance(event, FileCreatedEvent):
            return re.match(FILES_RX, event.src_path) is not None
        elif isinstance(event, DirCreatedEvent):
            return re.match(FOLDERS_RX, event.src_path) is not None
        elif isinstance(event, FileMovedEvent):
            return re.match(FILES_RX, event.dest_path) is not None
        elif isinstance(event, DirMovedEvent):
            return re.match(FOLDERS_RX, event.dest_path) is not None
