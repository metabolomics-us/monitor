#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import time

from watchdog.events import FileSystemEventHandler


class NewFileScanner(FileSystemEventHandler):
    """A custom file scanner

    Parameters
    ----------
        st_cli: StasisClient
            Rest client object to interact with the stasis api
        zipping_q: Queue
            Queue of folders to be zipped.
        conversion_q: Queue
            Queue of raw data files to be converted to mzml
        extensions: array
            An array of valid file extensions (['.d', '.D','.raw'])
    """

    def __init__(self, st_cli, zipping_q, conversion_q, upload_q, extensions):
        self.stasis_cli = st_cli
        self.zipping_q = zipping_q
        self.conversion_q = conversion_q
        self.upload_q = upload_q
        self.extensions = extensions

    def on_created(self, event):
        """Called when a file or directory is created.

        Parameters
        ----------
            event : DirCreatedEvent or FileCreatedEvent
                Event representing file/directory creation.
        """
        self.__process_event(event.src_path, event.is_directory, event.event_type)

    def on_moved(self, event):
        """Called when a file or directory is mopved or renamed.

        Parameters
        ----------
            event : DirCreatedEvent or FileCreatedEvent
                Event representing file/directory creation.
        """
        self.__process_event(event.dest_path, event.is_directory, event.event_type)

    def __process_event(self, path, is_directory, evt_type):
        """Does the actual processing of new and modified files (and agilent folders)

        Parameters
        ----------
            event : DirCreatedEvent or FileCreatedEvent
                Event representing file/directory creation.
        """
        if is_directory:
            # if it's a .d/.D folder
            if path.lower().endswith('.d'):
                print('Processing %s for path: %s' % (evt_type, path))
                # 3. add to zipping queue
                self.zipping_q.put(path)
                # 3.5 trigger status acquired
                self.stasis_cli.set_tracking(path, "acquired")
        else:
            # if it's a regular file
            fileName, fileExtension = os.path.splitext(path)

            if fileExtension.lower() in self.extensions and not '.mzml':
                print('Processing %s for path: %s' % (evt_type, path))
                # 2. wait till the size stays constant
                size = 0
                while (size < os.stat(path).st_size):
                    time.sleep(1)
                    size = os.stat(path).st_size

                # 3. trigger status acquired
                self.stasis_cli.set_tracking(path, "acquired")
                # 3.5 add to conversion queue
                self.conversion_q.put(path)
            elif fileExtension.lower() == '.mzml':
                print('Processing %s for path: %s' % (evt_type, path))
                self.upload_q.put(path)
