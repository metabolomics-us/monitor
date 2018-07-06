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

    def __init__(self, st_cli, conversion_q, upload_q, extensions):
        self.stasis_cli = st_cli
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
        if 'acqdata' not in event.src_path.lower():
            self.__process_event(event.src_path, event.is_directory, event.event_type)

    def on_moved(self, event):
        """Called when a file or directory is moved or renamed.

        Parameters
        ----------
            event : DirCreatedEvent or FileCreatedEvent
                Event representing file/directory creation.
        """
        if 'acqdata' not in event.src_path.lower():
            self.__process_event(event.dest_path, event.is_directory, event.event_type)

    def __process_event(self, fpath, is_directory, evt_type):
        """Does the actual processing of new and modified files (and agilent folders)

        Parameters
        ----------
            event : DirCreatedEvent or FileCreatedEvent
                Event representing file/directory creation.
        """

        fileName, fileExtension = os.path.splitext(fpath)

        if fileExtension.lower() in self.extensions and fileExtension.lower() != '.mzml':
            print('[NewFileScanner] - Processing "%s" event for path: %s' % (evt_type, fpath))
            # 2. wait till the size stays constant
            size = 0
            while size < os.stat(fpath).st_size:
                time.sleep(1)
                size = os.stat(fpath).st_size

            # 3. trigger status acquired
            self.stasis_cli.set_tracking(fpath, "acquired")
            # 3.5 add to conversion queue
            print('[NewFileScanner] - adding %s to conversion' % fpath)
            self.conversion_q.put(fpath)
        elif fileExtension.lower() == '.mzml':
            print('[NewFileScanner] - Processing %s event for path: %s' % (evt_type, fpath))
            self.stasis_cli.set_tracking(fpath, "entered")
            self.stasis_cli.set_tracking(fpath, "acquired")
            self.stasis_cli.set_tracking(fpath, "converted")
            print('[NewFileScanner] - adding %s to upload' % fpath)
            self.upload_q.put(fpath)
