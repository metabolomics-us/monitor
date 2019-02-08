#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import time

from watchdog.events import FileSystemEventHandler, FileSystemEvent


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
            An array of valid lowercased file extensions (['.d', '.raw', '.mzml])
        test: Boolean
            A boolean indicating test run when True
    """

    def __init__(self, st_cli, conversion_q, upload_q, extensions, test=False):
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
        self.__process_event(event)

    def on_moved(self, event):
        """Called when a file or directory is moved or renamed.

        Parameters
        ----------
            event : DirCreatedEvent or FileCreatedEvent
                Event representing file/directory creation.
        """
        self.__process_event(event)

    def on_deleted(self, event):
        """Called when a file or directory is deleted.

        Parameters
        ----------
            event : FileSystemEvent
                Event representing file/directory deletion.
        """
        if ((event.is_directory and event.src_path[:-2] == '.d') or
                (not event.is_directory and event.src_path[:-5] == '.mzml')):
            print("DELETED: ", event.key)

    def __process_event(self, event: FileSystemEvent):
        """Does the actual processing of new and modified files (and agilent folders)

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
            if file_extension == '.d':
                print('[NewFileScanner] - Processing "%s" event for path: %s' % (evt_type, evt_path))

                dir_size = 0
                while dir_size < self.get_folder_size2(evt_path):
                    time.sleep(6)
                    dir_size = self.get_folder_size2(evt_path)

                # 3. trigger status acquired
                if not self.test:
                    print('updating stasis')
                    self.stasis_cli.set_tracking(evt_path, "acquired")
                # 3.5 add to conversion queue
                print('[NewFileScanner] - adding to conversion %s' % evt_path)
                self.conversion_q.put(evt_path)
        else:
            if file_extension == '.mzml':
                print('[NewFileScanner] - Processing %s event for file: %s' % (evt_type, evt_path))
                size = 0
                while size < os.stat(evt_path).st_size:
                    time.sleep(3)
                    size = os.stat(evt_path).st_size

                # this should be set in aws already,
                # maybe i can check before setting tracking info, just in case
                # if not self.test:
                #     print('updating stasis')
                #     self.stasis_cli.set_tracking(evt_path, "entered")
                #     self.stasis_cli.set_tracking(evt_path, "acquired")
                #     self.stasis_cli.set_tracking(evt_path, "converted")

                print('[NewFileScanner] - adding to upload %s' % evt_path)
                self.upload_q.put(evt_path)
            elif file_extension in self.extensions:
                print('[NewFileScanner] - Processing "%s" event for file: %s' % (evt_type, evt_path))

                dir_size = 0
                while dir_size < self.get_folder_size2(evt_path):
                    time.sleep(5)
                    dir_size = self.get_folder_size2(evt_path)

                # 3. trigger status acquired
                if not self.test:
                    print('updating stasis')
                    self.stasis_cli.set_tracking(evt_path, "acquired")
                # 3.5 add to conversion queue
                print('[NewFileScanner] - adding to conversion %s' % evt_path)
                self.conversion_q.put(evt_path)
            elif file_extension not in self.extensions:
                print(f'ERROR: event {{{evt_type}}} - don\'t know what to do with: {evt_path}')

    def get_folder_size(self, path):
        return os.stat(path).st_size

    def get_folder_size2(self, path):
        folder_size = 0
        for (path, dirs, files) in os.walk(path):
            for file in files:
                filename = os.path.join(path, file)
                folder_size += os.path.getsize(filename)
        return folder_size
