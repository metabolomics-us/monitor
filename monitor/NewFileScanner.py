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
            An array of valid file extensions (['.d','.raw'])
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

        file = str(event.src_path)

        if (event.is_directory):
            # if it's a .d folder
            if (event.src_path.endswith('.d')):
                print("event %s" % event)
                # 3. add to zipping queue
                self.zipping_q.put(event.src_path)
                # 3.5 trigger status acquired
                self.stasis_cli.set_tracking(file, "acquired")
        else:
            # if it's a regular file
            fileName, fileExtension = os.path.splitext(file)

            if (fileExtension.lower() in self.extensions and not '.mzml'):
                print("event %s" % event)
                # 2. wait till the size stays constant
                size = 0
                while (size < os.stat(file).st_size):
                    time.sleep(1)
                    size = os.stat(str(file)).st_size
                    print(size)

                # 3. trigger status acquired
                self.stasis_cli.set_tracking(file, "acquired")
                # 3.5 add to conversion queue
                self.conversion_q.put(file)
            elif (fileExtension.lower() == '.mzml'):
                print("event %s" % event)
                self.upload_q.put(file)
