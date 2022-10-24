#!/usr/bin/env python
# -*- coding: utf-8 -*-
from multiprocessing.queues import Queue

from stasis_client.client import StasisClient
from watchdog.events import RegexMatchingEventHandler

FOLDERS_RX = r'^.*?\.d$'
FILES_RX = r'^.*?\.(:?raw|wiff|mzml)$'


class RawDataEventHandler(RegexMatchingEventHandler):
    """A custom file event handler for watchdog

    Parameters
    ----------
        st_cli: StasisClient
            Rest client object to interact with the stasis api
        conversion_q: Queue
            Queue of raw data files to be converted to mzml
        extensions: array
            An array of valid lower cased file extensions (['.d', '.raw', '.wiff', '.mzml])
        test: Boolean
            A boolean indicating test run when True
    """

    def __init__(self, st_cli: StasisClient, conversion_q: Queue, upload_q: Queue, extensions, test: bool = False):
        super().__init__(regexes=[FOLDERS_RX, FILES_RX])
        self.stasis_cli = st_cli
        self.conversion_q = conversion_q
        self.upload_q = upload_q
        self.extensions = extensions
        self.test = test

    def on_created(self, event):
        self.conversion_q.put_nowait(event.src_path)

    def on_moved(self, event):
        self.conversion_q.put_nowait(event.dest_path)
