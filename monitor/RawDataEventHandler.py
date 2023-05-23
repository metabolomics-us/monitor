#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import platform

import boto3
import watchtower
from stasis_client.client import StasisClient
from watchdog.events import RegexMatchingEventHandler

from monitor.QueueManager import QueueManager

FOLDERS_RX = r'^.*?\.d$'
FILES_RX = r'^.*?\.(?:raw|wiff|mzml)$'

logger = logging.getLogger('RawDataEventHandler')
h = watchtower.CloudWatchLogHandler(
    log_group_name=f'/lcb/monitor/{platform.node()}',
    log_group_retention_days=3,
    send_interval=30)
logger.addHandler(h)

class RawDataEventHandler(RegexMatchingEventHandler):
    """
    A custom file event handler for watchdog
    """

    def __init__(self, st_cli: StasisClient, queue_mgr: QueueManager, extensions, test: bool = False):
        """
        Args:
            st_cli: StasisClient
                Rest client object to interact with the stasis api
            queue_mgr: QueueManager
                A QueueManager object that handles setting up queues and sending/receiving messages
            extensions: array
                An array of valid lower cased file extensions (['.d', '.raw', '.wiff', '.mzml])
            test: Boolean
                A boolean indicating test run when True
        """

        super().__init__(regexes=[FOLDERS_RX, FILES_RX])

        self.sqs = boto3.client('sqs')

        self.stasis_cli = st_cli
        self.queue_mgr = queue_mgr
        self.extensions = extensions
        self.test = test

    def on_created(self, event):
        self.queue_mgr.put_message(self.queue_mgr.conversion_q(), event.src_path)

    def on_moved(self, event):
        self.queue_mgr.put_message(self.queue_mgr.conversion_q(), event.dest_path)
