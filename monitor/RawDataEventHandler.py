#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import platform

import boto3
import watchtower
from watchdog.events import RegexMatchingEventHandler

from monitor.QueueManager import QueueManager
from monitor.client.BackendClient import BackendClient

FOLDERS_RX = r'^.*?\.d$'
FILES_RX = r'^.*?\.(?:raw|wiff|mzml)$'

logger = logging.getLogger('RawDataEventHandler')
if not logger.handlers:
    h = watchtower.CloudWatchLogHandler(
        log_group_name=f'/lcb/monitor/{platform.node()}',
        log_group_retention_days=3,
        send_interval=30)
    logger.addHandler(h)


class RawDataEventHandler(RegexMatchingEventHandler):
    """
    A custom file event handler for watchdog
    """

    def __init__(self, backend_cli: BackendClient, queue_mgr: QueueManager, extensions, test: bool = False):
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

        self.backend_cli = backend_cli
        self.queue_mgr = queue_mgr
        self.extensions = extensions
        self.test = test

    def on_created(self, event):
        logger.debug(f'file created: {event.src_path}')
        self.queue_mgr.put_message(self.queue_mgr.conversion_q(), event.src_path)

    def on_moved(self, event):
        logger.debug(f'file moved: {event.dest_path}')
        self.queue_mgr.put_message(self.queue_mgr.conversion_q(), event.dest_path)
