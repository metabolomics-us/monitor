#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import os
import platform
from typing import Optional

import requests
import watchtower
from requests.adapters import HTTPAdapter
from retry import retry
from urllib3 import Retry

logger = logging.getLogger('PwizWorker')
h = watchtower.CloudWatchLogHandler(
    log_group_name=f'/lcb/monitor/{platform.node()}',
    log_group_retention_days=3,
    send_interval=30)
logger.addHandler(h)

RETRY_COUNT = 3


class BackendClient:

    def __init__(self, url: Optional[str] = None, token: Optional[str] = None):
        """
        the client requires an url where to connect against and the related token.
        """
        self.logger = logging.getLogger(self.__class__.__name__)

        self._url = url
        self._url_old = "https://api.metabolomics.us/stasis"
        self._token = token

        if self._token is None:
            # utilize env
            self._token = os.getenv('STASIS_API_TOKEN', os.getenv('PROD_STASIS_API_TOKEN'))
        if self._url is None:
            self._url = os.getenv('STASIS_API_URL', 'https://api.metabolomics.us/gostasis')
        if self._url_old is None:
            self._url_old = os.getenv('STASIS_API_URL', 'https://api.metabolomics.us/gostasis')

        if self._token is None:
            raise Exception("you need to to provide a stasis api token in the env variable 'STASIS_API_TOKEN'")

        if self._url is None:
            raise Exception("you need to provide a url in the env variable 'STASIS_API_URL'")

        self._schedule_url = self._url.replace('/gostasis', '/scheduler')
        self._minix_url = self._url.replace('/gostasis', '/experiment')
        self._header = {
            'Content-type': 'application/json',
            'Accept': 'application/json',
            'x-api-key': f'{self._token}'
        }

        retry_strategy = Retry(
            total=500,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "OPTIONS", "POST"],
            backoff_factor=1
        )

        logging.debug("configuring http client")
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.http = requests.Session()
        logging.debug("utilizing url %s", self._url)

    @retry(exceptions=Exception, tries=RETRY_COUNT, delay=1, backoff=2)
    def sample_acquisition_exists(self, sample_name) -> bool:
        """
        returns the acquisition data of this sample
        """
        logging.debug("getting acquisition data for sample %s", sample_name)
        result = self.http.get(f'{self._url_old}/acquisition/{sample_name}', headers=self._header)
        if result.status_code != 200:
            return False
        return True

    @retry(exceptions=Exception, tries=RETRY_COUNT, delay=1, backoff=2)
    def sample_acquisition_get(self, sample_name) -> dict:
        """
        returns the acquisition data of this sample
        """
        logging.debug("getting acquisition data for sample %s", sample_name)
        result = self.http.get(f'{self._url_old}/acquisition/{sample_name}', headers=self._header)
        if result.status_code == 200:
            return result.json()
        elif result.status_code == 404:
            raise Exception("acquisition data not found")
        else:
            raise Exception(f"we observed an error. Status code was {result.status_code} "
                            f"and error was {result.reason} for sample {sample_name}")

    @retry(exceptions=Exception, tries=RETRY_COUNT, delay=1, backoff=2)
    def sample_state_update(self, sample_name: str, state, file_handle: Optional[str] = None):
        """
        updates a sample state in the remote system
        """
        logging.debug("updating state for sample %s to %s", sample_name, state)
        data = {
            "sample": sample_name,
            "status": state,
        }

        if file_handle is not None:
            data['fileHandle'] = file_handle

        result = self.http.post(f'{self._url}/tracking', json=data, headers=self._header)
        if result.status_code != 200: raise Exception(
            f"we observed an error. Status code was {result.status_code} and error was {result.reason} and {sample_name} in {state} with {file_handle}")
        return result
