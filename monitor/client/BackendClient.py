#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import os
import platform
import urllib.parse
from typing import Optional

import requests
import watchtower
from requests.adapters import HTTPAdapter
from retry import retry
from urllib3 import Retry

logger = logging.getLogger('BackendClient')
h = watchtower.CloudWatchLogHandler(
    log_group_name=f'/lcb/monitor/{platform.node()}',
    log_group_retention_days=3,
    send_interval=30)
logger.addHandler(h)

RETRY_COUNT = 3


class BackendClient:

    def __init__(self, config, url: Optional[str] = None, token: Optional[str] = None):
        """
        the client requires an url where to connect against and the related token.
        """
        self.logger = logger

        if config['debug']:
            self.logger.setLevel(level='DEBUG')

        self._url = url
        self._token = token

        if self._token is None:
            # utilize env
            self._token = os.getenv('STASIS_API_TOKEN', os.getenv('PROD_STASIS_API_TOKEN'))
        if self._url is None:
            self._url = os.getenv('STASIS_API_URL', 'https://api.metabolomics.us')

        if self._token is None:
            raise Exception("you need to to provide a stasis api token in the env variable 'STASIS_API_TOKEN'")

        if self._url is None:
            raise Exception("you need to provide a url in the env variable 'STASIS_API_URL'")

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
        self.logger.debug("utilizing url %s", self._url)

    @retry(exceptions=Exception, tries=RETRY_COUNT, delay=1, backoff=2)
    def sample_acquisition_exists(self, sample_name) -> bool:
        """
        returns the acquisition data of this sample
        """
        self.logger.debug("getting acquisition data for sample %s", sample_name)
        result = self.http.get(f'{self._url}/stasis/acquisition/{sample_name}', headers=self._header)
        if result.status_code != 200:
            return False
        return True

    @retry(exceptions=Exception, tries=RETRY_COUNT, delay=1, backoff=2)
    def sample_acquisition_get(self, sample_name) -> dict:
        """
        returns the acquisition data of this sample
        """
        self.logger.debug("getting acquisition data for sample %s", sample_name)
        result = self.http.get(f'{self._url}/stasis/acquisition/{sample_name}', headers=self._header)
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

        result = self.http.post(f'{self._url}/gostasis/tracking', json=data, headers=self._header)
        if result.status_code != 200: raise Exception(
            f"we observed an error. Status code was {result.status_code} and error was {result.reason} and {sample_name} in {state} with {file_handle}")
        return result


    def get_method_last_version(self, method: str):
        """
        Get the latest or newest version label for a method/library from the compound table
        """

        cmethod = urllib.parse.quote(method)

        result = self.http.get(f'{self._url}/methods/last_version/{cmethod}', headers=self._header)

        if result.status_code == 200:
            return result.json()
        elif result.status_code == 404:
            return []
        else:
            raise Exception(result)


    def get_unique_method_profiles(self, method: str, version: str = 'fixed'):
        """
        Get the list of unique profiles for a specific method and version from CIS-Server
        :param method: method name
        :param version: method version
        :return: a list of profile names, or empty if a method or version is missing
        """
        if not method:
            raise Exception('parameter "method" cannot be null or empty')

        clib = urllib.parse.quote(method)
        cver = urllib.parse.quote(version)
        try:
            result = self.http.get(f'{self._url}/methods/unique_profiles/{clib}/{cver}', headers=self._header)

            if result.status_code == 200:
                return result.json()
            elif result.status_code == 404:
                return []
            else:
                self.logger.error(result.status_code)
                raise Exception(result)
        except Exception as ex:
            self.logger.error(ex.args)


    def store_job(self, job: dict, enable_progress_bar: bool = False):
        """
        stores a job in the system in preparation for scheduling
        """
        raise Exception("Not implemented")

        # self.logger.debug("storing job %s", job['id'])
        # from stasis_client.store_job import JobStorage
        # return JobStorage().store(job, enable_progress_bar, self)


    def schedule_job(self, job_id: str, sample: str = None) -> dict:
        """
        schedules a job for calculation
        """
        raise Exception("Not implemented")

        # if sample is None:
        #     self.logger.debug("schedule job %s", job_id)
        #     response = self.http.put(f"{self._schedule_url}/job/schedule/{job_id}", headers=self._header)
        # else:
        #     self.logger.debug(f"schedule job {job_id} and sample {sample}")
        #     response = self.http.put(f"{self._schedule_url}/job/schedule/{job_id}/{sample}", headers=self._header)
        # if response.status_code != 200:
        #     raise Exception(
        #         f"we observed an error. Status code was {response.status_code} and error was {response.reason} for {job_id}")
        # else:
        #     return json.loads(response.content)
