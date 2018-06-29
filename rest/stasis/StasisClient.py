#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from http.client import HTTPConnection

import requests
import simplejson as json


class StasisClient(object):
    """Simple Stasis rest rest client

    Parameters
    ----------
        api_url: str
            The base url where the Stasis api lives
    """

    HTTPConnection.debugLevel = 1
    stasis_url = ""

    def __init__(self, api_url):
        self.stasis_url = api_url
        self.states = self.get_states()

    def set_tracking(self, sample, status):
        """Creates a new status or changes the status of a sample

        Parameters
        ----------
            sample : str
                The filename of the sample to create/adjust tracking status
            status : str
                The new status of a file.
        """
        if status not in self.states.keys():
            return False

        url = self.stasis_url + "/stasis/tracking"
        filename, ext = os.path.splitext(sample.split(os.sep)[-1])

        payload = json.dumps({"sample": filename, "status": status, "fileHandle": (filename + ext)})

        headers = {"Content-Type": "application/json"}
        resp = requests.post(url, data=payload, headers=headers)

        if resp.status_code != 200:
            print("\tfail\n%d - %s" % (resp.status_code, resp.reason))

        return resp.status_code == 200

    def get_tracking(self, sample):
        url = self.stasis_url + '/stasis/tracking/' + sample

        resp = requests.get(url)

        return resp

    def get_states(self):
        url = self.stasis_url + '/stasis/status'

        resp = requests.get(url)
        if resp.status_code == 200:
            return resp.json()
        else:
            raise Exception("Failed to load stasis tracking states")
