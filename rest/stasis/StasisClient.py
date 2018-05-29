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

    HTTPConnection.debugLevel=1
    stasis_url = ""

    def __init__(self, api_url):
        self.stasis_url = api_url

    def set_tracking(self, sample, status):
        """Creates a new status or changes the status of a sample

        Parameters
        ----------
            sample : str
                The filename of the sample to create/adjust tracking status
            status : str
                The new status of a file. Can be one of: 'entered', 'acquired', 'converted', 'processed', 'exported'
        """
        if(status not in ['entered', 'acquired', 'converted', 'processed', 'exported']):
            return False


        url = self.stasis_url + "/stasis/tracking"
        filename, ext = os.path.splitext(sample.split('/')[-1])
        print("cleaned filename: %s" % filename)
        payload = json.dumps({"sample":filename, "status":status, "fileHandle":(filename + ext)})

        print("\tsetting tracking for sample %s to %s" % (filename, status))

        print("request data:\n\turl: %s\n\tdata: %s" % (url, payload))
        headers = {"Content-Type": "application/json"}
        resp = requests.post(url, data=payload, headers=headers)

        if(resp.status_code == 200):
            print("\tsuccess")
        else:
            print("\tfail\n%d - %s" % (resp.status_code, resp.reason))

        return resp.status_code == 200
