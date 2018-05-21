import os
import requests
import simplejson as json

from http.client import HTTPConnection

class StasisClient(object):
    """Simple Stasis rest api client"""

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

        print("...setting tracking for sample %s to %s ..." % (sample, status))

        url = self.stasis_url + "/stasis/tracking"
        filename, ext = os.path.splitext(sample.split('/')[-1])
        payload = json.dumps({"sample":filename, "status":status, "fileHandle":(filename + ext)})

        print("request data:\n\turl: %s\n\tdata: %s" % (url, payload))
        headers = {"Content-Type": "application/json"}
        resp = requests.post(url, data=payload, headers=headers)

        if(resp.status_code == 200):
            print("\tsuccess")
        else:
            print("\tfail\n%d - %s" % (resp.status_code, resp.reason))

        return resp.status_code == 200
