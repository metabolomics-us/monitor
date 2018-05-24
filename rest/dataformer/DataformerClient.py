#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests

class DataformerClient(object):
    """Simple DataFormer rest rest client

    Parameters
    ----------
        api_url: str
            The base url where the DataFormer api lives
        api_port: str
            The port number of the DataFormer api
        storage: str
            The destination folder for downloaded files
    """

    def __init__(self, api_url, api_port, storage):
        self.dataformer_url = f"{api_url}:{api_port}"
        self.storage = storage

    def convert(self, filename, type):
        """Converts a file to specified type

        Parameters
        ----------
            filename : str
                The name of the file to download
            type : str
                The converted type, valid values are: 'mzml' or 'mzxml'
        """
        print(" convert %s to %s" % (filename, type))

        try:
            if(self.__private_upload(filename)):
                d = self.__private_download(filename, type)
                print("downloaded %s" % d)
                return d
            else:
                return ""
        except Exception as ex:
            print("ERROR: " + str(ex.args))
            return ""


    def __private_upload(self, filename):
        """Uploads a file to be converted

        Parameters
        ----------
            filename : str
                The raw data file to be uploaded and converted (the conversion happens automatically)
        """
        print("...upoloading %s" % filename)

        url = f"{self.dataformer_url}/rest/conversion/upload"

        with open(filename, "rb") as upFile:
            uploaded = requests.post(url, files={"file":upFile})

        if uploaded.status_code == 200:
            print("\tuploaded")
            return True
        else:
            print("\terror uploading\n%d - %s" % (uploaded.status_code, uploaded.reason))
            return False

    def __private_download(self, filename, filetype):
        """Download a converted file of a particular type

        Parameters
        ----------
            filename : str
                The name of the file to download
            filetype : str
                The converted type, valid values are: 'mzml' or 'mzxml'
        """
        if(filetype.lower() not in ['mzml', 'mzxml']):
            raise Exception("Unsupported filename filetype, please use 'mzml' or 'mzxml'")

        filename = filename.split('/')[-1]

        print("...download %s version of %s" % (filetype, filename))
        data = requests.get("%s/rest/conversion/download/%s/%s" % (self.dataformer_url, filename, filetype))

        download = "%s/%s.%s" % (self.storage, filename.strip(".d.zip"), filetype)

        with open(download, "wb") as converted:
            converted.write(data.content)

        return download
