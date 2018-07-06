#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

import boto3
from botocore.exceptions import ClientError


class Bucket:
    """
        this defines an easy access to a AWS bucket
    """

    def __init__(self, bucket_name):

        print('[Bucket] - Created bucket object pointing at: %s' % bucket_name)
        self.bucket_name = bucket_name
        self.s3 = boto3.resource('s3')

        try:
            boto3.client('s3').create_bucket(Bucket=bucket_name, CreateBucketConfiguration={
                'LocationConstraint': 'us-west-2'})
        except Exception as e:
            print('[Bucket] - bucket exists, no reason to worry')

    def save(self, filename):
        """
            stores the specified file in the bucket
        :param filename: the name of the file to be uploaded
        :return:
        """

        try:
            # from https://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Object.upload_file
            self.s3.Object(self.bucket_name, filename.split(os.sep)[-1]).upload_file(filename)
            return filename.split(os.sep)[-1]
        except ConnectionResetError as cre:
            print('[Bucket] - ERROR-cre: %s uploading %s' % (cre.strerror, cre.filename))
            # raise
        except Exception as e:
            print('[Bucket] - ERROR-e: %s - file: %s' % (str(e), filename))
            # raise

    def load(self, name):
        """
            loads the specified content
        :param name: the name of the content
        :return:
        """
        try:
            print('[Bucket] - loading: %s' % name)
            data = self.s3.Object(self.bucket_name).get()['Body']

            return data.read().decode()
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                print('[Bucket] - The object does not exist.')
            else:
                print(str(e))
                # raise

    def exists(self, name) -> bool:
        """
        checks if the content with the given name exists
        :param name:
        :return:
        """

        try:
            self.s3.Object(self.bucket_name, name).load()
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                # The object does not exist.
                return False
            # else:
            #     # Something else has gone wrong.
            #     print("EXCEPTION:" + str(e))
            #     raise
        except Exception as other:
            print('[Bucket] - other exception: ' + str(other))
        else:
            return True

    def delete(self, name):
        """
            deletes the given data entry
        :param name:
        :return:
        """
        self.s3.Object(self.bucket_name, name).delete()

    def list(self):
        return boto3.client('s3').list_objects(Bucket=self.bucket_name)
