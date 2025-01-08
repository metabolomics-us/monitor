#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import os
from datetime import datetime, timezone, timedelta

import boto3
import botocore
from botocore.exceptions import ClientError

logger = logging.getLogger('BucketWorker')


class Bucket:
    """ this defines an easy access to a AWS bucket """

    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.s3 = boto3.resource('s3')

        try:
            response = boto3.client('s3').list_buckets()

            buckets = [bucket["Name"] for bucket in response['Buckets']]
            if not self.bucket_name in buckets:
                boto3.client('s3').create_bucket(Bucket=self.bucket_name, CreateBucketConfiguration={
                    'LocationConstraint': 'us-west-2'})
                logger.info(f'Created bucket: {self.bucket_name}')
            else:
                logger.info(f'Bucket exists: {self.bucket_name}')
        except Exception as ex:
            logger.error(f'Error checking for destination bucket: {str(ex)}')

    def save(self, filename):
        """
            stores the specified file in the bucket
        :param filename: the name of the file to be uploaded
        :return:
        """
        remote_name = filename.split(os.sep)[-1]
        try:
            # from https://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Object.upload_file
            logger.info(f'\tSaving file {remote_name} on {self.bucket_name}')
            self.s3.Object(self.bucket_name, remote_name).upload_file(filename)
            return remote_name
        except ConnectionResetError as cre:
            logger.error('Connection reset.', str(cre))
            raise cre
        except Exception as e:
            raise e

    def exists(self, name) -> bool:
        """
        checks if the content with the given name exists
        :param name:
        :return:
        """

        try:
            file = self.s3.Object(self.bucket_name, name)
            file.load()
            age = (datetime.now(timezone.utc) - file.last_modified)

            if age < timedelta(days=30):
                return True  # newer than 30 days, skip conversion
            else:
                return False  # older than 30 days, trigger conversion

        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                # The object does not exist.
                return False
        except Exception as other:
            logger.info(f'Other exception: {str(other)}')
            raise other

    def object_head(self, filename) -> bool:
        try:
            boto3.client('s3').head_object(Bucket=self.bucket_name, Key=filename)
            logger.info(f"\tKey: '{filename}' found!")
            return True
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                logger.error(f"\tKey: '{filename}' does not exist!")
            else:
                logger.error("Something else went wrong")

            return False


    def delete(self, name):
        """
            deletes the given data entry
        :param name:
        :return:
        """
        self.s3.Object(self.bucket_name, name).delete()


    def list(self):
        """
            lists the files in the raw data bucket

        :return:
        """
        return boto3.client('s3').list_objects(Bucket=self.bucket_name)
