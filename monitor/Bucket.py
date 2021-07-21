#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys

import boto3
from botocore.exceptions import ClientError
from loguru import logger


class Bucket:
    """ this defines an easy access to a AWS bucket """

    logger.add(sys.stdout, format="{time:YYYY-MM-DD} {level} {file} [{thread.name}]",
               filter=f"Bucket", level="INFO")

    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.s3 = boto3.resource('s3')

        try:
            boto3.client('s3').create_bucket(Bucket=bucket_name, CreateBucketConfiguration={
                'LocationConstraint': 'us-west-2'})
            logger.info(f'Created bucket: {bucket_name}')
        except Exception as e:
            logger.warning(f'Bucket exists, skipping creation.')

    def save(self, filename):
        """
            stores the specified file in the bucket
        :param filename: the name of the file to be uploaded
        :return:
        """

        try:
            # from https://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Object.upload_file
            logger.info(f'Saving file {filename} on {self.bucket_name}')
            self.s3.Object(self.bucket_name, filename.split(os.sep)[-1]).upload_file(filename)
            return filename.split(os.sep)[-1]
        except ConnectionResetError as cre:
            logger.error(f'connection Reset: {cre.strerror} uploading {cre.filename}')
            # raise
        except Exception as e:
            logger.error(f'Can\' upload file: {filename}, error: {str(e)}')
            # raise

    def load(self, name):
        """
            loads the specified content
        :param name: the name of the content
        :return:
        """
        try:
            logger.info('[Bucket] - loading: %s' % name)
            data = self.s3.Object(self.bucket_name).get()['Body']

            return data.read().decode()
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                logger.error('The object does not exist.')
            else:
                logger.error(str(e))
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
            #     logger.info("EXCEPTION:" + str(e))
            #     raise
        except Exception as other:
            logger.info(f'Other exception: {str(other)}')
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
