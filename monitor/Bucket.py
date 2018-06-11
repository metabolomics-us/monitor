import os

import boto3
import botocore


class Bucket:
    """
        this defines an easy access to a AWS bucket
    """

    def __init__(self, bucket_name):

        print(bucket_name)
        self.bucket_name = bucket_name
        self.s3 = boto3.resource('s3')

        try:
            boto3.client('s3').create_bucket(Bucket=bucket_name, CreateBucketConfiguration={
                'LocationConstraint': 'us-west-2'})
        except Exception as e:
            print("sorry this bucket caused an error - this mean it exist, no reason to worry")

    def save(self, filename):
        """
        stores the specified file in the bucket
        :param filename: the name of the file to be uploaded
        :return:
        """
        print('saving %s as %s' % (filename, filename.split(os.sep)[-1]))
        return self.s3.meta.client.upload_file(filename, self.bucket_name, filename.split(os.sep)[-1])

    def load(self, name):
        """
            loads the specified content
        :param name: the name of the content
        :return:
        """
        try:
            print("loading: {}".format(name))
            data = self.s3.Object(self.bucket_name).get()['Body']

            return data.read().decode()
        except Exception as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise

    def exists(self, name) -> bool:
        """
        checks if the content with the given name exists
        :param name:
        :return:
        """

        try:
            self.s3.Object(self.bucket_name, name).load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                # The object does not exist.
                return False
            else:
                # Something else has gone wrong.
                raise
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
        print('buckets: %s' % boto3.client('s3').list_buckets())
        print('location: %s' % boto3.client('s3').get_bucket_location(Bucket=self.bucket_name))

        return boto3.client('s3').list_objects(Bucket=self.bucket_name)
