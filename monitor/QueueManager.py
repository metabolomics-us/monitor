import logging
import platform

import boto3
import watchtower
from botocore.exceptions import ClientError

CONVERSION_QUEUE = "MonitorConversionQueue"
UPLOAD_QUEUE = "MonitorUploadQueue"
PREPROCESSING_QUEUE = "MonitorPreprocessingQueue"

logger = logging.getLogger('QueueManager')
h = watchtower.CloudWatchLogHandler(
    log_group_name=f'/lcb/monitor/{platform.node()}',
    log_group_retention_days=3,
    send_interval=30)
logger.addHandler(h)


class QueueManager:

    def __init__(self, stage: str, host: str = platform.node()):
        self.stage = stage
        self.sqs = boto3.client('sqs')
        self.host = host

    def conversion_q(self):
        return self.__get_queue(CONVERSION_QUEUE + '-' + self.host + '-' + self.stage)['QueueUrl']

    def upload_q(self):
        return self.__get_queue(UPLOAD_QUEUE + '-' + self.host + '-' + self.stage)['QueueUrl']

    def preprocess_q(self):
        return self.__get_queue(PREPROCESSING_QUEUE + '-' + self.host + '-' + self.stage)['QueueUrl']

    def get_next_message(self, queue_url: str):
        data = self.sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, VisibilityTimeout=10)
        msg = {}

        if data.get('Messages', []):
            msg = data['Messages'][0]
            self.sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg['ReceiptHandle'])

        return msg.get('Body', '')

    def put_message(self, queue_url, message: str):
        self.sqs.send_message(
            QueueUrl=queue_url,
            DelaySeconds=0,
            MessageBody=message
        )

    def __get_queue(self, name: str):
        try:
            return self.sqs.get_queue_url(QueueName=name)
        except ClientError as ce:
            logger.error(f'Queue "{name}" does not exist.')
            quit()

    def get_size(self, queue_url: str):
        return int(self.sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["ApproximateNumberOfMessages"]
        )["Attributes"]["ApproximateNumberOfMessages"])

    def clean(self, queue_url):
        self.sqs.purge_queue(QueueUrl=queue_url)
