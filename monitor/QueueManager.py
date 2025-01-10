import logging
import platform
import time

import boto3
import watchtower
from botocore.exceptions import ClientError

QUEUES = [{'type': 'conversion', 'name': 'MonitorConversionQueue'},
          {'type': 'upload', 'name': 'MonitorUploadQueue'},
          {'type': 'preprocess', 'name': 'MonitorPreprocessingQueue'}]

logger = logging.getLogger('QueueManager')
if not logger.handlers:
    h = watchtower.CloudWatchLogHandler(
        log_group_name=f'/lcb/monitor/{platform.node()}',
        log_group_retention_days=3,
        send_interval=30)
    logger.addHandler(h)
logger.setLevel(logging.INFO)


class QueueManager:

    def __init__(self, stage: str, host: str = platform.node()):
        self.stage = stage
        self.sqs = boto3.client('sqs')
        self.host = host
        self.init_queues()

    def conversion_q(self):
        q = list(filter(lambda x: x['type'] == 'conversion', QUEUES))[0]
        queue = self.__get_queue(f"{q['name']}-{self.host}-{self.stage}")
        if not queue:
            time.sleep(5)
            queue = self.__get_queue(f"{q['name']}-{self.host}-{self.stage}")
            if not queue:
                raise QueueClientException(f'Failed to get queue url twice')
                return None
        return queue['QueueUrl']

    def upload_q(self):
        q = list(filter(lambda x: x['type'] == 'upload', QUEUES))[0]
        return self.__get_queue(f"{q['name']}-{self.host}-{self.stage}")['QueueUrl']

    def get_next_message(self, queue_url: str):
        data = self.sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, VisibilityTimeout=400)
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
        except Exception as ex:
            print(f'Exception {type(ex)}')

    def get_size(self, queue_url: str):
        return int(self.sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["ApproximateNumberOfMessages"]
        )["Attributes"]["ApproximateNumberOfMessages"])

    def clean(self, queue_url):
        self.sqs.purge_queue(QueueUrl=queue_url)

    def init_queues(self):
        qs = self.sqs.list_queues()
        queues = [item.rsplit('/')[-1] for item in qs.get('QueueUrls',[])]

        for q in QUEUES:
            fqqn = f"{q['name']}-{self.host}-{self.stage}"
            logger.info(f'Checking queue {fqqn}')

            try:
                if fqqn not in queues:
                    self.sqs.create_queue(QueueName=fqqn)
                    logger.info(f'\tQueue {fqqn} created')
                else:
                    logger.info(f'\tQueue {fqqn} already exists')
            except ClientError as ex:
                logger.error(f'Error creating queue', ex.args)
                pass
