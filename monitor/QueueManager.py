import logging
import platform

import boto3
import watchtower
from botocore.exceptions import ClientError

QUEUES = [{'type': 'conversion', 'name': 'MonitorConversionQueue'},
          {'type': 'upload', 'name': 'MonitorUploadQueue'},
          {'type': 'preprocess', 'name': 'MonitorPreprocessingQueue'}]

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
        self.init_queues()

    def conversion_q(self):
        q = list(filter(lambda x: x['type'] == 'conversion', QUEUES))[0]
        return self.__get_queue(f"{q['name']}-{self.host}-{self.stage}")['QueueUrl']

    def upload_q(self):
        q = list(filter(lambda x: x['type'] == 'upload', QUEUES))[0]
        return self.__get_queue(f"{q['name']}-{self.host}-{self.stage}")['QueueUrl']

    def preprocess_q(self):
        q = list(filter(lambda x: x['type'] == 'preprocess', QUEUES))[0]
        return self.__get_queue(f"{q['name']}-{self.host}-{self.stage}")['QueueUrl']

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

    def init_queues(self):
        qs = self.sqs.list_queues()
        logger.debug(qs)
        queues = qs.get('QueueUrls',[])

        for q in QUEUES:
            fqqn = f"{q['name']}-{self.host}-{self.stage}"
            logger.debug(f'Checking queue {fqqn}')
            try:
                self.sqs.create_queue(QueueName=fqqn)
                logger.debug(f'Queue {fqqn} created')
            except ClientError as ex:
                logger.debug(f'Queue {fqqn} exists', ex.args)
                pass

            # TODO: Fix the above hack!!! Finish the following code, it's a better way to do it
            # if fqqn not in queues:
            #     self.sqs.create_queue(QueueName=fqqn)
            #     logger.info(f'Queue {fqqn} created')
            # else:
            #     logger.info(f'Queue {fqqn} already exists')
