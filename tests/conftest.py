import os
import shutil
import time

import boto3
import moto
import pytest
import yamlconf
from cisclient.client import CISClient
from loguru import logger
from stasis_client.client import StasisClient

from monitor.QueueManager import QueueManager


def pytest_generate_tests(metafunc):
    os.environ['TEST_CIS_API_URL'] = 'https://test-api.metabolomics.us/cis'
    os.environ['TEST_CIS_API_TOKEN'] = 'pniczYK74C6QvIPE4ZTyiL2H1oCbLFi1qMpyXshb'
    os.environ['TEST_STASIS_API_URL'] = 'https://test-api.metabolomics.us/stasis'
    os.environ['TEST_STASIS_API_TOKEN'] = 'pniczYK74C6QvIPE4ZTyiL2H1oCbLFi1qMpyXshb'
    os.environ['monitor_conversion_queue'] = 'MonitorConversionQueue-test'
    os.environ['monitor_upload_queue'] = 'MonitorUploadQueue-test'
    os.environ['monitor_preprocess_queue'] = 'MonitorPreprocessingQueue-test'
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ["AWS_DEFAULT_REGION"] = 'us-west-2'


@pytest.fixture
def mocks():
    s3 = moto.mock_s3()
    s3.start()

    sqs = moto.mock_sqs()
    sqs.start()

    ress3 = boto3.client('s3')
    ress3.create_bucket(Bucket='datatest-carrot',
                        CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})

    ressqs = boto3.resource('sqs')
    ressqs.create_queue(QueueName=os.environ["monitor_conversion_queue"])
    time.sleep(1)
    ressqs.create_queue(QueueName=os.environ["monitor_upload_queue"])
    time.sleep(1)
    ressqs.create_queue(QueueName=os.environ["monitor_preprocess_queue"])
    time.sleep(1)

    yield
    sqs.stop()
    s3.stop()
    pass


def cc(filepath):
    print(os.getcwd())
    with open(filepath, 'r') as conf:
        conf = yamlconf.load(conf)
        conf['test'] = True
    return conf


@pytest.fixture
def config():
    return cc('appconfig-test.yml')


@pytest.fixture
def wconfig():
    return cc('appconfig-test.yml')


@pytest.fixture
def stasis_cli():
    stasis_cli = StasisClient(os.getenv('TEST_STASIS_API_URL'), os.getenv('TEST_STASIS_API_TOKEN'))
    logger.info(f'STASIS URL: {stasis_cli._url} -- STASIS TOKEN: {stasis_cli._token}')
    return stasis_cli


@pytest.fixture
def cis_cli():
    return CISClient(os.getenv('TEST_CIS_API_URL'), os.getenv('TEST_CIS_API_TOKEN'))


@pytest.fixture
def raw(config):
    shutil.copytree('..\\resources\\monitor.d', config['paths'][0])


@pytest.fixture
def test_qm(mocks):
    return QueueManager(stage='test')
