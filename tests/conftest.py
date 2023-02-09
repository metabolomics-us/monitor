import logging
import os
import shutil

import boto3
import moto
import pytest
import yamlconf
from cisclient.client import CISClient
from stasis_client.client import StasisClient

from monitor.ObserverFactory import ObserverFactory
from monitor.QueueManager import QueueManager


def pytest_generate_tests(metafunc):
    os.environ['TEST_CIS_API_URL'] = 'https://test-api.metabolomics.us/cis'
    os.environ['TEST_CIS_API_TOKEN'] = 's45LgmYFPv8NbzVUbcIfRQI6NWlF7W09TUUMavx5'
    os.environ['TEST_STASIS_API_URL'] = 'https://test-api.metabolomics.us/stasis'
    os.environ['TEST_STASIS_API_TOKEN'] = 's45LgmYFPv8NbzVUbcIfRQI6NWlF7W09TUUMavx5'
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

    ddb = moto.mock_dynamodb()
    ddb.start()

    ress3 = boto3.client('s3')
    ress3.create_bucket(Bucket='datatest-carrot',
                        CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})

    yield
    sqs.stop()
    s3.stop()
    ddb.stop()
    pass


def load_config(filepath):
    with open(filepath, 'r') as conf:
        conf = yamlconf.load(conf)
        conf['test'] = True
    return conf


@pytest.fixture
def wconfig():
    return load_config('appconfig-test.yml')


@pytest.fixture
def stasis_cli():
    stasis_cli = StasisClient(os.getenv('TEST_STASIS_API_URL'), os.getenv('TEST_STASIS_API_TOKEN'))
    return stasis_cli


@pytest.fixture
def cis_cli():
    return CISClient(os.getenv('TEST_CIS_API_URL'), os.getenv('TEST_CIS_API_TOKEN'))


@pytest.fixture
def raw(config):
    shutil.copytree('..\\resources\\monitor.d', config['paths'][0])


@pytest.fixture
def test_qm(mocks):
    logging.getLogger().root.setLevel('DEBUG')

    return QueueManager(stage='test')

@pytest.fixture
def observer_factory():
    return ObserverFactory()