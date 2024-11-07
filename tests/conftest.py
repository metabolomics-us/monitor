import logging
import os
import shutil

import boto3
import moto
import pytest
import yamlconf

from monitor.ObserverFactory import ObserverFactory
from monitor.QueueManager import QueueManager
from monitor.client.BackendClient import BackendClient


def pytest_generate_tests(metafunc):
    os.environ['TEST_STASIS_API_URL'] = 'https://test-api.metabolomics.us/gostasis'
    os.environ['TEST_STASIS_API_TOKEN'] = 's45LgmYFPv8NbzVUbcIfRQI6NWlF7W09TUUMavx5'
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ["AWS_DEFAULT_REGION"] = 'us-west-2'


@pytest.fixture
def mocks():
    mock = moto.mock_aws()
    mock.start()

    # sqs = moto.mock_sqs()
    # sqs.start()
    #
    # ddb = moto.mock_dynamodb()
    # ddb.start()

    ress3 = boto3.client('s3')
    ress3.create_bucket(Bucket='datatest-carrot',
                        CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})

    yield
    mock.stop()
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
def backend_cli():
    backend_cli = BackendClient(os.getenv('TEST_STASIS_API_URL'), os.getenv('TEST_STASIS_API_TOKEN'))
    return backend_cli


@pytest.fixture
def raw(config):
    shutil.copytree('..\\resources\\monitor.d', config['paths'][0])


@pytest.fixture
def test_qm(mocks):
    logging.getLogger().root.setLevel('DEBUG')

    return QueueManager(stage='test')

@pytest.fixture
def observer_factory_eclipse():
    of = ObserverFactory()
    of.platform = 'eclipse'
    return of

@pytest.fixture
def observer_factory_other():
    of = ObserverFactory()
    of.platform = 'other'
    return of
