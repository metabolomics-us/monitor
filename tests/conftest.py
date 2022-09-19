import os
import shutil

import pytest
import yamlconf
from cisclient.client import CISClient
from stasis_client.client import StasisClient


def pytest_generate_tests(metafunc):
    os.environ['TEST_STASIS_API_URL'] = 'https://test-api.metabolomics.us/stasis'
    os.environ['TEST_STASIS_API_TOKEN'] = 's45LgmYFPv8NbzVUbcIfRQI6NWlF7W09TUUMavx5'
    os.environ['TEST_CIS_API_URL'] = 'https://test-api.metabolomics.us/cis'
    os.environ['TEST_CIS_API_TOKEN'] = 's45LgmYFPv8NbzVUbcIfRQI6NWlF7W09TUUMavx5'


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
    return StasisClient(os.getenv('TEST_STASIS_API_URL'), os.getenv('TEST_STASIS_API_TOKEN'))


@pytest.fixture
def cis_cli():
    return CISClient(os.getenv('TEST_CIS_API_URL'), os.getenv('TEST_CIS_API_TOKEN'))


@pytest.fixture
def raw(config):
    shutil.copytree('..\\resources\\monitor.d', config['paths'][0])
