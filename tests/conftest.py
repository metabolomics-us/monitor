import os
import shutil

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
def test_qm():
    return QueueManager(stage='test')
