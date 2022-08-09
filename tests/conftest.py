import os
from queue import Queue

import pytest
from cisclient.client import CISClient
from stasis_client.client import StasisClient

from monitor.workers.Scheduler import Scheduler


def pytest_generate_tests(metafunc):
    os.environ['STASIS_URL'] = 'https://test-api.metabolomics.us/stasis'
    os.environ['STASIS_API_TOKEN'] = 's45LgmYFPv8NbzVUbcIfRQI6NWlF7W09TUUMavx5'
    os.environ['CIS_URL'] = 'https://test-api.metabolomics.us/cis'
    os.environ['CIS_API_TOKEN'] = 's45LgmYFPv8NbzVUbcIfRQI6NWlF7W09TUUMavx5'


@pytest.fixture
def stasis_cli():
    return StasisClient(os.getenv('STASIS_URL'), os.getenv('STASIS_API_TOKEN'))


@pytest.fixture
def cis_cli():
    return CISClient(os.getenv('CIS_URL'), os.getenv('CIS_API_TOKEN'))


@pytest.fixture
def scheduler(stasis_cli, cis_cli):
    return Scheduler(None, stasis_cli, cis_cli, Queue(), schedule=True)

