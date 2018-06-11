import os

import moto
import pytest

if 'AWS_DEFAULT_REGION' not in os.environ:
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-2'


@pytest.fixture
def requireMocking():
    """
    method which should be called before all other methods in tests. It basically configures our
    mocking context for stasis
    """

    bucket = moto.mock_s3()
    bucket.start()

    yield
    bucket.stop()

    pass
