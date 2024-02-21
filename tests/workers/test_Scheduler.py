import logging

import pytest

from monitor.workers.Scheduler import Scheduler

logger = logging.getLogger(__name__)


@pytest.mark.skip('Deprecated')
def test_schedule_sample(mocks, stasis_cli, cis_cli, config, test_qm):
    sched = Scheduler(None, stasis_cli, cis_cli, config, test_qm)

    sched.schedule_sample('Jepsen290_MX677489_posBA_K-RU-P-0005-D-127')
