from queue import Queue

from monitor.workers.Scheduler import Scheduler


def test_schedule_sample(stasis_cli, cis_cli, config):
    sched_q = Queue()

    sched = Scheduler(None, stasis_cli, cis_cli, config, sched_q)

    sched.schedule_sample('Jepsen290_MX677489_posBA_K-RU-P-0005-D-127')
