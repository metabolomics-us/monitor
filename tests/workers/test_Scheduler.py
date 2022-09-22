from queue import Queue

from monitor.workers.Scheduler import Scheduler


def test_schedule_sample(stasis_cli, cis_cli, config):
    sched_q = Queue()

    sched = Scheduler(None, stasis_cli, cis_cli, config, sched_q)

    sched.schedule_sample('Babdor001_MX635067_negBA_IMucsf-6-086-300')
