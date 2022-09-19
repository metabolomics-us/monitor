from queue import Queue

from monitor.workers.Scheduler import Scheduler


def test_schedule_real_sample(stasis_cli, cis_cli, wconfig):
    smpl = 'Castro019_MX635819_negCSH_K-RU-P-0039-019'

    scheduler = Scheduler(None, stasis_cli, cis_cli, wconfig, Queue())

    scheduler.start()

    scheduler.schedule_q.put(smpl)

    scheduler.join(timeout=10)

    assert stasis_cli.sample_tracking_get(smpl) == 'scheduled'
