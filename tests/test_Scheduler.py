import time


# def test_schedule_missing_sample(scheduler):
#     result = scheduler.schedule_sample('blah')
#
#     assert result is None
#


def test_schedule_real_sample(scheduler, stasis_cli):

    result = scheduler.schedule_sample('Castro019_MX635819_negCSH_K-RU-P-0039-019')

    assert result['status'] == 'scheduling'

    time.sleep(2)
    assert stasis_cli.load_job_state(result['job'])['job_state'] == 'scheduled'
