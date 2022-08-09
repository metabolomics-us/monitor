def test_schedule_real_sample(scheduler, stasis_cli):
    smpl = 'Castro019_MX635819_negCSH_K-RU-P-0039-019'
    scheduler.start()

    scheduler.schedule_q.put(smpl)

    scheduler.join(timeout=10)

    assert stasis_cli.load_job_state(f'preprocess_{smpl}')['job_state'] == 'scheduled'

