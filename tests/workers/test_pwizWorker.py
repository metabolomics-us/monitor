import os
from queue import Queue

from monitor.workers.PwizWorker import PwizWorker

agi_file = os.path.join(os.path.dirname(__file__), '..', '..', 'resources', 'monitored.d')


def test_pwizworker(stasis_cli, wconfig):
    raw = wconfig['monitor']['paths'][0]

    print('creating queues', flush=True)
    cnv_q = Queue()
    aws_q = Queue()

    print('creating worker', flush=True)
    worker = PwizWorker(None, stasis_cli, cnv_q, aws_q, wconfig)

    worker.start()

    # should be skipped and
    print('adding bad file', flush=True)
    cnv_q.put_nowait(f'{raw}\\DNU\\bad_file.d')

    print('adding good file', flush=True)
    # process next valid item in queue
    cnv_q.put_nowait(f"{raw}\\monitored.d")

    worker.join(timeout=10)

    test_file = os.path.join(wconfig['monitor']['storage'], 'autoconv', f'monitored.mzml')

    assert os.path.exists(test_file)
    assert aws_q.qsize() == 1

    os.remove(test_file)


def test_update_storage(stasis_cli, wconfig):
    print('creating worker', flush=True)
    worker = PwizWorker(None, stasis_cli, Queue(), Queue(), wconfig)

    good_names = [
        'SantanaSerum031_MX625118_posCSH_CMS-21-2-H2S-Serum-035.d',
        'MX625118_posCSH_CMS-21-2-H2S-Serum-035.d',
        'SantanaSerum031_mx625118_posCSH_CMS-21-2-H2S-Serum-035.d',
        'mx625118_posCSH_CMS-21-2-H2S-Serum-035.d']
    bad_names = [
        '625118_posCSH_CMS-21-2-H2S-Serum-035.d',
        'MX62518_posCSH_CMS-21-2-H2S-Serum-035.d',
        'blahMX625118_posCSH_CMS-21-2-H2S-Serum-035.d'
    ]

    for n in good_names:
        res = worker.update_output(n)
        assert f"{wconfig['monitor']['storage']}\\mx625118\\".lower() == res.lower()

    for n in bad_names:
        res = worker.update_output(n)
        assert f"{wconfig['monitor']['storage']}\\autoconv\\".lower() == res.lower()


def test_pwizworker_conversion_skipping(stasis_cli, wconfig):
    wconfig['monitor']['exists'] = True
    raw = wconfig['monitor']['paths'][0]

    print('creating queues', flush=True)
    cnv_q = Queue()
    aws_q = Queue()

    print('creating worker', flush=True)
    worker = PwizWorker(None, stasis_cli, cnv_q, aws_q, wconfig)

    worker.start()

    # should be skipped and
    print('adding bad file', flush=True)
    cnv_q.put_nowait(f"{raw}\\monitored.d")

    worker.join(timeout=10)

    converted_fn = os.path.join(wconfig['monitor']['storage'], 'autoconv', f'monitored.mzml')
    assert not os.path.exists(converted_fn)
    assert aws_q.qsize() == 0
