import os
import tempfile
import time

from loguru import logger

from monitor.workers.PwizWorker import PwizWorker

agi_file = os.path.join(os.path.dirname(__file__), '..', '..', 'resources', 'monitored.d')


def test_pwizworker(mocks, stasis_cli, wconfig, test_qm):
    test_qm.clean(test_qm.conversion_q())
    test_qm.clean(test_qm.upload_q())

    raw = 'D:\\data\\raw'

    logger.info('creating worker', flush=True)
    worker = PwizWorker(None, stasis_cli, test_qm, wconfig)

    worker.start()

    # should be skipped and
    logger.info('adding bad file', flush=True)
    test_qm.put_message(test_qm.conversion_q(), f'{raw}\\DNU\\bad_file.d')

    # process next valid item in queue
    logger.info('adding good file', flush=True)
    test_qm.put_message(test_qm.conversion_q(), f"{raw}\\monitored.d")

    worker.join(timeout=15)

    test_file = os.path.join(tempfile.tempdir, f'monitored.mzml')
    assert os.path.exists(test_file)
    time.sleep(1)
    os.remove(test_file)

    assert test_qm.get_size(test_qm.upload_q()) == 1


def test_pwizworker_conversion_skipping(mocks, stasis_cli, wconfig, test_qm):
    # to avoid the 60 second wait time between SQS purges
    start_count = test_qm.get_size(test_qm.upload_q())

    wconfig['monitor']['exists'] = True
    raw = 'D:\\data\\raw'

    logger.info('creating worker', flush=True)
    worker = PwizWorker(None, stasis_cli, test_qm, wconfig)

    worker.start()

    # should be skipped and
    logger.info('adding bad file', flush=True)
    test_qm.put_message(test_qm.conversion_q(), f"{raw}\\monitored.d")

    worker.join(timeout=15)

    converted_fn = os.path.join(wconfig['monitor']['storage'], 'autoconv', f'monitored.mzml')
    assert not os.path.exists(converted_fn)
    assert test_qm.get_size(test_qm.upload_q()) == start_count
