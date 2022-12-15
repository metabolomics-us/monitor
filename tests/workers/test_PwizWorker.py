import os
import tempfile

import psutil as psutil
from loguru import logger

from monitor.workers.PwizWorker import PwizWorker


def __test_file_open__(fpath):
    for proc in psutil.process_iter():
        try:
            for item in proc.open_files():
                if fpath == item.path:
                    return True
        except Exception:
            pass

    return False


def test_pwizworker(mocks, stasis_cli, wconfig, test_qm):
    test_qm.clean(test_qm.conversion_q())
    test_qm.clean(test_qm.upload_q())

    raw = wconfig['monitor']['paths'][0]

    logger.info('creating worker')
    worker = PwizWorker(None, stasis_cli, test_qm, wconfig)

    worker.start()

    # should be skipped and
    logger.info('adding bad file')
    test_qm.put_message(test_qm.conversion_q(), f'{raw}\\DNU\\bad_file.d')

    # process next valid item in queue
    logger.info('adding good file')
    test_qm.put_message(test_qm.conversion_q(), f"{raw}\\monitored.d")

    worker.join(timeout=15)

    test_file = f'{tempfile.gettempdir()}/monitored.mzml'
    assert os.path.exists(test_file)

    os.remove(test_file)

    assert test_qm.get_size(test_qm.upload_q()) == 1

    test_qm.clean(test_qm.conversion_q())
    test_qm.clean(test_qm.upload_q())


def test_pwizworker_skip_file_not_in_aws(mocks, stasis_cli, wconfig, test_qm):
    test_qm.clean(test_qm.conversion_q())
    test_qm.clean(test_qm.upload_q())

    wconfig['monitor']['exists'] = True
    raw = wconfig['monitor']['paths'][0]

    logger.info(f'creating worker')
    worker = PwizWorker(None, stasis_cli, test_qm, wconfig)

    worker.start()

    # should be skipped and
    logger.info('try to convert file not in stasis')
    test_qm.put_message(test_qm.conversion_q(), f"{raw}\\monitored.d")

    worker.join(timeout=5)

    converted_fn = f'{tempfile.gettempdir()}/monitored.mzml'
    assert not os.path.exists(converted_fn)

    assert test_qm.get_size(test_qm.conversion_q()) == 0
    assert test_qm.get_size(test_qm.upload_q()) == 0

    test_qm.clean(test_qm.conversion_q())
    test_qm.clean(test_qm.upload_q())
