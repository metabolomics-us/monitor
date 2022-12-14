import re
import time

from monitor.QueueManager import CONVERSION_QUEUE, UPLOAD_QUEUE, PREPROCESSING_QUEUE


def test_create_queue_manager(test_qm):
    assert test_qm is not None

    assert re.match(f'https://.*?/{CONVERSION_QUEUE}-{test_qm.stage}', test_qm.conversion_q())
    assert re.match(f'https://.*?/{UPLOAD_QUEUE}-{test_qm.stage}', test_qm.upload_q())
    assert re.match(f'https://.*?/{PREPROCESSING_QUEUE}-{test_qm.stage}', test_qm.preprocess_q())


def test_get_size(test_qm):
    assert test_qm.get_size(test_qm.conversion_q()) == 0
    assert test_qm.get_size(test_qm.upload_q()) == 0
    assert test_qm.get_size(test_qm.preprocess_q()) == 0


def test_get_message_from_empty_queue(test_qm):
    assert test_qm.get_next_message(test_qm.conversion_q()) == ''


def test_put_message(test_qm):
    global msg
    msg = f'test message {time.time_ns()}'

    test_qm.put_message(test_qm.conversion_q(), msg)
    time.sleep(0.1)
    assert test_qm.get_size(test_qm.conversion_q()) == 1


def test_receive_message(test_qm):
    assert msg is not None

    data = test_qm.get_next_message(test_qm.conversion_q())
    print(msg, data)
    assert data == msg
    assert test_qm.get_size(test_qm.conversion_q()) == 0
