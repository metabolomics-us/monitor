import time


def test_create_queue_manager(test_qm):
    assert test_qm is not None

    assert 'MonitorConversionQueue' in test_qm.conversion_q()
    assert 'MonitorUploadQueue' in test_qm.upload_q()
    assert 'MonitorPreprocessingQueue' in test_qm.process_q()


def test_get_size(test_qm):
    assert test_qm.get_size(test_qm.conversion_q()) == 0
    assert test_qm.get_size(test_qm.upload_q()) == 0
    assert test_qm.get_size(test_qm.process_q()) == 0


def test_get_message_from_empty_queue(test_qm):
    assert test_qm.get_next_message(test_qm.conversion_q()) == ''


def test_put_receive_message(test_qm):
    msg = f'test message {time.time_ns()}'

    test_qm.put_message(test_qm.conversion_q(), msg)
    time.sleep(0.1)
    assert test_qm.get_size(test_qm.conversion_q()) == 1

    data = test_qm.get_next_message(test_qm.conversion_q())

    assert data == msg
    assert test_qm.get_size(test_qm.conversion_q()) == 0
