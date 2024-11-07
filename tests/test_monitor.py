import os
import shutil
import time
from os import path
from threading import Thread

import boto3

from monitor.Monitor import Monitor


def __create_file_delayed(raw_filename, dest, count, conv_q):
    for c in range(count):
        raw_fname, raw_ext = path.splitext(raw_filename)
        destination = path.join(dest, 'extra', 'path', f'{raw_fname.split(os.sep)[-1]}-{c}{raw_ext}')
        not path.exists(destination) or os.makedirs(destination)
        print(f'\tcopying {raw_filename} to {destination}')
        shutil.copytree(raw_filename, destination)


def __delete_files(destination):
    shutil.rmtree(destination, ignore_errors=True)


def test_start(config, backend_cli):
    __delete_files(path.join(config['monitor']['paths'][0], 'extra'))
    __delete_files(path.join(config['monitor']['storage'], 'autoconv'))

    sqs = boto3.client('sqs')

    conv_q = sqs.get_queue_url(QueueName='MonitorConversionQueue-test')
    aws_q = sqs.get_queue_url(QueueName='MonitorUploadQueue-test')
    sched_q = sqs.get_queue_url(QueueName='MonitorPreprocessingQueue-test')

    filemon = Monitor(config, backend_cli, conv_q, aws_q)

    print('')
    print(f'starting monitor - {time.time_ns()}')
    filemon.start()
    time.sleep(1)  # wait for monitor to start completely

    print('creating file-copy thread')
    count = 5  # how many files to create
    raw_filename = path.join(os.path.dirname(__file__), '..', 'resources', 'monitored.d')
    file_thread = Thread(target=__create_file_delayed,
                         args=(raw_filename, config['monitor']['paths'][0], count, conv_q))

    print(f'starting file creator - {time.time_ns()}, qsize: {conv_q.qsize()}')
    file_thread.start()

    print(f'joining  file creator - {time.time_ns()}, qsize: {conv_q.qsize()}')
    file_thread.join()
    print(f'finished file creator - {time.time_ns()}, qsize: {conv_q.qsize()}')

    filemon.join(timeout=60)
    filemon.running = False

    conv_files = [os.path.join(config['monitor']['storage'], 'autoconv', f'monitored-{x}.mzml') for x in range(count)]

    print('destination should have files: ' + ', '.join(conv_files))
    assert all([os.path.exists(conv_file) for conv_file in conv_files])
