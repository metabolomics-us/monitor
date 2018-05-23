import time
from multiprocessing import Process, JoinableQueue

from watchdog.observers import Observer

import monitor
from monitor.workers.AgilentWorker import AgilentWorker
from monitor.workers.ConversionWorker import ConversionWorker


class Monitor(object):
    """A file monitoring class

    Parameters
    ----------
        config: dict
            A yaml application configuration file loaded with yamlconf
        stasis_cli: StasisClient
            A client class to the Stasis rest API
        dataform_cli: DataformerClient
            A client class to the DataFormer rest API
    """

    def __init__(self, config, stasis_cli, dataform_cli):
        super().__init__()
        self.config = config
        self.stasis_cli = stasis_cli
        self.dataform_cli = dataform_cli
        self.running = True

    def start(self):
        """Initializes the scanning for files"""
        zipping_q = JoinableQueue()
        conversion_q = JoinableQueue()

        agilent_worker = AgilentWorker(self.stasis_cli, zipping_q, conversion_q)
        agilent_worker.daemon = True

        conversion_worker = ConversionWorker(
            self.stasis_cli,
            self.dataform_cli,
            conversion_q,
            self.config['monitor']['storage']
        )
        conversion_worker.daemon = True

        threads = [agilent_worker, conversion_worker]

        for t in threads:
            print(f"starting thread {t.name}...")
            t.start()

        event_handler = monitor.NewFileScanner(
            self.stasis_cli,
            zipping_q,
            conversion_q,
            self.config['monitor']['extensions']
        )

        observer = Observer()
        for p in self.config['monitor']['paths']:
            print(f'adding path {p} to observer')
            observer.schedule(event_handler, p, recursive=True)
        observer.start()

        print('monitor started')

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()

        observer.join()

        for t in threads:
            t.join()
