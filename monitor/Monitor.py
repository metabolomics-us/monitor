import os
import time
import logging
import zipfile

from watchdog.observers import Observer
from multiprocessing import Process

import monitor.NewFileScanner


class Monitor():

    config = ""
    stasis_cli = ""
    zipping_q = ""
    conversion_q = ""

    def __init__(self, config, stasis_cli, dataform_cli, zipping_q, conversion_q):
        self.config = config
        self.stasis_cli = stasis_cli
        self.dataform_cli = dataform_cli
        self.zipping_q = zipping_q
        self.conversion_q = conversion_q

    def agilent_worker(self):
        running = True
        while running:
            try:
                print('agilent_worker looking for something to do...')
                item = self.zipping_q.get()
                zipsize = 0

                while (os.stat(item).st_size > zipsize):
                    time.sleep(1)
                    zipsize = os.stat(item).st_size

                # 4. zip file
                self.compress(item)

                self.zipping_q.task_done()
            except KeyboardInterrupt:
                print("stopping agilent_worker")
                self.zipping_q.join()
                running = False

    def general_worker(self):
        running = True
        while running:
            try:
                print('general_worker looking for something to do...')
                item = self.conversion_q.get()

                print("from general worker %s" % item)

                # 5. upload file to converter
                # 6  wait for file conversion to finish
                # 7. store as mzML file
                if (self.dataform_cli.convert(item, 'mzml')):
                    # 8. trigger status converted
                    self.stasis_cli.set_tracking(item, "converted")
                else:
                    raise Exception("Error uploading/converting file %s" % item)

                self.conversion_q.task_done()
            except KeyboardInterrupt:
                print("stopping general_worker")
                self.conversion_q.join()
                running = False

    def compress(self, file):
        """Compresses a folder adding '.zip' to the original name

        Parameters
        ----------
            file : str
                The folder to be compressed
        """
        print("compressing folder %s..." % file)

        zipped = zipfile.ZipFile(f"{file}.zip", 'w', zipfile.ZIP_DEFLATED)

        # The root directory within the ZIP file.
        rootdir = os.path.basename(file)

        for dirpath, dirnames, filenames in os.walk(file):
            for filename in filenames:
                # Write the file named filename to the archive,
                # giving it the archive name 'arcname'.
                filepath = os.path.join(dirpath, filename)
                parentpath = os.path.relpath(filepath, file)
                arcname = os.path.join(rootdir, parentpath)
                print('*', end="", flush=True)
                zipped.write(filepath, arcname)

        zipped.close()

        print(f"\n... zipped %s" % zipped.filename)

        # 4.5 Add to conversion queue
        self.conversion_q.put(zipped.filename)


    def start(self):
        threads = [
            Process(name='Agilent worker', target=self.agilent_worker),
            Process(name='Convert worker', target=self.general_worker)
        ]

        for t in threads:
            print(f"starting thread {t.name}...")
            t.daemon = True
            t.start()

        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s - %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')

        event_handler = monitor.NewFileScanner(self.stasis_cli, self.zipping_q, self.conversion_q, self.config['monitor']['extensions'])

        observer = Observer()
        for p in self.config['monitor']['paths']:
            print(f'adding path {p} to observer')
            observer.schedule(event_handler, p, recursive=True)
        observer.start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()

        observer.join()

        for t in threads:
            t.join()
            t.terminate()
