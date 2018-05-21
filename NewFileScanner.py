import os
import time
import logging
import threading
from multiprocessing import JoinableQueue, Process

# hack to import properties
import zipfile

import appconfig

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from api import DataformerClient, StasisClient

zipping_q = JoinableQueue()
conversion_q = JoinableQueue()

stasis_cli = StasisClient("https://test-api.metabolomics.us")
dataform_cli = DataformerClient("http://phobos.fiehnlab.ucdavis.edu", "9090", "/home/diego/.carrot_storage")

def agilent_worker():
    running = True
    while running:
        try:
            print('agilent_worker looking for something to do...')
            item = zipping_q.get()
            zipsize = 0

            while (os.stat(item).st_size > zipsize):
                time.sleep(1)
                zipsize = os.stat(item).st_size

            # 4. zip file
            compress(item)

            zipping_q.task_done()
        except KeyboardInterrupt:
            print("stopping agilent_worker")
            zipping_q.join()
            running = False

def general_worker():
    running = True
    while running:
        try:
            print('general_worker looking for something to do...')
            item = conversion_q.get()

            print("from general worker %s" % item)

            # 5. upload file to converter
            # 6  wait for file conversion to finish
            # 7. store as mzML file
            if (dataform_cli.convert(item, 'mzml')):
                # 8. trigger status converted
                stasis_cli.set_tracking(item, "converted")
            else:
                raise Exception("Error uploading/converting file %s" % item)

            conversion_q.task_done()
        except KeyboardInterrupt:
            print("stopping general_worker")
            conversion_q.join()
            running = False

def compress(file):
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
    conversion_q.put(zipped.filename)


class NewFileScanner(FileSystemEventHandler):
    def on_created(self, event):
        """Called when a file or directory is created.

        Parameters
        ----------
            event : DirCreatedEvent or FileCreatedEvent
                Event representing file/directory creation.
        """

        file = str(event.src_path)

        if (event.is_directory):
            # if it's a .d folder
            if (event.src_path.endswith('.d')):
                print("event %s" % event)
                # 3. trigger status acquired
                stasis_cli.set_tracking(file, "acquired")
                # 3.5 add to zipping queue
                zipping_q.put(event.src_path)
        else:
            # if it's a regular file
            fileName, fileExtension = os.path.splitext(file)

            if (fileExtension.lower() in appconfig.extensions and not '.mzml'):
                print("event %s" % event)
                # 2. wait till the size stays constant
                size = 0
                while (size < os.stat(file).st_size):
                    time.sleep(1)
                    size = os.stat(str(file)).st_size
                    print(size)

                # 3. trigger status acquired
                stasis_cli.set_tracking(file, "acquired")
                # 3.5 add to conversion queue
                conversion_q.put(file)


def monitor(paths):
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    event_handler = NewFileScanner()

    observer = Observer()
    for p in paths:
        print(f'adding path {p} to observer')
        observer.schedule(event_handler, p, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()


if __name__ == "__main__":

    threads = [
        Process(name='Agilent worker', target=agilent_worker),
        Process(name='Convert worker', target=general_worker)
    ]

    for t in threads:
        print(f"starting thread {t.name}...")
        t.daemon = True
        t.start()

    monitor(appconfig.paths)

    for t in threads:
        t.join()
        t.terminate()

