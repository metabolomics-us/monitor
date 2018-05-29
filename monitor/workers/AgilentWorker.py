#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import time
import zipfile
from multiprocessing import Process


class AgilentWorker(Process):
    """Worker class that zips an agilent folder

    Parameters
    ----------
        st_cli: StasisClient
            Rest client object to interact with the stasis api
        zipping_q: multiprocessing.JoinableQueue
            A queue to hold the folders to be compressed
        conversion_q: multiprocessing.JoinableQueue
            A queue to hold the files to be converted to mzml
        storage: str
            Destiantion of the zipped file (to avoid zipping in the raw data folder and having permission issues)
    """

    def __init__(self, st_cli, zipping_q, conversion_q, storage, name='agilent_worker'):
        super().__init__(name=name)
        self.stasis_cli = st_cli
        self.zipping_q = zipping_q
        self.conversion_q = conversion_q
        self.storage = storage

    def run(self):
        """Starts the processing of elements in the zipping queue"""

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
                self.__compress(item)

                self.zipping_q.task_done()
            except KeyboardInterrupt:
                print("stopping agilent_worker")
                self.zipping_q.join()
                self.conversion_q.join()
                running = False
            except Exception as ex:
                print('Got exception: %s' % str(ex))
                print('Skipping current sample (%s)' % str(item))
                self.zipping_q.task_done()
                pass

    def __compress(self, folder):
        """Compresses a folder, and adds it to the conversion queue

        Parameters
        ----------
            folder : str
                The folder to be compressed
        """
        filename = folder.split('/')[-1]
        print('filename: %s' % filename)
        print("compressing folder %s to %s/%s.zip" % (folder, self.storage, filename))

        zf = '%s/%s.zip' % (self.storage, filename)
        zipped = zipfile.ZipFile(zf, 'w', zipfile.ZIP_DEFLATED)

        # The root directory within the ZIP folder.
        rootdir = os.path.basename(folder)

        for dirpath, dirnames, filenames in os.walk(folder):
            for filename in filenames:
                # Write the folder named filename to the archive,
                # giving it the archive name 'arcname'.
                filepath = os.path.join(dirpath, filename)
                parentpath = os.path.relpath(filepath, folder)
                arcname = os.path.join(rootdir, parentpath)
                print('*', end="", flush=True)
                zipped.write(filepath, arcname)

        zipped.close()

        print(f"\n... zipped %s" % zipped.filename)

        # 4.5 Add to conversion queue
        self.conversion_q.put(zipped.filename)
