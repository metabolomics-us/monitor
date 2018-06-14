#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from threading import Thread


class ConversionWorker(Thread):
    """Worker class that converts a raw data file to mzml

    Parameters
    ----------
        st_cli: StasisClient
            Rest client object to interact with the stasis api
        df_cli: DataformerClient
            Rest client object to interact with the dataformer api
        conversion_q: Queue
            A queue to hold the files to be converted to mzml
        upload_q: Queue
            A queue to hold the files to be uploaded to aws bucket
        storage: str
            A folder destination for the converted files
    """

    def __init__(self, st_cli, df_cli, conversion_q, upload_q, storage, name='conversion_worker'):
        super().__init__(name=name)
        self.stasis_cli = st_cli
        self.dataform_cli = df_cli
        self.conversion_q = conversion_q
        self.upload_q = upload_q
        self.storage = storage

    def run(self):
        """Starts the processing of elements in the conversion queue"""

        running = True
        item = None
        while running:
            try:
                print("general_worker looking for something to do...\n")
                item = self.conversion_q.get()

                # 5. upload file to converter
                # 6  wait for file conversion to finish
                # 7. store as mzML file
                converted_file = self.dataform_cli.convert(item, 'mzml')
                if (converted_file):
                    # 7.5 add to upload queue
                    print("putting %s in aws queue" % converted_file)
                    self.upload_q.put(converted_file)
                    # 8. trigger status converted
                    self.stasis_cli.set_tracking(os.path.splitext(converted_file)[0], 'converted')
                else:
                    raise Exception({'message': 'Error uploading/converting file %s' % item})

                self.conversion_q.task_done()
            except KeyboardInterrupt:
                print("stopping conversion_worker")
                self.conversion_q.join()
                running = False
            except Exception as ex:
                print("Error: %s" % ex.args)
                print("skipping this sample conversion (%s)" % str(item))
                self.conversion_q.task_done()
                pass
