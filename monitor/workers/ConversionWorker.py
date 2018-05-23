from multiprocessing import Process


class ConversionWorker(Process):
    """Worker class that converts a raw data file to mzml

    Parameters
    ----------
        st_cli: StasisClient
            Rest client object to interact with the stasis api
        df_cli: DataformerClient
            Rest client object to interact with the dataformer api
        conversion_q: JoinableQueue
            A queue to hold the files to be converted to mzml
        storage: str
            A folder destination for the converted files
    """

    def __init__(self, st_cli, df_cli, conversion_q, storage, name='conversion_worker'):
        super().__init__(name=name)
        self.stasis_cli = st_cli
        self.dataform_cli = df_cli
        self.conversion_q = conversion_q
        self.storage = storage

    def run(self):
        """Starts the processing of elements in the conversion queue"""

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
                    self.stasis_cli.set_tracking(item.replace(".zip", ""), "converted")
                else:
                    raise Exception({'message': "Error uploading/converting file %s" % item})

                self.conversion_q.task_done()
            except KeyboardInterrupt:
                print("stopping conversion_worker")
                self.conversion_q.join()
                running = False
