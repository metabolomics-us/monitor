from typing import Optional


class NoProfileException(Exception):
    def __init__(self, method: Optional[str] = None, version: Optional[str] = None):
        if method:
            self.method = method
        if version:
            self.version = version

        if self.method and self.version:
            self.message = f"\tCan't find valid profiles for method '{self.method}' and version '{self.version}'"
        else:
            self.message = f"\tCan't find valid profiles"

        super().__init__(self.message)


class SampleNotFoundException(Exception):
    def __init__(self, sample: Optional[str] = None):
        if sample:
            self.sample = sample
            self.message = f'\tAcquisition data for sample {self.sample} not found'
        else:
            self.message = f'\tAcquisition data not found'

        super().__init__(self.message)


class JobDataStoreException(Exception):
    def __init__(self, job: Optional[str] = None):
        if job:
            self.job = job
            self.message = f'\tError scheduling job {self.job}'
        else:
            self.message = f'\tError scheduling job'

        super().__init__(self.message)
