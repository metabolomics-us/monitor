class NoProfileException(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(args, kwargs)


class SampleNotFoundException(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(args, kwargs)


class JobDataStoreException(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(args, kwargs)
