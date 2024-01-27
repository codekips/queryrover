class RoverMgmt(object):
    """
    Register dataset to the system.
    :param location : storage location of the dataset
    :param schema (optional): schema that defines column names and types for the dataset.
    For csv files, this schema would be inferred.
    :param type: format of the passed dataset.
    :param kwargs: any additional args that would be needed to add/ access this dataset.
    """
    def add_dataset(self, location: str, type: str='csv', schema=None, **kwargs):
        pass
