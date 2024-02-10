from abc import ABC, abstractmethod
from typing import Dict
from ..log.logging import logger
from ..datasets.dataset import Dataset
from ..exceptions.exceptions import InvalidDatasetException
from .column_dataset_persistence import ColumnDatasetRepository, InmemColumnDatasetRepository


class DataRepository(ABC):
    def __new__(cls) :
        if not hasattr(cls, 'instance'):
            cls.instance = super(DataRepository, cls).__new__(cls)
        return cls.instance
    def __init__(self) -> None:
        self.metadata_persistence:ColumnDatasetRepository = InmemColumnDatasetRepository()
        pass
    def post(self, dataset: Dataset)-> None:
        db_schema = dataset.get_schema()
        if not db_schema:
            raise InvalidDatasetException("No schema for the dataset. Can not insert")
        else:
            if self.exists(dataset.name):
                logger.info(f"{dataset.name} already exists in persistence. Ignoring request")
            else:
                self.add_to_DB(dataset)
                self.metadata_persistence.add_dataset_columns(dataset)
    @abstractmethod
    def add_to_DB(self, dataset: Dataset) -> None:
        raise NotImplementedError
    @abstractmethod
    def get(self, dataset_name: str)->Dataset:
        raise NotImplementedError()
    def remove(self, dataset_name: str) -> bool:
        raise NotImplementedError()
    def exists(self, dataset_name: str) -> bool:
        raise NotImplementedError()
    
class InmemDataRepository(DataRepository):
    # Create InmemDataRepository as a singleton.
    def __init__(self) -> None:
        super().__init__()
        self.datasets:Dict[str, Dataset] = {}
    def add_to_DB(self, dataset: Dataset)-> None:
        logger.info("chosen db=inmem, adding dataset.")
        if dataset.name in self.datasets:
            logger.info(f"{dataset.name} already exists in persistence. Ignoring request")
        else:
            self.datasets[dataset.name] = dataset          
    def get(self, dataset_name: str)->Dataset:
        return self.datasets[dataset_name]
    def exists(self, dataset_name: str) -> bool:
        return dataset_name in self.datasets
