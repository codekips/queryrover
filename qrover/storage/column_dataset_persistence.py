from abc import ABC, abstractmethod
from typing import Iterable
from ..datasets.dataset import DBSchema, Dataset


class ColumnMapping (object):
    def __init__(self, name: str, datasets:Iterable[Dataset]) -> None:
        self._name = name
        self._datasets = datasets
    @property
    def name(self)->str:
        return self._name
    
    @property
    def datasets(self)->Iterable[Dataset]:
        return self._datasets
    
class ColumnDatasetRepository(ABC):
    
    def __new__(cls) :
        if not hasattr(cls, 'instance'):
            cls.instance = super(ColumnDatasetRepository, cls).__new__(cls)
        return cls.instance
    
    def __init__(self) -> None:
        pass
    '''
        Persist all columns in a dataset to the DB at once.
        :param dataset: input dataset.
        This method inserts the following entries to the DB
        for all columns in the dataset
            1. column.name -> dataset.name 
            2. column.qualified_name -> dataset.name 
    '''
    @abstractmethod
    # def add_schema(self, schema: DBSchema)->None:
    def add_dataset_columns(self, dataset: Dataset)->None:
        raise NotImplementedError
    


    '''
        Fetch [dataset.name] for given column name.
        If multiple datasets found for the same, return array of all.
        else array containing single dataset.
        :param column_name column name 
    '''
    @abstractmethod
    def get(self, column_name: str) -> ColumnMapping:
        raise NotImplementedError()
    @abstractmethod
    def exists(self, column_name:str) -> bool:
        raise NotImplementedError()



class InmemColumnDatasetRepository(ColumnDatasetRepository):
    def __init__(self) -> None:
        self.column_mapping:dict[str,set[Dataset]] = {}
    # def add_schema(self, schema: DBSchema)->None:
    #     db = schema.getDBName();
    #     for column in schema.get_columns():
    #         qualified_name = schema.get_qualified_name(column)
    #         if column not in self.column_mapping:
    #             self.column_mapping[column] = set()
    #         if qualified_name not in self.column_mapping:
    #             self.column_mapping[qualified_name] = set()
    #         self.column_mapping[column].add(db)
    #         self.column_mapping[qualified_name].add(db)
    def add_dataset_columns(self, dataset: Dataset)->None:
        schema = dataset.get_schema()
        for column in schema.get_columns():
            qualified_name = schema.get_qualified_name(column)
            if column not in self.column_mapping:
                self.column_mapping[column] = set()
            if qualified_name not in self.column_mapping:
                self.column_mapping[qualified_name] = set()
            self.column_mapping[column].add(dataset)
            self.column_mapping[qualified_name].add(dataset)

    def get_containing_databases(self, column_name: str) -> set[Dataset]:
        return self.column_mapping[column_name]
    
    def get(self, column_name: str) -> ColumnMapping:
        datasets = self.column_mapping[column_name]
        return ColumnMapping(column_name,datasets)
    
    def exists(self, column_name:str) -> bool:
        return column_name in self.column_mapping
