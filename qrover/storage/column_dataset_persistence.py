from abc import ABC, abstractmethod
from ..datasets.dataset import DBSchema

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
    def add_schema(self, schema: DBSchema)->None:
        raise NotImplementedError
    

    '''
        Fetch [dataset.name] for given column name.
        If multiple datasets found for the same, return array of all.
        else array containing single dataset.
        :param column_name column name 
    '''
    @abstractmethod
    def get(self, column_name: str) -> list[str]:
        raise NotImplementedError


class InmemColumnDatasetRepository(ColumnDatasetRepository):
    def __init__(self) -> None:
        self.column_mapping:dict[str,set[str]] = {}
        pass
    def add_schema(self, schema: DBSchema)->None:
        db = schema.getDBName();
        for column in schema.get_columns():
            qualified_name = schema.get_qualified_name(column)
            if column not in self.column_mapping:
                self.column_mapping[column] = set()
            if qualified_name not in self.column_mapping:
                self.column_mapping[qualified_name] = set()
            self.column_mapping[column].add(db)
            self.column_mapping[qualified_name].add(db)

    def get_containing_databases(self, column_name: str) -> set[str]:
        return self.column_mapping[column_name]
    def get(self, column_name: str) -> list[str]:
        raise NotImplementedError
