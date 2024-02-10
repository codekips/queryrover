from abc import ABC, abstractmethod
import csv
from typing import Sequence
from ..log.logging import logger
from ..config.constants import VALIDATION_CHUNK_SIZE
from ..exceptions.exceptions import InvalidDatasetException
class DBSchema():
    def __init__(self, name: str, columns: Sequence[str]) -> None:
        self.dbName=name
        self.column_names = columns
        self.qualifed_name_map = { 
            col: f"{name}.{col}" for col in columns
        }
    def getDBName(self)->str:
        return self.dbName
    def get_qualified_name(self, col_name:str):
        return self.qualifed_name_map.get(col_name, col_name)
    def get_columns(self):
        return self.column_names
    def __str__(self) -> str:
        return f"{self.dbName} \n qualified names: {self.qualifed_name_map}"

class Dataset(ABC):
    def __init__(self, name: str, location: str) -> None:
        self.location = location
        self.name = name
        self.schema = None
        pass
    def get_schema(self) -> DBSchema:
        if not self.schema:
            raise InvalidDatasetException("get schema invoked without a schema")
        return self.schema
    @abstractmethod
    def validate_access(self):
        pass
    @abstractmethod
    def fetch(self):
        pass

    @abstractmethod
    def type(self) -> str:
        raise NotImplementedError();
    pass

class CSVDataset(Dataset):
    def __infer_schema(self) -> DBSchema:
        try:
            columns = []
            print("my location", self.location)
            with open(self.location, 'r', encoding='utf-8-sig') as db:
                reader = csv.DictReader(db)
                columns = reader.fieldnames
            if not columns:
                raise InvalidDatasetException(self.name, "No columns found in dataset")
            return DBSchema(self.name, columns)
             
        except:
            logger.error("Error reading database for headers")
            raise
    def __init__(self, name: str, location: str, header: list[str]|None = None) -> None:
        super().__init__(name, location)
        self.name = name
        self._type = "csv"
        self.schema = DBSchema(name, header) if header else self.__infer_schema()
    
    def fetch(self):
        pass
    def validate_access(self):
        with open(self.location, 'r') as db:
            db.read(VALIDATION_CHUNK_SIZE)
    def type(self) -> str:
        return self._type
            

    