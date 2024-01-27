from .metadata import MetaData
from abc import ABC, abstractmethod
class Dataset(ABC):
    def __init__(self, location) -> None:
        self.location = None
        pass
    def getMetadata() -> MetaData:
        pass
    @abstractmethod
    def validate_access():
        pass
    @abstractmethod
    def fetch():
        pass
    pass

class CSVDataset(Dataset):
    def __init__(self) -> None:
        super().__init__()
        pass
    def fetch():
        pass
    def validate_access():
        pass
    pass