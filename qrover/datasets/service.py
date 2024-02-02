from .dataset import CSVDataset, Dataset
from ..storage.dataset_persistence import DataRepository, InmemDataRepository
from ..log.logging import logger


class DatasetService:
    def __init__(self) -> None:
        self.repository:DataRepository = InmemDataRepository()

    def add_csv_dataset(self, name: str, location: str, header:list[str]=[]) -> Dataset:
        dataset = CSVDataset(name, location, header);
        self.__persist(dataset)
        return dataset
    
    def __persist(self, dataset: Dataset):
        logger.info(f"persist {dataset.name} to db.")
        dataset.validate_access()
        self.repository.post(dataset);

    