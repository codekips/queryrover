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
    
    def intersection(self, datasets: list[Dataset]) -> set[str]:
        if len(datasets) == 1:
            return set() 
        common_columns:set[str] = set.intersection(*[set(dataset.get_schema().column_names) for dataset in datasets]) # type: ignore
        return common_columns
    def intersection_col(self, dataset1: Dataset, dataset2: Dataset) -> None|str:
        common_columns = self.intersection([dataset1, dataset2])
        if len(common_columns) == 0:
            return None
        return next(iter(common_columns)) 

    
    def __persist(self, dataset: Dataset):
        logger.info(f"persist {dataset.name} to db.")
        dataset.validate_access()
        self.repository.post(dataset);

    