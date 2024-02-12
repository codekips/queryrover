from qRover.storage.column_dataset_persistence import InmemColumnDatasetRepository
from .dataset import CSVDataset, Dataset
from ..storage.dataset_persistence import InmemDataRepository
from ..log.logging import logger


class MetaDataService:
    columnrepo = InmemColumnDatasetRepository()
    dbrepo = InmemDataRepository()   
    def __init__(self) -> None:
        pass

    def add_csv_dataset(self, name: str, location: str, header:list[str]=[]) -> Dataset:
        dataset = CSVDataset(name, location, header);
        dataset.validate_access()
        logger.info(f"persist {dataset.name} to db.")
        self.dbrepo.post(dataset);
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
    def get_datasets_containing(self, col: str) -> set[Dataset]:
        return self.columnrepo.get_containing_databases(col)
        

    


    