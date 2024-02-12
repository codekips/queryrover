import pytest
from qRover.datasets.dataset import Dataset
from qRover.datasets.service import MetaDataService
from qRover.storage.dataset_persistence import DataRepository

class MockDataRepository(DataRepository):
    def post(self, dataset:Dataset):
        pass
    def add_to_DB(self, dataset: Dataset) -> None:
        pass
    def get(self, dataset_name: str)->Dataset|None:
        pass
@pytest.fixture
def service():
    return MetaDataService()

def test_add_csv_dataset(service: MetaDataService):
    mock_repo = MockDataRepository()
    service.repository = mock_repo
    name = "test_dataset"
    location = "/Users/aarora7/personal/code/BE/python/queryrover/tests/dumps/small_file.csv"
    headers = ["header1", "header2"]

    dataset = service.add_csv_dataset(name, location, headers)

    assert isinstance(dataset, Dataset)
    assert dataset.name == name
    assert dataset.location == location
