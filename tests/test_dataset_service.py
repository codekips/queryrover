import pytest
from qrover.datasets.dataset import Dataset
from qrover.datasets.service import DatasetService
from qrover.storage.dataset_persistence import DataRepository

class MockDataRepository(DataRepository):
    def post(self, dataset:Dataset):
        pass
    def add_to_DB(self, dataset: Dataset) -> None:
        pass
    def get(self, dataset_name: str)->Dataset|None:
        pass
@pytest.fixture
def service():
    return DatasetService()

def test_add_csv_dataset(service: DatasetService):
    mock_repo = MockDataRepository()
    service.repository = mock_repo
    name = "test_dataset"
    location = "./dumps/small_file.csv"
    headers = ["header1", "header2"]

    dataset = service.add_csv_dataset(name, location, headers)

    assert isinstance(dataset, Dataset)
    assert dataset.name == name
    assert dataset.location == location
