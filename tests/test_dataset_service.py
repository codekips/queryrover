import pytest
from qRover.datasets.dataset import Dataset
from qRover.datasets.service import DatasetService
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
    return DatasetService()

def test_add_csv_dataset(service: DatasetService):
    mock_repo = MockDataRepository()
    service.repository = mock_repo
    name = "test_dataset"
    location = "/Users/aarora7/personal/code/BE/python/queryrover/tests/dumps/small_file.csv"
    headers = ["header1", "header2"]

    dataset = service.add_csv_dataset(name, location, headers)

    assert isinstance(dataset, Dataset)
    assert dataset.name == name
    assert dataset.location == location


#     qp = QueryProcessor()

#     qp.fetch("a").where("a>b").and_("a>10").and_("b>10").compute();
#     qp.fetch("sdf").compute();
#     qp.fetch("max(volume) * min(price)").where("price>10").compute();

# # Get latest previous close for all stocks whose face_value is 10

#     qp.fetch("max(prev_close)").agg("symbol").where("face_value>10")
#     select prev_close from equity,listing on equity.symbol=listing.symbol group by equity.symbol where 