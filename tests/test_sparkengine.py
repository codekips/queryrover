import pytest
from qRover.datasets.dataset import Dataset, CSVDataset
from qRover.spark.engine import SparkEngine
# from pyspark.sql.dataframe import DataFrame


@pytest.fixture
def sparkEngine():
    return SparkEngine()

@pytest.fixture
def datasets():
    return [CSVDataset(name="small_file", location="tests/dumps/small_file.csv"),
            CSVDataset(name="small_equity", location="tests/dumps/small_equity.csv")]