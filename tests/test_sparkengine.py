import pytest
from qRover.datasets.dataset import Dataset, CSVDataset
from qRover.spark.engine import SparkEngine
# from pyspark.sql.dataframe import DataFrame


@pytest.fixture
def sparkEngine():
    return SparkEngine()

@pytest.fixture
def datasets():
    return [CSVDataset(name="small_file", location="/Users/aarora7/personal/code/BE/python/queryrover/tests/dumps/small_file.csv"),
            CSVDataset(name="small_equity", location="/Users/aarora7/personal/code/BE/python/queryrover/tests/dumps/small_equity.csv")]