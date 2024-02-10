# type: ignore    
from qRover.execution.engine import Engine
from qRover.query.qualifier import QualifiedDataQuery
from ..datasets.dataset import Dataset
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as sparkDataFrame
import pandas as pd
from typing import Sequence


class SparkEngine(Engine):
    def configure(self, kwargs: dict[str,str]):
        appName = kwargs.get("APP_NAME", "qRover") 
        arrowEnabled:str = kwargs.get("ARROW_ENABLED", "true")

        session = SparkSession.builder \
            .master("local") \
            .appName(appName) \
            .config("spark.sql.execution.arrow.pyspark.enabled", arrowEnabled) \
            .getOrCreate()
        self.session = session

    def select(self, q: QualifiedDataQuery) -> pd.DataFrame:
        spark_df = self._select(q)
        return self._transform_to_pd_df(spark_df)
    def register_udf(self) -> None:
        raise NotImplementedError()
    def _select(self,q: QualifiedDataQuery) -> sparkDataFrame:
        raise NotImplementedError()
    def _transform_to_pd_df(self, df: sparkDataFrame) -> pd.DataFrame:
        return df.toPandas()
        
    


class SparkSqlEngine(SparkEngine):
    def __init__(self, kwargs: dict[str,str]) -> None:
        super().__init__()
        self.configure(kwargs)
    def _load(self,dataset: Dataset) -> None:
        self.session.read.option("header",True) \
          .csv(dataset.location) \
          .createOrReplaceTempView(dataset.name)
    def _load_if_not_available(self, datasets: Sequence[Dataset]) -> None:
        for dataset in datasets:
            self._load(dataset)    
    def _select(self, q: QualifiedDataQuery) -> sparkDataFrame:
        self._load_if_not_available(q.required_datasets)
        sql_str = q.to_sql()
        return self.session.sql(sql_str)