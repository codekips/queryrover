from abc import ABC, abstractmethod
import pandas as pd

from qRover.query.qualifier import QualifiedDataQuery

class Engine (ABC):
    @abstractmethod
    def configure(self, kwargs: dict[str,str]) -> None:
        raise NotImplementedError()
    @abstractmethod
    def select(self, q: QualifiedDataQuery) -> pd.DataFrame:
        raise NotImplementedError()
    @abstractmethod
    def register_udf(self) -> None:
        raise NotImplementedError()
    def load_dataset(self) -> None:
        raise NotImplementedError()
