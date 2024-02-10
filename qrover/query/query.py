from typing import Self
import pandas as pd
from collections.abc import Sequence
from qRover.execution.engine import Engine
from qRover.query.qualifier import QualifiedDataQuery, QualifiedDataQueryBuilder



class Query(object):
    
    def __init__(self, fetch_attributes:Sequence[str], executionEngine: Engine) -> None:
        # Take in attributes to fetch from user. Does not start computation.
        self._fetch_attributes = fetch_attributes
        self.engine = executionEngine

    @property
    def fetch_attributes(self) -> Sequence[str]:
        return self._fetch_attributes
    
    def where(self, condition:str)-> '_ConditionalQuery':
        # Take in row selectors. Does not start computation.
        raise NotImplementedError();
    def _validate(self)->None:
        # Validate the query. 
        pass
    def _qualify(self)->QualifiedDataQuery:
        queryBuilder = QualifiedDataQueryBuilder()
        qq = queryBuilder.build_output_dimensions(self.fetch_attributes).build()
        return qq
        pass

    def compute(self)->pd.DataFrame:
        '''
        Begins query computation in sync mode. This is the final function that should be called to get the result of the query.
        returns a pandas dataframe.
        :rtype: pd.DataFrame
        '''
        qq = self._qualify()
        return self.engine.select(qq)
        
    def compute_async(self) -> str:
        raise NotImplementedError

    def __str__(self) -> str:
        return f"fetch: {self.fetch_attributes}"


class _ConditionalQuery(Query):
    def and_(self, condition:str) -> Self:
        return self
    def or_(self) -> Self:
        return self



