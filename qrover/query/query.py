from typing import Self
import pandas as pd
from collections.abc import Sequence
from qRover.exceptions.exceptions import BadQueryException
from qRover.execution.engine import Engine
from qRover.query.comparison import ChainedConditions, Condition, ChainOperator, ComparisonOperator
from qRover.query.parser import Dimension
from qRover.query.qualifier import QualifiedDataQuery, QualifiedDataQueryBuilder


class Query(object):
    def __init__(self, fetch_attributes:Sequence[str], executionEngine: Engine) -> None:
        # Take in attributes to fetch from user. Does not start computation.
        self.set_op_dims(fetch_attributes)
        self._fetch_attributes = fetch_attributes
        self.chainedConditions:ChainedConditions|None = None
        self.engine = executionEngine

    
    def set_op_dims(self, fetch_attributes:Sequence[str]) -> None:
        self._op_dims = [Dimension.of(attribute) for attribute in fetch_attributes]


    @property
    def fetch_attributes(self) -> Sequence[str]:
        return self._fetch_attributes
    
    def where(self, operand1:str, operator: ComparisonOperator|str, operand2:str)-> Self:
        # Basic implementation of a where clause.
        # Takes in 2 operands and an operator. Does not start computation.
        # Operator can be one of any values in ComparisonOperator enum.
        condition = Condition(operand1, operator, operand2)
        self.chainedConditions = ChainedConditions(condition)
        return self
    def and_(self,  operand1:str, operator: str|ComparisonOperator, operand2:str) -> Self:
        if self.chainedConditions is None:
            raise BadQueryException("Cannot call and_ on an empty query")
        condition = Condition(operand1, operator, operand2)
        self.chainedConditions.add(condition, ChainOperator.and_)
        return self
    def or_(self, operand1:str, operator: str|ComparisonOperator, operand2:str) -> Self:
        if self.chainedConditions is None:
            raise BadQueryException("Cannot call and_ on an empty query")
        condition = Condition(operand1, operator, operand2)
        self.chainedConditions.add(condition, ChainOperator.or_)
        return self
    def _validate(self)->None:
        # Validate the query. 
        pass
    def _qualify(self)->QualifiedDataQuery:
        queryBuilder = QualifiedDataQueryBuilder()
        qq = queryBuilder \
        .build_output_dimensions(self._op_dims) \
        .build_conditional_clauses(self.chainedConditions) \
        .build()
        qq.deambiguate()
        return qq

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
    
    @property
    def num_conditions(self)->int:
        return 0 if not self.chainedConditions else self.chainedConditions.numNodes
    @property
    def condition_str(self)->str:
        return "" if not self.chainedConditions else self.chainedConditions.as_str()

   




