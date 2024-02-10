from collections.abc import Sequence

from qRover.exceptions.exceptions import BadQueryException
from qRover.execution.engine import Engine
from qRover.spark.engine import SparkSqlEngine
from .query.query import Query

        
class QueryRover(object):
    
    
    def __init__(self, configuration_ini:str) -> None:
        # self.execution_delegate = SparkEngine()
        # initialize the spark engine here.
        self.engine: Engine = SparkSqlEngine({})
        pass
    def load_udf(self) -> None:
        raise NotImplementedError();
    def fetch(self, attributes: Sequence[str]) -> Query:
        '''
        Initial Function of a query processor. Each query request MUST always contain some columns to fetch. More conditions can be added on this. Eventually, the compute function is called to actually run the specified query on the backend
        :param attributes type:Sequence[str] list of dimensions that the caller wants to get.        
        '''
        non_empty_attributes = [attr for attr in map(lambda attr: attr.strip(), attributes) if len(attr) > 0] 
        if len(non_empty_attributes) == 0:
            raise BadQueryException("Must request for atleast 1 non-empty attribute to be returned. Pass attrributes=[dimensions to fetch]")
        return Query(non_empty_attributes, self.engine)
