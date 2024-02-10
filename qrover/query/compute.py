from abc import ABC, abstractmethod

from qRover.query.query import QualifiedDataQuery




class QueryProcessorFactory(object):
    def __init__(self) -> None:
        pass
    @staticmethod
    def get_instance():
        pass



class QueryProcessor (ABC):
    engine = None
    def __init__(self) -> None:
        
        pass
    def execute(self, q: QualifiedDataQuery) -> None:
        sql = q.to_sql()
        print("sql: ", sql) 
        pass
