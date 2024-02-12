from qRover.storage.column_dataset_persistence import ColumnDatasetRepository, InmemColumnDatasetRepository
from qRover.storage.dataset_persistence import DataRepository, InmemDataRepository

    


class AttributeParser:
    
    column_storage:ColumnDatasetRepository = InmemColumnDatasetRepository()

    _instance = None
    def __new__(cls) -> 'AttributeParser':
        if cls._instance is None:
            print('Creating the object')
        cls._instance = super(AttributeParser, cls).__new__(cls)
        return cls._instance

    
    def is_column(self, expr:str) -> bool:
        return self.column_storage.exists(expr)

    def extract_columns(self, exprs: list[str]) -> list[str]:
        cols: list[str] = []
        for expr in exprs:
            if (self.is_column(expr)):
                cols.append(expr)
        return cols

class Dimension(object):
    attributeParser = AttributeParser()
    def __init__(self, raw_str: str, cols: list[str]) -> None:
        self._raw_str = raw_str
        self._identified_columns = cols
    @staticmethod
    def of(expr:str) -> 'Dimension':
        cols = Dimension.attributeParser.extract_columns([expr])
        return Dimension(expr, cols)
    @property
    def raw_str(self) -> str:
        return self._raw_str
    @property
    def identified_columns(self) -> list[str]:
        return self._identified_columns 


    # def to_Dimension(self, expr: str) -> Dimension:
    #     expr_lower = expr.lower().strip()
    #     if self.is_column(expr_lower):
    #         mapping = self.column_storage.get(expr_lower)
    #         return Dimension.withColumn(colname=expr_lower, datasets=list(mapping.datasets))
    #     else:
    #         # Convert expression to dimension.
    #         raise NotImplementedError()

    # def to_Condition(self, expr: str) -> None:
    #     raise NotImplementedError()