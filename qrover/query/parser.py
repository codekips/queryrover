from qRover.datasets.dataset import Dataset
from qRover.storage.column_dataset_persistence import ColumnDatasetRepository, InmemColumnDatasetRepository
from qRover.storage.dataset_persistence import DataRepository, InmemDataRepository

    
class Dimension:
    def __init__(self, raw_str: str, datasets: list[Dataset], qualified_str:str|None = "") -> None:
        self._raw_str = raw_str
        self._datasets = datasets
        self._qualified_str = qualified_str
        self._ambiguous = len(datasets) != 1
    @staticmethod
    def withColumn(colname:str, datasets: list[Dataset]) -> 'Dimension':
        qualified_name = datasets[0].get_schema().get_qualified_name(colname) if len(datasets) == 1 else ""
        return Dimension(colname,datasets, qualified_name)
    @property
    def datasets(self) -> list[Dataset]:
        return self._datasets
    @property
    def is_ambiguous(self) -> bool:
        return self._ambiguous
    @property
    def raw_str(self) -> str:
        return self._raw_str
    @property
    def qualified_str(self) -> str:
        return self._qualified_str if self._qualified_str else ""

class AttributeParser:
    column_storage:ColumnDatasetRepository = InmemColumnDatasetRepository()
    dataset_storage: DataRepository = InmemDataRepository()
    def _is_column(self, expr:str) -> bool:
        return self.column_storage.exists(expr)

    def to_Dimension(self, expr: str) -> Dimension:
        expr_lower = expr.lower().strip()
        if self._is_column(expr_lower):
            mapping = self.column_storage.get(expr_lower)
            return Dimension.withColumn(colname=expr_lower, datasets=list(mapping.datasets))
        else:
            # Convert expression to dimension.
            raise NotImplementedError()

    def to_Condition(self, expr: str) -> None:
        raise NotImplementedError()