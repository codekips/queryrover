
from typing import Self, Sequence
from qRover.datasets.dataset import Dataset

from qRover.query.parser import AttributeParser, Dimension


class QualifiedDataQuery:
    attributes_parser = AttributeParser()

    def __init__(self) -> None:
        self._datasets: set[Dataset] = set()
        self.is_ambiguous:bool = True
    
    @property
    def required_datasets(self) -> set[Dataset]:
        return self._datasets

    def clauses(self) -> None:
        raise NotImplementedError()


    @property
    def dimensions(self) -> list[Dimension]:
        return self.dims
    
    def _update_required_datasets(self, datasets_to_add: list[Dataset]) -> None:
        for ds in datasets_to_add:
            self._datasets.add(ds)
        

    @dimensions.setter
    def dimensions(self, dims: list[Dimension]) -> None:
        self.dims = dims
        used_datasets_confirmed:list[Dataset] = []
        for dimension in dims:
            if not dimension.is_ambiguous:
                for dataset in dimension.datasets:
                    used_datasets_confirmed.append(dataset)
        self._update_required_datasets(used_datasets_confirmed)

    def to_sql(self) -> str:
        comma_sep_cols  = ','.join([dim.qualified_str for dim in self.dims])
        joined_datasets = ','.join([ds.name for ds in self._datasets])
        # do inner join, on the common column between the two.
        return f"SELECT {comma_sep_cols} FROM {joined_datasets}"
    

class QualifiedDataQueryBuilder(object):
    attribute_parser = AttributeParser()
    def __init__(self) -> None:
        self.reset()
        pass
    def reset(self)->None:
        self._dataquery = QualifiedDataQuery()
    def build(self):
        dataquery = self._dataquery
        self.reset()
        return dataquery
    def build_output_dimensions(self, output_attribs:Sequence[str])->Self:
        dimensions = [self.attribute_parser.to_Dimension(attrib) for attrib in output_attribs]
        self._dataquery.dimensions = dimensions
        return self        
        
    def build_conditional_clauses(self):
        pass
    def build_joins(self):
        pass


