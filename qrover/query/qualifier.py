
from typing import Self, Sequence
from qRover.datasets.dataset import Dataset
from qRover.datasets.service import DatasetService
from qRover.query.parser import AttributeParser, Dimension


class QualifiedDataQuery:
    attributes_parser = AttributeParser()
    dataset_service = DatasetService()

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

    def _inner_join(self, dataset1: Dataset, dataset2: Dataset, common_column:str):
        db1name = dataset1.name
        db2name = dataset2.name
        return f"inner join {db2name} on {db1name}.{common_column} = {db2name}.{common_column}"
    def _cross_join(self, dataset: Dataset):
        db1name = dataset.name
        return f"cross join {db1name}"

    def _join_datasets_sql(self) -> str:
        datasets = list(self._datasets)
        joined = datasets[0].name
        if len(datasets) < 2:
            return joined
        for i in range (1, len(datasets)):
            common_column = self.dataset_service.intersection_col(datasets[i-1], datasets[i])
            if common_column:
                joined = f"{joined} {self._inner_join(datasets[i-1], datasets[i], common_column)}"
            else:
                # should raise a warning here, since cross join can potentially give very large results.
                joined = f"{joined} {self._cross_join(datasets[i])}"
        return joined

    def to_sql(self) -> str:
        comma_sep_cols  = ','.join([dim.qualified_str for dim in self.dims])
        joined_datasets = self._join_datasets_sql()
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


