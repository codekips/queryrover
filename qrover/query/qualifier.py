
from typing import Self, Sequence
from qRover.datasets.dataset import Dataset
from qRover.datasets.service import MetaDataService
from qRover.exceptions.exceptions import BadQueryException
from qRover.query.comparison import ChainedConditions
from qRover.query.parser import AttributeParser, Dimension
from ..log.logging import logger

class QualifiedDataQuery:
    metadataService = MetaDataService()
    attribute_parser = AttributeParser()

    

    def __init__(self) -> None:
       
        
        self.is_ambiguous:bool = True
        self._condition_attributes: set[str] = set()
        self._dims_attributes: set[str] = set()
        self._condition_chain = None

        self._required_columns : set[str] = set()
        self._column_dataset_map : dict[str, Dataset] = {}
        self._required_datasets : set[Dataset] = set()

    def set_required_datasets(self, datasets: set[Dataset]):
        self._required_datasets = datasets
    
    @property
    def required_datasets(self)-> set[Dataset]:
        return self._required_datasets
    
    def set_mapping (self, mapping: dict[str, Dataset]) -> None:
        self._column_dataset_map = mapping
    
    @property
    def required_attributes(self)-> set[str]:
        return set(map(lambda x: x[0], self._column_dataset_map.items()))


    def set_condition_clause_chain(self, condition_chain: ChainedConditions) -> None:
        embedded_columns: set[str] = set()
        for condition in condition_chain:
            [embedded_columns.add(column) for column in condition.identified_columns]
        self._condition_chain = condition_chain
        self._condition_attributes = embedded_columns
    
    @property
    def condition_chain(self) -> None|ChainedConditions:
        return self._condition_chain
    
    def set_output_dimensions(self, dimensions: Sequence[Dimension]) -> None:
        # Set output dimenstions, and extract set of columns required for all output dimensions. Store in instance fields. 
        embedded_columns: set[str] = set()
        for dim in dimensions:
            for col in dim.identified_columns:
                embedded_columns.add(col)
        self._dims_attributes = embedded_columns
        self._dims = dimensions
    
    @property
    def dims(self) -> Sequence[Dimension]:
        return self._dims
    
    def _is_mapping_unambiguous(self, col_dataset_map: tuple[str,list[Dataset]]) -> bool:
        (_, datasets) = col_dataset_map
        return len(datasets) == 1
    def _prepare_attribute_mappings(self) -> tuple[set[Dataset], dict[str, Dataset]]:
        confirmed_datasets: set[Dataset] = set()
        column_mapping: dict[str, Dataset] = dict()

        all_identified_attributes = self._dims_attributes.union(self._condition_attributes)
        all_column_mappings = [(attrib, list(self.metadataService.get_datasets_containing(attrib))) for attrib in all_identified_attributes]        
        unambiguous_mappings = filter(self._is_mapping_unambiguous,all_column_mappings)
        
        for (col, datasets) in unambiguous_mappings:
            confirmed_datasets.add(datasets[0])
            column_mapping[col] = list(datasets)[0]
        
        ambiguous_mappings = list(filter(lambda colmap: not self._is_mapping_unambiguous(colmap),all_column_mappings))
        if (not confirmed_datasets) & len(ambiguous_mappings)>0:
               raise BadQueryException("Query is ambiguous. No way of selecting columns from datasets")
        for (col, datasets) in ambiguous_mappings:
            logger.info(f"Attempting to deambiguate datasets {datasets} for column: {col}")
            matching_dataset = next(filter(lambda dataset: dataset in confirmed_datasets, datasets))
            column_mapping[col] = matching_dataset
        self.set_mapping(column_mapping)
        self.set_required_datasets(confirmed_datasets)
        self.is_ambiguous=False
        return (confirmed_datasets, column_mapping)
    def _update_qualifications(self, attrib_dataset_mappings: dict[str, Dataset])->None:
        [dim.qualify(attrib_dataset_mappings) for dim in self._dims]
        if self._condition_chain:
            [condition.qualify(attrib_dataset_mappings) for condition in self._condition_chain ]
        pass
    def deambiguate(self) -> None:
        # Takes in all required columns in the different parts of the query, and makes sure they only map to a single dataset. This is an expensive operation, and must only be done once, when the query is ready to be used.
        (_, attrib_dataset_mappings) = self._prepare_attribute_mappings()
        self._update_qualifications(attrib_dataset_mappings)
        pass
    def isColumnMappingDeterminate(self) -> bool:
        return not self.is_ambiguous
    def _inner_join(self, dataset1: Dataset, dataset2: Dataset, common_column:str):
        db1name = dataset1.name
        db2name = dataset2.name
        return f"inner join {db2name} on {db1name}.{common_column} = {db2name}.{common_column}"
    def _cross_join(self, dataset: Dataset):
        db1name = dataset.name
        return f"cross join {db1name}"
    def _join_datasets_sql(self) -> str:
        datasets = list(self.required_datasets)
        joined = datasets[0].name
        if len(datasets) < 2:
            return joined
        for i in range (1, len(datasets)):
            common_column = self.metadataService.intersection_col(datasets[i-1], datasets[i])
            if common_column:
                joined = f"{joined} {self._inner_join(datasets[i-1], datasets[i], common_column)}"
            else:
                # should raise a warning here, since cross join can potentially give very large results.
                joined = f"{joined} {self._cross_join(datasets[i])}"
        return joined
    def to_sql(self) -> str:
        # update this thing.. all should qualify their strings.
        comma_sep_cols  = ','.join([dim.qual_str for dim in self._dims])
        joined_datasets = self._join_datasets_sql()
        clause = "" if not self.condition_chain else self.condition_chain.as_str()
        # do inner join, on the common column between the two.
        return f"SELECT {comma_sep_cols} FROM {joined_datasets} {'WHERE'+clause if len(clause)>0 else ''}".strip()
    

class QualifiedDataQueryBuilder(object):
    def __init__(self) -> None:
        self.reset()
        pass
    def reset(self)->None:
        self._dataquery = QualifiedDataQuery()
    def build(self):
        dataquery = self._dataquery
        self.reset()
        return dataquery
    def build_output_dimensions(self, dims:Sequence[Dimension])->Self:
        self._dataquery.set_output_dimensions(dims)
        return self        
    def build_conditional_clauses(self, conditionChain: ChainedConditions|None) -> Self:
        if conditionChain is not None:
            self._dataquery.set_condition_clause_chain(conditionChain)
        return self
    def build_joins(self):
        pass


