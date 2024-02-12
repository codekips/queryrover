import pytest
from qRover.exceptions.exceptions import BadQueryException

from qRover.query.qualifier import QualifiedDataQuery
from qRover.query.query import Query
from qRover.spark.engine import SparkEngine

class DummySparkEngine(SparkEngine):
    def __init__(self) -> None:
        super().__init__()
    def _select(self, _): # type: ignore
        return None

@pytest.fixture
def baseQuery():
    engine = DummySparkEngine()
    return Query("col1", engine)

def test_create_conditional_query(baseQuery: Query):
    baseQuery.where("col1", ">", "5")
    assert baseQuery.chainedConditions is not None
    assert baseQuery.num_conditions == 1
def test_create_conditional_query_and(baseQuery: Query):
    baseQuery.where("col1", ">", "5").and_("col2", "<", "10")
    assert baseQuery.chainedConditions is not None
    assert baseQuery.num_conditions == 2
def test_create_conditional_query_or(baseQuery: Query):
    baseQuery.where("col1", ">", "5").and_("col2", "<", "10").or_("col1","==","12")
    assert baseQuery.chainedConditions is not None
    assert baseQuery.num_conditions == 3
def test_create_conditional_cannot_chain_without_base(baseQuery: Query):
    with pytest.raises(BadQueryException):
        baseQuery.and_("col1", ">", "5")
    with pytest.raises(BadQueryException):
        baseQuery.or_("col1", ">", "5")
def test_final_condition_str(baseQuery: Query):
    baseQuery.where("col1", ">", "5").and_("col2", "<", "10").or_("col1","==","12")
    expected_str = "(col1 > 5)and(col2 < 10)or(col1 == 12)"
    assert expected_str == baseQuery.condition_str
    


