import pytest
from qRover.exceptions.exceptions import BadQueryException
from qRover.mgmt import RoverMgmt
from qRover.qrover import QueryRover
from qRover.query.query import Query

@pytest.fixture
def rover():
    return QueryRover("")
@pytest.fixture
def rover_mgmt():
    return RoverMgmt()

def test_fetch_returns_query(rover: QueryRover):
    q: Query = rover.fetch(['column1', 'column2'])
    assert isinstance(q, Query)
def test_fetch_query_fails_if_no_attributes(rover: QueryRover):
    with pytest.raises(BadQueryException):
        rover.fetch([])
def test_fetch_query_fails_if_no_non_empty_attributes(rover: QueryRover):
    with pytest.raises(BadQueryException):
        rover.fetch(['', '', '  '])
def test_user_computes_query(rover: QueryRover, rover_mgmt: RoverMgmt):
    rover_mgmt.add_dataset(name="small_file", location="/Users/aarora7/personal/code/BE/python/queryrover/tests/dumps/small_file.csv") # type: ignore
    rover_mgmt.add_dataset(name="small_equity", location="/Users/aarora7/personal/code/BE/python/queryrover/tests/dumps/small_equity.csv") # type: ignore
    q: Query = rover.fetch(['family','product']);
    df = q.compute()
    assert len(df) == 100
