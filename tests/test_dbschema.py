import pytest

from qRover.datasets.dataset import DBSchema

@pytest.fixture
def dbschema():
    return DBSchema('dbname', ['column1', 'column2'])

def test_getDBName(dbschema: DBSchema):
    assert dbschema.getDBName() == 'dbname'

def test_get_qualified_name(dbschema: DBSchema):
    assert dbschema.get_qualified_name('column1') == 'dbname.column1'
    assert dbschema.get_qualified_name('column3') == 'column3'

def test_get_columns(dbschema: DBSchema):
    assert dbschema.get_columns() == ['column1', 'column2']

def test_str(dbschema: DBSchema):
    expected = "dbname \n qualified names: {'column1': 'dbname.column1', 'column2': 'dbname.column2'}"
    assert dbschema.__str__() == expected