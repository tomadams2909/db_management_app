import pytest
from db.schema import create_table, drop_table

# Uses the LOCAL docker DB defined in .env
TEST_DB_URL = "postgresql://admin:admin123@127.0.0.1:5433/testdb"
TEST_TABLE  = "pytest_crud_table"


@pytest.fixture(scope="session")
def db_url():
    return TEST_DB_URL


@pytest.fixture(scope="session")
def test_table(db_url):
    """Create a scratch table for the session, drop it on teardown."""
    create_table(
        db_url,
        TEST_TABLE,
        columns=[
            {"name": "name",  "type": "TEXT",    "nullable": False},
            {"name": "score", "type": "INTEGER", "nullable": True},
        ],
        pk_mode="serial",
    )
    yield TEST_TABLE
    drop_table(db_url, TEST_TABLE)
