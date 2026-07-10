import pytest
from src.config.db_config import get_db_config
from src.db_manager import PostgresManager
import psycopg


@pytest.fixture
def real_db_manager():
    """Provides a live PostgresManager instance pointing to the development database."""
    db_config = get_db_config()
    with PostgresManager(db_config) as db:
        yield db


def test_db_connection_success(real_db_manager):
    """Tests if the context manager successfully establishes a cursor."""
    assert real_db_manager.cur is not None, "Database cursor should be initialized"
    assert not real_db_manager.conn.closed, "Connection should be active"


def test_db_execute_and_fetch(real_db_manager):
    """Tests basic query execution and data retrieval."""
    # Act
    result = real_db_manager.fetch_one("SELECT 1 + 1 AS math_test")

    # Assert
    assert result[0] == 2, "Database calculation execution failed"


def test_db_automatic_rollback_on_error(real_db_manager):
    """
    Tests if the Context Manager correctly catches exceptions,
    allows us to manually trigger a rollback, and prevents the
    database state from permanently crashing.
    """
    with pytest.raises(psycopg.errors.UndefinedTable):
        real_db_manager.execute_query("SELECT * FROM non_existent_table;")

    real_db_manager.conn.rollback()

    recovery_test = real_db_manager.fetch_one("SELECT 'recovered'")
    assert (
        recovery_test[0] == "recovered"
    ), "DB Manager failed to recover after rollback"
