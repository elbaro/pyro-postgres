"""Tests for sync query_first method (simple query protocol)."""

from pyro_postgres.sync import Conn

from ..conftest import (
    get_test_db_url,
    setup_test_table_sync,
)


class TestSyncQueryFirstBasic:
    """Test basic query_first functionality."""

    def test_query_first_returns_first_row(self):
        """Test query_first returns first row only."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")
        conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Bob', 25)")
        result = conn.query_first("SELECT name, age FROM test_table ORDER BY age DESC")
        assert result is not None
        assert result[0] == "Alice"
        assert result[1] == 30
        conn.close()

    def test_query_first_select_literal(self):
        """Test query_first with literal value."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT 42 as num")
        assert result is not None
        assert result[0] == 42
        conn.close()

    def test_query_first_multiple_columns(self):
        """Test query_first with multiple columns."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT 1 as a, 'hello' as b, 3.14::float as c")
        assert result is not None
        assert result[0] == 1
        assert result[1] == "hello"
        assert abs(result[2] - 3.14) < 0.001
        conn.close()


class TestSyncQueryFirstNoResults:
    """Test query_first when query returns no rows."""

    def test_query_first_no_results_returns_none(self):
        """Test query_first returns None when no rows match."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")
        result = conn.query_first("SELECT name FROM test_table WHERE age > 100")
        assert result is None
        conn.close()

    def test_query_first_empty_table_returns_none(self):
        """Test query_first returns None on empty table."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        result = conn.query_first("SELECT name FROM test_table")
        assert result is None
        conn.close()


class TestSyncQueryFirstAsDict:
    """Test query_first with as_dict option."""

    def test_query_first_as_dict_returns_dict(self):
        """Test query_first with as_dict=True returns dictionary."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")
        result = conn.query_first(
            "SELECT name, age FROM test_table ORDER BY age DESC", as_dict=True
        )
        assert result is not None
        assert isinstance(result, dict)
        assert result["name"] == "Alice"
        assert result["age"] == 30
        conn.close()

    def test_query_first_as_dict_no_results(self):
        """Test query_first as_dict returns None when no rows match."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        result = conn.query_first(
            "SELECT name FROM test_table WHERE age > 100", as_dict=True
        )
        assert result is None
        conn.close()

    def test_query_first_as_dict_column_access(self):
        """Test as_dict result can be accessed by column name."""
        conn = Conn(get_test_db_url())
        result = conn.query_first(
            "SELECT 42 as value, 'test' as label", as_dict=True
        )
        assert result is not None
        assert result["value"] == 42
        assert result["label"] == "test"
        conn.close()


class TestSyncQueryFirstWithNull:
    """Test query_first with NULL values."""

    def test_query_first_returns_null_column(self):
        """Test query_first returns row with NULL column value."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.query_drop("INSERT INTO test_table (name, age) VALUES ('NoAge', NULL)")
        result = conn.query_first("SELECT name, age FROM test_table")
        assert result is not None
        assert result[0] == "NoAge"
        assert result[1] is None
        conn.close()

    def test_query_first_null_literal(self):
        """Test query_first with NULL literal."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT NULL as empty")
        assert result is not None
        assert result[0] is None
        conn.close()


class TestSyncQueryFirstConnectionState:
    """Test connection state after query_first operations."""

    def test_query_first_connection_usable_after(self):
        """Test connection is usable after query_first."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")
        result1 = conn.query_first("SELECT name FROM test_table")
        assert result1 is not None
        result2 = conn.query_first("SELECT age FROM test_table")
        assert result2 is not None
        assert result2[0] == 30
        conn.close()

    def test_query_first_multiple_calls(self):
        """Test multiple query_first calls in sequence."""
        conn = Conn(get_test_db_url())
        r1 = conn.query_first("SELECT 1")
        r2 = conn.query_first("SELECT 2")
        r3 = conn.query_first("SELECT 3")
        assert r1[0] == 1
        assert r2[0] == 2
        assert r3[0] == 3
        conn.close()
