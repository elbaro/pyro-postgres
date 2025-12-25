"""Tests for sync exec_first method."""

from pyro_postgres.sync import Conn

from ..conftest import (
    get_test_db_url,
    setup_test_table_sync,
)


class TestSyncExecFirstBasic:
    """Test basic exec_first functionality."""

    def test_exec_first_returns_first_row(self):
        """Test exec_first returns first row only."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", 30),
        )
        conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Bob", 25),
        )
        result = conn.exec_first(
            "SELECT name, age FROM test_table ORDER BY age DESC", ()
        )
        assert result is not None
        assert result[0] == "Alice"
        assert result[1] == 30
        conn.close()

    def test_exec_first_with_params(self):
        """Test exec_first with parameters."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2), ($3, $4)",
            ("Alice", 30, "Bob", 25),
        )
        result = conn.exec_first(
            "SELECT name FROM test_table WHERE age > $1", (20,)
        )
        assert result is not None
        assert result[0] in ("Alice", "Bob")
        conn.close()

    def test_exec_first_single_param(self):
        """Test exec_first with single parameter."""
        conn = Conn(get_test_db_url())
        result = conn.exec_first("SELECT $1::int as num", (42,))
        assert result is not None
        assert result[0] == 42
        conn.close()


class TestSyncExecFirstNoResults:
    """Test exec_first when query returns no rows."""

    def test_exec_first_no_results_returns_none(self):
        """Test exec_first returns None when no rows match."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", 30),
        )
        result = conn.exec_first(
            "SELECT name, age FROM test_table WHERE age > $1", (100,)
        )
        assert result is None
        conn.close()

    def test_exec_first_empty_table_returns_none(self):
        """Test exec_first returns None on empty table."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        result = conn.exec_first("SELECT name FROM test_table", ())
        assert result is None
        conn.close()


class TestSyncExecFirstAsDict:
    """Test exec_first with as_dict option."""

    def test_exec_first_as_dict_returns_dict(self):
        """Test exec_first with as_dict=True returns dictionary."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", 30),
        )
        result = conn.exec_first(
            "SELECT name, age FROM test_table ORDER BY age DESC", (), as_dict=True
        )
        assert result is not None
        assert isinstance(result, dict)
        assert result["name"] == "Alice"
        assert result["age"] == 30
        conn.close()

    def test_exec_first_as_dict_no_results(self):
        """Test exec_first as_dict returns None when no rows match."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        result = conn.exec_first(
            "SELECT name, age FROM test_table WHERE age > $1", (100,), as_dict=True
        )
        assert result is None
        conn.close()

    def test_exec_first_as_dict_column_access(self):
        """Test as_dict result can be accessed by column name."""
        conn = Conn(get_test_db_url())
        result = conn.exec_first(
            "SELECT $1::int as value, $2::text as label", (42, "test"), as_dict=True
        )
        assert result is not None
        assert result["value"] == 42
        assert result["label"] == "test"
        conn.close()


class TestSyncExecFirstWithNull:
    """Test exec_first with NULL values."""

    def test_exec_first_with_null_param(self):
        """Test exec_first with NULL parameter."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", None),
        )
        result = conn.exec_first(
            "SELECT age FROM test_table WHERE name = $1", ("Alice",)
        )
        assert result is not None
        assert result[0] is None
        conn.close()

    def test_exec_first_returns_null_column(self):
        """Test exec_first returns row with NULL column value."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("NoAge", None),
        )
        result = conn.exec_first("SELECT name, age FROM test_table", ())
        assert result is not None
        assert result[0] == "NoAge"
        assert result[1] is None
        conn.close()


class TestSyncExecFirstConnectionState:
    """Test connection state after exec_first operations."""

    def test_exec_first_connection_usable_after(self):
        """Test connection is usable after exec_first."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", 30),
        )
        result1 = conn.exec_first("SELECT name FROM test_table", ())
        assert result1 is not None
        result2 = conn.exec_first(
            "SELECT age FROM test_table WHERE name = $1", ("Alice",)
        )
        assert result2 is not None
        assert result2[0] == 30
        conn.close()

    def test_exec_first_multiple_calls(self):
        """Test multiple exec_first calls in sequence."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2), ($3, $4)",
            ("Alice", 30, "Bob", 25),
        )
        r1 = conn.exec_first("SELECT name FROM test_table ORDER BY age ASC", ())
        r2 = conn.exec_first("SELECT name FROM test_table ORDER BY age DESC", ())
        assert r1[0] == "Bob"
        assert r2[0] == "Alice"
        conn.close()
