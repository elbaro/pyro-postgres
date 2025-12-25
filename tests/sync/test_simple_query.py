"""Tests for sync simple query protocol (text protocol)."""

import pytest

from pyro_postgres.sync import Conn

from ..conftest import (
    cleanup_test_table_sync,
    get_test_db_url,
    setup_test_table_sync,
)


class TestSyncQuery:
    """Test sync query method (simple query protocol)."""

    def test_query_select_single_row(self):
        """Test basic SELECT returning single row."""
        conn = Conn(get_test_db_url())
        result = conn.query("SELECT 42 as answer")
        assert len(result) == 1
        assert result[0][0] == 42
        conn.close()

    def test_query_select_multiple_rows(self):
        """Test SELECT returning multiple rows."""
        conn = Conn(get_test_db_url())
        result = conn.query("SELECT 1 UNION SELECT 2 UNION SELECT 3 ORDER BY 1")
        assert len(result) == 3
        assert result[0][0] == 1
        assert result[1][0] == 2
        assert result[2][0] == 3
        conn.close()

    def test_query_select_multiple_columns(self):
        """Test SELECT returning multiple columns."""
        from decimal import Decimal

        conn = Conn(get_test_db_url())
        result = conn.query("SELECT 1 as a, 'hello' as b, 3.14 as c")
        assert len(result) == 1
        assert result[0][0] == 1
        assert result[0][1] == "hello"
        # PostgreSQL returns 3.14 as NUMERIC, which becomes Decimal
        assert abs(result[0][2] - Decimal("3.14")) < Decimal("0.001")
        conn.close()

    def test_query_empty_result(self):
        """Test query with no results."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        result = conn.query("SELECT * FROM test_table WHERE id = 99999")
        assert len(result) == 0
        cleanup_test_table_sync(conn)
        conn.close()

    def test_query_with_nulls(self):
        """Test handling of NULL values."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")
        conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Bob', NULL)")
        results = conn.query("SELECT name, age FROM test_table ORDER BY name")
        assert len(results) == 2
        assert (results[0][0], results[0][1]) == ("Alice", 30)
        assert (results[1][0], results[1][1]) == ("Bob", None)
        cleanup_test_table_sync(conn)
        conn.close()

    def test_query_as_dict(self):
        """Test query with as_dict=True returns dictionaries."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")
        conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Bob', 25)")
        results = conn.query(
            "SELECT name, age FROM test_table ORDER BY age", as_dict=True
        )
        assert len(results) == 2
        assert isinstance(results[0], dict)
        assert isinstance(results[1], dict)
        assert results[0]["name"] == "Bob"
        assert results[0]["age"] == 25
        assert results[1]["name"] == "Alice"
        assert results[1]["age"] == 30
        cleanup_test_table_sync(conn)
        conn.close()

    def test_query_as_dict_with_nulls(self):
        """Test query with as_dict=True handles NULL values."""
        conn = Conn(get_test_db_url())
        result = conn.query("SELECT NULL as value", as_dict=True)
        assert len(result) == 1
        assert result[0]["value"] is None
        conn.close()


class TestSyncQueryFirst:
    """Test sync query_first method."""

    def test_query_first_returns_first_row(self):
        """Test query_first returns first row only."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT 42")
        assert result
        assert result[0] == 42
        conn.close()

    def test_query_first_multiple_rows(self):
        """Test query_first with multiple rows returns first."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT 1 UNION SELECT 2 ORDER BY 1")
        assert result
        assert result[0] == 1
        conn.close()

    def test_query_first_no_results(self):
        """Test query_first with no results returns None."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        result = conn.query_first("SELECT * FROM test_table WHERE id = 99999")
        assert result is None
        cleanup_test_table_sync(conn)
        conn.close()

    def test_query_first_as_dict(self):
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
        cleanup_test_table_sync(conn)
        conn.close()

    def test_query_first_as_dict_no_results(self):
        """Test query_first as_dict with no results returns None."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        result = conn.query_first(
            "SELECT * FROM test_table WHERE id = 99999", as_dict=True
        )
        assert result is None
        cleanup_test_table_sync(conn)
        conn.close()


class TestSyncQueryDrop:
    """Test sync query_drop method."""

    def test_query_drop_insert(self):
        """Test query_drop for INSERT."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")
        result = conn.query_first("SELECT name, age FROM test_table")
        assert result
        assert result[0] == "Alice"
        assert result[1] == 30
        cleanup_test_table_sync(conn)
        conn.close()

    def test_query_drop_update(self):
        """Test query_drop for UPDATE."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")
        conn.query_drop("UPDATE test_table SET age = 31 WHERE name = 'Alice'")
        result = conn.query_first("SELECT age FROM test_table WHERE name = 'Alice'")
        assert result[0] == 31
        cleanup_test_table_sync(conn)
        conn.close()

    def test_query_drop_delete(self):
        """Test query_drop for DELETE."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")
        conn.query_drop("DELETE FROM test_table WHERE name = 'Alice'")
        result = conn.query_first("SELECT * FROM test_table WHERE name = 'Alice'")
        assert result is None
        cleanup_test_table_sync(conn)
        conn.close()

    def test_query_drop_ddl(self):
        """Test query_drop for DDL statements."""
        conn = Conn(get_test_db_url())
        conn.query_drop("DROP TABLE IF EXISTS temp_test_table")
        conn.query_drop("CREATE TABLE temp_test_table (id INT)")
        result = conn.query_first(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_name = 'temp_test_table'"
        )
        assert result is not None
        conn.query_drop("DROP TABLE temp_test_table")
        conn.close()
