"""Tests for sync extended query protocol (binary protocol with parameters)."""

import pytest

from pyro_postgres.sync import Conn

from ..conftest import (
    cleanup_test_table_sync,
    get_test_db_url,
    setup_test_table_sync,
)


class TestSyncExec:
    """Test sync exec method (extended query protocol)."""

    def test_exec_with_params(self):
        """Test exec with parameters."""
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
        results = conn.exec("SELECT name, age FROM test_table WHERE age > $1", (20,))
        assert len(results) == 2
        cleanup_test_table_sync(conn)
        conn.close()

    def test_exec_without_params(self):
        """Test exec without parameters."""
        conn = Conn(get_test_db_url())
        results = conn.exec("SELECT 1 as num", ())
        assert len(results) == 1
        assert results[0][0] == 1
        conn.close()

    def test_exec_empty_params(self):
        """Test exec with empty tuple params."""
        conn = Conn(get_test_db_url())
        results = conn.exec("SELECT 42 as answer", ())
        assert len(results) == 1
        assert results[0][0] == 42
        conn.close()

    def test_exec_single_param(self):
        """Test exec with single parameter."""
        conn = Conn(get_test_db_url())
        results = conn.exec("SELECT $1::int as num", (42,))
        assert len(results) == 1
        assert results[0][0] == 42
        conn.close()

    def test_exec_multiple_params(self):
        """Test exec with multiple parameters."""
        conn = Conn(get_test_db_url())
        results = conn.exec(
            "SELECT $1::int as a, $2::text as b, $3::float as c",
            (1, "hello", 3.14),
        )
        assert len(results) == 1
        assert results[0][0] == 1
        assert results[0][1] == "hello"
        assert abs(results[0][2] - 3.14) < 0.001
        conn.close()

    def test_exec_with_null_param(self):
        """Test exec with NULL parameter."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", None),
        )
        result = conn.query_first("SELECT age FROM test_table WHERE name = 'Alice'")
        assert result[0] is None
        cleanup_test_table_sync(conn)
        conn.close()

    def test_exec_as_dict(self):
        """Test exec with as_dict=True returns dictionaries."""
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
        results = conn.exec(
            "SELECT name, age FROM test_table WHERE age > $1", (20,), as_dict=True
        )
        assert len(results) == 2
        assert all(isinstance(r, dict) for r in results)
        names = {r["name"] for r in results}
        assert names == {"Alice", "Bob"}
        cleanup_test_table_sync(conn)
        conn.close()


class TestSyncExecFirst:
    """Test sync exec_first method."""

    def test_exec_first_returns_first(self):
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
        assert result
        assert (result[0], result[1]) == ("Alice", 30)
        cleanup_test_table_sync(conn)
        conn.close()

    def test_exec_first_no_results(self):
        """Test exec_first with no results returns None."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        result = conn.exec_first(
            "SELECT name, age FROM test_table WHERE age > $1", (100,)
        )
        assert result is None
        cleanup_test_table_sync(conn)
        conn.close()

    def test_exec_first_as_dict(self):
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
        cleanup_test_table_sync(conn)
        conn.close()

    def test_exec_first_as_dict_no_results(self):
        """Test exec_first as_dict with no results returns None."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        result = conn.exec_first(
            "SELECT name, age FROM test_table WHERE age > $1", (100,), as_dict=True
        )
        assert result is None
        cleanup_test_table_sync(conn)
        conn.close()


class TestSyncExecDrop:
    """Test sync exec_drop method."""

    def test_exec_drop_insert(self):
        """Test exec_drop for INSERT."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", 30),
        )
        result = conn.query_first("SELECT name, age FROM test_table")
        assert result
        assert result[0] == "Alice"
        assert result[1] == 30
        cleanup_test_table_sync(conn)
        conn.close()

    def test_exec_drop_update(self):
        """Test exec_drop for UPDATE."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", 30),
        )
        conn.exec_drop(
            "UPDATE test_table SET age = $1 WHERE name = $2",
            (31, "Alice"),
        )
        result = conn.query_first("SELECT age FROM test_table WHERE name = 'Alice'")
        assert result[0] == 31
        cleanup_test_table_sync(conn)
        conn.close()

    def test_exec_drop_delete(self):
        """Test exec_drop for DELETE."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", 30),
        )
        conn.exec_drop("DELETE FROM test_table WHERE name = $1", ("Alice",))
        result = conn.query_first("SELECT * FROM test_table WHERE name = 'Alice'")
        assert result is None
        cleanup_test_table_sync(conn)
        conn.close()


class TestSyncExecBatch:
    """Test sync exec_batch method."""

    def test_exec_batch_insert(self):
        """Test batch insertion."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        params = [
            ("Alice", 30),
            ("Bob", 25),
            ("Charlie", 35),
            ("David", 40),
            ("Eve", 28),
        ]
        conn.exec_batch("INSERT INTO test_table (name, age) VALUES ($1, $2)", params)
        count = conn.query_first("SELECT COUNT(*) FROM test_table")
        assert count
        assert count[0] == 5
        cleanup_test_table_sync(conn)
        conn.close()

    def test_exec_batch_empty(self):
        """Test batch with empty params list."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.exec_batch("INSERT INTO test_table (name, age) VALUES ($1, $2)", [])
        count = conn.query_first("SELECT COUNT(*) FROM test_table")
        assert count[0] == 0
        cleanup_test_table_sync(conn)
        conn.close()

    def test_exec_batch_single(self):
        """Test batch with single item."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        params = [("Alice", 30)]
        conn.exec_batch("INSERT INTO test_table (name, age) VALUES ($1, $2)", params)
        count = conn.query_first("SELECT COUNT(*) FROM test_table")
        assert count[0] == 1
        cleanup_test_table_sync(conn)
        conn.close()


class TestSyncPreparedStatementCaching:
    """Test prepared statement caching."""

    def test_statement_cache_reuse(self):
        """Test that prepared statements are cached and reused."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        query = "INSERT INTO test_table (name, age) VALUES ($1, $2)"
        # Execute same query multiple times
        conn.exec_drop(query, ("Alice", 30))
        conn.exec_drop(query, ("Bob", 25))
        conn.exec_drop(query, ("Charlie", 35))
        count = conn.query_first("SELECT COUNT(*) FROM test_table")
        assert count
        assert count[0] == 3
        cleanup_test_table_sync(conn)
        conn.close()

    def test_different_queries_different_statements(self):
        """Test that different queries use different prepared statements."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", 30),
        )
        results1 = conn.exec(
            "SELECT name FROM test_table WHERE age = $1", (30,)
        )
        results2 = conn.exec(
            "SELECT age FROM test_table WHERE name = $1", ("Alice",)
        )
        assert len(results1) == 1
        assert results1[0][0] == "Alice"
        assert len(results2) == 1
        assert results2[0][0] == 30
        cleanup_test_table_sync(conn)
        conn.close()


class TestSyncExecAffectedRows:
    """Test sync affected_rows after extended queries."""

    def test_affected_rows_insert(self):
        """Test affected_rows after INSERT."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2), ($3, $4), ($5, $6)",
            ("Alice", 30, "Bob", 25, "Charlie", 35),
        )
        affected = conn.affected_rows()
        assert affected == 3
        cleanup_test_table_sync(conn)
        conn.close()

    def test_affected_rows_update(self):
        """Test affected_rows after UPDATE."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2), ($3, $4), ($5, $6)",
            ("Alice", 30, "Bob", 25, "Charlie", 35),
        )
        conn.exec_drop("UPDATE test_table SET age = age + 1 WHERE age > $1", (25,))
        affected = conn.affected_rows()
        assert affected == 2
        cleanup_test_table_sync(conn)
        conn.close()

    def test_affected_rows_delete(self):
        """Test affected_rows after DELETE."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2), ($3, $4), ($5, $6)",
            ("Alice", 30, "Bob", 25, "Charlie", 35),
        )
        conn.exec_drop("DELETE FROM test_table WHERE age < $1", (30,))
        affected = conn.affected_rows()
        assert affected == 1
        cleanup_test_table_sync(conn)
        conn.close()
