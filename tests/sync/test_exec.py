"""Tests for sync exec method (extended query protocol)."""

from pyro_postgres.sync import Conn

from ..conftest import (
    get_test_db_url,
    setup_test_table_sync,
)


class TestSyncExecBasic:
    """Test basic exec functionality."""

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
        conn.close()

    def test_exec_without_params(self):
        """Test exec without parameters (empty tuple)."""
        conn = Conn(get_test_db_url())
        results = conn.exec("SELECT 1 as num", ())
        assert len(results) == 1
        assert results[0][0] == 1
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


class TestSyncExecWithNull:
    """Test exec with NULL values."""

    def test_exec_with_null_param(self):
        """Test exec with NULL parameter."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", None),
        )
        results = conn.exec(
            "SELECT age FROM test_table WHERE name = $1", ("Alice",)
        )
        assert len(results) == 1
        assert results[0][0] is None
        conn.close()


class TestSyncExecAsDict:
    """Test exec with as_dict option."""

    def test_exec_as_dict_returns_dicts(self):
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
        conn.close()

    def test_exec_as_dict_column_access(self):
        """Test that as_dict results can be accessed by column name."""
        conn = Conn(get_test_db_url())
        results = conn.exec(
            "SELECT $1::int as value, $2::text as label", (42, "test"), as_dict=True
        )
        assert len(results) == 1
        assert results[0]["value"] == 42
        assert results[0]["label"] == "test"
        conn.close()


class TestSyncExecNoResults:
    """Test exec when query returns no rows."""

    def test_exec_no_results_returns_empty_list(self):
        """Test exec returns empty list when no rows match."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", 30),
        )
        results = conn.exec(
            "SELECT name, age FROM test_table WHERE age > $1", (100,)
        )
        assert results == []
        conn.close()

    def test_exec_no_results_as_dict(self):
        """Test exec as_dict returns empty list when no rows match."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", 30),
        )
        results = conn.exec(
            "SELECT name, age FROM test_table WHERE age > $1", (100,), as_dict=True
        )
        assert results == []
        conn.close()


class TestSyncExecMultipleRows:
    """Test exec with multiple rows."""

    def test_exec_returns_multiple_rows(self):
        """Test exec returns all matching rows."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2), ($3, $4), ($5, $6)",
            ("Alice", 30, "Bob", 25, "Charlie", 35),
        )
        results = conn.exec("SELECT name FROM test_table ORDER BY name", ())
        assert len(results) == 3
        assert results[0][0] == "Alice"
        assert results[1][0] == "Bob"
        assert results[2][0] == "Charlie"
        conn.close()

    def test_exec_with_limit(self):
        """Test exec with LIMIT clause."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2), ($3, $4), ($5, $6)",
            ("Alice", 30, "Bob", 25, "Charlie", 35),
        )
        results = conn.exec(
            "SELECT name FROM test_table ORDER BY name LIMIT $1", (2,)
        )
        assert len(results) == 2
        conn.close()


class TestSyncExecConnectionState:
    """Test connection state after exec operations."""

    def test_exec_connection_usable_after(self):
        """Test connection is usable after exec."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", 30),
        )
        results1 = conn.exec("SELECT name FROM test_table", ())
        assert len(results1) == 1
        results2 = conn.exec("SELECT age FROM test_table WHERE name = $1", ("Alice",))
        assert len(results2) == 1
        assert results2[0][0] == 30
        conn.close()

    def test_exec_multiple_different_queries(self):
        """Test multiple different queries in sequence."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", 30),
        )
        r1 = conn.exec("SELECT name FROM test_table WHERE age = $1", (30,))
        r2 = conn.exec("SELECT age FROM test_table WHERE name = $1", ("Alice",))
        r3 = conn.exec("SELECT COUNT(*) FROM test_table", ())
        assert r1[0][0] == "Alice"
        assert r2[0][0] == 30
        assert r3[0][0] == 1
        conn.close()
