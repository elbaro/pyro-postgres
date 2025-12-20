"""Tests for async extended query protocol (binary protocol with parameters)."""

import pytest

from pyro_postgres.async_ import Conn

from ..conftest import (
    cleanup_test_table_async,
    get_test_db_url,
    setup_test_table_async,
)


class TestAsyncExec:
    """Test async exec method (extended query protocol)."""

    @pytest.mark.asyncio
    async def test_exec_with_params(self):
        """Test exec with parameters."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", 30),
        )
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Bob", 25),
        )
        results = await conn.exec(
            "SELECT name, age FROM test_table WHERE age > $1", (20,)
        )
        assert len(results) == 2
        await cleanup_test_table_async(conn)
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_without_params(self):
        """Test exec without parameters."""
        conn = await Conn.new(get_test_db_url())
        results = await conn.exec("SELECT 1 as num", ())
        assert len(results) == 1
        assert results[0][0] == 1
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_empty_params(self):
        """Test exec with empty tuple params."""
        conn = await Conn.new(get_test_db_url())
        results = await conn.exec("SELECT 42 as answer", ())
        assert len(results) == 1
        assert results[0][0] == 42
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_single_param(self):
        """Test exec with single parameter."""
        conn = await Conn.new(get_test_db_url())
        results = await conn.exec("SELECT $1::int as num", (42,))
        assert len(results) == 1
        assert results[0][0] == 42
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_multiple_params(self):
        """Test exec with multiple parameters."""
        conn = await Conn.new(get_test_db_url())
        results = await conn.exec(
            "SELECT $1::int as a, $2::text as b, $3::float as c",
            (1, "hello", 3.14),
        )
        assert len(results) == 1
        assert results[0][0] == 1
        assert results[0][1] == "hello"
        assert abs(results[0][2] - 3.14) < 0.001
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_with_null_param(self):
        """Test exec with NULL parameter."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", None),
        )
        result = await conn.query_first(
            "SELECT age FROM test_table WHERE name = 'Alice'"
        )
        assert result[0] is None
        await cleanup_test_table_async(conn)
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_as_dict(self):
        """Test exec with as_dict=True returns dictionaries."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", 30),
        )
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Bob", 25),
        )
        results = await conn.exec(
            "SELECT name, age FROM test_table WHERE age > $1", (20,), as_dict=True
        )
        assert len(results) == 2
        assert all(isinstance(r, dict) for r in results)
        names = {r["name"] for r in results}
        assert names == {"Alice", "Bob"}
        await cleanup_test_table_async(conn)
        await conn.close()


class TestAsyncExecFirst:
    """Test async exec_first method."""

    @pytest.mark.asyncio
    async def test_exec_first_returns_first(self):
        """Test exec_first returns first row only."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", 30),
        )
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Bob", 25),
        )
        result = await conn.exec_first(
            "SELECT name, age FROM test_table ORDER BY age DESC", ()
        )
        assert result
        assert (result[0], result[1]) == ("Alice", 30)
        await cleanup_test_table_async(conn)
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_first_no_results(self):
        """Test exec_first with no results returns None."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        result = await conn.exec_first(
            "SELECT name, age FROM test_table WHERE age > $1", (100,)
        )
        assert result is None
        await cleanup_test_table_async(conn)
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_first_as_dict(self):
        """Test exec_first with as_dict=True returns dictionary."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", 30),
        )
        result = await conn.exec_first(
            "SELECT name, age FROM test_table ORDER BY age DESC", (), as_dict=True
        )
        assert result is not None
        assert isinstance(result, dict)
        assert result["name"] == "Alice"
        assert result["age"] == 30
        await cleanup_test_table_async(conn)
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_first_as_dict_no_results(self):
        """Test exec_first as_dict with no results returns None."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        result = await conn.exec_first(
            "SELECT name, age FROM test_table WHERE age > $1", (100,), as_dict=True
        )
        assert result is None
        await cleanup_test_table_async(conn)
        await conn.close()


class TestAsyncExecDrop:
    """Test async exec_drop method."""

    @pytest.mark.asyncio
    async def test_exec_drop_insert(self):
        """Test exec_drop for INSERT."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", 30),
        )
        result = await conn.query_first("SELECT name, age FROM test_table")
        assert result
        assert result[0] == "Alice"
        assert result[1] == 30
        await cleanup_test_table_async(conn)
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_drop_update(self):
        """Test exec_drop for UPDATE."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", 30),
        )
        await conn.exec_drop(
            "UPDATE test_table SET age = $1 WHERE name = $2",
            (31, "Alice"),
        )
        result = await conn.query_first(
            "SELECT age FROM test_table WHERE name = 'Alice'"
        )
        assert result[0] == 31
        await cleanup_test_table_async(conn)
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_drop_delete(self):
        """Test exec_drop for DELETE."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", 30),
        )
        await conn.exec_drop("DELETE FROM test_table WHERE name = $1", ("Alice",))
        result = await conn.query_first(
            "SELECT * FROM test_table WHERE name = 'Alice'"
        )
        assert result is None
        await cleanup_test_table_async(conn)
        await conn.close()


class TestAsyncExecBatch:
    """Test async exec_batch method."""

    @pytest.mark.asyncio
    async def test_exec_batch_insert(self):
        """Test batch insertion."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        params = [
            ("Alice", 30),
            ("Bob", 25),
            ("Charlie", 35),
            ("David", 40),
            ("Eve", 28),
        ]
        await conn.exec_batch(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)", params
        )
        count = await conn.query_first("SELECT COUNT(*) FROM test_table")
        assert count
        assert count[0] == 5
        await cleanup_test_table_async(conn)
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_batch_empty(self):
        """Test batch with empty params list."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.exec_batch(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)", []
        )
        count = await conn.query_first("SELECT COUNT(*) FROM test_table")
        assert count[0] == 0
        await cleanup_test_table_async(conn)
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_batch_single(self):
        """Test batch with single item."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        params = [("Alice", 30)]
        await conn.exec_batch(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)", params
        )
        count = await conn.query_first("SELECT COUNT(*) FROM test_table")
        assert count[0] == 1
        await cleanup_test_table_async(conn)
        await conn.close()


class TestAsyncPreparedStatementCaching:
    """Test prepared statement caching."""

    @pytest.mark.asyncio
    async def test_statement_cache_reuse(self):
        """Test that prepared statements are cached and reused."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        query = "INSERT INTO test_table (name, age) VALUES ($1, $2)"
        # Execute same query multiple times
        await conn.exec_drop(query, ("Alice", 30))
        await conn.exec_drop(query, ("Bob", 25))
        await conn.exec_drop(query, ("Charlie", 35))
        count = await conn.query_first("SELECT COUNT(*) FROM test_table")
        assert count
        assert count[0] == 3
        await cleanup_test_table_async(conn)
        await conn.close()

    @pytest.mark.asyncio
    async def test_different_queries_different_statements(self):
        """Test that different queries use different prepared statements."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", 30),
        )
        results1 = await conn.exec("SELECT name FROM test_table WHERE age = $1", (30,))
        results2 = await conn.exec(
            "SELECT age FROM test_table WHERE name = $1", ("Alice",)
        )
        assert len(results1) == 1
        assert results1[0][0] == "Alice"
        assert len(results2) == 1
        assert results2[0][0] == 30
        await cleanup_test_table_async(conn)
        await conn.close()


class TestAsyncExecAffectedRows:
    """Test async affected_rows after extended queries."""

    @pytest.mark.asyncio
    async def test_affected_rows_insert(self):
        """Test affected_rows after INSERT."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2), ($3, $4), ($5, $6)",
            ("Alice", 30, "Bob", 25, "Charlie", 35),
        )
        affected = await conn.affected_rows()
        assert affected == 3
        await cleanup_test_table_async(conn)
        await conn.close()

    @pytest.mark.asyncio
    async def test_affected_rows_update(self):
        """Test affected_rows after UPDATE."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2), ($3, $4), ($5, $6)",
            ("Alice", 30, "Bob", 25, "Charlie", 35),
        )
        await conn.exec_drop(
            "UPDATE test_table SET age = age + 1 WHERE age > $1", (25,)
        )
        affected = await conn.affected_rows()
        assert affected == 2
        await cleanup_test_table_async(conn)
        await conn.close()

    @pytest.mark.asyncio
    async def test_affected_rows_delete(self):
        """Test affected_rows after DELETE."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2), ($3, $4), ($5, $6)",
            ("Alice", 30, "Bob", 25, "Charlie", 35),
        )
        await conn.exec_drop("DELETE FROM test_table WHERE age < $1", (30,))
        affected = await conn.affected_rows()
        assert affected == 1
        await cleanup_test_table_async(conn)
        await conn.close()
