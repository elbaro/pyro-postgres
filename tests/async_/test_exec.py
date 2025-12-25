"""Tests for async exec method (extended query protocol)."""

import pytest

from pyro_postgres.async_ import Conn

from ..conftest import (
    get_test_db_url,
    setup_test_table_async,
)


class TestAsyncExecBasic:
    """Test basic exec functionality."""

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
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_without_params(self):
        """Test exec without parameters (empty tuple)."""
        conn = await Conn.new(get_test_db_url())
        results = await conn.exec("SELECT 1 as num", ())
        assert len(results) == 1
        assert results[0][0] == 1
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


class TestAsyncExecWithNull:
    """Test exec with NULL values."""

    @pytest.mark.asyncio
    async def test_exec_with_null_param(self):
        """Test exec with NULL parameter."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", None),
        )
        results = await conn.exec(
            "SELECT age FROM test_table WHERE name = $1", ("Alice",)
        )
        assert len(results) == 1
        assert results[0][0] is None
        await conn.close()


class TestAsyncExecAsDict:
    """Test exec with as_dict option."""

    @pytest.mark.asyncio
    async def test_exec_as_dict_returns_dicts(self):
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
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_as_dict_column_access(self):
        """Test that as_dict results can be accessed by column name."""
        conn = await Conn.new(get_test_db_url())
        results = await conn.exec(
            "SELECT $1::int as value, $2::text as label", (42, "test"), as_dict=True
        )
        assert len(results) == 1
        assert results[0]["value"] == 42
        assert results[0]["label"] == "test"
        await conn.close()


class TestAsyncExecNoResults:
    """Test exec when query returns no rows."""

    @pytest.mark.asyncio
    async def test_exec_no_results_returns_empty_list(self):
        """Test exec returns empty list when no rows match."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", 30),
        )
        results = await conn.exec(
            "SELECT name, age FROM test_table WHERE age > $1", (100,)
        )
        assert results == []
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_no_results_as_dict(self):
        """Test exec as_dict returns empty list when no rows match."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", 30),
        )
        results = await conn.exec(
            "SELECT name, age FROM test_table WHERE age > $1", (100,), as_dict=True
        )
        assert results == []
        await conn.close()


class TestAsyncExecMultipleRows:
    """Test exec with multiple rows."""

    @pytest.mark.asyncio
    async def test_exec_returns_multiple_rows(self):
        """Test exec returns all matching rows."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2), ($3, $4), ($5, $6)",
            ("Alice", 30, "Bob", 25, "Charlie", 35),
        )
        results = await conn.exec("SELECT name FROM test_table ORDER BY name", ())
        assert len(results) == 3
        assert results[0][0] == "Alice"
        assert results[1][0] == "Bob"
        assert results[2][0] == "Charlie"
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_with_limit(self):
        """Test exec with LIMIT clause."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2), ($3, $4), ($5, $6)",
            ("Alice", 30, "Bob", 25, "Charlie", 35),
        )
        results = await conn.exec(
            "SELECT name FROM test_table ORDER BY name LIMIT $1", (2,)
        )
        assert len(results) == 2
        await conn.close()


class TestAsyncExecConnectionState:
    """Test connection state after exec operations."""

    @pytest.mark.asyncio
    async def test_exec_connection_usable_after(self):
        """Test connection is usable after exec."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", 30),
        )
        results1 = await conn.exec("SELECT name FROM test_table", ())
        assert len(results1) == 1
        results2 = await conn.exec(
            "SELECT age FROM test_table WHERE name = $1", ("Alice",)
        )
        assert len(results2) == 1
        assert results2[0][0] == 30
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_multiple_different_queries(self):
        """Test multiple different queries in sequence."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", 30),
        )
        r1 = await conn.exec("SELECT name FROM test_table WHERE age = $1", (30,))
        r2 = await conn.exec("SELECT age FROM test_table WHERE name = $1", ("Alice",))
        r3 = await conn.exec("SELECT COUNT(*) FROM test_table", ())
        assert r1[0][0] == "Alice"
        assert r2[0][0] == 30
        assert r3[0][0] == 1
        await conn.close()
