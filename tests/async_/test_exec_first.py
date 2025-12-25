"""Tests for async exec_first method."""

import pytest

from pyro_postgres.async_ import Conn

from ..conftest import (
    get_test_db_url,
    setup_test_table_async,
)


class TestAsyncExecFirstBasic:
    """Test basic exec_first functionality."""

    @pytest.mark.asyncio
    async def test_exec_first_returns_first_row(self):
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
        assert result is not None
        assert result[0] == "Alice"
        assert result[1] == 30
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_first_with_params(self):
        """Test exec_first with parameters."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2), ($3, $4)",
            ("Alice", 30, "Bob", 25),
        )
        result = await conn.exec_first(
            "SELECT name FROM test_table WHERE age > $1", (20,)
        )
        assert result is not None
        assert result[0] in ("Alice", "Bob")
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_first_single_param(self):
        """Test exec_first with single parameter."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.exec_first("SELECT $1::int as num", (42,))
        assert result is not None
        assert result[0] == 42
        await conn.close()


class TestAsyncExecFirstNoResults:
    """Test exec_first when query returns no rows."""

    @pytest.mark.asyncio
    async def test_exec_first_no_results_returns_none(self):
        """Test exec_first returns None when no rows match."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", 30),
        )
        result = await conn.exec_first(
            "SELECT name, age FROM test_table WHERE age > $1", (100,)
        )
        assert result is None
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_first_empty_table_returns_none(self):
        """Test exec_first returns None on empty table."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        result = await conn.exec_first("SELECT name FROM test_table", ())
        assert result is None
        await conn.close()


class TestAsyncExecFirstAsDict:
    """Test exec_first with as_dict option."""

    @pytest.mark.asyncio
    async def test_exec_first_as_dict_returns_dict(self):
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
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_first_as_dict_no_results(self):
        """Test exec_first as_dict returns None when no rows match."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        result = await conn.exec_first(
            "SELECT name, age FROM test_table WHERE age > $1", (100,), as_dict=True
        )
        assert result is None
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_first_as_dict_column_access(self):
        """Test as_dict result can be accessed by column name."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.exec_first(
            "SELECT $1::int as value, $2::text as label", (42, "test"), as_dict=True
        )
        assert result is not None
        assert result["value"] == 42
        assert result["label"] == "test"
        await conn.close()


class TestAsyncExecFirstWithNull:
    """Test exec_first with NULL values."""

    @pytest.mark.asyncio
    async def test_exec_first_with_null_param(self):
        """Test exec_first with NULL parameter."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", None),
        )
        result = await conn.exec_first(
            "SELECT age FROM test_table WHERE name = $1", ("Alice",)
        )
        assert result is not None
        assert result[0] is None
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_first_returns_null_column(self):
        """Test exec_first returns row with NULL column value."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("NoAge", None),
        )
        result = await conn.exec_first("SELECT name, age FROM test_table", ())
        assert result is not None
        assert result[0] == "NoAge"
        assert result[1] is None
        await conn.close()


class TestAsyncExecFirstConnectionState:
    """Test connection state after exec_first operations."""

    @pytest.mark.asyncio
    async def test_exec_first_connection_usable_after(self):
        """Test connection is usable after exec_first."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", 30),
        )
        result1 = await conn.exec_first("SELECT name FROM test_table", ())
        assert result1 is not None
        result2 = await conn.exec_first(
            "SELECT age FROM test_table WHERE name = $1", ("Alice",)
        )
        assert result2 is not None
        assert result2[0] == 30
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_first_multiple_calls(self):
        """Test multiple exec_first calls in sequence."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2), ($3, $4)",
            ("Alice", 30, "Bob", 25),
        )
        r1 = await conn.exec_first("SELECT name FROM test_table ORDER BY age ASC", ())
        r2 = await conn.exec_first("SELECT name FROM test_table ORDER BY age DESC", ())
        assert r1[0] == "Bob"
        assert r2[0] == "Alice"
        await conn.close()
