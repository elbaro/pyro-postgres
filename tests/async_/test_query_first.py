"""Tests for async query_first method (simple query protocol)."""

import pytest

from pyro_postgres.async_ import Conn

from ..conftest import (
    get_test_db_url,
    setup_test_table_async,
)


class TestAsyncQueryFirstBasic:
    """Test basic query_first functionality."""

    @pytest.mark.asyncio
    async def test_query_first_returns_first_row(self):
        """Test query_first returns first row only."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")
        await conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Bob', 25)")
        result = await conn.query_first(
            "SELECT name, age FROM test_table ORDER BY age DESC"
        )
        assert result is not None
        assert result[0] == "Alice"
        assert result[1] == 30
        await conn.close()

    @pytest.mark.asyncio
    async def test_query_first_select_literal(self):
        """Test query_first with literal value."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first("SELECT 42 as num")
        assert result is not None
        assert result[0] == 42
        await conn.close()

    @pytest.mark.asyncio
    async def test_query_first_multiple_columns(self):
        """Test query_first with multiple columns."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first("SELECT 1 as a, 'hello' as b, 3.14::float as c")
        assert result is not None
        assert result[0] == 1
        assert result[1] == "hello"
        assert abs(result[2] - 3.14) < 0.001
        await conn.close()


class TestAsyncQueryFirstNoResults:
    """Test query_first when query returns no rows."""

    @pytest.mark.asyncio
    async def test_query_first_no_results_returns_none(self):
        """Test query_first returns None when no rows match."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")
        result = await conn.query_first("SELECT name FROM test_table WHERE age > 100")
        assert result is None
        await conn.close()

    @pytest.mark.asyncio
    async def test_query_first_empty_table_returns_none(self):
        """Test query_first returns None on empty table."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        result = await conn.query_first("SELECT name FROM test_table")
        assert result is None
        await conn.close()


class TestAsyncQueryFirstAsDict:
    """Test query_first with as_dict option."""

    @pytest.mark.asyncio
    async def test_query_first_as_dict_returns_dict(self):
        """Test query_first with as_dict=True returns dictionary."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")
        result = await conn.query_first(
            "SELECT name, age FROM test_table ORDER BY age DESC", as_dict=True
        )
        assert result is not None
        assert isinstance(result, dict)
        assert result["name"] == "Alice"
        assert result["age"] == 30
        await conn.close()

    @pytest.mark.asyncio
    async def test_query_first_as_dict_no_results(self):
        """Test query_first as_dict returns None when no rows match."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        result = await conn.query_first(
            "SELECT name FROM test_table WHERE age > 100", as_dict=True
        )
        assert result is None
        await conn.close()

    @pytest.mark.asyncio
    async def test_query_first_as_dict_column_access(self):
        """Test as_dict result can be accessed by column name."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first(
            "SELECT 42 as value, 'test' as label", as_dict=True
        )
        assert result is not None
        assert result["value"] == 42
        assert result["label"] == "test"
        await conn.close()


class TestAsyncQueryFirstWithNull:
    """Test query_first with NULL values."""

    @pytest.mark.asyncio
    async def test_query_first_returns_null_column(self):
        """Test query_first returns row with NULL column value."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.query_drop(
            "INSERT INTO test_table (name, age) VALUES ('NoAge', NULL)"
        )
        result = await conn.query_first("SELECT name, age FROM test_table")
        assert result is not None
        assert result[0] == "NoAge"
        assert result[1] is None
        await conn.close()

    @pytest.mark.asyncio
    async def test_query_first_null_literal(self):
        """Test query_first with NULL literal."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first("SELECT NULL as empty")
        assert result is not None
        assert result[0] is None
        await conn.close()


class TestAsyncQueryFirstConnectionState:
    """Test connection state after query_first operations."""

    @pytest.mark.asyncio
    async def test_query_first_connection_usable_after(self):
        """Test connection is usable after query_first."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")
        result1 = await conn.query_first("SELECT name FROM test_table")
        assert result1 is not None
        result2 = await conn.query_first("SELECT age FROM test_table")
        assert result2 is not None
        assert result2[0] == 30
        await conn.close()

    @pytest.mark.asyncio
    async def test_query_first_multiple_calls(self):
        """Test multiple query_first calls in sequence."""
        conn = await Conn.new(get_test_db_url())
        r1 = await conn.query_first("SELECT 1")
        r2 = await conn.query_first("SELECT 2")
        r3 = await conn.query_first("SELECT 3")
        assert r1[0] == 1
        assert r2[0] == 2
        assert r3[0] == 3
        await conn.close()
