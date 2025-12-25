"""Tests for async query method (simple query protocol)."""

import pytest

from pyro_postgres.async_ import Conn

from ..conftest import (
    get_test_db_url,
    setup_test_table_async,
)


class TestAsyncQueryBasic:
    """Test basic query functionality."""

    @pytest.mark.asyncio
    async def test_query_select_literal(self):
        """Test query with literal values."""
        conn = await Conn.new(get_test_db_url())
        results = await conn.query("SELECT 1 as num")
        assert len(results) == 1
        assert results[0][0] == 1
        await conn.close()

    @pytest.mark.asyncio
    async def test_query_select_multiple_columns(self):
        """Test query returning multiple columns."""
        conn = await Conn.new(get_test_db_url())
        results = await conn.query("SELECT 1 as a, 'hello' as b, 3.14::float as c")
        assert len(results) == 1
        assert results[0][0] == 1
        assert results[0][1] == "hello"
        assert abs(results[0][2] - 3.14) < 0.001
        await conn.close()

    @pytest.mark.asyncio
    async def test_query_from_table(self):
        """Test query from table."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")
        await conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Bob', 25)")
        results = await conn.query("SELECT name, age FROM test_table ORDER BY name")
        assert len(results) == 2
        assert results[0][0] == "Alice"
        assert results[0][1] == 30
        assert results[1][0] == "Bob"
        assert results[1][1] == 25
        await conn.close()


class TestAsyncQueryNoResults:
    """Test query when no rows match."""

    @pytest.mark.asyncio
    async def test_query_no_results_returns_empty_list(self):
        """Test query returns empty list when no rows match."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")
        results = await conn.query("SELECT name FROM test_table WHERE age > 100")
        assert results == []
        await conn.close()

    @pytest.mark.asyncio
    async def test_query_empty_table(self):
        """Test query on empty table."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        results = await conn.query("SELECT name FROM test_table")
        assert results == []
        await conn.close()


class TestAsyncQueryAsDict:
    """Test query with as_dict option."""

    @pytest.mark.asyncio
    async def test_query_as_dict_returns_dicts(self):
        """Test query with as_dict=True returns dictionaries."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")
        await conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Bob', 25)")
        results = await conn.query(
            "SELECT name, age FROM test_table ORDER BY name", as_dict=True
        )
        assert len(results) == 2
        assert all(isinstance(r, dict) for r in results)
        assert results[0]["name"] == "Alice"
        assert results[0]["age"] == 30
        await conn.close()

    @pytest.mark.asyncio
    async def test_query_as_dict_column_access(self):
        """Test as_dict results can be accessed by column name."""
        conn = await Conn.new(get_test_db_url())
        results = await conn.query("SELECT 42 as value, 'test' as label", as_dict=True)
        assert len(results) == 1
        assert results[0]["value"] == 42
        assert results[0]["label"] == "test"
        await conn.close()

    @pytest.mark.asyncio
    async def test_query_as_dict_no_results(self):
        """Test query as_dict returns empty list when no rows match."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        results = await conn.query(
            "SELECT name FROM test_table WHERE age > 100", as_dict=True
        )
        assert results == []
        await conn.close()


class TestAsyncQueryWithNull:
    """Test query with NULL values."""

    @pytest.mark.asyncio
    async def test_query_returns_null(self):
        """Test query returns NULL values correctly."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.query_drop(
            "INSERT INTO test_table (name, age) VALUES ('NoAge', NULL)"
        )
        results = await conn.query("SELECT name, age FROM test_table")
        assert len(results) == 1
        assert results[0][0] == "NoAge"
        assert results[0][1] is None
        await conn.close()

    @pytest.mark.asyncio
    async def test_query_null_literal(self):
        """Test query with NULL literal."""
        conn = await Conn.new(get_test_db_url())
        results = await conn.query("SELECT NULL as empty")
        assert len(results) == 1
        assert results[0][0] is None
        await conn.close()


class TestAsyncQueryMultipleRows:
    """Test query with multiple rows."""

    @pytest.mark.asyncio
    async def test_query_returns_all_rows(self):
        """Test query returns all matching rows."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.query_drop(
            "INSERT INTO test_table (name, age) VALUES ('Alice', 30), ('Bob', 25), ('Charlie', 35)"
        )
        results = await conn.query("SELECT name FROM test_table ORDER BY name")
        assert len(results) == 3
        assert results[0][0] == "Alice"
        assert results[1][0] == "Bob"
        assert results[2][0] == "Charlie"
        await conn.close()

    @pytest.mark.asyncio
    async def test_query_with_limit(self):
        """Test query with LIMIT clause."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.query_drop(
            "INSERT INTO test_table (name, age) VALUES ('Alice', 30), ('Bob', 25), ('Charlie', 35)"
        )
        results = await conn.query("SELECT name FROM test_table ORDER BY name LIMIT 2")
        assert len(results) == 2
        await conn.close()

    @pytest.mark.asyncio
    async def test_query_with_where(self):
        """Test query with WHERE clause."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.query_drop(
            "INSERT INTO test_table (name, age) VALUES ('Alice', 30), ('Bob', 25), ('Charlie', 35)"
        )
        results = await conn.query(
            "SELECT name FROM test_table WHERE age > 28 ORDER BY name"
        )
        assert len(results) == 2
        assert results[0][0] == "Alice"
        assert results[1][0] == "Charlie"
        await conn.close()


class TestAsyncQueryConnectionState:
    """Test connection state after query operations."""

    @pytest.mark.asyncio
    async def test_query_connection_usable_after(self):
        """Test connection is usable after query."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")
        results1 = await conn.query("SELECT name FROM test_table")
        assert len(results1) == 1
        results2 = await conn.query("SELECT age FROM test_table")
        assert len(results2) == 1
        assert results2[0][0] == 30
        await conn.close()

    @pytest.mark.asyncio
    async def test_query_multiple_calls(self):
        """Test multiple query calls in sequence."""
        conn = await Conn.new(get_test_db_url())
        r1 = await conn.query("SELECT 1")
        r2 = await conn.query("SELECT 2")
        r3 = await conn.query("SELECT 3")
        assert r1[0][0] == 1
        assert r2[0][0] == 2
        assert r3[0][0] == 3
        await conn.close()
