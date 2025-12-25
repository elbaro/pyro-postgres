"""Tests for async exec_drop method returning affected rows count."""

import pytest

from pyro_postgres.async_ import Conn

from ..conftest import (
    get_test_db_url,
    setup_test_table_async,
)


class TestAsyncExecDropReturnsAffectedRows:
    """Test that exec_drop returns affected rows count."""

    @pytest.mark.asyncio
    async def test_exec_drop_insert_returns_1(self):
        """Test exec_drop for single INSERT returns 1."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        affected = await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", 30),
        )
        assert affected == 1
        assert isinstance(affected, int)
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_drop_update_returns_affected_count(self):
        """Test exec_drop for UPDATE returns number of updated rows."""
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
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Charlie", 35),
        )
        affected = await conn.exec_drop(
            "UPDATE test_table SET age = age + 1 WHERE age > $1",
            (25,),
        )
        assert affected == 2
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_drop_delete_returns_affected_count(self):
        """Test exec_drop for DELETE returns number of deleted rows."""
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
        affected = await conn.exec_drop(
            "DELETE FROM test_table WHERE age < $1",
            (30,),
        )
        assert affected == 1
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_drop_multi_row_insert(self):
        """Test exec_drop for multi-row INSERT returns correct count."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        affected = await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2), ($3, $4), ($5, $6)",
            ("Alice", 30, "Bob", 25, "Charlie", 35),
        )
        assert affected == 3
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_drop_no_rows_affected_returns_0(self):
        """Test exec_drop returns 0 when no rows are affected."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", 30),
        )
        affected = await conn.exec_drop(
            "UPDATE test_table SET age = 100 WHERE age > $1",
            (1000,),
        )
        assert affected == 0
        await conn.close()


class TestAsyncExecDropWithParameters:
    """Test exec_drop with various parameter types."""

    @pytest.mark.asyncio
    async def test_exec_drop_with_null_param(self):
        """Test exec_drop with NULL parameter."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        affected = await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", None),
        )
        assert affected == 1
        result = await conn.query_first(
            "SELECT age FROM test_table WHERE name = 'Alice'"
        )
        assert result[0] is None
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_drop_with_empty_params(self):
        """Test exec_drop with empty params tuple."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        affected = await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ('Alice', 30)",
            (),
        )
        assert affected == 1
        await conn.close()


class TestAsyncExecDropDDLStatements:
    """Test exec_drop with DDL statements."""

    @pytest.mark.asyncio
    async def test_exec_drop_create_temp_table(self):
        """Test exec_drop for CREATE TEMP TABLE returns 0."""
        conn = await Conn.new(get_test_db_url())
        affected = await conn.exec_drop(
            "CREATE TEMP TABLE temp_test (id SERIAL, value INT)",
            (),
        )
        assert affected == 0
        insert_affected = await conn.exec_drop(
            "INSERT INTO temp_test (value) VALUES ($1)",
            (42,),
        )
        assert insert_affected == 1
        await conn.close()


class TestAsyncExecDropConnectionState:
    """Test that exec_drop leaves connection in clean state."""

    @pytest.mark.asyncio
    async def test_exec_drop_multiple_consecutive_operations(self):
        """Test multiple exec_drop operations in sequence."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        affected1 = await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", 30),
        )
        assert affected1 == 1
        affected2 = await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Bob", 25),
        )
        assert affected2 == 1
        affected3 = await conn.exec_drop(
            "UPDATE test_table SET age = age + 1 WHERE name = $1",
            ("Alice",),
        )
        assert affected3 == 1
        affected4 = await conn.exec_drop(
            "DELETE FROM test_table WHERE name = $1",
            ("Bob",),
        )
        assert affected4 == 1
        count = await conn.query_first("SELECT COUNT(*) FROM test_table")
        assert count[0] == 1
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_drop_followed_by_exec(self):
        """Test exec_drop followed by exec (extended query)."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        affected = await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", 30),
        )
        assert affected == 1
        results = await conn.exec(
            "SELECT name, age FROM test_table WHERE age = $1",
            (30,),
        )
        assert len(results) == 1
        assert results[0][0] == "Alice"
        await conn.close()


class TestAsyncExecDropUpdateVariants:
    """Test exec_drop UPDATE with various scenarios."""

    @pytest.mark.asyncio
    async def test_exec_drop_update_all_rows(self):
        """Test exec_drop UPDATE affecting all rows."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2), ($3, $4), ($5, $6)",
            ("Alice", 30, "Bob", 25, "Charlie", 35),
        )
        affected = await conn.exec_drop(
            "UPDATE test_table SET age = age + 10",
            (),
        )
        assert affected == 3
        await conn.close()


class TestAsyncExecDropDeleteVariants:
    """Test exec_drop DELETE with various scenarios."""

    @pytest.mark.asyncio
    async def test_exec_drop_delete_all_rows(self):
        """Test exec_drop DELETE affecting all rows."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2), ($3, $4), ($5, $6)",
            ("Alice", 30, "Bob", 25, "Charlie", 35),
        )
        affected = await conn.exec_drop(
            "DELETE FROM test_table",
            (),
        )
        assert affected == 3
        count = await conn.query_first("SELECT COUNT(*) FROM test_table")
        assert count[0] == 0
        await conn.close()


class TestAsyncExecDropReturnType:
    """Test that exec_drop returns proper integer type."""

    @pytest.mark.asyncio
    async def test_exec_drop_returns_int(self):
        """Test exec_drop returns Python int."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        affected = await conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", 30),
        )
        assert isinstance(affected, int)
        await conn.close()
