"""Tests for async exec_batch method."""

import pytest

from pyro_postgres.async_ import Conn

from ..conftest import (
    get_test_db_url,
    setup_test_table_async,
)


class TestAsyncExecBatchBasic:
    """Test basic exec_batch functionality."""

    @pytest.mark.asyncio
    async def test_exec_batch_multiple_inserts(self):
        """Test exec_batch for multiple INSERTTs."""
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
        assert count is not None
        assert count[0] == 5
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_batch_empty_params_list(self):
        """Test exec_batch with empty params list (no-op)."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        await conn.exec_batch("INSERT INTO test_table (name, age) VALUES ($1, $2)", [])
        count = await conn.query_first("SELECT COUNT(*) FROM test_table")
        assert count[0] == 0
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_batch_single_item(self):
        """Test exec_batch with single item in params list."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        params = [("SinglePerson", 42)]
        await conn.exec_batch(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)", params
        )
        count = await conn.query_first("SELECT COUNT(*) FROM test_table")
        assert count[0] == 1
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_batch_large_batch(self):
        """Test exec_batch with large batch (100 items)."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        params = [(f"Person_{i}", i) for i in range(100)]
        await conn.exec_batch(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)", params
        )
        count = await conn.query_first("SELECT COUNT(*) FROM test_table")
        assert count[0] == 100
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_batch_returns_none(self):
        """Test exec_batch returns None (fire-and-forget)."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        params = [("Alice", 30), ("Bob", 25)]
        result = await conn.exec_batch(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)", params
        )
        assert result is None
        await conn.close()


class TestAsyncExecBatchDataIntegrity:
    """Test exec_batch data integrity."""

    @pytest.mark.asyncio
    async def test_exec_batch_data_integrity(self):
        """Test exec_batch data integrity - verify all rows inserted correctly."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        params = [
            ("Alice", 30),
            ("Bob", 25),
            ("Charlie", 35),
        ]
        await conn.exec_batch(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)", params
        )
        for name, age in params:
            row = await conn.query_first(
                f"SELECT name, age FROM test_table WHERE name = '{name}'"
            )
            assert row is not None
            assert row[0] == name
            assert row[1] == age
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_batch_with_null_values(self):
        """Test exec_batch with NULL values."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        params = [
            ("WithAge", 30),
            ("WithoutAge", None),
            ("AnotherWithAge", 25),
        ]
        await conn.exec_batch(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)", params
        )
        row_with_null = await conn.query_first(
            "SELECT age FROM test_table WHERE name = 'WithoutAge'"
        )
        assert row_with_null is not None
        assert row_with_null[0] is None
        await conn.close()


class TestAsyncExecBatchConnectionState:
    """Test connection state after exec_batch operations."""

    @pytest.mark.asyncio
    async def test_exec_batch_connection_usable_after(self):
        """Test connection is usable after exec_batch."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        params = [("Alice", 30), ("Bob", 25)]
        await conn.exec_batch(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)", params
        )
        results = await conn.query("SELECT * FROM test_table")
        assert len(results) == 2
        params2 = [("Charlie", 35)]
        await conn.exec_batch(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)", params2
        )
        count = await conn.query_first("SELECT COUNT(*) FROM test_table")
        assert count[0] == 3
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_batch_multiple_calls(self):
        """Test multiple exec_batch calls in sequence."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        params1 = [("Alice", 30), ("Bob", 25)]
        await conn.exec_batch(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)", params1
        )
        params2 = [("Charlie", 35), ("David", 40)]
        await conn.exec_batch(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)", params2
        )
        count = await conn.query_first("SELECT COUNT(*) FROM test_table")
        assert count[0] == 4
        await conn.close()


class TestAsyncExecBatchDifferentOperations:
    """Test exec_batch with different SQL operations."""

    @pytest.mark.asyncio
    async def test_exec_batch_update(self):
        """Test exec_batch for UPDATE operations."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        insert_params = [
            ("Alice", 30),
            ("Bob", 25),
            ("Charlie", 35),
        ]
        await conn.exec_batch(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)", insert_params
        )
        update_params = [
            (31, "Alice"),
            (26, "Bob"),
            (36, "Charlie"),
        ]
        await conn.exec_batch(
            "UPDATE test_table SET age = $1 WHERE name = $2", update_params
        )
        row = await conn.query_first("SELECT age FROM test_table WHERE name = 'Alice'")
        assert row[0] == 31
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_batch_delete(self):
        """Test exec_batch for DELETE operations."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        insert_params = [
            ("Alice", 30),
            ("Bob", 25),
            ("Charlie", 35),
            ("David", 40),
        ]
        await conn.exec_batch(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)", insert_params
        )
        delete_params = [
            ("Alice",),
            ("Charlie",),
        ]
        await conn.exec_batch("DELETE FROM test_table WHERE name = $1", delete_params)
        count = await conn.query_first("SELECT COUNT(*) FROM test_table")
        assert count[0] == 2
        remaining = await conn.query("SELECT name FROM test_table ORDER BY name")
        assert len(remaining) == 2
        assert remaining[0][0] == "Bob"
        assert remaining[1][0] == "David"
        await conn.close()


class TestAsyncExecBatchVeryLargeBatch:
    """Test exec_batch with very large batches."""

    @pytest.mark.asyncio
    async def test_exec_batch_1000_items(self):
        """Test exec_batch with 1000 items."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        params = [(f"Person_{i}", i % 100) for i in range(1000)]
        await conn.exec_batch(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)", params
        )
        count = await conn.query_first("SELECT COUNT(*) FROM test_table")
        assert count[0] == 1000
        await conn.close()
