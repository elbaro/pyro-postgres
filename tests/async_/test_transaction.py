"""Tests for async transactions."""

import pytest

from pyro_postgres import IsolationLevel
from pyro_postgres.async_ import Conn
from pyro_postgres.error import IncorrectApiUsageError, TransactionClosedError

from ..conftest import (
    cleanup_test_table_async,
    get_test_db_url,
    setup_test_table_async,
)


class TestAsyncTransactionContextManager:
    """Test async transaction context manager."""

    @pytest.mark.asyncio
    async def test_transaction_commit_on_success(self):
        """Test transaction commits on successful exit."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)

        async with conn.tx() as txn:
            await conn.query_drop(
                "INSERT INTO test_table (name, age) VALUES ('Alice', 30)"
            )

        # Data should be committed
        result = await conn.query_first("SELECT name FROM test_table")
        assert result
        assert result[0] == "Alice"

        await cleanup_test_table_async(conn)
        await conn.close()

    @pytest.mark.asyncio
    async def test_transaction_rollback_on_exception(self):
        """Test transaction rolls back on exception."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)

        try:
            async with conn.tx() as txn:
                await conn.query_drop(
                    "INSERT INTO test_table (name, age) VALUES ('Alice', 30)"
                )
                raise ValueError("Simulated error")
        except ValueError:
            pass

        # Data should be rolled back
        result = await conn.query_first("SELECT name FROM test_table")
        assert result is None

        await cleanup_test_table_async(conn)
        await conn.close()

    @pytest.mark.asyncio
    async def test_transaction_multiple_operations(self):
        """Test transaction with multiple operations."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)

        async with conn.tx() as txn:
            await conn.query_drop(
                "INSERT INTO test_table (name, age) VALUES ('Alice', 30)"
            )
            await conn.query_drop(
                "INSERT INTO test_table (name, age) VALUES ('Bob', 25)"
            )
            await conn.query_drop(
                "UPDATE test_table SET age = age + 1 WHERE name = 'Alice'"
            )

        results = await conn.query("SELECT name, age FROM test_table ORDER BY name")
        assert len(results) == 2
        assert results[0] == ("Alice", 31)
        assert results[1] == ("Bob", 25)

        await cleanup_test_table_async(conn)
        await conn.close()


class TestAsyncTransactionExplicit:
    """Test async transaction with explicit commit/rollback."""

    @pytest.mark.asyncio
    async def test_explicit_commit(self):
        """Test explicit commit."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)

        txn = conn.tx()
        async with txn:
            await conn.query_drop(
                "INSERT INTO test_table (name, age) VALUES ('Alice', 30)"
            )
            await txn.commit()

        result = await conn.query_first("SELECT name FROM test_table")
        assert result
        assert result[0] == "Alice"

        await cleanup_test_table_async(conn)
        await conn.close()

    @pytest.mark.asyncio
    async def test_explicit_rollback(self):
        """Test explicit rollback."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)

        txn = conn.tx()
        async with txn:
            await conn.query_drop(
                "INSERT INTO test_table (name, age) VALUES ('Alice', 30)"
            )
            await txn.rollback()

        result = await conn.query_first("SELECT name FROM test_table")
        assert result is None

        await cleanup_test_table_async(conn)
        await conn.close()

    @pytest.mark.asyncio
    async def test_commit_without_context_raises(self):
        """Test commit without starting transaction raises error."""
        conn = await Conn.new(get_test_db_url())

        txn = conn.tx()
        with pytest.raises(IncorrectApiUsageError):
            await txn.commit()

        await conn.close()

    @pytest.mark.asyncio
    async def test_rollback_without_context_raises(self):
        """Test rollback without starting transaction raises error."""
        conn = await Conn.new(get_test_db_url())

        txn = conn.tx()
        with pytest.raises(IncorrectApiUsageError):
            await txn.rollback()

        await conn.close()

    @pytest.mark.asyncio
    async def test_commit_after_commit_raises(self):
        """Test commit after commit raises error."""
        conn = await Conn.new(get_test_db_url())

        txn = conn.tx()
        async with txn:
            await txn.commit()
            with pytest.raises(TransactionClosedError):
                await txn.commit()

        await conn.close()

    @pytest.mark.asyncio
    async def test_rollback_after_rollback_raises(self):
        """Test rollback after rollback raises error."""
        conn = await Conn.new(get_test_db_url())

        txn = conn.tx()
        async with txn:
            await txn.rollback()
            with pytest.raises(TransactionClosedError):
                await txn.rollback()

        await conn.close()


class TestAsyncTransactionIsolationLevel:
    """Test async transaction isolation levels."""

    @pytest.mark.asyncio
    async def test_read_uncommitted(self):
        """Test READ UNCOMMITTED isolation level."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)

        async with conn.tx(isolation_level=IsolationLevel.ReadUncommitted):
            await conn.query_drop(
                "INSERT INTO test_table (name, age) VALUES ('Alice', 30)"
            )

        result = await conn.query_first("SELECT name FROM test_table")
        assert result[0] == "Alice"

        await cleanup_test_table_async(conn)
        await conn.close()

    @pytest.mark.asyncio
    async def test_read_committed(self):
        """Test READ COMMITTED isolation level."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)

        async with conn.tx(isolation_level=IsolationLevel.ReadCommitted):
            await conn.query_drop(
                "INSERT INTO test_table (name, age) VALUES ('Alice', 30)"
            )

        result = await conn.query_first("SELECT name FROM test_table")
        assert result[0] == "Alice"

        await cleanup_test_table_async(conn)
        await conn.close()

    @pytest.mark.asyncio
    async def test_repeatable_read(self):
        """Test REPEATABLE READ isolation level."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)

        async with conn.tx(isolation_level=IsolationLevel.RepeatableRead):
            await conn.query_drop(
                "INSERT INTO test_table (name, age) VALUES ('Alice', 30)"
            )

        result = await conn.query_first("SELECT name FROM test_table")
        assert result[0] == "Alice"

        await cleanup_test_table_async(conn)
        await conn.close()

    @pytest.mark.asyncio
    async def test_serializable(self):
        """Test SERIALIZABLE isolation level."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)

        async with conn.tx(isolation_level=IsolationLevel.Serializable):
            await conn.query_drop(
                "INSERT INTO test_table (name, age) VALUES ('Alice', 30)"
            )

        result = await conn.query_first("SELECT name FROM test_table")
        assert result[0] == "Alice"

        await cleanup_test_table_async(conn)
        await conn.close()


class TestAsyncTransactionReadOnly:
    """Test async transaction readonly mode."""

    @pytest.mark.asyncio
    async def test_readonly_true(self):
        """Test transaction with readonly=True."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)

        # Insert data first
        await conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")

        async with conn.tx(readonly=True):
            # Reading should work
            result = await conn.query_first("SELECT name FROM test_table")
            assert result[0] == "Alice"

        await cleanup_test_table_async(conn)
        await conn.close()

    @pytest.mark.asyncio
    async def test_readonly_false(self):
        """Test transaction with readonly=False (read-write)."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)

        async with conn.tx(readonly=False):
            await conn.query_drop(
                "INSERT INTO test_table (name, age) VALUES ('Alice', 30)"
            )

        result = await conn.query_first("SELECT name FROM test_table")
        assert result[0] == "Alice"

        await cleanup_test_table_async(conn)
        await conn.close()

    @pytest.mark.asyncio
    async def test_readonly_with_isolation_level(self):
        """Test transaction with both readonly and isolation level."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)

        await conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")

        async with conn.tx(isolation_level=IsolationLevel.Serializable, readonly=True):
            result = await conn.query_first("SELECT name FROM test_table")
            assert result[0] == "Alice"

        await cleanup_test_table_async(conn)
        await conn.close()
