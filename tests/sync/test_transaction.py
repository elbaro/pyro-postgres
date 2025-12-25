"""Tests for sync transactions."""

import pytest

from pyro_postgres import IsolationLevel
from pyro_postgres.sync import Conn
from pyro_postgres.error import IncorrectApiUsageError, TransactionClosedError

from ..conftest import (
    cleanup_test_table_sync,
    get_test_db_url,
    setup_test_table_sync,
)


class TestSyncTransactionContextManager:
    """Test sync transaction context manager."""

    def test_transaction_commit_on_success(self):
        """Test transaction commits on successful exit."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)

        with conn.tx() as txn:
            conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")

        # Data should be committed
        result = conn.query_first("SELECT name FROM test_table")
        assert result
        assert result[0] == "Alice"

        cleanup_test_table_sync(conn)
        conn.close()

    def test_transaction_rollback_on_exception(self):
        """Test transaction rolls back on exception."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)

        try:
            with conn.tx() as txn:
                conn.query_drop(
                    "INSERT INTO test_table (name, age) VALUES ('Alice', 30)"
                )
                raise ValueError("Simulated error")
        except ValueError:
            pass

        # Data should be rolled back
        result = conn.query_first("SELECT name FROM test_table")
        assert result is None

        cleanup_test_table_sync(conn)
        conn.close()

    def test_transaction_multiple_operations(self):
        """Test transaction with multiple operations."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)

        with conn.tx() as txn:
            conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")
            conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Bob', 25)")
            conn.query_drop("UPDATE test_table SET age = age + 1 WHERE name = 'Alice'")

        results = conn.query("SELECT name, age FROM test_table ORDER BY name")
        assert len(results) == 2
        assert results[0] == ("Alice", 31)
        assert results[1] == ("Bob", 25)

        cleanup_test_table_sync(conn)
        conn.close()


class TestSyncTransactionExplicit:
    """Test sync transaction with explicit begin/commit/rollback."""

    def test_explicit_begin_commit(self):
        """Test explicit begin and commit."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)

        txn = conn.tx()
        txn.begin()
        conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")
        txn.commit()

        result = conn.query_first("SELECT name FROM test_table")
        assert result
        assert result[0] == "Alice"

        cleanup_test_table_sync(conn)
        conn.close()

    def test_explicit_begin_rollback(self):
        """Test explicit begin and rollback."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)

        txn = conn.tx()
        txn.begin()
        conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")
        txn.rollback()

        result = conn.query_first("SELECT name FROM test_table")
        assert result is None

        cleanup_test_table_sync(conn)
        conn.close()

    def test_commit_without_begin_raises(self):
        """Test commit without begin raises error."""
        conn = Conn(get_test_db_url())

        txn = conn.tx()
        with pytest.raises(IncorrectApiUsageError):
            txn.commit()

        conn.close()

    def test_rollback_without_begin_raises(self):
        """Test rollback without begin raises error."""
        conn = Conn(get_test_db_url())

        txn = conn.tx()
        with pytest.raises(IncorrectApiUsageError):
            txn.rollback()

        conn.close()

    def test_double_begin_raises(self):
        """Test calling begin twice raises error."""
        conn = Conn(get_test_db_url())

        txn = conn.tx()
        txn.begin()
        with pytest.raises(IncorrectApiUsageError):
            txn.begin()

        txn.rollback()
        conn.close()

    def test_commit_after_commit_raises(self):
        """Test commit after commit raises error."""
        conn = Conn(get_test_db_url())

        txn = conn.tx()
        txn.begin()
        txn.commit()
        with pytest.raises(TransactionClosedError):
            txn.commit()

        conn.close()

    def test_rollback_after_rollback_raises(self):
        """Test rollback after rollback raises error."""
        conn = Conn(get_test_db_url())

        txn = conn.tx()
        txn.begin()
        txn.rollback()
        with pytest.raises(TransactionClosedError):
            txn.rollback()

        conn.close()

    def test_commit_after_rollback_raises(self):
        """Test commit after rollback raises error."""
        conn = Conn(get_test_db_url())

        txn = conn.tx()
        txn.begin()
        txn.rollback()
        with pytest.raises(TransactionClosedError):
            txn.commit()

        conn.close()


class TestSyncTransactionIsolationLevel:
    """Test sync transaction isolation levels."""

    def test_read_uncommitted(self):
        """Test READ UNCOMMITTED isolation level."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)

        with conn.tx(isolation_level=IsolationLevel.ReadUncommitted):
            conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")

        result = conn.query_first("SELECT name FROM test_table")
        assert result[0] == "Alice"

        cleanup_test_table_sync(conn)
        conn.close()

    def test_read_committed(self):
        """Test READ COMMITTED isolation level."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)

        with conn.tx(isolation_level=IsolationLevel.ReadCommitted):
            conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")

        result = conn.query_first("SELECT name FROM test_table")
        assert result[0] == "Alice"

        cleanup_test_table_sync(conn)
        conn.close()

    def test_repeatable_read(self):
        """Test REPEATABLE READ isolation level."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)

        with conn.tx(isolation_level=IsolationLevel.RepeatableRead):
            conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")

        result = conn.query_first("SELECT name FROM test_table")
        assert result[0] == "Alice"

        cleanup_test_table_sync(conn)
        conn.close()

    def test_serializable(self):
        """Test SERIALIZABLE isolation level."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)

        with conn.tx(isolation_level=IsolationLevel.Serializable):
            conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")

        result = conn.query_first("SELECT name FROM test_table")
        assert result[0] == "Alice"

        cleanup_test_table_sync(conn)
        conn.close()

    def test_isolation_level_from_string(self):
        """Test creating isolation level from string."""
        level = IsolationLevel("read committed")
        assert level == IsolationLevel.ReadCommitted

        level = IsolationLevel("repeatable_read")
        assert level == IsolationLevel.RepeatableRead

        level = IsolationLevel("serializable")
        assert level == IsolationLevel.Serializable

    def test_isolation_level_static_methods(self):
        """Test isolation level static factory methods."""
        assert IsolationLevel.read_uncommitted() == IsolationLevel.ReadUncommitted
        assert IsolationLevel.read_committed() == IsolationLevel.ReadCommitted
        assert IsolationLevel.repeatable_read() == IsolationLevel.RepeatableRead
        assert IsolationLevel.serializable() == IsolationLevel.Serializable

    def test_isolation_level_repr(self):
        """Test isolation level __repr__."""
        level = IsolationLevel.ReadCommitted
        assert "ReadCommitted" in repr(level)


class TestSyncTransactionReadOnly:
    """Test sync transaction readonly mode."""

    def test_readonly_true(self):
        """Test transaction with readonly=True."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)

        # Insert data first
        conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")

        with conn.tx(readonly=True):
            # Reading should work
            result = conn.query_first("SELECT name FROM test_table")
            assert result[0] == "Alice"

        cleanup_test_table_sync(conn)
        conn.close()

    def test_readonly_false(self):
        """Test transaction with readonly=False (read-write)."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)

        with conn.tx(readonly=False):
            conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")

        result = conn.query_first("SELECT name FROM test_table")
        assert result[0] == "Alice"

        cleanup_test_table_sync(conn)
        conn.close()

    def test_readonly_with_isolation_level(self):
        """Test transaction with both readonly and isolation level."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)

        conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")

        with conn.tx(
            isolation_level=IsolationLevel.Serializable, readonly=True
        ):
            result = conn.query_first("SELECT name FROM test_table")
            assert result[0] == "Alice"

        cleanup_test_table_sync(conn)
        conn.close()
