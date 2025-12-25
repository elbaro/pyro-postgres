"""Tests for sync exec_batch method."""

from pyro_postgres.sync import Conn

from ..conftest import (
    get_test_db_url,
    setup_test_table_sync,
)


class TestSyncExecBatchBasic:
    """Test basic exec_batch functionality."""

    def test_exec_batch_multiple_inserts(self):
        """Test exec_batch for multiple INSERTTs."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        params = [
            ("Alice", 30),
            ("Bob", 25),
            ("Charlie", 35),
            ("David", 40),
            ("Eve", 28),
        ]
        conn.exec_batch("INSERT INTO test_table (name, age) VALUES ($1, $2)", params)
        count = conn.query_first("SELECT COUNT(*) FROM test_table")
        assert count is not None
        assert count[0] == 5
        conn.close()

    def test_exec_batch_empty_params_list(self):
        """Test exec_batch with empty params list (no-op)."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.exec_batch("INSERT INTO test_table (name, age) VALUES ($1, $2)", [])
        count = conn.query_first("SELECT COUNT(*) FROM test_table")
        assert count[0] == 0
        conn.close()

    def test_exec_batch_single_item(self):
        """Test exec_batch with single item in params list."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        params = [("SinglePerson", 42)]
        conn.exec_batch("INSERT INTO test_table (name, age) VALUES ($1, $2)", params)
        count = conn.query_first("SELECT COUNT(*) FROM test_table")
        assert count[0] == 1
        conn.close()

    def test_exec_batch_large_batch(self):
        """Test exec_batch with large batch (100 items)."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        params = [(f"Person_{i}", i) for i in range(100)]
        conn.exec_batch("INSERT INTO test_table (name, age) VALUES ($1, $2)", params)
        count = conn.query_first("SELECT COUNT(*) FROM test_table")
        assert count[0] == 100
        conn.close()

    def test_exec_batch_returns_none(self):
        """Test exec_batch returns None (fire-and-forget)."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        params = [("Alice", 30), ("Bob", 25)]
        result = conn.exec_batch(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)", params
        )
        assert result is None
        conn.close()


class TestSyncExecBatchDataIntegrity:
    """Test exec_batch data integrity."""

    def test_exec_batch_data_integrity(self):
        """Test exec_batch data integrity - verify all rows inserted correctly."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        params = [
            ("Alice", 30),
            ("Bob", 25),
            ("Charlie", 35),
        ]
        conn.exec_batch("INSERT INTO test_table (name, age) VALUES ($1, $2)", params)
        for name, age in params:
            row = conn.query_first(
                f"SELECT name, age FROM test_table WHERE name = '{name}'"
            )
            assert row is not None
            assert row[0] == name
            assert row[1] == age
        conn.close()

    def test_exec_batch_with_null_values(self):
        """Test exec_batch with NULL values."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        params = [
            ("WithAge", 30),
            ("WithoutAge", None),
            ("AnotherWithAge", 25),
        ]
        conn.exec_batch("INSERT INTO test_table (name, age) VALUES ($1, $2)", params)
        row_with_null = conn.query_first(
            "SELECT age FROM test_table WHERE name = 'WithoutAge'"
        )
        assert row_with_null is not None
        assert row_with_null[0] is None
        conn.close()


class TestSyncExecBatchConnectionState:
    """Test connection state after exec_batch operations."""

    def test_exec_batch_connection_usable_after(self):
        """Test connection is usable after exec_batch."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        params = [("Alice", 30), ("Bob", 25)]
        conn.exec_batch("INSERT INTO test_table (name, age) VALUES ($1, $2)", params)
        results = conn.query("SELECT * FROM test_table")
        assert len(results) == 2
        params2 = [("Charlie", 35)]
        conn.exec_batch("INSERT INTO test_table (name, age) VALUES ($1, $2)", params2)
        count = conn.query_first("SELECT COUNT(*) FROM test_table")
        assert count[0] == 3
        conn.close()

    def test_exec_batch_multiple_calls(self):
        """Test multiple exec_batch calls in sequence."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        params1 = [("Alice", 30), ("Bob", 25)]
        conn.exec_batch("INSERT INTO test_table (name, age) VALUES ($1, $2)", params1)
        params2 = [("Charlie", 35), ("David", 40)]
        conn.exec_batch("INSERT INTO test_table (name, age) VALUES ($1, $2)", params2)
        count = conn.query_first("SELECT COUNT(*) FROM test_table")
        assert count[0] == 4
        conn.close()


class TestSyncExecBatchDifferentOperations:
    """Test exec_batch with different SQL operations."""

    def test_exec_batch_update(self):
        """Test exec_batch for UPDATE operations."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        insert_params = [
            ("Alice", 30),
            ("Bob", 25),
            ("Charlie", 35),
        ]
        conn.exec_batch(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)", insert_params
        )
        update_params = [
            (31, "Alice"),
            (26, "Bob"),
            (36, "Charlie"),
        ]
        conn.exec_batch("UPDATE test_table SET age = $1 WHERE name = $2", update_params)
        row = conn.query_first("SELECT age FROM test_table WHERE name = 'Alice'")
        assert row[0] == 31
        conn.close()

    def test_exec_batch_delete(self):
        """Test exec_batch for DELETE operations."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        insert_params = [
            ("Alice", 30),
            ("Bob", 25),
            ("Charlie", 35),
            ("David", 40),
        ]
        conn.exec_batch(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)", insert_params
        )
        delete_params = [
            ("Alice",),
            ("Charlie",),
        ]
        conn.exec_batch("DELETE FROM test_table WHERE name = $1", delete_params)
        count = conn.query_first("SELECT COUNT(*) FROM test_table")
        assert count[0] == 2
        remaining = conn.query("SELECT name FROM test_table ORDER BY name")
        assert len(remaining) == 2
        assert remaining[0][0] == "Bob"
        assert remaining[1][0] == "David"
        conn.close()


class TestSyncExecBatchVeryLargeBatch:
    """Test exec_batch with very large batches."""

    def test_exec_batch_1000_items(self):
        """Test exec_batch with 1000 items."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        params = [(f"Person_{i}", i % 100) for i in range(1000)]
        conn.exec_batch("INSERT INTO test_table (name, age) VALUES ($1, $2)", params)
        count = conn.query_first("SELECT COUNT(*) FROM test_table")
        assert count[0] == 1000
        conn.close()
