"""Tests for sync pipeline mode (batching multiple queries)."""

import pytest

from pyro_postgres.sync import Conn

from ..conftest import (
    cleanup_test_table_sync,
    get_test_db_url,
    setup_test_table_sync,
)


class TestSyncPipelineBasic:
    """Test basic sync pipeline functionality."""

    def test_pipeline_basic_usage(self):
        """Test basic pipeline with exec and claim_collect."""
        conn = Conn(get_test_db_url())
        with conn.pipeline() as p:
            t1 = p.exec("SELECT $1::int", (1,))
            t2 = p.exec("SELECT $1::int", (2,))
            p.sync()
            result1 = p.claim_collect(t1)
            result2 = p.claim_collect(t2)
        assert len(result1) == 1
        assert result1[0][0] == 1
        assert len(result2) == 1
        assert result2[0][0] == 2
        conn.close()

    def test_pipeline_claim_one(self):
        """Test pipeline with claim_one."""
        conn = Conn(get_test_db_url())
        with conn.pipeline() as p:
            t1 = p.exec("SELECT $1::int", (42,))
            p.sync()
            result = p.claim_one(t1)
        assert result is not None
        assert result[0] == 42
        conn.close()

    def test_pipeline_claim_one_no_results(self):
        """Test pipeline claim_one with no results returns None."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        with conn.pipeline() as p:
            t1 = p.exec("SELECT name FROM test_table WHERE age > $1", (1000,))
            p.sync()
            result = p.claim_one(t1)
        assert result is None
        cleanup_test_table_sync(conn)
        conn.close()

    def test_pipeline_claim_drop(self):
        """Test pipeline with claim_drop."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        with conn.pipeline() as p:
            t1 = p.exec(
                "INSERT INTO test_table (name, age) VALUES ($1, $2)",
                ("Alice", 30),
            )
            p.sync()
            p.claim_drop(t1)
        # Verify the insert happened
        result = conn.query_first("SELECT name FROM test_table WHERE name = 'Alice'")
        assert result is not None
        assert result[0] == "Alice"
        cleanup_test_table_sync(conn)
        conn.close()


class TestSyncPipelineMultiple:
    """Test pipeline with multiple operations."""

    def test_pipeline_multiple_queries(self):
        """Test pipeline with multiple queries."""
        conn = Conn(get_test_db_url())
        with conn.pipeline() as p:
            t1 = p.exec("SELECT 1::int", ())
            t2 = p.exec("SELECT 2::int", ())
            t3 = p.exec("SELECT 3::int", ())
            t4 = p.exec("SELECT 4::int", ())
            p.sync()
            r1 = p.claim_one(t1)
            r2 = p.claim_one(t2)
            r3 = p.claim_one(t3)
            r4 = p.claim_one(t4)
        assert r1[0] == 1
        assert r2[0] == 2
        assert r3[0] == 3
        assert r4[0] == 4
        conn.close()

    def test_pipeline_insert_and_select(self):
        """Test pipeline with INSERT and SELECT operations."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        with conn.pipeline() as p:
            t1 = p.exec(
                "INSERT INTO test_table (name, age) VALUES ($1, $2)",
                ("Alice", 30),
            )
            t2 = p.exec(
                "INSERT INTO test_table (name, age) VALUES ($1, $2)",
                ("Bob", 25),
            )
            t3 = p.exec("SELECT COUNT(*) FROM test_table", ())
            p.sync()
            p.claim_drop(t1)
            p.claim_drop(t2)
            count = p.claim_one(t3)
        assert count[0] == 2
        cleanup_test_table_sync(conn)
        conn.close()


class TestSyncPipelineAsDict:
    """Test pipeline with as_dict parameter."""

    def test_claim_one_as_dict(self):
        """Test claim_one with as_dict=True returns dictionary."""
        conn = Conn(get_test_db_url())
        with conn.pipeline() as p:
            t1 = p.exec("SELECT 42 as answer, 'hello' as greeting", ())
            p.sync()
            result = p.claim_one(t1, as_dict=True)
        assert result is not None
        assert isinstance(result, dict)
        assert result["answer"] == 42
        assert result["greeting"] == "hello"
        conn.close()

    def test_claim_collect_as_dict(self):
        """Test claim_collect with as_dict=True returns list of dictionaries."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Alice", 30),
        )
        conn.exec_drop(
            "INSERT INTO test_table (name, age) VALUES ($1, $2)",
            ("Bob", 25),
        )
        with conn.pipeline() as p:
            t1 = p.exec("SELECT name, age FROM test_table ORDER BY age", ())
            p.sync()
            results = p.claim_collect(t1, as_dict=True)
        assert len(results) == 2
        assert all(isinstance(r, dict) for r in results)
        assert results[0]["name"] == "Bob"
        assert results[0]["age"] == 25
        assert results[1]["name"] == "Alice"
        assert results[1]["age"] == 30
        cleanup_test_table_sync(conn)
        conn.close()


class TestSyncPipelineCleanup:
    """Test pipeline cleanup behavior."""

    def test_cleanup_on_exit(self):
        """Test that unclaimed operations are cleaned up on exit."""
        conn = Conn(get_test_db_url())
        with conn.pipeline() as p:
            t1 = p.exec("SELECT 1::int", ())
            t2 = p.exec("SELECT 2::int", ())
            p.sync()
            # Only claim t1, leave t2 unclaimed
            p.claim_one(t1)
            # __exit__ should drain t2
        # Connection should still be usable after cleanup
        result = conn.query_first("SELECT 42 as answer")
        assert result[0] == 42
        conn.close()

    def test_cleanup_without_sync(self):
        """Test that cleanup handles pending operations without sync."""
        conn = Conn(get_test_db_url())
        with conn.pipeline() as p:
            p.exec("SELECT 1::int", ())
            p.exec("SELECT 2::int", ())
            # No sync(), no claim - cleanup should handle this
        # Connection should still be usable after cleanup
        result = conn.query_first("SELECT 42 as answer")
        assert result[0] == 42
        conn.close()


class TestSyncPipelineState:
    """Test pipeline state methods."""

    def test_pending_count(self):
        """Test pending_count method."""
        conn = Conn(get_test_db_url())
        with conn.pipeline() as p:
            assert p.pending_count() == 0
            t1 = p.exec("SELECT 1::int", ())
            assert p.pending_count() == 1
            t2 = p.exec("SELECT 2::int", ())
            assert p.pending_count() == 2
            p.sync()
            p.claim_one(t1)
            assert p.pending_count() == 1
            p.claim_one(t2)
            assert p.pending_count() == 0
        conn.close()

    def test_is_aborted_false(self):
        """Test is_aborted returns False for successful operations."""
        conn = Conn(get_test_db_url())
        with conn.pipeline() as p:
            assert p.is_aborted() is False
            t1 = p.exec("SELECT 1::int", ())
            p.sync()
            assert p.is_aborted() is False
            p.claim_one(t1)
            assert p.is_aborted() is False
        conn.close()


class TestSyncPipelineErrors:
    """Test pipeline error handling."""

    def test_error_outside_context(self):
        """Test that operations fail outside context manager."""
        conn = Conn(get_test_db_url())
        p = conn.pipeline()
        with pytest.raises(Exception):
            p.exec("SELECT 1::int", ())
        conn.close()

    def test_double_enter(self):
        """Test that entering twice raises error."""
        conn = Conn(get_test_db_url())
        with conn.pipeline() as p:
            with pytest.raises(Exception):
                p.__enter__()
        conn.close()
