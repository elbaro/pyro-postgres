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

    def test_claim_order_error(self):
        """Test that claiming out of order raises an error."""
        conn = Conn(get_test_db_url())
        with conn.pipeline() as p:
            t1 = p.exec("SELECT 1::int", ())
            t2 = p.exec("SELECT 2::int", ())
            p.sync()
            # Try to claim t2 before t1 - should fail
            with pytest.raises(Exception) as exc_info:
                p.claim_one(t2)
            assert "out of order" in str(exc_info.value).lower()
            # Now claim in correct order should work
            r1 = p.claim_one(t1)
            assert r1[0] == 1
        conn.close()

    def test_sql_error(self):
        """Test SQL error propagation in pipeline."""
        conn = Conn(get_test_db_url())
        with conn.pipeline() as p:
            t = p.exec("SELECT 1/0", ())
            p.sync()
            # Claiming should fail with division by zero
            with pytest.raises(Exception) as exc_info:
                p.claim_one(t)
            assert "division by zero" in str(exc_info.value).lower()
        conn.close()

    def test_aborted_state(self):
        """Test pipeline abort state after error."""
        conn = Conn(get_test_db_url())
        with conn.pipeline() as p:
            t1 = p.exec("SELECT 1::int", ())
            t2 = p.exec("SELECT 1/0", ())  # This will fail
            t3 = p.exec("SELECT 2::int", ())
            p.sync()

            # First operation succeeds
            r1 = p.claim_one(t1)
            assert r1[0] == 1
            assert p.is_aborted() is False

            # t2 fails - pipeline becomes aborted
            with pytest.raises(Exception):
                p.claim_one(t2)
            assert p.is_aborted() is True

            # Subsequent claims should also fail due to aborted state
            with pytest.raises(Exception) as exc_info:
                p.claim_one(t3)
            assert "aborted" in str(exc_info.value).lower()
        conn.close()


class TestSyncPipelineAdvanced:
    """Advanced pipeline tests - multiple rows, prepared statements, etc."""

    def test_multiple_rows(self):
        """Test query with multiple rows."""
        conn = Conn(get_test_db_url())
        with conn.pipeline() as p:
            t = p.exec("SELECT * FROM (VALUES (1), (2), (3)) AS t(n)", ())
            p.sync()
            results = p.claim_collect(t)
        assert len(results) == 3
        assert results[0][0] == 1
        assert results[1][0] == 2
        assert results[2][0] == 3
        conn.close()

    def test_no_rows(self):
        """Test query returning no rows."""
        conn = Conn(get_test_db_url())
        with conn.pipeline() as p:
            t = p.exec("SELECT 1 WHERE false", ())
            p.sync()
            results = p.claim_collect(t)
        assert len(results) == 0
        conn.close()

    def test_with_prepared_statement(self):
        """Test using prepared statements in pipeline."""
        conn = Conn(get_test_db_url())
        # Prepare statement outside pipeline
        stmt = conn.prepare("SELECT $1::int * 2")
        with conn.pipeline() as p:
            t1 = p.exec(stmt, (5,))
            t2 = p.exec(stmt, (10,))
            p.sync()
            r1 = p.claim_one(t1)
            r2 = p.claim_one(t2)
        assert r1[0] == 10
        assert r2[0] == 20
        conn.close()

    def test_insert_returning(self):
        """Test INSERT with RETURNING in pipeline."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        with conn.pipeline() as p:
            t1 = p.exec(
                "INSERT INTO test_table (name, age) VALUES ($1, $2) RETURNING id",
                ("Alice", 30),
            )
            t2 = p.exec(
                "INSERT INTO test_table (name, age) VALUES ($1, $2) RETURNING id",
                ("Bob", 25),
            )
            p.sync()
            r1 = p.claim_one(t1)
            r2 = p.claim_one(t2)
        assert r1 is not None
        assert r2 is not None
        # IDs should be sequential
        assert r1[0] < r2[0]
        cleanup_test_table_sync(conn)
        conn.close()

    def test_empty_pipeline(self):
        """Test empty pipeline (just sync)."""
        conn = Conn(get_test_db_url())
        with conn.pipeline() as p:
            p.sync()
        # Should complete without error
        # Connection should still be usable
        result = conn.query_first("SELECT 42 as answer")
        assert result[0] == 42
        conn.close()


class TestSyncPipelineAutoSync:
    """Test auto-sync behavior (claim without explicit sync)."""

    def test_auto_sync_basic(self):
        """Test basic auto-sync: claim without explicit sync()."""
        conn = Conn(get_test_db_url())
        with conn.pipeline() as p:
            t1 = p.exec("SELECT $1::int", (1,))
            t2 = p.exec("SELECT $1::int", (2,))
            # No explicit sync() - claim should auto-sync
            r1 = p.claim_one(t1)
            r2 = p.claim_one(t2)
        assert r1[0] == 1
        assert r2[0] == 2
        conn.close()

    def test_interleaved_exec_claim(self):
        """Test interleaved exec/claim pattern without explicit sync."""
        conn = Conn(get_test_db_url())
        with conn.pipeline() as p:
            # First batch of execs
            t1 = p.exec("SELECT $1::int", (1,))
            t2 = p.exec("SELECT $1::int", (2,))

            # Claim first two (auto-syncs)
            r1 = p.claim_one(t1)
            r2 = p.claim_one(t2)

            # Second batch of execs
            t3 = p.exec("SELECT $1::int", (3,))
            t4 = p.exec("SELECT $1::int", (4,))

            # Claim remaining (auto-syncs again)
            r3 = p.claim_one(t3)
            r4 = p.claim_one(t4)

        assert r1[0] == 1
        assert r2[0] == 2
        assert r3[0] == 3
        assert r4[0] == 4
        conn.close()

    def test_partial_claim_then_exec(self):
        """Test partial claims then more execs before claiming rest."""
        conn = Conn(get_test_db_url())
        with conn.pipeline() as p:
            # Queue 3 operations
            t1 = p.exec("SELECT $1::int", (1,))
            t2 = p.exec("SELECT $1::int", (2,))
            t3 = p.exec("SELECT $1::int", (3,))

            # Claim only first one (auto-syncs all 3)
            r1 = p.claim_one(t1)

            # Queue more operations before claiming t2, t3
            t4 = p.exec("SELECT $1::int", (4,))
            t5 = p.exec("SELECT $1::int", (5,))

            # Claim t2 (no sync needed, was synced with t1)
            r2 = p.claim_one(t2)

            # Claim t3 - this should trigger sync for t4, t5
            r3 = p.claim_one(t3)

            # Claim remaining
            r4 = p.claim_one(t4)
            r5 = p.claim_one(t5)

        assert r1[0] == 1
        assert r2[0] == 2
        assert r3[0] == 3
        assert r4[0] == 4
        assert r5[0] == 5
        conn.close()

    def test_complex_interleave(self):
        """Test complex interleaving: exec, claim, exec, claim, exec, claim."""
        conn = Conn(get_test_db_url())
        with conn.pipeline() as p:
            results = []
            for i in range(1, 6):
                t = p.exec("SELECT $1::int", (i,))
                r = p.claim_one(t)
                results.append(r[0])
        assert results == [1, 2, 3, 4, 5]
        conn.close()


class TestSyncPipelineErrorRecovery:
    """Test pipeline error recovery behavior."""

    def test_multiple_batches_with_error(self):
        """Test multiple exec/claim batches with error in between."""
        conn = Conn(get_test_db_url())
        with conn.pipeline() as p:
            # First batch - all succeed
            t1 = p.exec("SELECT $1::int", (1,))
            t2 = p.exec("SELECT $1::int", (2,))

            r1 = p.claim_one(t1)
            r2 = p.claim_one(t2)

            assert r1[0] == 1
            assert r2[0] == 2

            # Second batch - has an error
            t3 = p.exec("SELECT $1::int", (3,))
            t4 = p.exec("SELECT 1/0", ())  # Error
            t5 = p.exec("SELECT $1::int", (5,))

            r3 = p.claim_one(t3)
            assert r3[0] == 3

            # t4 fails
            with pytest.raises(Exception):
                p.claim_one(t4)

            # t5 should fail due to aborted state
            with pytest.raises(Exception) as exc_info:
                p.claim_one(t5)
            assert "aborted" in str(exc_info.value).lower()

        # Pipeline should recover after error via cleanup
        # Connection should still be usable
        check = conn.query_first("SELECT 42")
        assert check[0] == 42
        conn.close()

    def test_error_recovery_new_batch(self):
        """Test recovery after error - can start new pipeline."""
        conn = Conn(get_test_db_url())

        # First pipeline with error
        with conn.pipeline() as p:
            t1 = p.exec("SELECT 1/0", ())
            p.sync()
            with pytest.raises(Exception):
                p.claim_one(t1)

        # Second pipeline should work fine
        with conn.pipeline() as p:
            t1 = p.exec("SELECT $1::int", (100,))
            p.sync()
            r1 = p.claim_one(t1)
        assert r1[0] == 100
        conn.close()

    def test_continue_after_error_batch(self):
        """Test pipeline continues after error batch with new batch."""
        conn = Conn(get_test_db_url())
        with conn.pipeline() as p:
            # First batch - has an error in the middle
            t1 = p.exec("SELECT $1::int", (1,))
            t2 = p.exec("SELECT 1/0", ())  # Error
            t3 = p.exec("SELECT $1::int", (3,))

            # Claim first batch
            r1 = p.claim_one(t1)
            assert r1[0] == 1

            # t2 fails
            with pytest.raises(Exception):
                p.claim_one(t2)

            # t3 fails due to aborted
            with pytest.raises(Exception):
                p.claim_one(t3)

            # After consuming all claims from error batch,
            # pipeline should recover and allow new batch
            t4 = p.exec("SELECT $1::int", (4,))
            t5 = p.exec("SELECT $1::int", (5,))

            r4 = p.claim_one(t4)
            r5 = p.claim_one(t5)

        assert r4[0] == 4
        assert r5[0] == 5
        conn.close()
