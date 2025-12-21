"""Tests for async pipeline mode (batching multiple queries)."""

import pytest

from pyro_postgres.async_ import Conn

from ..conftest import (
    cleanup_test_table_async,
    get_test_db_url,
    setup_test_table_async,
)


class TestAsyncPipelineBasic:
    """Test basic async pipeline functionality."""

    @pytest.mark.asyncio
    async def test_pipeline_basic_usage(self):
        """Test basic pipeline with exec and claim_collect."""
        conn = await Conn.new(get_test_db_url())
        async with conn.pipeline() as p:
            t1 = await p.exec("SELECT $1::int", (1,))
            t2 = await p.exec("SELECT $1::int", (2,))
            await p.sync()
            result1 = await p.claim_collect(t1)
            result2 = await p.claim_collect(t2)
        assert len(result1) == 1
        assert result1[0][0] == 1
        assert len(result2) == 1
        assert result2[0][0] == 2
        await conn.close()

    @pytest.mark.asyncio
    async def test_pipeline_claim_one(self):
        """Test pipeline with claim_one."""
        conn = await Conn.new(get_test_db_url())
        async with conn.pipeline() as p:
            t1 = await p.exec("SELECT $1::int", (42,))
            await p.sync()
            result = await p.claim_one(t1)
        assert result is not None
        assert result[0] == 42
        await conn.close()

    @pytest.mark.asyncio
    async def test_pipeline_claim_one_no_results(self):
        """Test pipeline claim_one with no results returns None."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        async with conn.pipeline() as p:
            t1 = await p.exec("SELECT name FROM test_table WHERE age > $1", (1000,))
            await p.sync()
            result = await p.claim_one(t1)
        assert result is None
        await cleanup_test_table_async(conn)
        await conn.close()

    @pytest.mark.asyncio
    async def test_pipeline_claim_drop(self):
        """Test pipeline with claim_drop."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        async with conn.pipeline() as p:
            t1 = await p.exec(
                "INSERT INTO test_table (name, age) VALUES ($1, $2)",
                ("Alice", 30),
            )
            await p.sync()
            await p.claim_drop(t1)
        # Verify the insert happened
        result = await conn.query_first("SELECT name FROM test_table WHERE name = 'Alice'")
        assert result is not None
        assert result[0] == "Alice"
        await cleanup_test_table_async(conn)
        await conn.close()


class TestAsyncPipelineMultiple:
    """Test pipeline with multiple operations."""

    @pytest.mark.asyncio
    async def test_pipeline_multiple_queries(self):
        """Test pipeline with multiple queries."""
        conn = await Conn.new(get_test_db_url())
        async with conn.pipeline() as p:
            t1 = await p.exec("SELECT 1::int", ())
            t2 = await p.exec("SELECT 2::int", ())
            t3 = await p.exec("SELECT 3::int", ())
            t4 = await p.exec("SELECT 4::int", ())
            await p.sync()
            r1 = await p.claim_one(t1)
            r2 = await p.claim_one(t2)
            r3 = await p.claim_one(t3)
            r4 = await p.claim_one(t4)
        assert r1[0] == 1
        assert r2[0] == 2
        assert r3[0] == 3
        assert r4[0] == 4
        await conn.close()

    @pytest.mark.asyncio
    async def test_pipeline_insert_and_select(self):
        """Test pipeline with INSERT and SELECT operations."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)
        async with conn.pipeline() as p:
            t1 = await p.exec(
                "INSERT INTO test_table (name, age) VALUES ($1, $2)",
                ("Alice", 30),
            )
            t2 = await p.exec(
                "INSERT INTO test_table (name, age) VALUES ($1, $2)",
                ("Bob", 25),
            )
            t3 = await p.exec("SELECT COUNT(*) FROM test_table", ())
            await p.sync()
            await p.claim_drop(t1)
            await p.claim_drop(t2)
            count = await p.claim_one(t3)
        assert count[0] == 2
        await cleanup_test_table_async(conn)
        await conn.close()


class TestAsyncPipelineAsDict:
    """Test pipeline with as_dict parameter."""

    @pytest.mark.asyncio
    async def test_claim_one_as_dict(self):
        """Test claim_one with as_dict=True returns dictionary."""
        conn = await Conn.new(get_test_db_url())
        async with conn.pipeline() as p:
            t1 = await p.exec("SELECT 42 as answer, 'hello' as greeting", ())
            await p.sync()
            result = await p.claim_one(t1, as_dict=True)
        assert result is not None
        assert isinstance(result, dict)
        assert result["answer"] == 42
        assert result["greeting"] == "hello"
        await conn.close()

    @pytest.mark.asyncio
    async def test_claim_collect_as_dict(self):
        """Test claim_collect with as_dict=True returns list of dictionaries."""
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
        async with conn.pipeline() as p:
            t1 = await p.exec("SELECT name, age FROM test_table ORDER BY age", ())
            await p.sync()
            results = await p.claim_collect(t1, as_dict=True)
        assert len(results) == 2
        assert all(isinstance(r, dict) for r in results)
        assert results[0]["name"] == "Bob"
        assert results[0]["age"] == 25
        assert results[1]["name"] == "Alice"
        assert results[1]["age"] == 30
        await cleanup_test_table_async(conn)
        await conn.close()


class TestAsyncPipelineCleanup:
    """Test pipeline cleanup behavior."""

    @pytest.mark.asyncio
    async def test_cleanup_on_exit(self):
        """Test that unclaimed operations are cleaned up on exit."""
        conn = await Conn.new(get_test_db_url())
        async with conn.pipeline() as p:
            t1 = await p.exec("SELECT 1::int", ())
            t2 = await p.exec("SELECT 2::int", ())
            await p.sync()
            # Only claim t1, leave t2 unclaimed
            await p.claim_one(t1)
            # __aexit__ should drain t2
        # Connection should still be usable after cleanup
        result = await conn.query_first("SELECT 42 as answer")
        assert result[0] == 42
        await conn.close()

    @pytest.mark.asyncio
    async def test_cleanup_without_sync(self):
        """Test that cleanup handles pending operations without sync."""
        conn = await Conn.new(get_test_db_url())
        async with conn.pipeline() as p:
            await p.exec("SELECT 1::int", ())
            await p.exec("SELECT 2::int", ())
            # No sync(), no claim - cleanup should handle this
        # Connection should still be usable after cleanup
        result = await conn.query_first("SELECT 42 as answer")
        assert result[0] == 42
        await conn.close()


class TestAsyncPipelineState:
    """Test pipeline state methods."""

    @pytest.mark.asyncio
    async def test_pending_count(self):
        """Test pending_count method."""
        conn = await Conn.new(get_test_db_url())
        async with conn.pipeline() as p:
            assert await p.pending_count() == 0
            t1 = await p.exec("SELECT 1::int", ())
            assert await p.pending_count() == 1
            t2 = await p.exec("SELECT 2::int", ())
            assert await p.pending_count() == 2
            await p.sync()
            await p.claim_one(t1)
            assert await p.pending_count() == 1
            await p.claim_one(t2)
            assert await p.pending_count() == 0
        await conn.close()

    @pytest.mark.asyncio
    async def test_is_aborted_false(self):
        """Test is_aborted returns False for successful operations."""
        conn = await Conn.new(get_test_db_url())
        async with conn.pipeline() as p:
            assert await p.is_aborted() is False
            t1 = await p.exec("SELECT 1::int", ())
            await p.sync()
            assert await p.is_aborted() is False
            await p.claim_one(t1)
            assert await p.is_aborted() is False
        await conn.close()


class TestAsyncPipelineErrors:
    """Test pipeline error handling."""

    @pytest.mark.asyncio
    async def test_error_outside_context(self):
        """Test that operations fail outside context manager."""
        conn = await Conn.new(get_test_db_url())
        p = conn.pipeline()
        with pytest.raises(Exception):
            await p.exec("SELECT 1::int", ())
        await conn.close()

    @pytest.mark.asyncio
    async def test_double_enter(self):
        """Test that entering twice raises error."""
        conn = await Conn.new(get_test_db_url())
        async with conn.pipeline() as p:
            with pytest.raises(Exception):
                await p.__aenter__()
        await conn.close()
