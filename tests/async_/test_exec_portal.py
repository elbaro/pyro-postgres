"""Tests for async exec_portal method."""

import pytest

from pyro_postgres.async_ import Conn

from ..conftest import get_test_db_url, setup_test_table_async


class TestAsyncExecPortalBasic:
    """Test basic exec_portal functionality."""

    @pytest.mark.asyncio
    async def test_exec_portal_fetch_all(self):
        """Test fetching all rows at once."""
        conn = await Conn.new(get_test_db_url())

        async with conn.tx() as tx:
            portal = await tx.exec_portal("SELECT generate_series(1, 5) as n")
            rows, has_more = await portal.exec_collect(0)  # 0 = fetch all
            assert not has_more
            total = sum(row[0] for row in rows)
            assert total == 15  # 1+2+3+4+5
            await portal.close()

        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_portal_batched(self):
        """Test fetching in batches."""
        conn = await Conn.new(get_test_db_url())
        all_rows = []

        async with conn.tx() as tx:
            portal = await tx.exec_portal("SELECT generate_series(1, 10) as n")
            batches = 0
            while True:
                rows, has_more = await portal.exec_collect(3)  # fetch 3 at a time
                all_rows.extend(row[0] for row in rows)
                batches += 1
                if not has_more:
                    break

            assert batches == 4  # 3+3+3+1 rows in 4 batches
            assert all_rows == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
            await portal.close()

        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_portal_no_params(self):
        """Test exec_portal with query using literal values (no bind params)."""
        conn = await Conn.new(get_test_db_url())

        async with conn.tx() as tx:
            portal = await tx.exec_portal("SELECT 42 as n")
            rows, has_more = await portal.exec_collect(0)
            assert not has_more
            assert len(rows) == 1
            assert rows[0][0] == 42
            await portal.close()

        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_portal_empty_result(self):
        """Test exec_portal with empty result."""
        conn = await Conn.new(get_test_db_url())

        async with conn.tx() as tx:
            portal = await tx.exec_portal("SELECT 1 WHERE false")
            rows, has_more = await portal.exec_collect(0)
            assert not has_more
            assert len(rows) == 0
            await portal.close()

        await conn.close()


class TestAsyncExecPortalAsDict:
    """Test exec_portal with as_dict option."""

    @pytest.mark.asyncio
    async def test_exec_portal_as_dict(self):
        """Test fetching rows as dicts."""
        conn = await Conn.new(get_test_db_url())

        async with conn.tx() as tx:
            portal = await tx.exec_portal("SELECT 1 as a, 2 as b, 3 as c")
            rows, has_more = await portal.exec_collect(0, as_dict=True)
            assert not has_more
            assert len(rows) == 1
            assert rows[0] == {"a": 1, "b": 2, "c": 3}
            await portal.close()

        await conn.close()


class TestAsyncExecPortalInterleaving:
    """Test interleaving multiple portals."""

    @pytest.mark.asyncio
    async def test_exec_portal_two_portals_same_query(self):
        """Test interleaving two portals with the same query."""
        conn = await Conn.new(get_test_db_url())

        async with conn.tx() as tx:
            # Use the same query for both portals - it will be cached after first prepare
            query = "SELECT * FROM generate_series(1, 10) as n"
            portal1 = await tx.exec_portal(query)
            portal2 = await tx.exec_portal(query)

            all_rows1 = []
            all_rows2 = []

            # Interleave fetching from both portals
            while True:
                rows1, has_more1 = await portal1.exec_collect(3)
                rows2, has_more2 = await portal2.exec_collect(3)
                all_rows1.extend(row[0] for row in rows1)
                all_rows2.extend(row[0] for row in rows2)
                if not has_more1 and not has_more2:
                    break

            # Both portals should fetch the same data
            assert all_rows1 == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
            assert all_rows2 == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

            await portal1.close()
            await portal2.close()

        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_portal_two_portals_different_queries(self):
        """Test interleaving two portals with different queries.

        Within a transaction, we can use different queries because
        prepare() doesn't sync and close portals.
        """
        conn = await Conn.new(get_test_db_url())

        async with conn.tx() as tx:
            query1 = "SELECT generate_series(1, 5) as n"
            query2 = "SELECT generate_series(10, 15) as n"
            portal1 = await tx.exec_portal(query1)
            portal2 = await tx.exec_portal(query2)

            all_rows1 = []
            all_rows2 = []

            # Interleave fetching from both portals
            while True:
                rows1, has_more1 = await portal1.exec_collect(2)
                rows2, has_more2 = await portal2.exec_collect(2)
                all_rows1.extend(row[0] for row in rows1)
                all_rows2.extend(row[0] for row in rows2)
                if not has_more1 and not has_more2:
                    break

            assert all_rows1 == [1, 2, 3, 4, 5]
            assert all_rows2 == [10, 11, 12, 13, 14, 15]

            await portal1.close()
            await portal2.close()

        await conn.close()


class TestAsyncExecPortalWithTable:
    """Test exec_portal with a real table."""

    @pytest.mark.asyncio
    async def test_exec_portal_large_result(self):
        """Test processing a large result set in batches."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)

        # Insert many rows
        for i in range(100):
            await conn.exec_drop(
                "INSERT INTO test_table (name, age) VALUES ($1, $2)",
                (f"User{i}", i),
            )

        async with conn.tx() as tx:
            portal = await tx.exec_portal(
                "SELECT id, name, age FROM test_table ORDER BY id"
            )

            processed_count = 0
            ages_sum = 0

            while True:
                rows, has_more = await portal.exec_collect(25)  # 25 rows per batch
                for row in rows:
                    processed_count += 1
                    ages_sum += row[2]  # age is third column (id, name, age)
                if not has_more:
                    break

            assert processed_count == 100
            assert ages_sum == sum(range(100))  # 0+1+2+...+99

            await portal.close()

        await conn.close()
