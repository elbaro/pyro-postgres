"""Tests for async exec_iter method."""

import pytest

from pyro_postgres.async_ import Conn

from ..conftest import get_test_db_url, setup_test_table_async


class TestAsyncExecIterBasic:
    """Test basic exec_iter functionality."""

    @pytest.mark.asyncio
    async def test_exec_iter_fetch_all(self):
        """Test fetching all rows at once."""
        conn = await Conn.new(get_test_db_url())

        def process(portal):
            rows, has_more = portal.fetch(0)  # 0 = fetch all
            assert not has_more
            return sum(row[0] for row in rows)

        total = await conn.exec_iter("SELECT generate_series(1, 5) as n", (), process)
        assert total == 15  # 1+2+3+4+5
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_iter_batched(self):
        """Test fetching in batches."""
        conn = await Conn.new(get_test_db_url())
        all_rows = []

        def process(portal):
            batches = 0
            while True:
                rows, has_more = portal.fetch(3)  # fetch 3 at a time
                all_rows.extend(row[0] for row in rows)
                batches += 1
                if not has_more:
                    break
            return batches

        batch_count = await conn.exec_iter(
            "SELECT generate_series(1, 10) as n", (), process
        )
        assert batch_count == 4  # 3+3+3+1 rows in 4 batches
        assert all_rows == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_iter_with_params(self):
        """Test exec_iter with parameters."""
        conn = await Conn.new(get_test_db_url())

        def process(portal):
            rows, has_more = portal.fetch(0)
            assert not has_more
            return sum(row[0] for row in rows)

        total = await conn.exec_iter(
            "SELECT generate_series(1, $1) as n", (5,), process
        )
        assert total == 15
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_iter_empty_result(self):
        """Test exec_iter with empty result."""
        conn = await Conn.new(get_test_db_url())

        def process(portal):
            rows, has_more = portal.fetch(0)
            assert not has_more
            return len(rows)

        count = await conn.exec_iter("SELECT 1 WHERE false", (), process)
        assert count == 0
        await conn.close()

    @pytest.mark.asyncio
    async def test_exec_iter_returns_value(self):
        """Test that exec_iter returns the callback's return value."""
        conn = await Conn.new(get_test_db_url())

        def process(portal):
            rows, _ = portal.fetch(0)
            return rows[0][0]

        answer = await conn.exec_iter("SELECT 42 as answer", (), process)
        assert answer == 42
        await conn.close()


class TestAsyncExecIterAsDict:
    """Test exec_iter with as_dict option."""

    @pytest.mark.asyncio
    async def test_exec_iter_as_dict(self):
        """Test fetching rows as dicts."""
        conn = await Conn.new(get_test_db_url())

        def process(portal):
            rows, has_more = portal.fetch(0, as_dict=True)
            assert not has_more
            return rows

        rows = await conn.exec_iter("SELECT 1 as a, 2 as b, 3 as c", (), process)
        assert len(rows) == 1
        assert rows[0] == {"a": 1, "b": 2, "c": 3}
        await conn.close()


class TestAsyncExecIterWithTable:
    """Test exec_iter with a real table."""

    @pytest.mark.asyncio
    async def test_exec_iter_large_result(self):
        """Test processing a large result set in batches."""
        conn = await Conn.new(get_test_db_url())
        await setup_test_table_async(conn)

        # Insert many rows
        for i in range(100):
            await conn.exec_drop(
                "INSERT INTO test_table (name, age) VALUES ($1, $2)",
                (f"User{i}", i),
            )

        processed_count = 0
        ages_sum = 0

        def process(portal):
            nonlocal processed_count, ages_sum
            while True:
                rows, has_more = portal.fetch(25)  # 25 rows per batch
                for row in rows:
                    processed_count += 1
                    ages_sum += row[2]  # age is third column (id, name, age, ...)
                if not has_more:
                    break
            return processed_count

        result = await conn.exec_iter(
            "SELECT id, name, age FROM test_table ORDER BY id", (), process
        )
        assert result == 100
        assert processed_count == 100
        assert ages_sum == sum(range(100))  # 0+1+2+...+99
        await conn.close()
