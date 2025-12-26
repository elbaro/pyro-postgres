"""Tests for sync exec_iter method."""

from pyro_postgres.sync import Conn

from ..conftest import get_test_db_url, setup_test_table_sync


class TestSyncExecIterBasic:
    """Test basic exec_iter functionality."""

    def test_exec_iter_fetch_all(self):
        """Test fetching all rows at once."""
        conn = Conn(get_test_db_url())

        def process(portal):
            rows, has_more = portal.fetch(0)  # 0 = fetch all
            assert not has_more
            return sum(row[0] for row in rows)

        total = conn.exec_iter("SELECT generate_series(1, 5) as n", (), process)
        assert total == 15  # 1+2+3+4+5
        conn.close()

    def test_exec_iter_batched(self):
        """Test fetching in batches."""
        conn = Conn(get_test_db_url())
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

        batch_count = conn.exec_iter("SELECT generate_series(1, 10) as n", (), process)
        assert batch_count == 4  # 3+3+3+1 rows in 4 batches
        assert all_rows == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        conn.close()

    def test_exec_iter_with_params(self):
        """Test exec_iter with parameters."""
        conn = Conn(get_test_db_url())

        def process(portal):
            rows, has_more = portal.fetch(0)
            assert not has_more
            return sum(row[0] for row in rows)

        total = conn.exec_iter("SELECT generate_series(1, $1) as n", (5,), process)
        assert total == 15
        conn.close()

    def test_exec_iter_empty_result(self):
        """Test exec_iter with empty result."""
        conn = Conn(get_test_db_url())

        def process(portal):
            rows, has_more = portal.fetch(0)
            assert not has_more
            return len(rows)

        count = conn.exec_iter("SELECT 1 WHERE false", (), process)
        assert count == 0
        conn.close()

    def test_exec_iter_returns_value(self):
        """Test that exec_iter returns the callback's return value."""
        conn = Conn(get_test_db_url())

        def process(portal):
            rows, _ = portal.fetch(0)
            return rows[0][0]

        answer = conn.exec_iter("SELECT 42 as answer", (), process)
        assert answer == 42
        conn.close()


class TestSyncExecIterAsDict:
    """Test exec_iter with as_dict option."""

    def test_exec_iter_as_dict(self):
        """Test fetching rows as dicts."""
        conn = Conn(get_test_db_url())

        def process(portal):
            rows, has_more = portal.fetch(0, as_dict=True)
            assert not has_more
            return rows

        rows = conn.exec_iter("SELECT 1 as a, 2 as b, 3 as c", (), process)
        assert len(rows) == 1
        assert rows[0] == {"a": 1, "b": 2, "c": 3}
        conn.close()


class TestSyncExecIterWithTable:
    """Test exec_iter with a real table."""

    def test_exec_iter_large_result(self):
        """Test processing a large result set in batches."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)

        # Insert many rows
        for i in range(100):
            conn.exec_drop(
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

        result = conn.exec_iter(
            "SELECT id, name, age FROM test_table ORDER BY id", (), process
        )
        assert result == 100
        assert processed_count == 100
        assert ages_sum == sum(range(100))  # 0+1+2+...+99
        conn.close()
