"""Tests for sync exec_portal method."""

from pyro_postgres.sync import Conn

from ..conftest import get_test_db_url, setup_test_table_sync


class TestSyncExecPortalBasic:
    """Test basic exec_portal functionality."""

    def test_exec_portal_fetch_all(self):
        """Test fetching all rows at once."""
        conn = Conn(get_test_db_url())

        with conn.tx() as tx:
            portal = tx.exec_portal("SELECT generate_series(1, 5) as n")
            rows = portal.exec_collect(0)  # 0 = fetch all
            assert portal.is_complete()
            total = sum(row[0] for row in rows)
            assert total == 15  # 1+2+3+4+5
            portal.close()

        conn.close()

    def test_exec_portal_batched(self):
        """Test fetching in batches."""
        conn = Conn(get_test_db_url())
        all_rows = []

        with conn.tx() as tx:
            portal = tx.exec_portal("SELECT generate_series(1, 10) as n")
            batches = 0
            while True:
                rows = portal.exec_collect(3)  # fetch 3 at a time
                all_rows.extend(row[0] for row in rows)
                batches += 1
                if portal.is_complete():
                    break

            assert batches == 4  # 3+3+3+1 rows in 4 batches
            assert all_rows == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
            portal.close()

        conn.close()

    def test_exec_portal_no_params(self):
        """Test exec_portal with query using literal values (no bind params)."""
        conn = Conn(get_test_db_url())

        with conn.tx() as tx:
            portal = tx.exec_portal("SELECT 42 as n")
            rows = portal.exec_collect(0)
            assert portal.is_complete()
            assert len(rows) == 1
            assert rows[0][0] == 42
            portal.close()

        conn.close()

    def test_exec_portal_empty_result(self):
        """Test exec_portal with empty result."""
        conn = Conn(get_test_db_url())

        with conn.tx() as tx:
            portal = tx.exec_portal("SELECT 1 WHERE false")
            rows = portal.exec_collect(0)
            assert portal.is_complete()
            assert len(rows) == 0
            portal.close()

        conn.close()


class TestSyncExecPortalAsDict:
    """Test exec_portal with as_dict option."""

    def test_exec_portal_as_dict(self):
        """Test fetching rows as dicts."""
        conn = Conn(get_test_db_url())

        with conn.tx() as tx:
            portal = tx.exec_portal("SELECT 1 as a, 2 as b, 3 as c")
            rows = portal.exec_collect(0, as_dict=True)
            assert portal.is_complete()
            assert len(rows) == 1
            assert rows[0] == {"a": 1, "b": 2, "c": 3}
            portal.close()

        conn.close()


class TestSyncExecPortalInterleaving:
    """Test interleaving multiple portals."""

    def test_exec_portal_two_portals_same_query(self):
        """Test interleaving two portals with the same query."""
        conn = Conn(get_test_db_url())

        with conn.tx() as tx:
            # Use the same query for both portals - it will be cached after first prepare
            query = "SELECT * FROM generate_series(1, 10) as n"
            portal1 = tx.exec_portal(query)
            portal2 = tx.exec_portal(query)

            all_rows1 = []
            all_rows2 = []

            # Interleave fetching from both portals
            while True:
                rows1 = portal1.exec_collect(3)
                rows2 = portal2.exec_collect(3)
                all_rows1.extend(row[0] for row in rows1)
                all_rows2.extend(row[0] for row in rows2)
                if portal1.is_complete() and portal2.is_complete():
                    break

            # Both portals should fetch the same data
            assert all_rows1 == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
            assert all_rows2 == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

            portal1.close()
            portal2.close()

        conn.close()

    def test_exec_portal_two_portals_different_queries(self):
        """Test interleaving two portals with different queries.

        Within a transaction, we can use different queries because
        prepare() doesn't sync and close portals.
        """
        conn = Conn(get_test_db_url())

        with conn.tx() as tx:
            query1 = "SELECT generate_series(1, 5) as n"
            query2 = "SELECT generate_series(10, 15) as n"
            portal1 = tx.exec_portal(query1)
            portal2 = tx.exec_portal(query2)

            all_rows1 = []
            all_rows2 = []

            # Interleave fetching from both portals
            while True:
                rows1 = portal1.exec_collect(2)
                rows2 = portal2.exec_collect(2)
                all_rows1.extend(row[0] for row in rows1)
                all_rows2.extend(row[0] for row in rows2)
                if portal1.is_complete() and portal2.is_complete():
                    break

            assert all_rows1 == [1, 2, 3, 4, 5]
            assert all_rows2 == [10, 11, 12, 13, 14, 15]

            portal1.close()
            portal2.close()

        conn.close()


class TestSyncExecPortalWithTable:
    """Test exec_portal with a real table."""

    def test_exec_portal_large_result(self):
        """Test processing a large result set in batches."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)

        # Insert many rows
        for i in range(100):
            conn.exec_drop(
                "INSERT INTO test_table (name, age) VALUES ($1, $2)",
                (f"User{i}", i),
            )

        with conn.tx() as tx:
            portal = tx.exec_portal("SELECT id, name, age FROM test_table ORDER BY id")

            processed_count = 0
            ages_sum = 0

            while True:
                rows = portal.exec_collect(25)  # 25 rows per batch
                for row in rows:
                    processed_count += 1
                    ages_sum += row[2]  # age is third column (id, name, age)
                if portal.is_complete():
                    break

            assert processed_count == 100
            assert ages_sum == sum(range(100))  # 0+1+2+...+99

            portal.close()

        conn.close()
