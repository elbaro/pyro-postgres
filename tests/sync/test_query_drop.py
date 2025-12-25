"""Tests for sync query_drop method returning affected rows count."""

from pyro_postgres.sync import Conn

from ..conftest import (
    get_test_db_url,
    setup_test_table_sync,
)


class TestSyncQueryDropReturnsAffectedRows:
    """Test that query_drop returns affected rows count."""

    def test_query_drop_insert_returns_1(self):
        """Test query_drop for single INSERT returns 1."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        affected = conn.query_drop(
            "INSERT INTO test_table (name, age) VALUES ('Alice', 30)"
        )
        assert affected == 1
        assert isinstance(affected, int)
        conn.close()

    def test_query_drop_update_returns_affected_count(self):
        """Test query_drop for UPDATE returns number of updated rows."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")
        conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Bob', 25)")
        conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Charlie', 35)")
        affected = conn.query_drop("UPDATE test_table SET age = age + 1 WHERE age > 25")
        assert affected == 2
        conn.close()

    def test_query_drop_delete_returns_affected_count(self):
        """Test query_drop for DELETE returns number of deleted rows."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")
        conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Bob', 25)")
        affected = conn.query_drop("DELETE FROM test_table WHERE age < 30")
        assert affected == 1
        conn.close()

    def test_query_drop_multi_row_insert(self):
        """Test query_drop for multi-row INSERT returns correct count."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        affected = conn.query_drop(
            "INSERT INTO test_table (name, age) VALUES ('Alice', 30), ('Bob', 25), ('Charlie', 35)"
        )
        assert affected == 3
        conn.close()

    def test_query_drop_no_rows_affected_returns_0(self):
        """Test query_drop returns 0 when no rows are affected."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")
        affected = conn.query_drop("UPDATE test_table SET age = 100 WHERE age > 1000")
        assert affected == 0
        conn.close()


class TestSyncQueryDropDDLStatements:
    """Test query_drop with DDL statements."""

    def test_query_drop_create_temp_table(self):
        """Test query_drop for CREATE TEMP TABLE returns 0."""
        conn = Conn(get_test_db_url())
        affected = conn.query_drop("CREATE TEMP TABLE temp_test (id SERIAL, value INT)")
        assert affected == 0
        insert_affected = conn.query_drop("INSERT INTO temp_test (value) VALUES (42)")
        assert insert_affected == 1
        conn.close()

    def test_query_drop_drop_table(self):
        """Test query_drop for DROP TABLE returns 0."""
        conn = Conn(get_test_db_url())
        conn.query_drop("CREATE TEMP TABLE temp_drop_test (id SERIAL)")
        affected = conn.query_drop("DROP TABLE temp_drop_test")
        assert affected == 0
        conn.close()


class TestSyncQueryDropConnectionState:
    """Test that query_drop leaves connection in clean state."""

    def test_query_drop_multiple_consecutive_operations(self):
        """Test multiple query_drop operations in sequence."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        affected1 = conn.query_drop(
            "INSERT INTO test_table (name, age) VALUES ('Alice', 30)"
        )
        assert affected1 == 1
        affected2 = conn.query_drop(
            "INSERT INTO test_table (name, age) VALUES ('Bob', 25)"
        )
        assert affected2 == 1
        affected3 = conn.query_drop(
            "UPDATE test_table SET age = age + 1 WHERE name = 'Alice'"
        )
        assert affected3 == 1
        affected4 = conn.query_drop("DELETE FROM test_table WHERE name = 'Bob'")
        assert affected4 == 1
        count = conn.query_first("SELECT COUNT(*) FROM test_table")
        assert count[0] == 1
        conn.close()

    def test_query_drop_followed_by_query(self):
        """Test query_drop followed by query."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        affected = conn.query_drop(
            "INSERT INTO test_table (name, age) VALUES ('Alice', 30)"
        )
        assert affected == 1
        results = conn.query("SELECT name, age FROM test_table WHERE age = 30")
        assert len(results) == 1
        assert results[0][0] == "Alice"
        conn.close()


class TestSyncQueryDropUpdateVariants:
    """Test query_drop UPDATE with various scenarios."""

    def test_query_drop_update_all_rows(self):
        """Test query_drop UPDATE affecting all rows."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.query_drop(
            "INSERT INTO test_table (name, age) VALUES ('Alice', 30), ('Bob', 25), ('Charlie', 35)"
        )
        affected = conn.query_drop("UPDATE test_table SET age = age + 10")
        assert affected == 3
        conn.close()


class TestSyncQueryDropDeleteVariants:
    """Test query_drop DELETE with various scenarios."""

    def test_query_drop_delete_all_rows(self):
        """Test query_drop DELETE affecting all rows."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        conn.query_drop(
            "INSERT INTO test_table (name, age) VALUES ('Alice', 30), ('Bob', 25), ('Charlie', 35)"
        )
        affected = conn.query_drop("DELETE FROM test_table")
        assert affected == 3
        count = conn.query_first("SELECT COUNT(*) FROM test_table")
        assert count[0] == 0
        conn.close()


class TestSyncQueryDropReturnType:
    """Test that query_drop returns proper integer type."""

    def test_query_drop_returns_int(self):
        """Test query_drop returns Python int."""
        conn = Conn(get_test_db_url())
        setup_test_table_sync(conn)
        affected = conn.query_drop(
            "INSERT INTO test_table (name, age) VALUES ('Alice', 30)"
        )
        assert isinstance(affected, int)
        conn.close()
