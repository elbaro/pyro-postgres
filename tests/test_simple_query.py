"""Tests for simple query protocol (text protocol)."""

import pytest

from pyro_postgres.sync import Conn

from .conftest import (
    cleanup_test_table_async,
    cleanup_test_table_sync,
    get_async_conn,
    get_test_db_url,
    setup_test_table_async,
    setup_test_table_sync,
)


# ─── Sync Simple Query Tests ─────────────────────────────────────────────────


def test_sync_simple_query_select():
    """Test basic synchronous query execution."""
    conn = Conn(get_test_db_url())

    result = conn.query("SELECT 1 UNION SELECT 2 UNION SELECT 3 ORDER BY 1")

    assert len(result) == 3
    assert result[0][0] == 1
    assert result[1][0] == 2
    assert result[2][0] == 3

    conn.close()


def test_sync_simple_query_first():
    """Test sync query_first method."""
    conn = Conn(get_test_db_url())

    result = conn.query_first("SELECT 42")
    assert result
    assert result[0] == 42

    conn.close()


def test_sync_simple_query_first_no_results():
    """Test sync query_first with no results returns None."""
    conn = Conn(get_test_db_url())

    setup_test_table_sync(conn)

    result = conn.query_first("SELECT * FROM test_table WHERE id = 99999")
    assert result is None

    cleanup_test_table_sync(conn)
    conn.close()


def test_sync_simple_query_drop():
    """Test sync query_drop for DDL/DML."""
    conn = Conn(get_test_db_url())

    setup_test_table_sync(conn)

    conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")

    result = conn.query_first("SELECT name, age FROM test_table")
    assert result
    assert result[0] == "Alice"
    assert result[1] == 30

    cleanup_test_table_sync(conn)
    conn.close()


def test_sync_simple_query_with_nulls():
    """Test sync handling of NULL values in queries."""
    conn = Conn(get_test_db_url())

    setup_test_table_sync(conn)

    conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")
    conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Bob', NULL)")

    results = conn.query("SELECT name, age FROM test_table ORDER BY name")

    assert len(results) == 2
    assert (results[0][0], results[0][1]) == ("Alice", 30)
    assert (results[1][0], results[1][1]) == ("Bob", None)

    cleanup_test_table_sync(conn)
    conn.close()


def test_sync_simple_query_as_dict():
    """Test sync query with as_dict=True returns dictionaries."""
    conn = Conn(get_test_db_url())

    setup_test_table_sync(conn)

    conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")
    conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Bob', 25)")

    results = conn.query("SELECT name, age FROM test_table ORDER BY age", as_dict=True)

    assert len(results) == 2
    assert isinstance(results[0], dict)
    assert isinstance(results[1], dict)
    assert results[0]["name"] == "Bob"
    assert results[0]["age"] == 25
    assert results[1]["name"] == "Alice"
    assert results[1]["age"] == 30

    cleanup_test_table_sync(conn)
    conn.close()


def test_sync_simple_query_first_as_dict():
    """Test sync query_first with as_dict=True returns dictionary."""
    conn = Conn(get_test_db_url())

    setup_test_table_sync(conn)

    conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")

    result = conn.query_first(
        "SELECT name, age FROM test_table ORDER BY age DESC", as_dict=True
    )

    assert result is not None
    assert isinstance(result, dict)
    assert result["name"] == "Alice"
    assert result["age"] == 30

    cleanup_test_table_sync(conn)
    conn.close()


# ─── Async Simple Query Tests ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_async_simple_query_select():
    """Test basic async query execution."""
    conn = await get_async_conn(get_test_db_url())

    result = await conn.query("SELECT 1 UNION SELECT 2 UNION SELECT 3 ORDER BY 1")

    assert len(result) == 3
    assert result[0][0] == 1
    assert result[1][0] == 2
    assert result[2][0] == 3

    await conn.close()


@pytest.mark.asyncio
async def test_async_simple_query_first():
    """Test async query_first method."""
    conn = await get_async_conn(get_test_db_url())

    result = await conn.query_first("SELECT 42")
    assert result
    assert result[0] == 42

    await conn.close()


@pytest.mark.asyncio
async def test_async_simple_query_first_no_results():
    """Test async query_first with no results returns None."""
    conn = await get_async_conn(get_test_db_url())

    await setup_test_table_async(conn)

    result = await conn.query_first("SELECT * FROM test_table WHERE id = 99999")
    assert result is None

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_async_simple_query_drop():
    """Test async query_drop for DDL/DML."""
    conn = await get_async_conn(get_test_db_url())

    await setup_test_table_async(conn)

    await conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")

    result = await conn.query_first("SELECT name, age FROM test_table")
    assert result
    assert result[0] == "Alice"
    assert result[1] == 30

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_async_simple_query_with_nulls():
    """Test async handling of NULL values in queries."""
    conn = await get_async_conn(get_test_db_url())

    await setup_test_table_async(conn)

    await conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")
    await conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Bob', NULL)")

    results = await conn.query("SELECT name, age FROM test_table ORDER BY name")

    assert len(results) == 2
    assert (results[0][0], results[0][1]) == ("Alice", 30)
    assert (results[1][0], results[1][1]) == ("Bob", None)

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_async_simple_query_as_dict():
    """Test async query with as_dict=True returns dictionaries."""
    conn = await get_async_conn(get_test_db_url())

    await setup_test_table_async(conn)

    await conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")
    await conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Bob', 25)")

    results = await conn.query(
        "SELECT name, age FROM test_table ORDER BY age", as_dict=True
    )

    assert len(results) == 2
    assert isinstance(results[0], dict)
    assert isinstance(results[1], dict)
    assert results[0]["name"] == "Bob"
    assert results[0]["age"] == 25
    assert results[1]["name"] == "Alice"
    assert results[1]["age"] == 30

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_async_simple_query_first_as_dict():
    """Test async query_first with as_dict=True returns dictionary."""
    conn = await get_async_conn(get_test_db_url())

    await setup_test_table_async(conn)

    await conn.query_drop("INSERT INTO test_table (name, age) VALUES ('Alice', 30)")

    result = await conn.query_first(
        "SELECT name, age FROM test_table ORDER BY age DESC", as_dict=True
    )

    assert result is not None
    assert isinstance(result, dict)
    assert result["name"] == "Alice"
    assert result["age"] == 30

    await cleanup_test_table_async(conn)
    await conn.close()
