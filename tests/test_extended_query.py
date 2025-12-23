"""Tests for extended query protocol (binary protocol with parameters)."""

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


# ─── Sync Extended Query Tests ───────────────────────────────────────────────


def test_sync_exec_with_params():
    """Test sync exec with parameters."""
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

    results = conn.exec("SELECT name, age FROM test_table WHERE age > $1", (20,))

    assert len(results) == 2

    results = conn.exec("SELECT name, age FROM test_table WHERE age = $1", (25,))

    assert len(results) == 1
    assert (results[0][0], results[0][1]) == ("Bob", 25)

    cleanup_test_table_sync(conn)
    conn.close()


def test_sync_exec_first():
    """Test sync exec_first method."""
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

    result = conn.exec_first("SELECT name, age FROM test_table ORDER BY age DESC", ())
    assert result
    assert (result[0], result[1]) == ("Alice", 30)

    result = conn.exec_first("SELECT name, age FROM test_table WHERE age > $1", (100,))
    assert result is None

    cleanup_test_table_sync(conn)
    conn.close()


def test_sync_exec_drop():
    """Test sync exec_drop for parameterized DML."""
    conn = Conn(get_test_db_url())

    setup_test_table_sync(conn)

    conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES ($1, $2)",
        ("Alice", 30),
    )

    result = conn.query_first("SELECT name, age FROM test_table")
    assert result
    assert result[0] == "Alice"
    assert result[1] == 30

    cleanup_test_table_sync(conn)
    conn.close()


def test_sync_exec_batch():
    """Test sync batch execution."""
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
    assert count
    assert count[0] == 5

    cleanup_test_table_sync(conn)
    conn.close()


def test_sync_exec_with_nulls():
    """Test sync handling of NULL values with extended query."""
    conn = Conn(get_test_db_url())

    setup_test_table_sync(conn)

    conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES ($1, $2)",
        ("Alice", 30),
    )
    conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES ($1, $2)",
        ("Bob", None),
    )

    results = conn.exec("SELECT name, age FROM test_table ORDER BY name", ())

    assert len(results) == 2
    assert (results[0][0], results[0][1]) == ("Alice", 30)
    assert (results[1][0], results[1][1]) == ("Bob", None)

    cleanup_test_table_sync(conn)
    conn.close()


def test_sync_exec_as_dict():
    """Test sync exec with as_dict=True returns dictionaries."""
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

    results = conn.exec(
        "SELECT name, age FROM test_table WHERE age > $1", (20,), as_dict=True
    )

    assert len(results) == 2
    assert all(isinstance(r, dict) for r in results)

    names = {r["name"] for r in results}
    assert names == {"Alice", "Bob"}

    cleanup_test_table_sync(conn)
    conn.close()


def test_sync_exec_first_as_dict():
    """Test sync exec_first with as_dict=True returns dictionary."""
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

    result = conn.exec_first(
        "SELECT name, age FROM test_table ORDER BY age DESC", (), as_dict=True
    )

    assert result is not None
    assert isinstance(result, dict)
    assert result["name"] == "Alice"
    assert result["age"] == 30

    result = conn.exec_first(
        "SELECT name, age FROM test_table WHERE age > $1", (100,), as_dict=True
    )
    assert result is None

    cleanup_test_table_sync(conn)
    conn.close()


def test_sync_exec_affected_rows():
    """Test sync affected_rows after extended query."""
    conn = Conn(get_test_db_url())

    setup_test_table_sync(conn)

    conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES ($1, $2), ($3, $4), ($5, $6)",
        ("Alice", 30, "Bob", 25, "Charlie", 35),
    )

    affected_rows = conn.affected_rows()
    assert affected_rows == 3

    conn.exec_drop("UPDATE test_table SET age = age + 1 WHERE age > $1", (25,))

    affected_rows = conn.affected_rows()
    assert affected_rows == 2

    conn.exec_drop("DELETE FROM test_table WHERE age < $1", (30,))

    affected_rows = conn.affected_rows()
    assert affected_rows == 1

    cleanup_test_table_sync(conn)
    conn.close()


def test_sync_exec_prepared_statement_caching():
    """Test that prepared statements are cached and reused."""
    conn = Conn(get_test_db_url())

    setup_test_table_sync(conn)

    query = "INSERT INTO test_table (name, age) VALUES ($1, $2)"

    conn.exec_drop(query, ("Alice", 30))
    conn.exec_drop(query, ("Bob", 25))
    conn.exec_drop(query, ("Charlie", 35))

    count = conn.query_first("SELECT COUNT(*) FROM test_table")
    assert count
    assert count[0] == 3

    cleanup_test_table_sync(conn)
    conn.close()


# ─── Async Extended Query Tests ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_async_exec_with_params():
    """Test async exec with parameters."""
    conn = await get_async_conn(get_test_db_url())

    await setup_test_table_async(conn)

    await conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES ($1, $2)",
        ("Alice", 30),
    )
    await conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES ($1, $2)",
        ("Bob", 25),
    )

    results = await conn.exec("SELECT name, age FROM test_table WHERE age > $1", (20,))

    assert len(results) == 2

    results = await conn.exec("SELECT name, age FROM test_table WHERE age = $1", (25,))

    assert len(results) == 1
    assert (results[0][0], results[0][1]) == ("Bob", 25)

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_async_exec_first():
    """Test async exec_first method."""
    conn = await get_async_conn(get_test_db_url())

    await setup_test_table_async(conn)

    await conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES ($1, $2)",
        ("Alice", 30),
    )
    await conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES ($1, $2)",
        ("Bob", 25),
    )

    result = await conn.exec_first(
        "SELECT name, age FROM test_table ORDER BY age DESC", ()
    )
    assert result
    assert (result[0], result[1]) == ("Alice", 30)

    result = await conn.exec_first(
        "SELECT name, age FROM test_table WHERE age > $1", (100,)
    )
    assert result is None

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_async_exec_drop():
    """Test async exec_drop for parameterized DML."""
    conn = await get_async_conn(get_test_db_url())

    await setup_test_table_async(conn)

    await conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES ($1, $2)",
        ("Alice", 30),
    )

    result = await conn.query_first("SELECT name, age FROM test_table")
    assert result
    assert result[0] == "Alice"
    assert result[1] == 30

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_async_exec_batch():
    """Test async batch execution."""
    conn = await get_async_conn(get_test_db_url())

    await setup_test_table_async(conn)

    params = [
        ("Alice", 30),
        ("Bob", 25),
        ("Charlie", 35),
        ("David", 40),
        ("Eve", 28),
    ]

    await conn.exec_batch("INSERT INTO test_table (name, age) VALUES ($1, $2)", params)

    count = await conn.query_first("SELECT COUNT(*) FROM test_table")
    assert count
    assert count[0] == 5

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_async_exec_with_nulls():
    """Test async handling of NULL values with extended query."""
    conn = await get_async_conn(get_test_db_url())

    await setup_test_table_async(conn)

    await conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES ($1, $2)",
        ("Alice", 30),
    )
    await conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES ($1, $2)",
        ("Bob", None),
    )

    results = await conn.exec("SELECT name, age FROM test_table ORDER BY name", ())

    assert len(results) == 2
    assert (results[0][0], results[0][1]) == ("Alice", 30)
    assert (results[1][0], results[1][1]) == ("Bob", None)

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_async_exec_as_dict():
    """Test async exec with as_dict=True returns dictionaries."""
    conn = await get_async_conn(get_test_db_url())

    await setup_test_table_async(conn)

    await conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES ($1, $2)",
        ("Alice", 30),
    )
    await conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES ($1, $2)",
        ("Bob", 25),
    )

    results = await conn.exec(
        "SELECT name, age FROM test_table WHERE age > $1", (20,), as_dict=True
    )

    assert len(results) == 2
    assert all(isinstance(r, dict) for r in results)

    names = {r["name"] for r in results}
    assert names == {"Alice", "Bob"}

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_async_exec_first_as_dict():
    """Test async exec_first with as_dict=True returns dictionary."""
    conn = await get_async_conn(get_test_db_url())

    await setup_test_table_async(conn)

    await conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES ($1, $2)",
        ("Alice", 30),
    )
    await conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES ($1, $2)",
        ("Bob", 25),
    )

    result = await conn.exec_first(
        "SELECT name, age FROM test_table ORDER BY age DESC", (), as_dict=True
    )

    assert result is not None
    assert isinstance(result, dict)
    assert result["name"] == "Alice"
    assert result["age"] == 30

    result = await conn.exec_first(
        "SELECT name, age FROM test_table WHERE age > $1", (100,), as_dict=True
    )
    assert result is None

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_async_exec_affected_rows():
    """Test async affected_rows after extended query."""
    conn = await get_async_conn(get_test_db_url())

    await setup_test_table_async(conn)

    await conn.exec_drop(
        "INSERT INTO test_table (name, age) VALUES ($1, $2), ($3, $4), ($5, $6)",
        ("Alice", 30, "Bob", 25, "Charlie", 35),
    )

    affected_rows = await conn.affected_rows()
    assert affected_rows == 3

    await conn.exec_drop("UPDATE test_table SET age = age + 1 WHERE age > $1", (25,))

    affected_rows = await conn.affected_rows()
    assert affected_rows == 2

    await conn.exec_drop("DELETE FROM test_table WHERE age < $1", (30,))

    affected_rows = await conn.affected_rows()
    assert affected_rows == 1

    await cleanup_test_table_async(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_async_exec_prepared_statement_caching():
    """Test that prepared statements are cached and reused."""
    conn = await get_async_conn(get_test_db_url())

    await setup_test_table_async(conn)

    query = "INSERT INTO test_table (name, age) VALUES ($1, $2)"

    await conn.exec_drop(query, ("Alice", 30))
    await conn.exec_drop(query, ("Bob", 25))
    await conn.exec_drop(query, ("Charlie", 35))

    count = await conn.query_first("SELECT COUNT(*) FROM test_table")
    assert count
    assert count[0] == 3

    await cleanup_test_table_async(conn)
    await conn.close()
