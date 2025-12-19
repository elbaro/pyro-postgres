import logging
import os

import pytest

from pyro_postgres import Opts


def pytest_configure(config):
    logging.getLogger("pyro_postgres").setLevel(logging.DEBUG)


def get_test_db_url() -> str:
    """Get the test database URL from environment or default."""
    return os.environ.get(
        "DATABASE_URL", "postgresql://test:1234@localhost:5432/test"
    )


def get_async_opts() -> Opts:
    """Get async connection options for testing."""
    url = get_test_db_url()
    return Opts(url)


def get_sync_opts() -> Opts:
    """Get sync connection options for testing."""
    url = get_test_db_url()
    return Opts(url)


@pytest.fixture
async def async_conn():
    """Provide an async database connection for tests."""
    from pyro_postgres.async_ import Conn

    conn = await Conn.new(get_test_db_url())

    try:
        yield conn
    finally:
        await conn.close()


@pytest.fixture
def sync_conn():
    """Provide a sync database connection for tests."""
    from pyro_postgres.sync import Conn

    conn = Conn(get_test_db_url())

    yield conn

    conn.close()


async def setup_test_table_async(conn):
    """Set up a test table for async tests."""
    await conn.query_drop("DROP TABLE IF EXISTS test_table")
    await conn.query_drop(
        """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            age INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )


def setup_test_table_sync(conn):
    """Set up a test table for sync tests."""
    conn.query_drop("DROP TABLE IF EXISTS test_table")
    conn.query_drop(
        """
        CREATE TABLE test_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            age INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )


async def cleanup_test_table_async(conn):
    """Clean up test table for async tests."""
    await conn.query_drop("DROP TABLE IF EXISTS test_table")


def cleanup_test_table_sync(conn):
    """Clean up test table for sync tests."""
    conn.query_drop("DROP TABLE IF EXISTS test_table")


@pytest.fixture
async def async_conn_with_table():
    """Provide an async connection with test table set up."""
    from pyro_postgres.async_ import Conn

    conn = await Conn.new(get_test_db_url())

    try:
        await setup_test_table_async(conn)
        yield conn
        await cleanup_test_table_async(conn)
    finally:
        await conn.close()


@pytest.fixture
def sync_conn_with_table():
    """Provide a sync connection with test table set up."""
    from pyro_postgres.sync import Conn

    conn = Conn(get_test_db_url())

    setup_test_table_sync(conn)
    yield conn
    cleanup_test_table_sync(conn)

    conn.close()


async def get_async_conn(url_or_opts):
    """Helper function to create async connection."""
    from pyro_postgres.async_ import Conn

    return await Conn.new(url_or_opts)
