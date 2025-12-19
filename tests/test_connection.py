import pytest

from pyro_postgres import Opts
from pyro_postgres.sync import Conn

from .conftest import get_async_conn, get_test_db_url


# ─── Sync Connection Tests ───────────────────────────────────────────────────


def test_basic_sync_connection():
    """Test basic synchronous connection."""
    conn = Conn(get_test_db_url())

    result = conn.query_first("SELECT 1")
    assert result
    assert result[0] == 1

    conn.close()


def test_sync_connection_ping():
    """Test sync connection ping functionality."""
    conn = Conn(get_test_db_url())
    conn.ping()
    conn.close()


def test_sync_connection_server_info():
    """Test retrieving server information."""
    conn = Conn(get_test_db_url())

    server_version = conn.server_version()
    assert len(server_version) >= 1

    connection_id = conn.id()
    assert connection_id > 0

    conn.close()


def test_sync_connection_with_opts():
    """Test sync connection using Opts object."""
    opts = Opts(get_test_db_url())
    conn = Conn(opts)

    result = conn.query_first("SELECT 1")
    assert result
    assert result[0] == 1

    conn.close()


def test_sync_connection_with_wrong_credentials():
    """Test sync connection failure with wrong credentials."""
    with pytest.raises(Exception):
        Conn("postgresql://nonexistent_user:wrong_password@localhost:5432/test")


def test_sync_connection_to_invalid_host():
    """Test sync connection failure to invalid host."""
    with pytest.raises(Exception):
        Conn("postgresql://test:1234@invalid.host.that.does.not.exist:5432/test")


# ─── Async Connection Tests ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_basic_async_connection():
    """Test basic async connection establishment."""
    conn = await get_async_conn(get_test_db_url())

    result = await conn.query_first("SELECT 1")
    assert result and result[0] == 1

    await conn.close()


@pytest.mark.asyncio
async def test_async_connection_ping():
    """Test async connection ping functionality."""
    conn = await get_async_conn(get_test_db_url())

    await conn.ping()

    await conn.close()


@pytest.mark.asyncio
async def test_async_connection_server_info():
    """Test retrieving server information via async connection."""
    conn = await get_async_conn(get_test_db_url())

    server_version = await conn.server_version()
    assert len(server_version) >= 1

    connection_id = await conn.id()
    assert connection_id > 0

    await conn.close()


@pytest.mark.asyncio
async def test_async_connection_with_opts():
    """Test async connection using Opts object."""
    from pyro_postgres.async_ import Conn

    opts = Opts(get_test_db_url())
    conn = await Conn.new(opts)

    result = await conn.query_first("SELECT 1")
    assert result
    assert result[0] == 1

    await conn.close()


@pytest.mark.asyncio
async def test_async_connection_with_wrong_credentials():
    """Test async connection failure with wrong credentials."""
    with pytest.raises(Exception):
        await get_async_conn(
            "postgresql://nonexistent_user:wrong_password@localhost:5432/test"
        )


@pytest.mark.asyncio
async def test_async_connection_to_invalid_host():
    """Test async connection failure to invalid host."""
    with pytest.raises(Exception):
        await get_async_conn(
            "postgresql://test:1234@invalid.host.that.does.not.exist:5432/test"
        )
