"""Async connection tests."""

import pytest

from pyro_postgres import Opts
from pyro_postgres.async_ import Conn
from pyro_postgres.error import ConnectionClosedError, IncorrectApiUsageError

from ..conftest import get_test_db_url


class TestAsyncConnection:
    """Test async connection establishment and management."""

    @pytest.mark.asyncio
    async def test_basic_connection(self):
        """Test basic async connection."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first("SELECT 1")
        assert result
        assert result[0] == 1
        await conn.close()

    @pytest.mark.asyncio
    async def test_connection_with_url_string(self):
        """Test connection using URL string."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first("SELECT 'hello'")
        assert result
        assert result[0] == "hello"
        await conn.close()

    @pytest.mark.asyncio
    async def test_connection_with_opts(self):
        """Test async connection using Opts object."""
        opts = Opts(get_test_db_url())
        conn = await Conn.new(opts)
        result = await conn.query_first("SELECT 1")
        assert result
        assert result[0] == 1
        await conn.close()

    @pytest.mark.asyncio
    async def test_connection_ping(self):
        """Test async connection ping functionality."""
        conn = await Conn.new(get_test_db_url())
        await conn.ping()
        await conn.close()

    @pytest.mark.asyncio
    async def test_connection_id(self):
        """Test getting connection ID."""
        conn = await Conn.new(get_test_db_url())
        connection_id = await conn.id()
        assert isinstance(connection_id, int)
        assert connection_id > 0
        await conn.close()

    @pytest.mark.asyncio
    async def test_server_version(self):
        """Test retrieving server version."""
        conn = await Conn.new(get_test_db_url())
        server_version = await conn.server_version()
        assert isinstance(server_version, str)
        assert len(server_version) >= 1
        await conn.close()

    @pytest.mark.asyncio
    async def test_close_connection(self):
        """Test closing connection."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first("SELECT 1")
        assert result[0] == 1
        await conn.close()
        # After close, operations should fail
        with pytest.raises(ConnectionClosedError):
            await conn.query("SELECT 1")

    @pytest.mark.asyncio
    async def test_close_connection_multiple_times(self):
        """Test closing connection multiple times is safe."""
        conn = await Conn.new(get_test_db_url())
        await conn.close()
        await conn.close()  # Should not raise

    @pytest.mark.asyncio
    async def test_connection_with_wrong_credentials(self):
        """Test async connection failure with wrong credentials."""
        with pytest.raises(Exception):
            await Conn.new(
                "postgresql://nonexistent_user:wrong_password@localhost:5432/test"
            )

    @pytest.mark.asyncio
    async def test_connection_to_invalid_host(self):
        """Test async connection failure to invalid host."""
        with pytest.raises(Exception):
            await Conn.new(
                "postgresql://test:1234@invalid.host.that.does.not.exist:5432/test"
            )

    @pytest.mark.asyncio
    async def test_connection_to_invalid_port(self):
        """Test async connection failure to invalid port."""
        with pytest.raises(Exception):
            await Conn.new("postgresql://test:1234@localhost:59999/test")

    def test_sync_constructor_raises(self):
        """Test that sync constructor raises error."""
        with pytest.raises(IncorrectApiUsageError):
            Conn()


class TestAsyncOpts:
    """Test Opts class functionality with async connections."""

    @pytest.mark.asyncio
    async def test_opts_from_url(self):
        """Test creating Opts from URL and using with async connection."""
        opts = Opts(get_test_db_url())
        conn = await Conn.new(opts)
        await conn.ping()
        await conn.close()

    @pytest.mark.asyncio
    async def test_opts_builder_chain(self):
        """Test Opts builder pattern with chaining for async connection."""
        opts = (
            Opts().host("localhost").port(5432).user("test").password("1234").db("test")
        )
        conn = await Conn.new(opts)
        result = await conn.query_first("SELECT 1")
        assert result[0] == 1
        await conn.close()

    @pytest.mark.asyncio
    async def test_opts_application_name(self):
        """Test setting application name."""
        opts = Opts(get_test_db_url()).application_name("test_async_app")
        conn = await Conn.new(opts)
        result = await conn.query_first("SELECT current_setting('application_name')")
        assert result
        assert result[0] == "test_async_app"
        await conn.close()

    @pytest.mark.asyncio
    async def test_opts_ssl_mode_disable(self):
        """Test setting SSL mode to disable."""
        opts = Opts(get_test_db_url()).ssl_mode("disable")
        conn = await Conn.new(opts)
        await conn.ping()
        await conn.close()

    @pytest.mark.asyncio
    async def test_opts_prefer_unix_socket(self):
        """Test prefer_unix_socket option."""
        opts = Opts(get_test_db_url()).prefer_unix_socket(False)
        conn = await Conn.new(opts)
        await conn.ping()
        await conn.close()
