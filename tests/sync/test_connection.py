"""Sync connection tests."""

import pytest

from pyro_postgres import Opts
from pyro_postgres.sync import Conn
from pyro_postgres.error import ConnectionClosedError, IncorrectApiUsageError

from ..conftest import get_test_db_url


class TestSyncConnection:
    """Test sync connection establishment and management."""

    def test_basic_connection(self):
        """Test basic synchronous connection."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT 1")
        assert result
        assert result[0] == 1
        conn.close()

    def test_connection_with_url_string(self):
        """Test connection using URL string."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT 'hello'")
        assert result
        assert result[0] == "hello"
        conn.close()

    def test_connection_with_opts(self):
        """Test sync connection using Opts object."""
        opts = Opts(get_test_db_url())
        conn = Conn(opts)
        result = conn.query_first("SELECT 1")
        assert result
        assert result[0] == 1
        conn.close()

    def test_connection_ping(self):
        """Test sync connection ping functionality."""
        conn = Conn(get_test_db_url())
        conn.ping()
        conn.close()

    def test_connection_id(self):
        """Test getting connection ID."""
        conn = Conn(get_test_db_url())
        connection_id = conn.id()
        assert isinstance(connection_id, int)
        assert connection_id > 0
        conn.close()

    def test_server_version(self):
        """Test retrieving server version."""
        conn = Conn(get_test_db_url())
        server_version = conn.server_version()
        assert isinstance(server_version, str)
        assert len(server_version) >= 1
        conn.close()

    def test_close_connection(self):
        """Test closing connection."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT 1")
        assert result[0] == 1
        conn.close()
        # After close, operations should fail
        with pytest.raises(ConnectionClosedError):
            conn.query("SELECT 1")

    def test_close_connection_multiple_times(self):
        """Test closing connection multiple times is safe."""
        conn = Conn(get_test_db_url())
        conn.close()
        conn.close()  # Should not raise

    def test_connection_with_wrong_credentials(self):
        """Test sync connection failure with wrong credentials."""
        with pytest.raises(Exception):
            Conn("postgresql://nonexistent_user:wrong_password@localhost:5432/test")

    def test_connection_to_invalid_host(self):
        """Test sync connection failure to invalid host."""
        with pytest.raises(Exception):
            Conn("postgresql://test:1234@invalid.host.that.does.not.exist:5432/test")

    def test_connection_to_invalid_port(self):
        """Test sync connection failure to invalid port."""
        with pytest.raises(Exception):
            Conn("postgresql://test:1234@localhost:59999/test")


class TestSyncOpts:
    """Test Opts class functionality."""

    def test_opts_from_url(self):
        """Test creating Opts from URL."""
        opts = Opts("postgresql://user:pass@localhost:5432/mydb")
        assert opts is not None

    def test_opts_default(self):
        """Test creating default Opts."""
        opts = Opts()
        assert opts is not None

    def test_opts_builder_chain(self):
        """Test Opts builder pattern with chaining."""
        opts = (
            Opts().host("localhost").port(5432).user("test").password("1234").db("test")
        )
        conn = Conn(opts)
        result = conn.query_first("SELECT 1")
        assert result[0] == 1
        conn.close()

    def test_opts_application_name(self):
        """Test setting application name."""
        opts = Opts(get_test_db_url()).application_name("test_app")
        conn = Conn(opts)
        result = conn.query_first("SELECT current_setting('application_name')")
        assert result
        assert result[0] == "test_app"
        conn.close()

    def test_opts_ssl_mode_disable(self):
        """Test setting SSL mode to disable."""
        opts = Opts(get_test_db_url()).ssl_mode("disable")
        conn = Conn(opts)
        conn.ping()
        conn.close()

    def test_opts_invalid_ssl_mode(self):
        """Test invalid SSL mode raises error."""
        with pytest.raises(IncorrectApiUsageError):
            Opts(get_test_db_url()).ssl_mode("invalid")

    def test_opts_upgrade_to_unix_socket(self):
        """Test upgrade_to_unix_socket option."""
        opts = Opts(get_test_db_url()).upgrade_to_unix_socket(False)
        conn = Conn(opts)
        conn.ping()
        conn.close()

    def test_opts_repr(self):
        """Test Opts __repr__."""
        opts = Opts(get_test_db_url())
        repr_str = repr(opts)
        assert isinstance(repr_str, str)
        assert len(repr_str) > 0
