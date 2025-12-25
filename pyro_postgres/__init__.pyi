"""
pyro_postgres - High-performance PostgreSQL driver for Python, written in Rust.

- pyro_postgres.sync: The synchronous API
- pyro_postgres.async_: The asynchronous API
- pyro_postgres.error: Exceptions
"""

import datetime
import decimal
import uuid
from collections.abc import Generator, Sequence
from typing import Any, Awaitable, TypeVar

from . import async_, sync
from . import error as error

def init(worker_threads: int | None = 1, thread_name: str | None = None) -> None:
    """
    Initialize the Tokio runtime for async operations.
    This function can be called multiple times until any async operation is called.

    Args:
        worker_threads: Number of worker threads for the Tokio runtime. If None, set to the number of CPUs.
        thread_name: Name prefix for worker threads.
    """
    ...

# Compatibility aliases for backward compatibility
AsyncConn = async_.Conn
AsyncTransaction = async_.Transaction

SyncConn = sync.Conn
SyncTransaction = sync.Transaction

class Opts:
    """
    Connection options for PostgreSQL connections.

    This class provides a builder API for configuring PostgreSQL connection parameters.
    Methods can be chained to configure multiple options.

    Examples:
        # Create from URL
        opts = Opts("postgres://user:pass@localhost:5432/mydb")

        # Create with builder pattern
        opts = Opts().host("localhost").port(5432).user("postgres").password("secret").db("mydb")
    """

    def __new__(cls, url: str | None = None) -> "Opts":
        """
        Create a new Opts instance.

        Args:
            url: Optional PostgreSQL connection URL. If provided, parses the URL.
                 If not provided, creates default opts.
        """
        ...

    def host(self, hostname: str) -> "Opts":
        """Set the hostname or IP address."""
        ...

    def port(self, port: int) -> "Opts":
        """Set the TCP port number (default: 5432)."""
        ...

    def socket(self, path: str | None) -> "Opts":
        """Set the Unix socket path for local connections."""
        ...

    def user(self, username: str) -> "Opts":
        """Set the username for authentication."""
        ...

    def password(self, password: str | None) -> "Opts":
        """Set the password for authentication."""
        ...

    def db(self, database: str | None) -> "Opts":
        """Set the database name to connect to."""
        ...

    def application_name(self, name: str | None) -> "Opts":
        """Set the application name to report to the server."""
        ...

    def ssl_mode(self, mode: str) -> "Opts":
        """
        Set the SSL mode for the connection.

        Args:
            mode: One of "disable", "prefer", "require".
        """
        ...

    def prefer_unix_socket(self, enable: bool) -> "Opts":
        """
        Enable or disable automatic upgrade from TCP to Unix socket.

        When enabled and connected via TCP to loopback, the driver will query
        `unix_socket_directories` and reconnect using the Unix socket for better performance.
        """
        ...

    def pool_max_idle_conn(self, count: int) -> "Opts":
        """Set the maximum number of idle connections in the pool (default: 100)."""
        ...

    def pool_max_concurrency(self, count: int | None) -> "Opts":
        """Set the maximum number of concurrent connections (active + idle), or None for unlimited."""
        ...

JsonEncodable = (
    dict[str, "JsonEncodable"] | list["JsonEncodable"] | str | int | float | bool | None
)

type Value = (
    None
    | bool
    | int
    | float
    | str
    | bytes
    | bytearray
    | tuple[JsonEncodable, ...]
    | list[JsonEncodable]
    | set[JsonEncodable]
    | frozenset[JsonEncodable]
    | dict[str, JsonEncodable]
    | datetime.datetime
    | datetime.date
    | datetime.time
    | datetime.timedelta
    | decimal.Decimal
    | uuid.UUID
)

"""
Parameters that can be passed to query execution methods:
- `None`: No parameters
- `tuple[Value, ...]`: Positional parameters for queries with $1, $2, ... placeholders
- `list[Value]`: List of parameters for queries with $1, $2, ... placeholders

Examples:
No parameters:

    `await conn.exec("SELECT * FROM users")`

Positional parameters:

    `await conn.exec("SELECT * FROM users WHERE id = $1", (123,))`

Multiple positional parameters:

    `await conn.exec("SELECT * FROM users WHERE age > $1 AND city = $2", (18, "NYC"))`
"""
type Params = None | tuple[Value, ...] | Sequence[Value]

T = TypeVar("T")

class PyroFuture(Awaitable[T]):
    def __await__(self) -> Generator[Any, Any, T]: ...
    def cancel(self) -> bool: ...
    def get_loop(self): ...

class IsolationLevel:
    """Transaction isolation level enum."""

    ReadUncommitted: "IsolationLevel"
    ReadCommitted: "IsolationLevel"
    RepeatableRead: "IsolationLevel"
    Serializable: "IsolationLevel"

    @property
    def name(self) -> str:
        """Return the isolation level as a string."""
        ...
