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

def init() -> None:
    """
    Initialize the Tokio runtime for async operations.

    This function is called automatically when the module is loaded.
    It can be called explicitly but has no effect after first initialization.
    """
    ...

# Compatibility aliases for backward compatibility
AsyncConn = async_.Conn
AsyncPipeline = async_.Pipeline
AsyncTransaction = async_.Transaction

SyncConn = sync.Conn
SyncPipeline = sync.Pipeline
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

    def upgrade_to_unix_socket(self, enable: bool) -> "Opts":
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

    def __new__(cls, level: str) -> "IsolationLevel":
        """
        Create an IsolationLevel from a string.

        Args:
            level: One of "read uncommitted", "read committed", "repeatable read", "serializable"
                   (also accepts underscore and camelCase variants).
        """
        ...

    def __repr__(self) -> str:
        """Return the isolation level representation."""
        ...

class PreparedStatement:
    """
    A prepared statement that can be reused for efficient query execution.

    Created via `conn.prepare()` and used with `conn.exec()` or `pipeline.exec()`:

    ```python
    prepared = await conn.prepare("INSERT INTO users (name) VALUES ($1)")

    # Use with connection
    await conn.exec(prepared, ("Alice",))

    # Or use with pipeline
    async with conn.pipeline() as p:
        t1 = p.exec(prepared, ("Alice",))
        t2 = p.exec(prepared, ("Bob",))
        await p.sync()
        await p.claim_drop(t1)
        await p.claim_drop(t2)
    ```
    """

    ...

Statement = str | PreparedStatement

class Ticket:
    """
    A ticket representing a queued pipeline operation.

    Created by `Pipeline.exec()` and used to claim results after `Pipeline.sync()`.
    Tickets must be claimed in the order they were created.
    """

    ...

class Json:
    """
    Wrapper for JSON data to explicitly send as PostgreSQL JSON type.

    Use this when you want to ensure data is sent as JSON (OID 114) rather than text.

    Examples:
        # From a dict/list (will be serialized)
        await conn.exec("INSERT INTO t (data) VALUES ($1)", (Json({"key": "value"}),))

        # From a string (used as-is)
        await conn.exec("INSERT INTO t (data) VALUES ($1)", (Json('{"key": "value"}'),))
    """

    def __new__(cls, data: Any) -> "Json":
        """
        Create a Json wrapper.

        Args:
            data: Python object to serialize as JSON, or a JSON string.
        """
        ...

    def __repr__(self) -> str: ...

class Jsonb:
    """
    Wrapper for JSONB data to explicitly send as PostgreSQL JSONB type.

    Use this when you want to ensure data is sent as JSONB (OID 3802) rather than text.

    Examples:
        # From a dict/list (will be serialized)
        await conn.exec("INSERT INTO t (data) VALUES ($1)", (Jsonb({"key": "value"}),))

        # From a string (used as-is)
        await conn.exec("INSERT INTO t (data) VALUES ($1)", (Jsonb('{"key": "value"}'),))
    """

    def __new__(cls, data: Any) -> "Jsonb":
        """
        Create a Jsonb wrapper.

        Args:
            data: Python object to serialize as JSONB, or a JSON string.
        """
        ...

    def __repr__(self) -> str: ...
