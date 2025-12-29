"""Asynchronous PostgreSQL driver components."""

from types import TracebackType
from typing import Any, Callable, Literal, Self, Sequence, TypeVar, overload

from pyro_postgres import (
    IsolationLevel,
    Opts,
    Params,
    PreparedStatement,
    PyroFuture,
    Statement,
    Ticket,
)

T = TypeVar("T")

class UnnamedPortal:
    """
    An unnamed portal for iterative row fetching.

    Created by `Conn.exec_iter()` and passed to the callback function.
    Use `fetch()` to retrieve rows in batches.

    Note: The callback is synchronous. The `fetch()` method blocks internally
    using the tokio runtime.
    """

    @overload
    def fetch(
        self, max_rows: int, *, as_dict: Literal[False] = False
    ) -> tuple[list[tuple[Any, ...]], bool]: ...
    @overload
    def fetch(
        self, max_rows: int, *, as_dict: Literal[True]
    ) -> tuple[list[dict[str, Any]], bool]: ...
    def fetch(
        self, max_rows: int, *, as_dict: bool = False
    ) -> tuple[list[tuple[Any, ...]], bool] | tuple[list[dict[str, Any]], bool]:
        """
        Fetch up to `max_rows` rows from the portal.

        This method blocks internally using the tokio runtime.

        Args:
            max_rows: Maximum number of rows to fetch. Use 0 to fetch all remaining rows.
            as_dict: If True, return rows as dictionaries. If False (default), return rows as tuples.

        Returns:
            A tuple of (rows, has_more) where:
            - rows: List of tuples (default) or dictionaries.
            - has_more: True if more rows are available, False if all rows have been fetched.
        """
        ...

class NamedPortal:
    """
    A named portal for iterative row fetching with interleaving support.

    Created by `Transaction.exec_portal()`. Unlike unnamed portals, named portals
    can be interleaved - you can create multiple portals and fetch from them
    alternately. Named portals must be created within an explicit transaction.

    Use `exec_collect()` to fetch rows and `close()` to release resources.
    """

    @overload
    def exec_collect(
        self, max_rows: int, *, as_dict: Literal[False] = False
    ) -> PyroFuture[tuple[list[tuple[Any, ...]], bool]]: ...
    @overload
    def exec_collect(
        self, max_rows: int, *, as_dict: Literal[True]
    ) -> PyroFuture[tuple[list[dict[str, Any]], bool]]: ...
    def exec_collect(
        self, max_rows: int, *, as_dict: bool = False
    ) -> PyroFuture[
        tuple[list[tuple[Any, ...]], bool] | tuple[list[dict[str, Any]], bool]
    ]:
        """
        Execute the portal and collect up to `max_rows` rows.

        Args:
            max_rows: Maximum number of rows to fetch. Use 0 to fetch all remaining rows.
            as_dict: If True, return rows as dictionaries. If False (default), return rows as tuples.

        Returns:
            A tuple of (rows, has_more) where:
            - rows: List of tuples (default) or dictionaries.
            - has_more: True if more rows are available.
        """
        ...

    def is_complete(self) -> bool:
        """
        Check if all rows have been fetched from this portal.

        Note: For async, use the `has_more` return value from `exec_collect()` instead,
        as this property cannot be updated from async operations.

        Returns:
            True if the last `exec_collect()` call fetched all remaining rows.
        """
        ...

    def close(self) -> PyroFuture[None]:
        """
        Close the portal, releasing server resources.

        After closing, the portal cannot be used for further fetching.
        """
        ...

class Transaction:
    """
    Represents a PostgreSQL transaction with async context manager support.

    Use as an async context manager to automatically commit or rollback.
    Create named portals with `exec_portal()` for iterative row fetching.
    """

    def __aenter__(self) -> PyroFuture["Transaction"]:
        """Enter the async context manager. Returns the transaction."""
        ...

    def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> PyroFuture[bool]:
        """Exit the async context manager. Automatically commits or rolls back."""
        ...

    def commit(self) -> PyroFuture[None]:
        """Commit the transaction."""
        ...

    def rollback(self) -> PyroFuture[None]:
        """Rollback the transaction."""
        ...

    def exec_portal(self, query: str, params: Params = ()) -> PyroFuture[NamedPortal]:
        """
        Create a named portal for iterative row fetching.

        Named portals allow interleaving multiple row streams. Unlike unnamed
        portals (used in exec_iter), named portals can be executed multiple
        times and can coexist with other portals.

        Named portals must be created within an explicit transaction because
        SYNC messages (which occur at transaction boundaries) close all portals.

        Args:
            query: SQL query string with $1, $2, ... placeholders.
            params: Query parameters.

        Returns:
            A future that resolves to a NamedPortal that can be used to fetch rows.

        Example:
            ```python
            async with conn.tx() as tx:
                portal1 = await tx.exec_portal("SELECT * FROM table1")
                portal2 = await tx.exec_portal("SELECT * FROM table2")

                while True:
                    rows1, has_more1 = await portal1.exec_collect(100)
                    rows2, has_more2 = await portal2.exec_collect(100)
                    process(rows1, rows2)
                    if not has_more1 and not has_more2:
                        break

                await portal1.close()
                await portal2.close()
            ```
        """
        ...

class Pipeline:
    """
    Async pipeline mode for batching multiple queries.

    Created via `conn.pipeline()` and used as an async context manager.
    Pipeline mode allows sending multiple queries without waiting for responses,
    then syncing and claiming results in order.

    Example:
        ```python
        async with conn.pipeline() as p:
            t1 = p.exec("SELECT $1::int", (1,))
            t2 = p.exec("SELECT $1::int", (2,))
            await p.sync()
            result1 = await p.claim_one(t1)
            result2 = await p.claim_collect(t2)
        ```
    """

    def __aenter__(self) -> PyroFuture["Pipeline"]:
        """Enter the async context manager. Returns the pipeline."""
        ...

    def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> PyroFuture[bool]:
        """Exit the async context manager. Cleans up the pipeline."""
        ...

    def exec(self, query: Statement, params: Params = ()) -> Ticket:
        """
        Queue a statement execution.

        Accepts either a SQL query string or a PreparedStatement.
        Returns a Ticket that must be claimed later using claim_one, claim_collect, or claim_drop.

        Args:
            query: SQL query string or PreparedStatement.
            params: Query parameters.

        Returns:
            A Ticket to claim results after sync().
        """
        ...

    def sync(self) -> PyroFuture[None]:
        """
        Send SYNC message to establish transaction boundary.

        After calling sync(), you must claim all queued operations in order.
        """
        ...

    @overload
    def claim_one(
        self, ticket: Ticket, *, as_dict: Literal[False] = False
    ) -> PyroFuture[tuple[Any, ...] | None]: ...
    @overload
    def claim_one(
        self, ticket: Ticket, *, as_dict: Literal[True]
    ) -> PyroFuture[dict[str, Any] | None]: ...
    def claim_one(
        self, ticket: Ticket, *, as_dict: bool = False
    ) -> PyroFuture[tuple[Any, ...] | dict[str, Any] | None]:
        """
        Claim and return just the first row (or None).

        Results must be claimed in the same order they were queued.

        Args:
            ticket: The ticket from exec().
            as_dict: If True, return row as dictionary.

        Returns:
            First row or None if no results.
        """
        ...

    @overload
    def claim_collect(
        self, ticket: Ticket, *, as_dict: Literal[False] = False
    ) -> PyroFuture[list[tuple[Any, ...]]]: ...
    @overload
    def claim_collect(
        self, ticket: Ticket, *, as_dict: Literal[True]
    ) -> PyroFuture[list[dict[str, Any]]]: ...
    def claim_collect(
        self, ticket: Ticket, *, as_dict: bool = False
    ) -> PyroFuture[list[tuple[Any, ...]] | list[dict[str, Any]]]:
        """
        Claim and collect all rows.

        Results must be claimed in the same order they were queued.

        Args:
            ticket: The ticket from exec().
            as_dict: If True, return rows as dictionaries.

        Returns:
            List of rows.
        """
        ...

    def claim_drop(self, ticket: Ticket) -> PyroFuture[None]:
        """
        Claim and discard all rows.

        Results must be claimed in the same order they were queued.

        Args:
            ticket: The ticket from exec().
        """
        ...

    @overload
    def claim(
        self, ticket: Ticket, *, as_dict: Literal[False] = False
    ) -> PyroFuture[list[tuple[Any, ...]]]: ...
    @overload
    def claim(
        self, ticket: Ticket, *, as_dict: Literal[True]
    ) -> PyroFuture[list[dict[str, Any]]]: ...
    def claim(
        self, ticket: Ticket, *, as_dict: bool = False
    ) -> PyroFuture[list[tuple[Any, ...]] | list[dict[str, Any]]]:
        """
        Claim and collect all rows (alias for claim_collect).

        Results must be claimed in the same order they were queued.

        Args:
            ticket: The ticket from exec().
            as_dict: If True, return rows as dictionaries.

        Returns:
            List of rows.
        """
        ...

    def pending_count(self) -> int:
        """
        Returns the number of operations that have been queued but not yet claimed.
        """
        ...

    def is_aborted(self) -> bool:
        """
        Returns true if the pipeline is in aborted state due to an error.
        """
        ...

class Conn:
    """
    Asynchronous PostgreSQL connection.

    The API is thread-safe. The underlying implementation is protected by RwLock.
    """

    def __init__(self) -> None:
        """
        Direct instantiation is not allowed.
        Use Conn.new() instead.
        """
        ...

    @staticmethod
    def new(url_or_opts: str | Opts) -> PyroFuture["Conn"]:
        """
        Create a new connection.

        Args:
            url_or_opts: PostgreSQL connection URL (e.g., 'postgres://user:password@host:port/database')
                or Opts object with connection configuration.

        Returns:
            New Conn instance.
        """
        ...

    def tx(
        self,
        isolation_level: IsolationLevel | None = None,
        readonly: bool | None = None,
    ) -> Transaction:
        """
        Start a new transaction.

        Args:
            isolation_level: Transaction isolation level.
            readonly: Whether the transaction is read-only.

        Returns:
            New Transaction instance.
        """
        ...

    def pipeline(self) -> Pipeline:
        """
        Create a pipeline for batching multiple queries.

        Use as an async context manager:
        ```python
        async with conn.pipeline() as p:
            t1 = p.exec("SELECT $1::int", (1,))
            t2 = p.exec("SELECT $1::int", (2,))
            await p.sync()
            result1 = await p.claim_one(t1)
            result2 = await p.claim_collect(t2)
        ```
        """
        ...

    def id(self) -> PyroFuture[int]:
        """Return the connection ID."""
        ...

    def ping(self) -> PyroFuture[None]:
        """Ping the server to check connection."""
        ...

    @overload
    def query(
        self, query: str, *, as_dict: Literal[False] = False
    ) -> PyroFuture[list[tuple[Any, ...]]]: ...
    @overload
    def query(
        self, query: str, *, as_dict: Literal[True]
    ) -> PyroFuture[list[dict[str, Any]]]: ...
    def query(
        self, query: str, *, as_dict: bool = False
    ) -> PyroFuture[list[tuple[Any, ...]] | list[dict[str, Any]]]:
        """
        Execute a query using text protocol and return all rows.

        Args:
            query: SQL query string.
            as_dict: If True, return rows as dictionaries. If False (default), return rows as tuples.

        Returns:
            List of tuples (default) or dictionaries.
        """
        ...

    @overload
    def query_first(
        self, query: str, *, as_dict: Literal[False] = False
    ) -> PyroFuture[tuple[Any, ...] | None]: ...
    @overload
    def query_first(
        self, query: str, *, as_dict: Literal[True]
    ) -> PyroFuture[dict[str, Any] | None]: ...
    def query_first(
        self, query: str, *, as_dict: bool = False
    ) -> PyroFuture[tuple[Any, ...] | dict[str, Any] | None]:
        """
        Execute a query using text protocol and return the first row.

        Args:
            query: SQL query string.
            as_dict: If True, return row as dictionary. If False (default), return row as tuple.

        Returns:
            First row as tuple (default) or dictionary, or None if no results.
        """
        ...

    def query_drop(self, query: str) -> PyroFuture[int]:
        """
        Execute a query using text protocol and discard the results.

        Args:
            query: SQL query string.

        Returns:
            Number of rows affected by the query.
        """
        ...

    def prepare(self, query: str) -> PyroFuture[PreparedStatement]:
        """
        Prepare a statement for later execution.

        Args:
            query: SQL query string with $1, $2, ... placeholders.

        Returns:
            A future that resolves to a PreparedStatement that can be reused with exec methods.

        Example:
            ```python
            stmt = await conn.prepare("SELECT * FROM users WHERE id = $1")
            row1 = await conn.exec_first(stmt, (1,))
            row2 = await conn.exec_first(stmt, (2,))
            ```
        """
        ...

    def prepare_batch(self, sqls: Sequence[str]) -> PyroFuture[list[PreparedStatement]]:
        """
        Prepare multiple statements in a single round trip.

        Args:
            sqls: List of SQL query strings.

        Returns:
            A future that resolves to a list of PreparedStatements.

        Example:
            ```python
            stmts = await conn.prepare_batch([
                "SELECT * FROM users WHERE id = $1",
                "INSERT INTO logs (msg) VALUES ($1)",
            ])
            ```
        """
        ...

    @overload
    def exec(
        self, stmt: Statement, params: Params = (), *, as_dict: Literal[False] = False
    ) -> PyroFuture[list[tuple[Any, ...]]]: ...
    @overload
    def exec(
        self, stmt: Statement, params: Params = (), *, as_dict: Literal[True]
    ) -> PyroFuture[list[dict[str, Any]]]: ...
    def exec(
        self, stmt: Statement, params: Params = (), *, as_dict: bool = False
    ) -> PyroFuture[list[tuple[Any, ...]] | list[dict[str, Any]]]:
        """
        Execute a statement using extended protocol and return all rows.

        Args:
            stmt: SQL query string or PreparedStatement.
            params: Query parameters.
            as_dict: If True, return rows as dictionaries. If False (default), return rows as tuples.

        Returns:
            List of tuples (default) or dictionaries.
        """
        ...

    @overload
    def exec_first(
        self, stmt: Statement, params: Params = (), *, as_dict: Literal[False] = False
    ) -> PyroFuture[tuple[Any, ...] | None]: ...
    @overload
    def exec_first(
        self, stmt: Statement, params: Params = (), *, as_dict: Literal[True]
    ) -> PyroFuture[dict[str, Any] | None]: ...
    def exec_first(
        self, stmt: Statement, params: Params = (), *, as_dict: bool = False
    ) -> PyroFuture[tuple[Any, ...] | dict[str, Any] | None]:
        """
        Execute a statement using extended protocol and return the first row.

        Args:
            stmt: SQL query string or PreparedStatement.
            params: Query parameters.
            as_dict: If True, return row as dictionary. If False (default), return row as tuple.

        Returns:
            First row as tuple (default) or dictionary, or None if no results.
        """
        ...

    def exec_drop(self, stmt: Statement, params: Params = ()) -> PyroFuture[int]:
        """
        Execute a statement using extended protocol and discard the results.

        Args:
            stmt: SQL query string or PreparedStatement.
            params: Query parameters.

        Returns:
            Number of rows affected by the query.
        """
        ...

    def exec_batch(
        self, stmt: Statement, params_list: Sequence[Params]
    ) -> PyroFuture[None]:
        """
        Execute a statement multiple times with different parameters.

        Args:
            stmt: SQL query string or PreparedStatement.
            params_list: List of parameter sets.
        """
        ...

    def exec_iter(
        self, stmt: Statement, params: Params, callback: Callable[[UnnamedPortal], T]
    ) -> PyroFuture[T]:
        """
        Execute a statement and process rows iteratively via a callback.

        The callback receives an UnnamedPortal that can fetch rows in batches.
        Useful for processing large result sets that don't fit in memory.

        Note: The callback is synchronous. The portal's `fetch()` method blocks
        internally using the tokio runtime.

        Args:
            stmt: SQL query string or PreparedStatement.
            params: Query parameters.
            callback: A synchronous function that receives an UnnamedPortal and returns a result.

        Returns:
            A future that resolves to the value returned by the callback.

        Example:
            ```python
            def process(portal):
                total = 0
                while True:
                    rows, has_more = portal.fetch(1000)
                    total += sum(row[0] for row in rows)
                    if not has_more:
                        break
                return total

            result = await conn.exec_iter("SELECT value FROM large_table", (), process)
            ```
        """
        ...

    def close(self) -> PyroFuture[None]:
        """
        Disconnect from the PostgreSQL server.

        This closes the connection and makes it unusable for further operations.
        """
        ...

    def server_version(self) -> PyroFuture[str]:
        """
        Return the PostgreSQL server version.
        """
        ...
