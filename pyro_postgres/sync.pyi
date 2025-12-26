"""Synchronous PostgreSQL driver components."""

from types import TracebackType
from typing import Any, Callable, Literal, Self, Sequence, TypeVar, overload

from pyro_postgres import IsolationLevel, Opts, Params, PreparedStatement, Statement

T = TypeVar("T")

class UnnamedPortal:
    """
    An unnamed portal for iterative row fetching.

    Created by `Conn.exec_iter()` and passed to the callback function.
    Use `fetch()` to retrieve rows in batches.
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

    Use `execute_collect()` to fetch rows, `is_complete()` to check if all
    rows have been fetched, and `close()` to release resources.
    """

    @overload
    def execute_collect(
        self, conn: "Conn", max_rows: int, *, as_dict: Literal[False] = False
    ) -> list[tuple[Any, ...]]: ...
    @overload
    def execute_collect(
        self, conn: "Conn", max_rows: int, *, as_dict: Literal[True]
    ) -> list[dict[str, Any]]: ...
    def execute_collect(
        self, conn: "Conn", max_rows: int, *, as_dict: bool = False
    ) -> list[tuple[Any, ...]] | list[dict[str, Any]]:
        """
        Execute the portal and collect up to `max_rows` rows.

        Args:
            conn: The connection to use for fetching.
            max_rows: Maximum number of rows to fetch. Use 0 to fetch all remaining rows.
            as_dict: If True, return rows as dictionaries. If False (default), return rows as tuples.

        Returns:
            List of tuples (default) or dictionaries.
        """
        ...

    def is_complete(self) -> bool:
        """
        Check if all rows have been fetched from this portal.

        Returns:
            True if the last `execute_collect()` call fetched all remaining rows.
        """
        ...

    def close(self, conn: "Conn") -> None:
        """
        Close the portal, releasing server resources.

        After closing, the portal cannot be used for further fetching.
        """
        ...

class Transaction:
    """
    Represents a synchronous PostgreSQL transaction.

    Use as a context manager to automatically commit or rollback.
    Create named portals with `exec_portal()` for iterative row fetching.
    """

    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        type_: type[BaseException] | None,
        value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...
    def commit(self) -> None:
        """Commit the transaction."""
        ...

    def rollback(self) -> None:
        """Rollback the transaction."""
        ...

    def exec_portal(self, query: str, params: Params = None) -> NamedPortal:
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
            A NamedPortal that can be used to fetch rows.

        Example:
            ```python
            with conn.tx() as tx:
                portal1 = tx.exec_portal("SELECT * FROM table1")
                portal2 = tx.exec_portal("SELECT * FROM table2")

                while True:
                    rows1 = portal1.execute_collect(conn, 100)
                    rows2 = portal2.execute_collect(conn, 100)
                    process(rows1, rows2)
                    if portal1.is_complete() and portal2.is_complete():
                        break

                portal1.close(conn)
                portal2.close(conn)
            ```
        """
        ...

class Conn:
    """
    Synchronous PostgreSQL connection.
    """

    def __init__(self, url_or_opts: str | Opts) -> None:
        """
        Create a new synchronous connection.

        Args:
            url_or_opts: PostgreSQL connection URL (e.g., 'postgres://user:password@host:port/database') or Opts object.
        """
        ...

    def tx(
        self,
        isolation_level: IsolationLevel | None = None,
        readonly: bool | None = None,
    ) -> Transaction: ...
    def id(self) -> int: ...
    def ping(self) -> None:
        """Ping the server to check connection."""
        ...

    @overload
    def query(
        self, query: str, *, as_dict: Literal[False] = False
    ) -> list[tuple[Any, ...]]: ...
    @overload
    def query(self, query: str, *, as_dict: Literal[True]) -> list[dict[str, Any]]: ...
    def query(
        self, query: str, *, as_dict: bool = False
    ) -> list[tuple[Any, ...]] | list[dict[str, Any]]:
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
    ) -> tuple[Any, ...] | None: ...
    @overload
    def query_first(
        self, query: str, *, as_dict: Literal[True]
    ) -> dict[str, Any] | None: ...
    def query_first(
        self, query: str, *, as_dict: bool = False
    ) -> tuple[Any, ...] | dict[str, Any] | None:
        """
        Execute a query using text protocol and return the first row.

        Args:
            query: SQL query string.
            as_dict: If True, return row as dictionary. If False (default), return row as tuple.

        Returns:
            First row as tuple (default) or dictionary, or None if no results.
        """
        ...

    def query_drop(self, query: str) -> int:
        """
        Execute a query using text protocol and discard the results.

        Args:
            query: SQL query string.

        Returns:
            Number of rows affected by the query.
        """
        ...

    def prepare(self, query: str) -> PreparedStatement:
        """
        Prepare a statement for later execution.

        Args:
            query: SQL query string with $1, $2, ... placeholders.

        Returns:
            A PreparedStatement that can be reused with exec methods.

        Example:
            ```python
            stmt = conn.prepare("SELECT * FROM users WHERE id = $1")
            row1 = conn.exec_first(stmt, (1,))
            row2 = conn.exec_first(stmt, (2,))
            ```
        """
        ...

    def prepare_batch(self, sqls: Sequence[str]) -> list[PreparedStatement]:
        """
        Prepare multiple statements in a single round trip.

        Args:
            sqls: List of SQL query strings.

        Returns:
            A list of PreparedStatements.

        Example:
            ```python
            stmts = conn.prepare_batch([
                "SELECT * FROM users WHERE id = $1",
                "INSERT INTO logs (msg) VALUES ($1)",
            ])
            ```
        """
        ...

    @overload
    def exec(
        self, stmt: Statement, params: Params = None, *, as_dict: Literal[False] = False
    ) -> list[tuple[Any, ...]]: ...
    @overload
    def exec(
        self, stmt: Statement, params: Params = None, *, as_dict: Literal[True]
    ) -> list[dict[str, Any]]: ...
    def exec(
        self, stmt: Statement, params: Params = None, *, as_dict: bool = False
    ) -> list[tuple[Any, ...]] | list[dict[str, Any]]:
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
        self, stmt: Statement, params: Params = None, *, as_dict: Literal[False] = False
    ) -> tuple[Any, ...] | None: ...
    @overload
    def exec_first(
        self, stmt: Statement, params: Params = None, *, as_dict: Literal[True]
    ) -> dict[str, Any] | None: ...
    def exec_first(
        self, stmt: Statement, params: Params = None, *, as_dict: bool = False
    ) -> tuple[Any, ...] | dict[str, Any] | None:
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

    def exec_drop(self, stmt: Statement, params: Params = None) -> int:
        """
        Execute a statement using extended protocol and discard the results.

        Args:
            stmt: SQL query string or PreparedStatement.
            params: Query parameters.

        Returns:
            Number of rows affected by the query.
        """
        ...

    def exec_batch(self, stmt: Statement, params_list: Sequence[Params] = []) -> None:
        """
        Execute a statement multiple times with different parameters.

        Args:
            stmt: SQL query string or PreparedStatement.
            params_list: List of parameter sets.
        """
        ...

    def exec_iter(
        self, stmt: Statement, params: Params, callback: Callable[[UnnamedPortal], T]
    ) -> T:
        """
        Execute a statement and process rows iteratively via a callback.

        The callback receives an UnnamedPortal that can fetch rows in batches.
        Useful for processing large result sets that don't fit in memory.

        Args:
            stmt: SQL query string or PreparedStatement.
            params: Query parameters.
            callback: A function that receives an UnnamedPortal and returns a result.

        Returns:
            The value returned by the callback.

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

            result = conn.exec_iter("SELECT value FROM large_table", (), process)
            ```
        """
        ...

    def close(self) -> None:
        """
        Disconnect from the PostgreSQL server.

        This closes the connection and makes it unusable for further operations.
        """
        ...

    def server_version(self) -> str: ...
