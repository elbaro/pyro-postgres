# Transaction

Transactions ensure a group of operations either all succeed (commit) or all fail (rollback).

```py
class Conn:
  def tx(
    self,
    isolation_level: IsolationLevel | None = None,
    readonly: bool | None = None,
  ) -> Transaction: ...

class Transaction:
  def commit(self) -> None: ...
  def rollback(self) -> None: ...
```

## Context Manager

The recommended way to use transactions is with a context manager.
On successful exit, the transaction commits. On exception, it rolls back.

```py
with conn.tx() as txn:
    conn.query_drop("INSERT INTO users (name) VALUES ('Alice')")
    conn.query_drop("INSERT INTO users (name) VALUES ('Bob')")
# auto-committed here
```

```py
try:
    with conn.tx() as txn:
        conn.query_drop("INSERT INTO users (name) VALUES ('Alice')")
        raise ValueError("oops")
except ValueError:
    pass
# auto-rolled back, no data inserted
```

## Explicit Commit / Rollback

You can also call `commit()` or `rollback()` explicitly inside the context manager.

```py
with conn.tx() as txn:
    conn.query_drop("INSERT ...")
    if some_condition:
        txn.commit()
    else:
        txn.rollback()
```

For sync connections, you can also use the explicit `begin()` / `commit()` / `rollback()` pattern without a context manager:

```py
txn = conn.tx()
txn.begin()
conn.query_drop("INSERT ...")
txn.commit()  # or txn.rollback()
```

## Isolation Level

PostgreSQL supports four isolation levels. Pass `isolation_level` to `tx()`.

```py
from pyro_postgres import IsolationLevel

with conn.tx(isolation_level=IsolationLevel.Serializable):
    ...
```

| Level | Description |
|-------|-------------|
| `ReadUncommitted` | Allows dirty reads (PostgreSQL treats as ReadCommitted) |
| `ReadCommitted` | Default. Only sees committed data |
| `RepeatableRead` | Snapshot at transaction start |
| `Serializable` | Full serializability |

You can also create isolation levels from strings:

```py
level = IsolationLevel("read committed")
level = IsolationLevel("repeatable_read")
level = IsolationLevel("serializable")
```

Or use static factory methods:

```py
IsolationLevel.read_uncommitted()
IsolationLevel.read_committed()
IsolationLevel.repeatable_read()
IsolationLevel.serializable()
```

## Read-Only Transactions

Set `readonly=True` for read-only transactions. This can improve performance and is required for read replicas.

```py
with conn.tx(readonly=True):
    rows = conn.query("SELECT * FROM users")
```

You can combine isolation level and readonly:

```py
with conn.tx(
    isolation_level=IsolationLevel.Serializable,
    readonly=True
):
    ...
```

## Async

For async connections, use `async with` and `await`:

```py
async with conn.tx() as txn:
    await conn.query_drop("INSERT ...")

# explicit commit/rollback
async with conn.tx() as txn:
    await conn.query_drop("INSERT ...")
    await txn.commit()
```
