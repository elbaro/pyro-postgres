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

The recommended way to use transactions is with a context manager.
On successful exit, the transaction commits. On exception, it rolls back.

```py
with conn.tx():
    conn.query_drop("INSERT INTO users (name) VALUES ('Alice')")
    conn.query_drop("INSERT INTO users (name) VALUES ('Bob')")
# auto-committed here
```

```py
try:
    with conn.tx():
        conn.query_drop("INSERT INTO users (name) VALUES ('Alice')")
        raise ValueError("oops")
except ValueError:
    pass
# auto-rolled back, no data inserted
```

## Explicit Commit / Rollback

You can also call `commit()` or `rollback()` explicitly inside the context manager.
After the call, the transaction object cannot be used anymore.

```py
with conn.tx() as tx:
    conn.query_drop("INSERT ...")
    if some_condition:
        tx.commit()
    else:
        tx.rollback()
```

## Isolation Level

```py
from pyro_postgres import IsolationLevel

with conn.tx(isolation_level=IsolationLevel.Serializable):
    ...
```

| Level             | Description                                             |
| ----------------- | ------------------------------------------------------- |
| `ReadUncommitted` | Allows dirty reads (PostgreSQL treats as ReadCommitted) |
| `ReadCommitted`   | Default. Only sees committed data                       |
| `RepeatableRead`  | Snapshot at transaction start                           |
| `Serializable`    | Full serializability                                    |

You can also create isolation levels from strings:

```py
level = IsolationLevel("READ COMMITTED")
level = IsolationLevel("repeatable_read")
level = IsolationLevel("sErIaLiZaBle")

assert level.as_str() == "SERIALIZABLE"
```

## Read-Only Transactions

Set `readonly=True` for read-only transactions. This can improve performance and is required for read replicas.

```py
with conn.tx(readonly=True):
    rows = conn.query("SELECT * FROM users")
```

## Async

For async connections, use `async with` and `await`:

```py
async with conn.tx() as tx:
    await conn.query_drop("INSERT ...")

# explicit commit/rollback
async with conn.tx() as tx:
    await conn.query_drop("INSERT ...")
    await tx.commit()
```
