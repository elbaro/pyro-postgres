# Query

There are two sets of query API: Simple Query and Extended Query.

## Simple Query

Simple query is simple and does not support passing parameters.

```py
class Conn:
  def query(self, sql: str, *, as_dict: bool = False) -> list[tuple] | list[dict]: ...
  def query_first(self, sql: str, *, as_dict: bool = False) -> tuple | dict | None: ...
  def query_drop(self, sql: str) -> int: ...
```

- `query`: executes `sql` and returns the list of rows.
- `query_first`: executes `sql` and returns the first row (or None).
- `query_drop`: executes `sql` and drops the result. Returns the number of affected rows. Useful for `INSERT`/`UPDATE`.

### Example

```py
rows: list = conn.query("SELECT field1, field2 FROM table")
row = conn.query_first("SELECT ..")  # store only the first row and throw away others.
conn.query_drop("INSERT ..")  # use `query_drop` not interested in result
```

## Extended Query

```py
Statement = PreparedStatement | str

class Conn:
  def prepare(self, sql: str) -> PreparedStatement: ...
  def prepare_batch(self, sqls: list[str]) -> list[PreparedStatement]: ...
  def exec(self, stmt: Statement, params = (), *, as_dict: bool = False) -> list[tuple] | list[dict]: ...
  def exec_first(self, stmt: Statement, params = (), *, as_dict: bool = False) -> tuple | dict | None: ...
  def exec_drop(self, stmt: Statement, params = ()) -> int: ...
  def exec_batch(self, stmt: Statement, params_list = []) -> None: ...
  def exec_iter(self, stmt: Statement, params, callback: Callable[[UnnamedPortal], T]) -> T: ...

class Transaction:
  def exec_portal(self, query: str, params = ()) -> NamedPortal: ...
```

- `exec`: execute a statement and returns the list of rows
- `exec_first`: execute a statement and returns the first row (or None)
- `exec_drop`: execute a statement and returns the number of affected rows. useful for `INSERT` or `UPDATE`
- `exec_batch`: execute a statement many times with parameters in a single round trip. useful for bulk `INSERT` or `UPDATE`
- `exec_iter`: execute a statement and process rows on demand via a callback. useful to read rows larger than memory.
- `exec_portal`: create and returns a [portal](https://www.postgresql.org/docs/current/protocol-overview.html) which can read rows on demand. use `exec_iter` for a single row stream, and `exec_portal` to interleave multiple row streams.

### Example: basic

```py
# One-off query
row = conn.exec_first("SELECT field1 WHERE id = $1", (300,))

# Repeat query - parse once, execute many times
stmt = conn.prepare("SELECT field1 WHERE id = $1")
for i in [100, 200, 300]:
    conn.exec_first(stmt, (i,))
```

### Example: executing many homogeneous queries

```py
conn.exec_batch("INSERT INTO users (age, name) VALUES ($1, $2)", [
    (20, "Alice"),
    (21, "Bob"),
    (22, "Charlie"),
])
```

### Example: fetching many rows larger than RAM

```py
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

### Example: interleaving two row streams

```py
with conn.tx() as tx:
    # Create portals within a transaction
    portal1 = tx.exec_portal("SELECT * FROM table1")
    portal2 = tx.exec_portal("SELECT * FROM table2")

    # Interleave execution
    while True:
        rows1 = portal1.exec_collect(100)
        rows2 = portal2.exec_collect(100)
        process(rows1, rows2)
        if portal1.is_complete() and portal2.is_complete():
            break

    # Cleanup
    portal1.close()
    portal2.close()
```
