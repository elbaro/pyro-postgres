# Query

There are two sets of query API: Simple Query and Extended Query.

## Simple Query

Simple query is simple and does not support passing parameters.

```py
class Conn:
  def query(self, sql: str, *, as_dict: bool = False) -> list[tuple] | list[dict]: ...
  def query_first(self, sql: str, *, as_dict: bool = False) -> tuple | dict | None: ...
  def query_drop(self, sql: str) -> None: ...
```

`Conn.query_first` and `Conn.query_drop` are just convenience methods for `Conn.query`.

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
  def prepare_batch(self, sqls: Sequence[str]) -> list[PreparedStatement]: ...
  def exec(self, stmt: Statement, params, *, as_dict: bool = False) -> list[tuple] | list[dict]: ...
  def exec_first(self, stmt: Statement, params, *, as_dict: bool = False) -> tuple | dict | None: ...
  def exec_drop(self, stmt: Statement, params) -> None: ...
  def exec_batch(self, stmt: Statement, params_list) -> None: ...
  def exec_iter(self, stmt: Statement, params) -> ?: ...
  def exec_portal(self, stmt: Statement, params) -> ?: ...
```

### Example: basic

```py
rows = conn.exec("SELECT ...", ())
row = conn.exec_first("SELECT ... WHERE id = $1", (300,))
conn.exec_drop("INSERT ...", (20, "Alice"))
```

### Example: executing many homogeneous queries

```py
conn.exec_batch("INSERT ...", [
  (20, "Alice")
  (21, "Bob")
  ...
])
```

### Example: fetching many rows larger than RAM

```py
conn.exec_iter(lambda portal: ...)
```

### Example: interleaving two row streams

```py
conn.exec_portal(...)
```
