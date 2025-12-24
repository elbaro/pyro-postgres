# Pipelining

Pipelining is an advanced feature to reduce the client-side waiting and the number of network round trips.

Use `conn.exec_batch` for executing many homogeneous queries,
and `conn.pipeline` for executing many heterogenous queries.

```py

with conn.pipeline() as p:
    ticket1 = p.execute("SELECT ...")
    ticket2 = p.execute("INSERT ...")
    ticket3 = p.execute("INSERT ...")

    p.sync()

    rows = p.claim(ticket1)
    p.claim_drop(ticket2)
    p.claim_drop(ticket3)

    ticket4 = p.execute("SELECT ...")
    ticket5 = p.execute("INSERT ...")
    ticket6 = p.execute("INSERT ...")

    p.sync()

    ...
```

It is recommended to prepare a set of statements before entering the pipeline.

```py
stmt1, stmt2, stmt3 = conn.prepare_batch([
      "SELECT ...",
      "INSERT ...",
      "INSERT ...",
  ])

with conn.pipeline() as p:
    t1 = p.execute(stmt1, params)
    t2 = p.execute(stmt2, params)
    t3 = p.execute(stmt3, params)

    p.sync()

    rows = p.claim(t1)
    p.claim_drop(t2)
    p.claim_drop(t3)   
```

On exit of pipeline context manager, any pending response is read and dropped.
