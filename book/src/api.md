# API Reference

## Module Structure

```
pyro_postgres           # Opts, IsolationLevel, PreparedStatement, Ticket, init()
├── sync                # Conn, Transaction, Pipeline, Portal
├── async_              # Conn, Transaction, Pipeline, Portal (async)
└── error               # Exception classes
```

## Type Stubs

- [`pyro_postgres`](https://github.com/elbaro/pyro-postgres/blob/main/pyro_postgres/__init__.pyi) - Core types and initialization
- [`pyro_postgres.sync`](https://github.com/elbaro/pyro-postgres/blob/main/pyro_postgres/sync.pyi) - Synchronous API
- [`pyro_postgres.async_`](https://github.com/elbaro/pyro-postgres/blob/main/pyro_postgres/async_.pyi) - Asynchronous API
- [`pyro_postgres.error`](https://github.com/elbaro/pyro-postgres/blob/main/pyro_postgres/error.pyi) - Exceptions
