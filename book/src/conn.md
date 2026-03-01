# Connection

A connection can be made with an URL string or `Opts`.

An URL can start with

- `pg://`
- `postgres://`
- `postgresql://`

The URL `pg://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}?sslmode=require` is equivalent to

```py
Opts()
  .user('USER')
  .password('PASSWORD')
  .host('HOST')
  .port(5432)
  .db('DATABASE')
  .ssl_mode('require')
```

For the full list of options, see the [type stub](https://github.com/elbaro/pyro-postgres/blob/main/pyro_postgres/__init__.pyi).

### Example: basic

```py
from pyro_postgres.sync import Conn
from pyro_postgres import Opts

# url
conn1 = Conn("pg://test:1234@localhost:5432/test_db?sslmode=require")

# url + Opts
conn2 = Conn(Opts("pg://test@localhost").ssl_mode("require"))

# Opts
conn3 = Conn(
    Opts()
        .socket("/tmp/pg/.s.PGSQL.5432")
        .db("test_db")
)
```

### Example: async

```py
from pyro_postgres.async_ import Conn, Opts

conn = await Conn.new("pg://test:1234@localhost:5432/test_db")
```

### Example: unix socket

```py
from pyro_postgres.sync import Conn

# hostname 'localhost' is ignored
conn = Conn("pg://localhost/test?socket=/tmp/pg/.s.PGSQL.5432")
```

## Advanced: Upgrade to Unix Socket

By default, `Opts.upgrade_to_unix_socket` is `True`.

If `upgrade_to_unix_socket` is True and the tcp peer IP is local, the library sends `SHOW unix_socket_directories` to get the unix socket path, and then tries to reconnect to `{unix_socket_directories}/.s.PGSQL.{opts.port}`.
This upgrade happens transparently in connection time. If succeeds, the constructor returns the new unix socket connection. If fails, returns the original TCP connection.

```
conn = Conn("pg://test:1234@localhost")  # `socket` parameter is not provided, but `conn` can be a TCP connection or Unix socket connection.
```

This feature is useful if your local socket address is located at a dynamic location like `/run/user/1000/devenv-8c67ae1/postgres/.s.PGSQL.5432`.
For production, disable this flag and use the TCP connection or manually specify the socket address.
