# Connection

A connection can be made with an URL string or `Opts`.

An URL can start with

- `pg://`
- `postgres://`
- `postgresql://`

and additional parameters can be

#### Example: basic

```py
from pyro_postgres.sync import Conn, Opts

# url
conn1 = Conn("pg://test:1234@localhost:5432/test_db")

# Opts from url
conn2 = Conn(Opts::from_url("pg://test:1234@localhost:5432/test_db"))

# Opts API
conn3 = Conn(
    Opts()
        .username("test")
        .password("1234")
        .hostname("localhost")
        .port("5432"")
        .db("test_db")
)

# url + Opts API
conn4 = Conn(Opts("pg://test:1234@localhost:5432").db("test_db"))
```

#### Example: async

```py
from pyro_postgres.async import Conn, Opts

conn = await Conn.new("pg://test:1234@localhost:5432/test_db")
```

#### Example: unix socket

```py
from pyro_postgres.sync import Conn

# hostname 'localhost' is ignored
conn = Conn("pg://test:1234@localhost:5432/test?socket=/tmp/pg/.s.PGSQL.5432")
```

## Advanced: Upgrade to Unix Socket

By default, `Opts.prefer_unix_socket` is `True`.

If `prefer_unix_socket` is True and the tcp peer IP is local, the library sends `SHOW unix_socket_directories` to get the unix socket path, and then tries to reconnect to `{unix_socket_directories}/.s.PGSQL.{opts.port}`.
This upgrade happens transparently in connection time. If succeds, the constructor returns the new unix socket connection. If fails, returns the original TCP connection.

```
conn = Conn("pg://test:1234@localhost")  # `socket` parameter is not provided, but `conn` can be a TCP connection or Unix socket connection.
```

This feature is useful if your local socket address is located at a dynamic location `/run/user/1000/devenv-8c67ae1/postgres/.s.PGSQL.5432`.
For production, disable this flag and use the TCP connection or manually specify the socket address.
