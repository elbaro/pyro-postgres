# Introduction

pyro-postgres is a high-performance PostgreSQL driver for Python, backed by Rust. It provides both synchronous and asynchronous APIs with a focus on speed and ergonomics.

```bash
pip install pyro-postgres
```

## Quick Start

```py
from pyro_postgres.sync import Conn

conn = Conn("pg://user:password@localhost/mydb")

# Simple query
rows = conn.query("SELECT id, name FROM users")

# Parameterized query
user = conn.exec_first("SELECT * FROM users WHERE id = $1", (42,))

# Transaction
with conn.tx():
    conn.exec_drop("INSERT INTO users (name) VALUES ($1)", ("Alice",))
    conn.exec_drop("INSERT INTO users (name) VALUES ($1)", ("Bob",))
```

## Features

- **High Performance**: Minimal allocations and copies
- **Sync and Async**: The library provides both sync and async APIs
- **Pipelining**: Batch multiple queries in a single round trip
- **Streaming**: Process large result sets without loading everything into memory

## Comparisons

pyro-postgres is built on [zero-postgres](https://crates.io/crates/zero-postgres), providing significant performance benefits over pure Python implementations.

| Query            | pyro-postgres | psycopg | Speedup     |
| ---------------- | ------------- | ------- | ----------- |
| SELECT 1 row     | 39 us         | 104 us  | 2.7x faster |
| SELECT 10 rows   | 46 us         | 113 us  | 2.4x faster |
| SELECT 100 rows  | 112 us        | 178 us  | 1.6x faster |
| SELECT 1000 rows | 570 us        | 810 us  | 1.4x faster |
| INSERT           | 6 us          | 20 us   | 3.0x faster |

## Limitations

- **Python 3.10+**: Requires Python 3.10 or later
- **PostgreSQL 18**: Supports PostgresSQL 18 or later
- **Limited Type Coverage**: Not all PostgreSQL types are supported yet
- **Limited Performance Gain in Async API**: Due to the overhead of Python 3.14 Free-threading, the async module pays a significant cost switching between Python thread and Rust thread. Upon receiving the network packet, the Rust thread needs to attach to GIL to construct objects like PyList/PyInt/PyString and detach soon after. It was observed in one of the benchmarks that more than 30% time is spent on the destruction of `GILState` c struct. To avoid this, we accumulate the received row data in the Rust buffer and convert to Python at once. The async performance has a potential to be much faster than now with the advance of single-threaded overhead of Python Free-threading.
