import asyncio

import asyncpg
import psycopg
import pyro_postgres

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

DATA = [
    (
        f"user_{i}",
        20 + (i % 5),
        f"user{i}@example.com",
        float(i % 10),
        f"Description for user {i}",
    )
    for i in range(10000)
]


# --- Connection Setup Helpers ------------------------------------------------


async def create_pyro_async_conn():
    url = "postgres://test:1234@localhost:5432/test"
    return await pyro_postgres.AsyncConn.new(url)


async def create_asyncpg_conn():
    return await asyncpg.connect(
        host="localhost",
        port=5432,
        user="test",
        password="1234",
        database="test",
    )


async def create_psycopg_async_conn():
    return await psycopg.AsyncConnection.connect(
        host="localhost",
        port=5432,
        user="test",
        password="1234",
        dbname="test",
        autocommit=True,
    )


def create_psycopg_sync_conn():
    return psycopg.connect(
        host="localhost",
        port=5432,
        user="test",
        password="1234",
        dbname="test",
        autocommit=True,
    )


# --- Insert ------------------------------------------------------------------


async def insert_pyro_async(conn, n):
    for i in range(n):
        await conn.exec_drop(
            "INSERT INTO benchmark_test (name, age, email, score, description) VALUES ($1, $2, $3, $4, $5)",
            DATA[i % 10000],
        )


async def insert_pyro_async_batch(conn, n):
    """Insert using exec_batch with batches of up to 1000 rows"""
    batch_size = 1000
    for batch_start in range(0, n, batch_size):
        batch_end = min(batch_start + batch_size, n)
        batch_data = [DATA[i % 10000] for i in range(batch_start, batch_end)]
        await conn.exec_batch(
            "INSERT INTO benchmark_test (name, age, email, score, description) VALUES ($1, $2, $3, $4, $5)",
            batch_data,
        )


def insert_pyro_sync(conn, n):
    for i in range(n):
        conn.exec_drop(
            "INSERT INTO benchmark_test (name, age, email, score, description) VALUES ($1, $2, $3, $4, $5)",
            DATA[i % 10000],
        )


def insert_pyro_sync_batch(conn, n):
    """Insert using exec_batch with batches of up to 1000 rows"""
    batch_size = 1000
    for batch_start in range(0, n, batch_size):
        batch_end = min(batch_start + batch_size, n)
        batch_data = [DATA[i % 10000] for i in range(batch_start, batch_end)]
        conn.exec_batch(
            "INSERT INTO benchmark_test (name, age, email, score, description) VALUES ($1, $2, $3, $4, $5)",
            batch_data,
        )


async def insert_asyncpg(conn, n):
    for i in range(n):
        await conn.execute(
            "INSERT INTO benchmark_test (name, age, email, score, description) VALUES ($1, $2, $3, $4, $5)",
            *DATA[i % 10000],
        )


async def insert_asyncpg_batch(conn, n):
    """Insert using executemany with batches of up to 1000 rows"""
    batch_size = 1000
    for batch_start in range(0, n, batch_size):
        batch_end = min(batch_start + batch_size, n)
        batch_data = [DATA[i % 10000] for i in range(batch_start, batch_end)]
        await conn.executemany(
            "INSERT INTO benchmark_test (name, age, email, score, description) VALUES ($1, $2, $3, $4, $5)",
            batch_data,
        )


async def insert_psycopg_async(conn, n):
    async with conn.cursor() as cursor:
        for i in range(n):
            await cursor.execute(
                "INSERT INTO benchmark_test (name, age, email, score, description) VALUES (%s, %s, %s, %s, %s)",
                DATA[i % 10000],
            )


def insert_psycopg_sync(conn, n):
    with conn.cursor() as cursor:
        for i in range(n):
            cursor.execute(
                "INSERT INTO benchmark_test (name, age, email, score, description) VALUES (%s, %s, %s, %s, %s)",
                DATA[i % 10000],
            )


def insert_psycopg_sync_batch(conn, n):
    """Insert using executemany with batches of up to 1000 rows"""
    with conn.cursor() as cursor:
        batch_size = 1000
        for batch_start in range(0, n, batch_size):
            batch_end = min(batch_start + batch_size, n)
            batch_data = [DATA[i % 10000] for i in range(batch_start, batch_end)]
            cursor.executemany(
                "INSERT INTO benchmark_test (name, age, email, score, description) VALUES (%s, %s, %s, %s, %s)",
                batch_data,
            )


# --- Select ------------------------------------------------------------------


async def select_pyro_async(conn):
    await conn.exec("SELECT * FROM benchmark_test")


def select_pyro_sync(conn):
    conn.exec("SELECT * FROM benchmark_test")


async def select_asyncpg(conn):
    await conn.fetch("SELECT * FROM benchmark_test")


async def select_psycopg_async(conn):
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT * FROM benchmark_test")
        await cursor.fetchall()


def select_psycopg_sync(conn):
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM benchmark_test")
        cursor.fetchall()
