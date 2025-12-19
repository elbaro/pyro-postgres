use criterion::{Criterion, criterion_group, criterion_main};
use pyo3::{ffi::c_str, prelude::*};

fn setup_db(py: Python) {
    py.run(
        c"pyro_setup_conn = pyro_postgres.SyncConn('postgres://test:1234@localhost:5432/test')",
        None,
        None,
    )
    .unwrap();
    py.run(
        c"pyro_setup_conn.query_drop('DROP TABLE IF EXISTS benchmark_test')",
        None,
        None,
    )
    .unwrap();
    py.run(
        c"pyro_setup_conn.query_drop('''CREATE TABLE benchmark_test (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            age INT,
            email VARCHAR(100),
            score FLOAT,
            description VARCHAR(100)
        )''')",
        None,
        None,
    )
    .unwrap();
    py.run(c"pyro_setup_conn.close()", None, None).unwrap();
}

fn clear_table(py: Python) {
    py.run(
        c"pyro_clear_conn = pyro_postgres.SyncConn('postgres://test:1234@localhost:5432/test')",
        None,
        None,
    )
    .unwrap();
    py.run(
        c"pyro_clear_conn.query_drop('TRUNCATE TABLE benchmark_test RESTART IDENTITY')",
        None,
        None,
    )
    .unwrap();
    py.run(c"pyro_clear_conn.close()", None, None).unwrap();
}

fn populate_table(py: Python, n: usize) {
    py.run(
        c"pyro_pop_conn = pyro_postgres.SyncConn('postgres://test:1234@localhost:5432/test')",
        None,
        None,
    )
    .unwrap();
    py.run(
        c"pyro_pop_conn.query_drop('TRUNCATE TABLE benchmark_test RESTART IDENTITY')",
        None,
        None,
    )
    .unwrap();

    let insert_code = format!(
        r#"
for i in range({}):
    pyro_pop_conn.exec_drop(
        "INSERT INTO benchmark_test (name, age, email, score, description) VALUES ($1, $2, $3, $4, $5)",
        (f"user_{{i}}", 20 + (i % 50), f"user{{i}}@example.com", float(i % 100), f"User description {{i}}")
    )
"#,
        n
    );
    let c_insert_code = std::ffi::CString::new(insert_code).unwrap();
    py.run(c_insert_code.as_c_str(), None, None).unwrap();
    py.run(c"pyro_pop_conn.close()", None, None).unwrap();
}

pub fn bench(c: &mut Criterion) {
    Python::attach(|py| {
        Python::run(py, c_str!(include_str!("./bench.py")), None, None).unwrap();
        setup_db(py);
    });

    // SELECT benchmarks
    for select_size in [1, 10, 100, 1000] {
        let mut group = c.benchmark_group(format!("SELECT_{}", select_size));
        Python::attach(|py| populate_table(py, select_size));

        // Async benchmarks: pyro vs asyncpg vs psycopg
        for (name, setup, statement) in [
            (
                "pyro (async)",
                cr"pyro_async_conn = loop.run_until_complete(create_pyro_async_conn())",
                c"loop.run_until_complete(select_pyro_async(pyro_async_conn))",
            ),
            (
                "asyncpg (async)",
                cr"asyncpg_conn = loop.run_until_complete(create_asyncpg_conn())",
                c"loop.run_until_complete(select_asyncpg(asyncpg_conn))",
            ),
            (
                "psycopg (async)",
                cr"psycopg_async_conn = loop.run_until_complete(create_psycopg_async_conn())",
                c"loop.run_until_complete(select_psycopg_async(psycopg_async_conn))",
            ),
        ] {
            group.bench_function(name, |b| {
                Python::attach(|py| {
                    Python::run(py, setup, None, None).unwrap();
                    b.iter(|| py.run(&statement, None, None).unwrap());
                });
            });
        }

        // Sync benchmarks: pyro vs psycopg
        for (name, setup, statement) in [
            (
                "pyro (sync)",
                cr"pyro_sync_conn = pyro_postgres.SyncConn('postgres://test:1234@localhost:5432/test')",
                c"select_pyro_sync(pyro_sync_conn)",
            ),
            (
                "psycopg (sync)",
                cr"psycopg_sync_conn = create_psycopg_sync_conn()",
                c"select_psycopg_sync(psycopg_sync_conn)",
            ),
        ] {
            group.bench_function(name, |b| {
                Python::attach(|py| {
                    Python::run(py, setup, None, None).unwrap();
                    b.iter(|| py.run(&statement, None, None).unwrap());
                });
            });
        }
    }

    // INSERT benchmarks
    {
        let mut group = c.benchmark_group("INSERT");

        // Async benchmarks: pyro vs asyncpg vs psycopg
        for (name, setup, stmt_template) in [
            (
                "pyro (async)",
                cr"pyro_async_conn = loop.run_until_complete(create_pyro_async_conn())",
                "loop.run_until_complete(insert_pyro_async(pyro_async_conn, {}))",
            ),
            (
                "pyro (async, batch)",
                cr"pyro_async_batch_conn = loop.run_until_complete(create_pyro_async_conn())",
                "loop.run_until_complete(insert_pyro_async_batch(pyro_async_batch_conn, {}))",
            ),
            (
                "asyncpg (async)",
                cr"asyncpg_conn = loop.run_until_complete(create_asyncpg_conn())",
                "loop.run_until_complete(insert_asyncpg(asyncpg_conn, {}))",
            ),
            (
                "asyncpg (async, batch)",
                cr"asyncpg_batch_conn = loop.run_until_complete(create_asyncpg_conn())",
                "loop.run_until_complete(insert_asyncpg_batch(asyncpg_batch_conn, {}))",
            ),
            (
                "psycopg (async)",
                cr"psycopg_async_conn = loop.run_until_complete(create_psycopg_async_conn())",
                "loop.run_until_complete(insert_psycopg_async(psycopg_async_conn, {}))",
            ),
        ] {
            group.bench_function(name, |b| {
                Python::attach(|py| {
                    Python::run(py, setup, None, None).unwrap();
                    b.iter_custom(|iters| {
                        let mut sum = std::time::Duration::ZERO;
                        for g in 0..((iters - 1) / 10000 + 1) {
                            clear_table(py);
                            let start_idx = g * 10000;
                            let end = iters.min(start_idx + 10000);

                            let statement = stmt_template.replace("{}", &(end - start_idx).to_string());
                            let c_statement = std::ffi::CString::new(statement).unwrap();

                            let start = std::time::Instant::now();
                            py.eval(c_statement.as_c_str(), None, None).unwrap();
                            sum += start.elapsed();

                            // Check no background tasks remain
                            py.run(c"assert len(__import__('asyncio').all_tasks(loop)) == 0", None, None).unwrap();
                        }
                        sum
                    });
                });
            });
        }

        // Sync benchmarks: pyro vs psycopg
        for (name, setup, stmt_template) in [
            (
                "pyro (sync)",
                cr"pyro_sync_conn = pyro_postgres.SyncConn('postgres://test:1234@localhost:5432/test')",
                "insert_pyro_sync(pyro_sync_conn, {})",
            ),
            (
                "pyro (sync, batch)",
                cr"pyro_sync_batch_conn = pyro_postgres.SyncConn('postgres://test:1234@localhost:5432/test')",
                "insert_pyro_sync_batch(pyro_sync_batch_conn, {})",
            ),
            (
                "psycopg (sync)",
                cr"psycopg_sync_conn = create_psycopg_sync_conn()",
                "insert_psycopg_sync(psycopg_sync_conn, {})",
            ),
            (
                "psycopg (sync, batch)",
                cr"psycopg_sync_batch_conn = create_psycopg_sync_conn()",
                "insert_psycopg_sync_batch(psycopg_sync_batch_conn, {})",
            ),
        ] {
            group.bench_function(name, |b| {
                Python::attach(|py| {
                    Python::run(py, setup, None, None).unwrap();
                    b.iter_custom(|iters| {
                        let mut sum = std::time::Duration::ZERO;
                        for g in 0..((iters - 1) / 10000 + 1) {
                            clear_table(py);
                            let start_idx = g * 10000;
                            let end = iters.min(start_idx + 10000);

                            let statement = stmt_template.replace("{}", &(end - start_idx).to_string());
                            let c_statement = std::ffi::CString::new(statement).unwrap();

                            let start = std::time::Instant::now();
                            py.eval(c_statement.as_c_str(), None, None).unwrap();
                            sum += start.elapsed();
                        }
                        sum
                    });
                });
            });
        }
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
