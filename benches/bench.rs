use criterion::{Criterion, criterion_group, criterion_main};
use pyo3::{ffi::c_str, prelude::*};

pub fn bench(c: &mut Criterion) {
    Python::attach(|py| {
        Python::run(py, c_str!(include_str!("./bench.py")), None, None).unwrap();
    });

    // SELECT benchmarks
    for select_size in [1, 10, 100, 1000] {
        let mut group = c.benchmark_group(format!("SELECT_{}", select_size));

        // Async benchmarks: pyro vs asyncpg vs psycopg
        for (name, setup_template, statement) in [
            (
                "pyro (async)",
                "conn = loop.run_until_complete(create_pyro_async_conn()); loop.run_until_complete(populate_table_pyro_async(conn, {}))",
                c"loop.run_until_complete(select_pyro_async(conn))",
            ),
            (
                "asyncpg (async)",
                "conn = loop.run_until_complete(create_asyncpg_conn()); loop.run_until_complete(populate_table_asyncpg(conn, {}))",
                c"loop.run_until_complete(select_asyncpg(conn))",
            ),
            (
                "psycopg (async)",
                "conn = loop.run_until_complete(create_psycopg_async_conn()); loop.run_until_complete(populate_table_psycopg_async(conn, {}))",
                c"loop.run_until_complete(select_psycopg_async(conn))",
            ),
        ] {
            group.bench_function(name, |b| {
                Python::attach(|py| {
                    let setup = setup_template.replace("{}", &select_size.to_string());
                    let c_setup = std::ffi::CString::new(setup).unwrap();
                    Python::run(py, c_setup.as_c_str(), None, None).unwrap();
                    b.iter(|| py.run(&statement, None, None).unwrap());
                });
            });
        }

        // Async pipeline/batch benchmarks
        for (name, setup_template, stmt_template) in [
            (
                "pyro (async, pipeline)",
                "conn = loop.run_until_complete(create_pyro_async_conn()); loop.run_until_complete(populate_table_pyro_async(conn, {}))",
                "loop.run_until_complete(select_pyro_async_pipeline(conn, {}))",
            ),
            (
                "pyro (async, batch)",
                "conn = loop.run_until_complete(create_pyro_async_conn()); loop.run_until_complete(populate_table_pyro_async(conn, {}))",
                "loop.run_until_complete(select_pyro_async_batch(conn, {}))",
            ),
            (
                "asyncpg (async, executemany)",
                "conn = loop.run_until_complete(create_asyncpg_conn()); loop.run_until_complete(populate_table_asyncpg(conn, {}))",
                "loop.run_until_complete(select_asyncpg_executemany(conn, {}))",
            ),
            (
                "psycopg (async, pipeline)",
                "conn = loop.run_until_complete(create_psycopg_async_conn()); loop.run_until_complete(populate_table_psycopg_async(conn, {}))",
                "loop.run_until_complete(select_psycopg_async_pipeline(conn, {}))",
            ),
        ] {
            group.bench_function(name, |b| {
                Python::attach(|py| {
                    let setup = setup_template.replace("{}", &select_size.to_string());
                    let c_setup = std::ffi::CString::new(setup).unwrap();
                    Python::run(py, c_setup.as_c_str(), None, None).unwrap();
                    let statement = stmt_template.replace("{}", &select_size.to_string());
                    let c_statement = std::ffi::CString::new(statement).unwrap();
                    b.iter(|| py.eval(c_statement.as_c_str(), None, None).unwrap());
                });
            });
        }

        // Sync benchmarks: pyro vs psycopg
        for (name, setup_template, statement) in [
            (
                "pyro (sync)",
                "conn = create_pyro_sync_conn(); populate_table_pyro_sync(conn, {})",
                c"select_pyro_sync(conn)",
            ),
            (
                "psycopg (sync)",
                "conn = create_psycopg_sync_conn(); populate_table_psycopg_sync(conn, {})",
                c"select_psycopg_sync(conn)",
            ),
        ] {
            group.bench_function(name, |b| {
                Python::attach(|py| {
                    let setup = setup_template.replace("{}", &select_size.to_string());
                    let c_setup = std::ffi::CString::new(setup).unwrap();
                    Python::run(py, c_setup.as_c_str(), None, None).unwrap();
                    b.iter(|| py.run(&statement, None, None).unwrap());
                });
            });
        }

        // Sync pipeline/batch benchmarks
        for (name, setup_template, stmt_template) in [
            (
                "pyro (sync, pipeline)",
                "conn = create_pyro_sync_conn(); populate_table_pyro_sync(conn, {})",
                "select_pyro_sync_pipeline(conn, {})",
            ),
            (
                "pyro (sync, batch)",
                "conn = create_pyro_sync_conn(); populate_table_pyro_sync(conn, {})",
                "select_pyro_sync_batch(conn, {})",
            ),
            (
                "psycopg (sync, pipeline)",
                "conn = create_psycopg_sync_conn(); populate_table_psycopg_sync(conn, {})",
                "select_psycopg_sync_pipeline(conn, {})",
            ),
        ] {
            group.bench_function(name, |b| {
                Python::attach(|py| {
                    let setup = setup_template.replace("{}", &select_size.to_string());
                    let c_setup = std::ffi::CString::new(setup).unwrap();
                    Python::run(py, c_setup.as_c_str(), None, None).unwrap();
                    let statement = stmt_template.replace("{}", &select_size.to_string());
                    let c_statement = std::ffi::CString::new(statement).unwrap();
                    b.iter(|| py.eval(c_statement.as_c_str(), None, None).unwrap());
                });
            });
        }
    }

    // INSERT benchmarks
    {
        let mut group = c.benchmark_group("INSERT");

        // Async benchmarks: pyro vs asyncpg vs psycopg
        for (name, setup, clear_stmt, stmt_template) in [
            (
                "pyro (async)",
                cr"conn = loop.run_until_complete(create_pyro_async_conn())",
                c"loop.run_until_complete(clear_table_pyro_async(conn))",
                "loop.run_until_complete(insert_pyro_async(conn, {}))",
            ),
            (
                "pyro (async, pipeline)",
                cr"conn = loop.run_until_complete(create_pyro_async_conn())",
                c"loop.run_until_complete(clear_table_pyro_async(conn))",
                "loop.run_until_complete(insert_pyro_async_pipeline(conn, {}))",
            ),
            (
                "pyro (async, batch)",
                cr"conn = loop.run_until_complete(create_pyro_async_conn())",
                c"loop.run_until_complete(clear_table_pyro_async(conn))",
                "loop.run_until_complete(insert_pyro_async_batch(conn, {}))",
            ),
            (
                "asyncpg (async)",
                cr"conn = loop.run_until_complete(create_asyncpg_conn())",
                c"loop.run_until_complete(clear_table_asyncpg(conn))",
                "loop.run_until_complete(insert_asyncpg(conn, {}))",
            ),
            (
                "asyncpg (async, executemany)",
                cr"conn = loop.run_until_complete(create_asyncpg_conn())",
                c"loop.run_until_complete(clear_table_asyncpg(conn))",
                "loop.run_until_complete(insert_asyncpg_batch(conn, {}))",
            ),
            (
                "psycopg (async)",
                cr"conn = loop.run_until_complete(create_psycopg_async_conn())",
                c"loop.run_until_complete(clear_table_psycopg_async(conn))",
                "loop.run_until_complete(insert_psycopg_async(conn, {}))",
            ),
            (
                "psycopg (async, executemany)",
                cr"conn = loop.run_until_complete(create_psycopg_async_conn())",
                c"loop.run_until_complete(clear_table_psycopg_async(conn))",
                "loop.run_until_complete(insert_psycopg_async_batch(conn, {}))",
            ),
            (
                "psycopg (async, pipeline)",
                cr"conn = loop.run_until_complete(create_psycopg_async_conn())",
                c"loop.run_until_complete(clear_table_psycopg_async(conn))",
                "loop.run_until_complete(insert_psycopg_async_pipeline(conn, {}))",
            ),
        ] {
            group.bench_function(name, |b| {
                Python::attach(|py| {
                    Python::run(py, setup, None, None).unwrap();
                    b.iter_custom(|iters| {
                        let mut sum = std::time::Duration::ZERO;
                        for g in 0..((iters - 1) / 10000 + 1) {
                            py.run(clear_stmt, None, None).unwrap();
                            let start_idx = g * 10000;
                            let end = iters.min(start_idx + 10000);

                            let statement =
                                stmt_template.replace("{}", &(end - start_idx).to_string());
                            let c_statement = std::ffi::CString::new(statement).unwrap();

                            let start = std::time::Instant::now();
                            py.eval(c_statement.as_c_str(), None, None).unwrap();
                            sum += start.elapsed();

                            // Check no background tasks remain
                            py.run(
                                c"assert len(__import__('asyncio').all_tasks(loop)) == 0",
                                None,
                                None,
                            )
                            .unwrap();
                        }
                        sum
                    });
                });
            });
        }

        // Sync benchmarks: pyro vs psycopg
        for (name, setup, clear_stmt, stmt_template) in [
            (
                "pyro (sync)",
                cr"conn = create_pyro_sync_conn()",
                c"clear_table_pyro_sync(conn)",
                "insert_pyro_sync(conn, {})",
            ),
            (
                "pyro (sync, pipeline)",
                cr"conn = create_pyro_sync_conn()",
                c"clear_table_pyro_sync(conn)",
                "insert_pyro_sync_pipeline(conn, {})",
            ),
            (
                "pyro (sync, batch)",
                cr"conn = create_pyro_sync_conn()",
                c"clear_table_pyro_sync(conn)",
                "insert_pyro_sync_batch(conn, {})",
            ),
            (
                "psycopg (sync)",
                cr"conn = create_psycopg_sync_conn()",
                c"clear_table_psycopg_sync(conn)",
                "insert_psycopg_sync(conn, {})",
            ),
            (
                "psycopg (sync, executemany)",
                cr"conn = create_psycopg_sync_conn()",
                c"clear_table_psycopg_sync(conn)",
                "insert_psycopg_sync_batch(conn, {})",
            ),
            (
                "psycopg (sync, pipeline)",
                cr"conn = create_psycopg_sync_conn()",
                c"clear_table_psycopg_sync(conn)",
                "insert_psycopg_sync_pipeline(conn, {})",
            ),
        ] {
            group.bench_function(name, |b| {
                Python::attach(|py| {
                    Python::run(py, setup, None, None).unwrap();
                    b.iter_custom(|iters| {
                        let mut sum = std::time::Duration::ZERO;
                        for g in 0..((iters - 1) / 10000 + 1) {
                            py.run(clear_stmt, None, None).unwrap();
                            let start_idx = g * 10000;
                            let end = iters.min(start_idx + 10000);

                            let statement =
                                stmt_template.replace("{}", &(end - start_idx).to_string());
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
