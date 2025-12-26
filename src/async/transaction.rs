use std::sync::atomic::{AtomicU64, Ordering};

use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;

use crate::r#async::conn::AsyncConn;
use crate::r#async::handler::DropHandler;
use crate::r#async::named_portal::AsyncNamedPortal;
use crate::error::Error;
use crate::params::Params;
use crate::util::{PyroFuture, rust_future_into_py};
use crate::zero_params_adapter::ParamsAdapter;

static NAME_COUNTER: AtomicU64 = AtomicU64::new(0);

#[pyclass(module = "pyro_postgres.async_", name = "Transaction")]
pub struct AsyncTransaction {
    conn: Py<AsyncConn>,
    isolation_level: Option<String>,
    readonly: Option<bool>,
    started: bool,
    finished: bool,
}

impl AsyncTransaction {
    pub fn new(
        conn: Py<AsyncConn>,
        isolation_level: Option<String>,
        readonly: Option<bool>,
    ) -> Self {
        Self {
            conn,
            isolation_level,
            readonly,
            started: false,
            finished: false,
        }
    }
}

#[pymethods]
impl AsyncTransaction {
    fn __aenter__(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyroFuture>> {
        let tx = slf.clone_ref(py);

        let (conn, isolation_level, readonly) = {
            let mut borrowed = slf.borrow_mut(py);
            borrowed.started = true;
            (
                borrowed.conn.clone_ref(py),
                borrowed.isolation_level.clone(),
                borrowed.readonly,
            )
        };

        rust_future_into_py(py, async move {
            let conn_inner = Python::attach(|py| {
                let conn_ref = conn.bind(py).borrow();
                conn_ref.inner.clone()
            });

            // Build BEGIN command
            let mut begin_sql = String::from("BEGIN");
            if let Some(ref level) = isolation_level {
                begin_sql.push_str(" ISOLATION LEVEL ");
                begin_sql.push_str(level);
            }
            if let Some(readonly) = readonly {
                if readonly {
                    begin_sql.push_str(" READ ONLY");
                } else {
                    begin_sql.push_str(" READ WRITE");
                }
            }

            let mut guard = conn_inner.lock().await;
            let inner_conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

            let mut handler = DropHandler::default();
            inner_conn.query(&begin_sql, &mut handler).await?;

            Python::attach(|py| {
                let conn_ref = conn.bind(py).borrow();
                conn_ref.in_transaction.store(true, Ordering::SeqCst);
            });

            Ok(tx)
        })
    }

    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __aexit__(
        &mut self,
        py: Python<'_>,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyroFuture>> {
        let conn = self.conn.clone_ref(py);
        let had_exception = _exc_type.is_some();

        if self.finished {
            // Already finished, return immediately
            return rust_future_into_py(py, async move { Ok(false) });
        }

        self.finished = true;

        rust_future_into_py(py, async move {
            let conn_inner = Python::attach(|py| {
                let conn_ref = conn.bind(py).borrow();
                conn_ref.inner.clone()
            });

            let sql = if had_exception { "ROLLBACK" } else { "COMMIT" };

            let mut guard = conn_inner.lock().await;
            if let Some(inner_conn) = guard.as_mut() {
                let mut handler = DropHandler::default();
                let _ = inner_conn.query(sql, &mut handler).await;
            }

            Python::attach(|py| {
                let conn_ref = conn.bind(py).borrow();
                conn_ref.in_transaction.store(false, Ordering::SeqCst);
            });

            Ok(false) // Don't suppress exceptions
        })
    }

    fn commit(&mut self, py: Python<'_>) -> PyResult<Py<PyroFuture>> {
        if !self.started {
            return Err(Error::IncorrectApiUsageError("Transaction not started").into());
        }
        if self.finished {
            return Err(Error::TransactionClosedError.into());
        }

        let conn = self.conn.clone_ref(py);
        self.finished = true;

        rust_future_into_py(py, async move {
            let conn_inner = Python::attach(|py| {
                let conn_ref = conn.bind(py).borrow();
                conn_ref.inner.clone()
            });

            let mut guard = conn_inner.lock().await;
            let inner_conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

            let mut handler = DropHandler::default();
            inner_conn.query("COMMIT", &mut handler).await?;

            Python::attach(|py| {
                let conn_ref = conn.bind(py).borrow();
                conn_ref.in_transaction.store(false, Ordering::SeqCst);
            });

            Ok(())
        })
    }

    fn rollback(&mut self, py: Python<'_>) -> PyResult<Py<PyroFuture>> {
        if !self.started {
            return Err(Error::IncorrectApiUsageError("Transaction not started").into());
        }
        if self.finished {
            return Err(Error::TransactionClosedError.into());
        }

        let conn = self.conn.clone_ref(py);
        self.finished = true;

        rust_future_into_py(py, async move {
            let conn_inner = Python::attach(|py| {
                let conn_ref = conn.bind(py).borrow();
                conn_ref.inner.clone()
            });

            let mut guard = conn_inner.lock().await;
            let inner_conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

            let mut handler = DropHandler::default();
            inner_conn.query("ROLLBACK", &mut handler).await?;

            Python::attach(|py| {
                let conn_ref = conn.bind(py).borrow();
                conn_ref.in_transaction.store(false, Ordering::SeqCst);
            });

            Ok(())
        })
    }

    /// Create a named portal for iterative row fetching.
    ///
    /// Named portals allow interleaving multiple row streams. Unlike unnamed portals
    /// (used in exec_iter), named portals can be executed multiple times and can
    /// coexist with other portals.
    ///
    /// Named portals must be created within an explicit transaction because SYNC
    /// messages (which occur at transaction boundaries) close all unnamed portals.
    ///
    /// ```python
    /// async with conn.tx() as tx:
    ///     portal1 = await tx.exec_portal("SELECT * FROM table1")
    ///     portal2 = await tx.exec_portal("SELECT * FROM table2")
    ///
    ///     while True:
    ///         rows1, has_more1 = await portal1.execute_collect(conn, 100)
    ///         rows2, has_more2 = await portal2.execute_collect(conn, 100)
    ///         process(rows1, rows2)
    ///         if not has_more1 and not has_more2:
    ///             break
    ///
    ///     await portal1.close(conn)
    ///     await portal2.close(conn)
    /// ```
    #[pyo3(signature = (query, params=None))]
    fn exec_portal(
        &self,
        py: Python<'_>,
        query: PyBackedStr,
        params: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyroFuture>> {
        if !self.started {
            return Err(Error::IncorrectApiUsageError("Transaction not started").into());
        }
        if self.finished {
            return Err(Error::TransactionClosedError.into());
        }

        let params_obj: Params = params
            .map(|p| p.extract(py))
            .transpose()?
            .unwrap_or_default();
        let query_string = query.to_string();

        let conn = self.conn.clone_ref(py);
        let portal_id = NAME_COUNTER.fetch_add(1, Ordering::Relaxed);

        rust_future_into_py(py, async move {
            let conn_inner = Python::attach(|py| {
                let conn_ref = conn.bind(py).borrow();
                conn_ref.inner.clone()
            });

            let mut guard = conn_inner.lock().await;
            let inner = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

            // Prepare the statement
            let stmt = inner.prepare(&query_string).await?;

            // Generate unique portal name
            let portal_name = format!("pyro_p_{portal_id}");

            // Bind the statement to the named portal
            let params_adapter = ParamsAdapter::new(&params_obj);
            inner
                .lowlevel_bind(&portal_name, &stmt.wire_name(), params_adapter)
                .await?;

            Ok(AsyncNamedPortal::new(portal_name))
        })
    }
}
