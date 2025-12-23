use std::sync::atomic::Ordering;

use pyo3::prelude::*;

use crate::r#async::conn::AsyncConn;
use crate::r#async::handler::DropHandler;
use crate::error::Error;
use crate::util::{PyroFuture, rust_future_into_py};

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
    fn __aenter__<'py>(mut slf: PyRefMut<'py, Self>, py: Python<'py>) -> PyResult<Py<PyroFuture>> {
        let conn = slf.conn.clone_ref(py);
        let isolation_level = slf.isolation_level.clone();
        let readonly = slf.readonly;

        slf.started = true;

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

            Ok(conn)
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
}
