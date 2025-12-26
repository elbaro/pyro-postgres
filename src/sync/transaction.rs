use std::sync::atomic::{AtomicU64, Ordering};

use pyo3::prelude::*;

use crate::error::{Error, PyroResult};
use crate::params::Params;
use crate::sync::conn::SyncConn;
use crate::sync::named_portal::SyncNamedPortal;
use crate::zero_params_adapter::ParamsAdapter;

static NAME_COUNTER: AtomicU64 = AtomicU64::new(0);

#[pyclass(module = "pyro_postgres.sync", name = "Transaction")]
pub struct SyncTransaction {
    conn: Py<SyncConn>,
    isolation_level: Option<String>,
    readonly: Option<bool>,
    started: bool,
    finished: bool,
}

impl SyncTransaction {
    pub fn new(
        conn: Py<SyncConn>,
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
impl SyncTransaction {
    fn __enter__(mut slf: PyRefMut<'_, Self>) -> PyResult<PyRefMut<'_, Self>> {
        slf.begin()?;
        Ok(slf)
    }

    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &mut self,
        py: Python<'_>,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        if !self.finished {
            if _exc_type.is_some() {
                // Exception occurred - rollback
                let _ = self.rollback(py);
            } else {
                // No exception - commit
                let _ = self.commit(py);
            }
        }
        Ok(false) // Don't suppress exceptions
    }

    fn begin(&mut self) -> PyroResult<()> {
        if self.started {
            return Err(Error::IncorrectApiUsageError("Transaction already started"));
        }
        if self.finished {
            return Err(Error::TransactionClosedError);
        }

        Python::attach(|py| {
            let conn = self.conn.bind(py).borrow();

            // Build BEGIN command
            let mut begin_sql = String::from("BEGIN");

            if let Some(ref level) = self.isolation_level {
                begin_sql.push_str(" ISOLATION LEVEL ");
                begin_sql.push_str(level);
            }

            if let Some(readonly) = self.readonly {
                if readonly {
                    begin_sql.push_str(" READ ONLY");
                } else {
                    begin_sql.push_str(" READ WRITE");
                }
            }

            conn.query_drop_internal(begin_sql)?;
            conn.in_transaction.store(true, Ordering::SeqCst);
            PyroResult::Ok(())
        })?;

        self.started = true;
        Ok(())
    }

    fn commit(&mut self, py: Python<'_>) -> PyroResult<()> {
        if !self.started {
            return Err(Error::IncorrectApiUsageError("Transaction not started"));
        }
        if self.finished {
            return Err(Error::TransactionClosedError);
        }

        let conn = self.conn.bind(py).borrow();
        conn.query_drop_internal("COMMIT".to_string())?;
        conn.in_transaction.store(false, Ordering::SeqCst);

        self.finished = true;
        Ok(())
    }

    fn rollback(&mut self, py: Python<'_>) -> PyroResult<()> {
        if !self.started {
            return Err(Error::IncorrectApiUsageError("Transaction not started"));
        }
        if self.finished {
            return Err(Error::TransactionClosedError);
        }

        let conn = self.conn.bind(py).borrow();
        conn.query_drop_internal("ROLLBACK".to_string())?;
        conn.in_transaction.store(false, Ordering::SeqCst);

        self.finished = true;
        Ok(())
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
    /// with conn.tx() as tx:
    ///     portal1 = tx.exec_portal("SELECT * FROM table1")
    ///     portal2 = tx.exec_portal("SELECT * FROM table2")
    ///
    ///     while True:
    ///         rows1 = portal1.execute_collect(conn, 100)
    ///         rows2 = portal2.execute_collect(conn, 100)
    ///         process(rows1, rows2)
    ///         if portal1.is_complete() and portal2.is_complete():
    ///             break
    ///
    ///     portal1.close(conn)
    ///     portal2.close(conn)
    /// ```
    #[pyo3(signature = (query, params=Params::default()))]
    fn exec_portal(
        &self,
        py: Python<'_>,
        query: String,
        params: Params,
    ) -> PyroResult<SyncNamedPortal> {
        if !self.started {
            return Err(Error::IncorrectApiUsageError("Transaction not started"));
        }
        if self.finished {
            return Err(Error::TransactionClosedError);
        }

        let conn = self.conn.bind(py).borrow();
        let mut guard = conn.inner.lock();
        let inner = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

        // Prepare the statement
        let stmt = inner.prepare(&query)?;

        // Generate unique portal name
        let portal_id = NAME_COUNTER.fetch_add(1, Ordering::Relaxed);
        let portal_name = format!("pyro_p_{portal_id}");

        // Bind the statement to the named portal
        let params_adapter = ParamsAdapter::new(&params);
        inner.lowlevel_bind(&portal_name, &stmt.wire_name(), params_adapter)?;

        Ok(SyncNamedPortal::new(portal_name))
    }
}
