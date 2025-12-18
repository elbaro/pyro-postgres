use std::sync::atomic::Ordering;

use pyo3::prelude::*;

use crate::error::{Error, PyroResult};
use crate::sync::conn::SyncConn;

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
            return Err(Error::IncorrectApiUsageError(
                "Transaction already started",
            ));
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
}
