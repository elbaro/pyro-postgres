use either::Either;
use parking_lot::MutexGuard;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::PyList;
use zero_postgres::sync::{Conn, Pipeline};

use crate::error::{Error, PyroResult};
use crate::params::Params;
use crate::statement::Statement;
use crate::sync::conn::SyncConn;
use crate::sync::handler::{DictHandler, DropHandler, TupleHandler};
use crate::ticket::PyTicket;
use crate::zero_params_adapter::ParamsAdapter;

/// Pipeline mode for batching multiple queries.
///
/// Created via `conn.pipeline()` and used as a context manager:
///
/// ```python
/// with conn.pipeline() as p:
///     t1 = p.exec("SELECT $1::int", (1,))
///     t2 = p.exec("SELECT $1::int", (2,))
///     p.sync()
///     result1 = p.claim_one(t1)
///     result2 = p.claim_collect(t2)
/// ```
#[pyclass(module = "pyro_postgres.sync", name = "Pipeline", unsendable)]
pub struct SyncPipeline {
    conn: Py<SyncConn>,
    // Transmuted to 'static - safe because we hold the guard and Py<SyncConn>
    // SAFETY: The guard keeps the Mutex locked, and Py<SyncConn> keeps SyncConn alive.
    // Pipeline is dropped before guard in cleanup().
    guard: Option<MutexGuard<'static, Option<Conn>>>,
    pipeline: Option<Pipeline<'static>>,
    /// Statements stored here to ensure they outlive their tickets.
    statements: Vec<Py<Statement>>,
    entered: bool,
}

impl SyncPipeline {
    pub fn new(conn: Py<SyncConn>) -> Self {
        Self {
            conn,
            guard: None,
            pipeline: None,
            statements: Vec::new(),
            entered: false,
        }
    }

    fn cleanup(&mut self) {
        // Drop pipeline first (calls Pipeline::cleanup internally via Drop or explicit call)
        if let Some(ref mut pipeline) = self.pipeline {
            pipeline.cleanup();
        }
        self.pipeline = None;
        // Clear statements
        self.statements.clear();
        // Then drop guard to release the mutex
        self.guard = None;
        self.entered = false;
    }
}

#[pymethods]
impl SyncPipeline {
    fn __enter__(mut slf: PyRefMut<'_, Self>) -> PyResult<PyRefMut<'_, Self>> {
        if slf.entered {
            return Err(Error::IncorrectApiUsageError("Pipeline already entered").into());
        }

        // Acquire the mutex guard
        let conn_ref = slf.conn.bind(slf.py());
        let sync_conn = conn_ref.borrow();

        // Get a lock on the inner connection
        let guard = sync_conn.inner.lock();

        // SAFETY: We transmute the lifetime to 'static because:
        // 1. We hold Py<SyncConn> which keeps SyncConn alive
        // 2. We hold the MutexGuard which prevents other access
        // 3. Pipeline is dropped before guard in cleanup()
        let guard: MutexGuard<'static, Option<Conn>> = unsafe { std::mem::transmute(guard) };

        // Store the guard
        slf.guard = Some(guard);

        // Now create the pipeline from the connection
        let guard_ref = slf.guard.as_mut().ok_or(Error::ConnectionClosedError)?;
        let conn = guard_ref.as_mut().ok_or(Error::ConnectionClosedError)?;

        // SAFETY: Same reasoning as above for the lifetime transmute
        let pipeline = Pipeline::new(conn);
        let pipeline: Pipeline<'static> = unsafe { std::mem::transmute(pipeline) };

        slf.pipeline = Some(pipeline);
        slf.entered = true;

        Ok(slf)
    }

    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &mut self,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        self.cleanup();
        Ok(false) // Don't suppress exceptions
    }

    /// Queue a statement execution.
    ///
    /// Accepts either a SQL query string or a prepared Statement.
    /// Returns a Ticket that must be claimed later using claim_one, claim_collect, or claim_drop.
    #[pyo3(signature = (query, params=Params::default()))]
    fn exec(
        &mut self,
        py: Python<'_>,
        query: Either<PyBackedStr, Py<Statement>>,
        params: Params,
    ) -> PyroResult<PyTicket> {
        let pipeline = self.pipeline.as_mut().ok_or(Error::IncorrectApiUsageError(
            "Pipeline not entered - use 'with conn.pipeline() as p:'",
        ))?;

        let params_adapter = ParamsAdapter::new(&params);
        match query {
            Either::Left(sql) => {
                let ticket = pipeline.exec(&*sql, params_adapter)?;
                // SAFETY: SQL tickets have no stmt reference
                Ok(unsafe { PyTicket::new(ticket) })
            }
            Either::Right(stmt_py) => {
                // Store the statement to keep it alive
                self.statements.push(stmt_py);
                // Get a pointer to the inner PreparedStatement
                let inner_ptr = {
                    let stmt_ref = self.statements.last().expect("just pushed").borrow(py);
                    &stmt_ref.inner as *const _
                };
                // SAFETY: The Py<Statement> in self.statements keeps the Statement alive,
                // and the pipeline holds &mut self, so the reference is valid.
                let stmt_ref = unsafe { &*inner_ptr };
                let ticket = pipeline.exec(stmt_ref, params_adapter)?;
                // SAFETY: The stmt reference points to a Statement stored in
                // self.statements, which lives until pipeline cleanup.
                Ok(unsafe { PyTicket::new(ticket) })
            }
        }
    }

    /// Send SYNC message to establish transaction boundary.
    ///
    /// After calling sync(), you must claim all queued operations in order.
    fn sync(&mut self) -> PyroResult<()> {
        let pipeline = self.pipeline.as_mut().ok_or(Error::IncorrectApiUsageError(
            "Pipeline not entered - use 'with conn.pipeline() as p:'",
        ))?;

        pipeline.sync()?;
        Ok(())
    }

    /// Claim and return just the first row (or None).
    ///
    /// Results must be claimed in the same order they were queued.
    #[pyo3(signature = (ticket, *, as_dict=false))]
    fn claim_one(
        &mut self,
        py: Python<'_>,
        ticket: &PyTicket,
        as_dict: bool,
    ) -> PyroResult<Option<Py<PyAny>>> {
        let pipeline = self.pipeline.as_mut().ok_or(Error::IncorrectApiUsageError(
            "Pipeline not entered - use 'with conn.pipeline() as p:'",
        ))?;

        if as_dict {
            let mut handler = DictHandler::new(py);
            pipeline.claim(ticket.inner, &mut handler)?;
            let rows = handler.into_rows();
            Ok(if rows.bind(py).len() > 0 {
                Some(rows.bind(py).get_item(0)?.unbind())
            } else {
                None
            })
        } else {
            let mut handler = TupleHandler::new(py);
            pipeline.claim(ticket.inner, &mut handler)?;
            let rows = handler.into_rows();
            Ok(if rows.bind(py).len() > 0 {
                Some(rows.bind(py).get_item(0)?.unbind())
            } else {
                None
            })
        }
    }

    /// Claim and collect all rows.
    ///
    /// Results must be claimed in the same order they were queued.
    #[pyo3(signature = (ticket, *, as_dict=false))]
    fn claim_collect(
        &mut self,
        py: Python<'_>,
        ticket: &PyTicket,
        as_dict: bool,
    ) -> PyroResult<Py<PyList>> {
        let pipeline = self.pipeline.as_mut().ok_or(Error::IncorrectApiUsageError(
            "Pipeline not entered - use 'with conn.pipeline() as p:'",
        ))?;

        if as_dict {
            let mut handler = DictHandler::new(py);
            pipeline.claim(ticket.inner, &mut handler)?;
            Ok(handler.into_rows())
        } else {
            let mut handler = TupleHandler::new(py);
            pipeline.claim(ticket.inner, &mut handler)?;
            Ok(handler.into_rows())
        }
    }

    /// Claim and discard all rows.
    ///
    /// Results must be claimed in the same order they were queued.
    fn claim_drop(&mut self, ticket: &PyTicket) -> PyroResult<()> {
        let pipeline = self.pipeline.as_mut().ok_or(Error::IncorrectApiUsageError(
            "Pipeline not entered - use 'with conn.pipeline() as p:'",
        ))?;

        let mut handler = DropHandler::default();
        pipeline.claim(ticket.inner, &mut handler)?;
        Ok(())
    }

    /// Returns the number of operations that have been queued but not yet claimed.
    fn pending_count(&self) -> PyroResult<usize> {
        let pipeline = self.pipeline.as_ref().ok_or(Error::IncorrectApiUsageError(
            "Pipeline not entered - use 'with conn.pipeline() as p:'",
        ))?;

        Ok(pipeline.pending_count())
    }

    /// Returns true if the pipeline is in aborted state due to an error.
    fn is_aborted(&self) -> PyroResult<bool> {
        let pipeline = self.pipeline.as_ref().ok_or(Error::IncorrectApiUsageError(
            "Pipeline not entered - use 'with conn.pipeline() as p:'",
        ))?;

        Ok(pipeline.is_aborted())
    }

    /// Claim and collect all rows (alias for claim_collect).
    ///
    /// Results must be claimed in the same order they were queued.
    #[pyo3(signature = (ticket, *, as_dict=false))]
    fn claim(
        &mut self,
        py: Python<'_>,
        ticket: &PyTicket,
        as_dict: bool,
    ) -> PyroResult<Py<PyList>> {
        self.claim_collect(py, ticket, as_dict)
    }
}

impl Drop for SyncPipeline {
    fn drop(&mut self) {
        self.cleanup();
    }
}
