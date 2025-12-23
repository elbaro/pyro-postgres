use std::sync::Arc;

use either::Either;
use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::{PyDict, PyTuple};
use tokio::sync::OwnedMutexGuard;
use zero_postgres::tokio::{Conn, Pipeline};

use crate::r#async::conn::AsyncConn;
use crate::r#async::handler::{DictHandler, DropHandler, TupleHandler};
use crate::error::Error;
use crate::params::Params;
use crate::statement::Statement;
use crate::ticket::PyTicket;
use crate::util::{PyroFuture, rust_future_into_py};
use crate::zero_params_adapter::ParamsAdapter;

/// Async pipeline mode for batching multiple queries.
///
/// Created via `conn.pipeline()` and used as an async context manager:
///
/// ```python
/// async with conn.pipeline() as p:
///     t1 = p.exec("SELECT $1::int", (1,))
///     t2 = p.exec("SELECT $1::int", (2,))
///     await p.sync()
///     result1 = await p.claim_one(t1)
///     result2 = await p.claim_collect(t2)
/// ```
#[pyclass(module = "pyro_postgres.async_", name = "Pipeline")]
pub struct AsyncPipeline {
    conn: Py<AsyncConn>,
    // We store the guard and pipeline in a mutex so we can access them from async methods
    // Using OwnedMutexGuard which is Send and can be moved across tasks
    state: Arc<Mutex<Option<PipelineState>>>,
    entered: std::sync::atomic::AtomicBool,
}

struct PipelineState {
    #[allow(dead_code)]
    guard: OwnedMutexGuard<Option<Conn>>,
    pipeline: Pipeline<'static>,
    /// Statements stored here to ensure they outlive their tickets.
    /// The Ticket's `stmt` field references the inner PreparedStatement.
    statements: Vec<Py<Statement>>,
}

impl AsyncPipeline {
    pub fn new(conn: Py<AsyncConn>) -> Self {
        Self {
            conn,
            state: Arc::new(Mutex::new(None)),
            entered: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

#[pymethods]
impl AsyncPipeline {
    fn __aenter__(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyroFuture>> {
        let slf_clone = slf.clone_ref(py);
        let conn = slf.borrow(py).conn.clone_ref(py);
        let state_arc = slf.borrow(py).state.clone();

        // Check if already entered
        let already_entered = slf
            .borrow(py)
            .entered
            .swap(true, std::sync::atomic::Ordering::SeqCst);
        if already_entered {
            return Err(Error::IncorrectApiUsageError("Pipeline already entered").into());
        }

        rust_future_into_py(py, async move {
            let conn_ref = Python::attach(|py| conn.bind(py).borrow().inner.clone());

            // Acquire the owned mutex guard - this can be moved across tasks
            let mut guard = conn_ref.lock_owned().await;

            // Get mutable reference to the connection
            if guard.is_none() {
                return Err(Error::ConnectionClosedError);
            }

            // Create the pipeline
            // SAFETY: We transmute the Pipeline lifetime to 'static because:
            // 1. We hold Py<AsyncConn> which keeps AsyncConn alive
            // 2. We hold the OwnedMutexGuard which prevents other access and keeps Conn alive
            // 3. Pipeline is dropped before guard in cleanup
            let conn_mut = guard.as_mut().ok_or(Error::ConnectionClosedError)?;
            let pipeline = Pipeline::new(conn_mut);
            let pipeline: Pipeline<'static> = unsafe { std::mem::transmute(pipeline) };

            {
                let mut state_guard = state_arc.lock();
                *state_guard = Some(PipelineState {
                    guard,
                    pipeline,
                    statements: Vec::new(),
                });
            }

            // Return self (the pipeline) for the context manager
            Ok(slf_clone)
        })
    }

    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyroFuture>> {
        let state_arc = self.state.clone();
        let entered = &self.entered;
        entered.store(false, std::sync::atomic::Ordering::SeqCst);

        rust_future_into_py::<_, bool>(py, async move {
            // Take ownership of the state so we can call async cleanup
            let mut state_opt = {
                let mut state_guard = state_arc.lock();
                state_guard.take()
            };
            if let Some(ref mut state) = state_opt {
                state.pipeline.cleanup().await;
            }
            // state_opt drops here, releasing the connection guard
            Ok(false) // Don't suppress exceptions
        })
    }

    /// Queue a statement execution.
    ///
    /// Accepts either a SQL query string or a prepared Statement.
    /// Returns a Ticket that must be claimed later using claim_one, claim_collect, or claim_drop.
    #[pyo3(signature = (query, params=Params::default()))]
    fn exec(
        &self,
        py: Python<'_>,
        query: Either<PyBackedStr, Py<Statement>>,
        params: Params,
    ) -> PyResult<PyTicket> {
        let mut state_guard = self.state.lock();
        let state = state_guard.as_mut().ok_or(Error::IncorrectApiUsageError(
            "Pipeline not entered - use 'async with conn.pipeline() as p:'",
        ))?;

        let params_adapter = ParamsAdapter::new(&params);
        match query {
            Either::Left(sql) => {
                let ticket = state
                    .pipeline
                    .exec(&*sql, params_adapter)
                    .map_err(Error::from)?;
                // SAFETY: SQL tickets have no stmt reference (stmt field is None).
                Ok(unsafe { PyTicket::new(ticket) })
            }
            Either::Right(stmt_py) => {
                // Store the statement in the pipeline state to keep it alive
                state.statements.push(stmt_py);

                // Get a reference to the stored statement's inner PreparedStatement
                // SAFETY: We just pushed the statement, so it's at the last index.
                // The reference is valid as long as PipelineState exists.
                let stmt_ref = {
                    let stmt = state.statements.last().expect("just pushed");
                    let inner_ptr = &stmt.borrow(py).inner as *const _;
                    // SAFETY: The Py<Statement> in state.statements keeps the Statement alive,
                    // and we hold the state_guard lock, so the reference is valid.
                    unsafe { &*inner_ptr }
                };

                let ticket = state
                    .pipeline
                    .exec(stmt_ref, params_adapter)
                    .map_err(Error::from)?;
                // SAFETY: The stmt reference points to a Statement stored in
                // state.statements, which lives until pipeline cleanup.
                Ok(unsafe { PyTicket::new(ticket) })
            }
        }
    }

    /// Send SYNC message to establish transaction boundary.
    ///
    /// After calling sync(), you must claim all queued operations in order.
    fn sync(&self, py: Python<'_>) -> PyResult<Py<PyroFuture>> {
        let state_arc = self.state.clone();

        rust_future_into_py::<_, ()>(py, async move {
            // Take ownership of the state for the async operation
            let mut state_opt = {
                let mut guard = state_arc.lock();
                guard.take()
            };
            let state = state_opt.as_mut().ok_or(Error::IncorrectApiUsageError(
                "Pipeline not entered - use 'async with conn.pipeline() as p:'",
            ))?;

            let result = state.pipeline.sync().await;

            // Put the state back
            {
                let mut guard = state_arc.lock();
                *guard = state_opt;
            }

            result?;
            Ok(())
        })
    }

    /// Claim and return just the first row (or None).
    ///
    /// Results must be claimed in the same order they were queued.
    #[pyo3(signature = (ticket, *, as_dict=false))]
    fn claim_one(
        &self,
        py: Python<'_>,
        ticket: PyTicket,
        as_dict: bool,
    ) -> PyResult<Py<PyroFuture>> {
        let state_arc = self.state.clone();

        rust_future_into_py::<_, Option<Py<PyAny>>>(py, async move {
            let mut state_opt = { state_arc.lock().take() };
            let state = state_opt.as_mut().ok_or(Error::IncorrectApiUsageError(
                "Pipeline not entered - use 'async with conn.pipeline() as p:'",
            ))?;

            let result = if as_dict {
                let mut handler = DictHandler::new();
                state.pipeline.claim(ticket.inner, &mut handler).await?;
                Python::attach(|py| {
                    let rows = handler.rows_to_python(py)?;
                    Ok(rows.into_iter().next().map(pyo3::Py::into_any))
                })
            } else {
                let mut handler = TupleHandler::new();
                state.pipeline.claim(ticket.inner, &mut handler).await?;
                Python::attach(|py| {
                    let rows = handler.rows_to_python(py)?;
                    Ok(rows.into_iter().next().map(pyo3::Py::into_any))
                })
            };

            *state_arc.lock() = state_opt;
            result
        })
    }

    /// Claim and collect all rows.
    ///
    /// Results must be claimed in the same order they were queued.
    #[pyo3(signature = (ticket, *, as_dict=false))]
    fn claim_collect(
        &self,
        py: Python<'_>,
        ticket: PyTicket,
        as_dict: bool,
    ) -> PyResult<Py<PyroFuture>> {
        let state_arc = self.state.clone();

        rust_future_into_py::<_, Vec<Py<PyAny>>>(py, async move {
            let mut state_opt = { state_arc.lock().take() };
            let state = state_opt.as_mut().ok_or(Error::IncorrectApiUsageError(
                "Pipeline not entered - use 'async with conn.pipeline() as p:'",
            ))?;

            let result = if as_dict {
                let mut handler = DictHandler::new();
                state.pipeline.claim(ticket.inner, &mut handler).await?;
                Python::attach(|py| {
                    let rows: Vec<Py<PyDict>> = handler.rows_to_python(py)?;
                    Ok(rows.into_iter().map(pyo3::Py::into_any).collect())
                })
            } else {
                let mut handler = TupleHandler::new();
                state.pipeline.claim(ticket.inner, &mut handler).await?;
                Python::attach(|py| {
                    let rows: Vec<Py<PyTuple>> = handler.rows_to_python(py)?;
                    Ok(rows.into_iter().map(pyo3::Py::into_any).collect())
                })
            };

            *state_arc.lock() = state_opt;
            result
        })
    }

    /// Claim and discard all rows.
    ///
    /// Results must be claimed in the same order they were queued.
    fn claim_drop(&self, py: Python<'_>, ticket: PyTicket) -> PyResult<Py<PyroFuture>> {
        let state_arc = self.state.clone();

        rust_future_into_py::<_, ()>(py, async move {
            let mut state_opt = { state_arc.lock().take() };
            let state = state_opt.as_mut().ok_or(Error::IncorrectApiUsageError(
                "Pipeline not entered - use 'async with conn.pipeline() as p:'",
            ))?;

            let mut handler = DropHandler::default();
            let result = state.pipeline.claim(ticket.inner, &mut handler).await;

            *state_arc.lock() = state_opt;
            result?;
            Ok(())
        })
    }

    /// Returns the number of operations that have been queued but not yet claimed.
    fn pending_count(&self) -> PyResult<usize> {
        let state_guard = self.state.lock();
        let state = state_guard.as_ref().ok_or(Error::IncorrectApiUsageError(
            "Pipeline not entered - use 'async with conn.pipeline() as p:'",
        ))?;
        Ok(state.pipeline.pending_count())
    }

    /// Returns true if the pipeline is in aborted state due to an error.
    fn is_aborted(&self) -> PyResult<bool> {
        let state_guard = self.state.lock();
        let state = state_guard.as_ref().ok_or(Error::IncorrectApiUsageError(
            "Pipeline not entered - use 'async with conn.pipeline() as p:'",
        ))?;
        Ok(state.pipeline.is_aborted())
    }

    /// Claim and collect all rows (alias for claim_collect).
    ///
    /// Results must be claimed in the same order they were queued.
    #[pyo3(signature = (ticket, *, as_dict=false))]
    fn claim(&self, py: Python<'_>, ticket: PyTicket, as_dict: bool) -> PyResult<Py<PyroFuture>> {
        self.claim_collect(py, ticket, as_dict)
    }
}
