use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use tokio::sync::{Mutex, OwnedMutexGuard};
use zero_postgres::tokio::{Conn, Pipeline, Ticket};

use crate::error::Error;
use crate::params::Params;
use crate::r#async::conn::AsyncConn;
use crate::r#async::handler::{DictHandler, DropHandler, TupleHandler};
use crate::util::{rust_future_into_py, PyroFuture};
use crate::zero_params_adapter::ParamsAdapter;

/// Python wrapper for pipeline Ticket.
#[pyclass(module = "pyro_postgres.async_", name = "Ticket")]
#[derive(Clone, Copy)]
pub struct PyTicket {
    inner: Ticket,
}

/// Async pipeline mode for batching multiple queries.
///
/// Created via `conn.pipeline()` and used as an async context manager:
///
/// ```python
/// async with conn.pipeline() as p:
///     t1 = await p.exec("SELECT $1::int", (1,))
///     t2 = await p.exec("SELECT $1::int", (2,))
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
        let conn = Python::with_gil(|py| slf.borrow(py).conn.clone_ref(py));
        let state_arc = Python::with_gil(|py| slf.borrow(py).state.clone());

        // Check if already entered
        let already_entered = Python::with_gil(|py| {
            slf.borrow(py)
                .entered
                .swap(true, std::sync::atomic::Ordering::SeqCst)
        });
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

            let mut state_guard = state_arc.lock().await;
            *state_guard = Some(PipelineState { guard, pipeline });

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
            let mut state_guard = state_arc.lock().await;
            if let Some(ref mut state) = *state_guard {
                state.pipeline.cleanup().await;
            }
            *state_guard = None;
            Ok(false) // Don't suppress exceptions
        })
    }

    /// Queue a statement execution.
    ///
    /// Returns a Ticket that must be claimed later using claim_one, claim_collect, or claim_drop.
    #[pyo3(signature = (query, params=Params::default()))]
    fn exec(&self, py: Python<'_>, query: String, params: Params) -> PyResult<Py<PyroFuture>> {
        let state_arc = self.state.clone();

        rust_future_into_py(py, async move {
            let mut state_guard = state_arc.lock().await;
            let state = state_guard.as_mut().ok_or(Error::IncorrectApiUsageError(
                "Pipeline not entered - use 'async with conn.pipeline() as p:'",
            ))?;

            let params_adapter = ParamsAdapter::new(&params);
            let ticket = state.pipeline.exec(query.as_str(), params_adapter).await?;

            Ok(PyTicket { inner: ticket })
        })
    }

    /// Send SYNC message to establish transaction boundary.
    ///
    /// After calling sync(), you must claim all queued operations in order.
    fn sync(&self, py: Python<'_>) -> PyResult<Py<PyroFuture>> {
        let state_arc = self.state.clone();

        rust_future_into_py::<_, ()>(py, async move {
            let mut state_guard = state_arc.lock().await;
            let state = state_guard.as_mut().ok_or(Error::IncorrectApiUsageError(
                "Pipeline not entered - use 'async with conn.pipeline() as p:'",
            ))?;

            state.pipeline.sync().await?;
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
            let mut state_guard = state_arc.lock().await;
            let state = state_guard.as_mut().ok_or(Error::IncorrectApiUsageError(
                "Pipeline not entered - use 'async with conn.pipeline() as p:'",
            ))?;

            if as_dict {
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
            }
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
            let mut state_guard = state_arc.lock().await;
            let state = state_guard.as_mut().ok_or(Error::IncorrectApiUsageError(
                "Pipeline not entered - use 'async with conn.pipeline() as p:'",
            ))?;

            if as_dict {
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
            }
        })
    }

    /// Claim and discard all rows.
    ///
    /// Results must be claimed in the same order they were queued.
    fn claim_drop(&self, py: Python<'_>, ticket: PyTicket) -> PyResult<Py<PyroFuture>> {
        let state_arc = self.state.clone();

        rust_future_into_py::<_, ()>(py, async move {
            let mut state_guard = state_arc.lock().await;
            let state = state_guard.as_mut().ok_or(Error::IncorrectApiUsageError(
                "Pipeline not entered - use 'async with conn.pipeline() as p:'",
            ))?;

            let mut handler = DropHandler::default();
            state.pipeline.claim(ticket.inner, &mut handler).await?;
            Ok(())
        })
    }

    /// Returns the number of operations that have been queued but not yet claimed.
    fn pending_count(&self, py: Python<'_>) -> PyResult<Py<PyroFuture>> {
        let state_arc = self.state.clone();

        rust_future_into_py(py, async move {
            let state_guard = state_arc.lock().await;
            let state = state_guard.as_ref().ok_or(Error::IncorrectApiUsageError(
                "Pipeline not entered - use 'async with conn.pipeline() as p:'",
            ))?;

            Ok(state.pipeline.pending_count())
        })
    }

    /// Returns true if the pipeline is in aborted state due to an error.
    fn is_aborted(&self, py: Python<'_>) -> PyResult<Py<PyroFuture>> {
        let state_arc = self.state.clone();

        rust_future_into_py(py, async move {
            let state_guard = state_arc.lock().await;
            let state = state_guard.as_ref().ok_or(Error::IncorrectApiUsageError(
                "Pipeline not entered - use 'async with conn.pipeline() as p:'",
            ))?;

            Ok(state.pipeline.is_aborted())
        })
    }
}
