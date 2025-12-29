use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::{PyAny, PyDict, PyList, PyTuple};
use tokio::sync::Mutex;
use zero_postgres::state::extended::PreparedStatement as ZeroPreparedStatement;
use zero_postgres::tokio::Conn;

use crate::r#async::handler::{DictHandler, DropHandler, TupleHandler};
use crate::r#async::pipeline::AsyncPipeline;
use crate::r#async::transaction::AsyncTransaction;
use crate::r#async::unnamed_portal::AsyncUnnamedPortal;
use crate::error::{Error, PyroResult};
use crate::isolation_level::IsolationLevel;
use crate::opts::resolve_opts;
use crate::params::Params;
use crate::statement::PreparedStatement;
use crate::util::{PyroFuture, rust_future_into_py};
use crate::zero_params_adapter::ParamsAdapter;

/// Represents either a query string or a prepared statement for async operations.
enum StatementInput {
    Query(String),
    Prepared(ZeroPreparedStatement),
}

#[pyclass(module = "pyro_postgres.async_", name = "Conn")]
pub struct AsyncConn {
    pub inner: Arc<Mutex<Option<Conn>>>,
    pub in_transaction: AtomicBool,
    tuple_handler: Arc<Mutex<TupleHandler>>,
    dict_handler: Arc<Mutex<DictHandler>>,
}

#[pymethods]
impl AsyncConn {
    #[new]
    fn _new() -> PyroResult<Self> {
        Err(Error::IncorrectApiUsageError(
            "use `await Conn.new(url)` instead of `Conn()`.",
        ))
    }

    #[expect(clippy::new_ret_no_self)]
    #[staticmethod]
    #[pyo3(signature = (url_or_opts))]
    pub fn new(py: Python<'_>, url_or_opts: &Bound<'_, PyAny>) -> PyResult<Py<PyroFuture>> {
        let opts = resolve_opts(py, url_or_opts)?;
        rust_future_into_py(py, async move {
            let conn = Conn::new(opts).await?;
            Ok(Self {
                inner: Arc::new(Mutex::new(Some(conn))),
                in_transaction: AtomicBool::new(false),
                tuple_handler: Arc::new(Mutex::new(TupleHandler::new())),
                dict_handler: Arc::new(Mutex::new(DictHandler::new())),
            })
        })
    }

    #[pyo3(signature = (isolation_level=None, readonly=None))]
    fn tx(
        slf: Py<Self>,
        isolation_level: Option<&IsolationLevel>,
        readonly: Option<bool>,
    ) -> AsyncTransaction {
        let isolation_level_str: Option<String> = isolation_level.map(|l| l.as_str().to_string());
        AsyncTransaction::new(slf, isolation_level_str, readonly)
    }

    /// Create a pipeline for batching multiple queries.
    ///
    /// Use as an async context manager:
    /// ```python
    /// async with conn.pipeline() as p:
    ///     t1 = await p.exec("SELECT $1::int", (1,))
    ///     t2 = await p.exec("SELECT $1::int", (2,))
    ///     await p.sync()
    ///     result1 = await p.claim_one(t1)
    ///     result2 = await p.claim_collect(t2)
    /// ```
    fn pipeline(slf: Py<Self>) -> AsyncPipeline {
        AsyncPipeline::new(slf)
    }

    fn id(&self, py: Python<'_>) -> PyResult<Py<PyroFuture>> {
        let inner = self.inner.clone();
        rust_future_into_py(py, async move {
            let guard = inner.lock().await;
            let conn = guard.as_ref().ok_or(Error::ConnectionClosedError)?;
            Ok(conn.connection_id())
        })
    }

    fn close(&self, py: Python<'_>) -> PyResult<Py<PyroFuture>> {
        let inner = self.inner.clone();
        rust_future_into_py(py, async move {
            let mut guard = inner.lock().await;
            *guard = None;
            Ok(())
        })
    }

    fn server_version(&self, py: Python<'_>) -> PyResult<Py<PyroFuture>> {
        let inner = self.inner.clone();
        rust_future_into_py(py, async move {
            let guard = inner.lock().await;
            let conn = guard.as_ref().ok_or(Error::ConnectionClosedError)?;
            for (key, value) in conn.server_params() {
                if key == "server_version" {
                    return Ok(value.clone());
                }
            }
            Ok(String::new())
        })
    }

    fn ping(&self, py: Python<'_>) -> PyResult<Py<PyroFuture>> {
        let inner = self.inner.clone();
        rust_future_into_py(py, async move {
            let mut guard = inner.lock().await;
            let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;
            conn.ping().await?;
            Ok(())
        })
    }

    // ─── Simple Query Protocol (Text) ───────────────────────────────────────

    #[pyo3(signature = (query, *, as_dict=false))]
    fn query(&self, py: Python<'_>, query: String, as_dict: bool) -> PyResult<Py<PyroFuture>> {
        let inner = self.inner.clone();
        let tuple_handler = self.tuple_handler.clone();
        let dict_handler = self.dict_handler.clone();

        rust_future_into_py::<_, Vec<Py<PyAny>>>(py, async move {
            let mut guard = inner.lock().await;
            let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

            if as_dict {
                let mut handler = dict_handler.lock().await;
                handler.clear();
                conn.query(&query, &mut *handler).await?;
                Python::attach(|py| {
                    let rows: Vec<Py<PyDict>> = handler.rows_to_python(py)?;
                    Ok(rows.into_iter().map(pyo3::Py::into_any).collect())
                })
            } else {
                let mut handler = tuple_handler.lock().await;
                handler.clear();
                conn.query(&query, &mut *handler).await?;
                Python::attach(|py| {
                    let rows: Vec<Py<PyTuple>> = handler.rows_to_python(py)?;
                    Ok(rows.into_iter().map(pyo3::Py::into_any).collect())
                })
            }
        })
    }

    #[pyo3(signature = (query, *, as_dict=false))]
    fn query_first(
        &self,
        py: Python<'_>,
        query: String,
        as_dict: bool,
    ) -> PyResult<Py<PyroFuture>> {
        let inner = self.inner.clone();
        let tuple_handler = self.tuple_handler.clone();
        let dict_handler = self.dict_handler.clone();

        rust_future_into_py::<_, Option<Py<PyAny>>>(py, async move {
            let mut guard = inner.lock().await;
            let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

            if as_dict {
                let mut handler = dict_handler.lock().await;
                handler.clear();
                conn.query(&query, &mut *handler).await?;
                Python::attach(|py| {
                    let rows = handler.rows_to_python(py)?;
                    Ok(rows.into_iter().next().map(pyo3::Py::into_any))
                })
            } else {
                let mut handler = tuple_handler.lock().await;
                handler.clear();
                conn.query(&query, &mut *handler).await?;
                Python::attach(|py| {
                    let rows = handler.rows_to_python(py)?;
                    Ok(rows.into_iter().next().map(pyo3::Py::into_any))
                })
            }
        })
    }

    fn query_drop(&self, py: Python<'_>, query: String) -> PyResult<Py<PyroFuture>> {
        let inner = self.inner.clone();

        rust_future_into_py::<_, u64>(py, async move {
            let mut guard = inner.lock().await;
            let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

            let mut handler = DropHandler::default();
            conn.query(&query, &mut handler).await?;

            Ok(handler.rows_affected.unwrap_or(0))
        })
    }

    // ─── Extended Query Protocol (Binary) ─────────────────────────────────────

    #[pyo3(signature = (stmt, params=Params::default(), *, as_dict=false))]
    fn exec(
        &self,
        py: Python<'_>,
        stmt: &Bound<'_, PyAny>,
        params: Params,
        as_dict: bool,
    ) -> PyResult<Py<PyroFuture>> {
        // Extract statement before async block (PreparedStatement is not Send)
        let stmt_input = if let Ok(prepared) = Bound::cast_exact::<PreparedStatement>(stmt) {
            StatementInput::Prepared(prepared.borrow().inner.clone())
        } else {
            StatementInput::Query(stmt.extract::<String>()?)
        };

        let inner = self.inner.clone();
        let tuple_handler = self.tuple_handler.clone();
        let dict_handler = self.dict_handler.clone();

        rust_future_into_py::<_, Vec<Py<PyAny>>>(py, async move {
            let mut guard = inner.lock().await;
            let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

            let stmt_ref: ZeroPreparedStatement = match stmt_input {
                StatementInput::Query(query) => conn.prepare(&query).await?,
                StatementInput::Prepared(prepared) => prepared,
            };

            let params_adapter = ParamsAdapter::new(&params);
            if as_dict {
                let mut handler = dict_handler.lock().await;
                handler.clear();
                conn.exec(&stmt_ref, params_adapter, &mut *handler).await?;
                Python::attach(|py| {
                    let rows: Vec<Py<PyDict>> = handler.rows_to_python(py)?;
                    Ok(rows.into_iter().map(pyo3::Py::into_any).collect())
                })
            } else {
                let mut handler = tuple_handler.lock().await;
                handler.clear();
                conn.exec(&stmt_ref, params_adapter, &mut *handler).await?;
                Python::attach(|py| {
                    let rows: Vec<Py<PyTuple>> = handler.rows_to_python(py)?;
                    Ok(rows.into_iter().map(pyo3::Py::into_any).collect())
                })
            }
        })
    }

    #[pyo3(signature = (stmt, params=Params::default(), *, as_dict=false))]
    fn exec_first(
        &self,
        py: Python<'_>,
        stmt: &Bound<'_, PyAny>,
        params: Params,
        as_dict: bool,
    ) -> PyResult<Py<PyroFuture>> {
        // Extract statement before async block (PreparedStatement is not Send)
        let stmt_input = if let Ok(prepared) = Bound::cast_exact::<PreparedStatement>(stmt) {
            StatementInput::Prepared(prepared.borrow().inner.clone())
        } else {
            StatementInput::Query(stmt.extract::<String>()?)
        };

        let inner = self.inner.clone();
        let tuple_handler = self.tuple_handler.clone();
        let dict_handler = self.dict_handler.clone();

        rust_future_into_py::<_, Option<Py<PyAny>>>(py, async move {
            let mut guard = inner.lock().await;
            let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

            let stmt_ref: ZeroPreparedStatement = match stmt_input {
                StatementInput::Query(query) => conn.prepare(&query).await?,
                StatementInput::Prepared(prepared) => prepared,
            };

            let params_adapter = ParamsAdapter::new(&params);
            if as_dict {
                let mut handler = dict_handler.lock().await;
                handler.clear();
                conn.exec(&stmt_ref, params_adapter, &mut *handler).await?;
                Python::attach(|py| {
                    let rows = handler.rows_to_python(py)?;
                    Ok(rows.into_iter().next().map(pyo3::Py::into_any))
                })
            } else {
                let mut handler = tuple_handler.lock().await;
                handler.clear();
                conn.exec(&stmt_ref, params_adapter, &mut *handler).await?;
                Python::attach(|py| {
                    let rows = handler.rows_to_python(py)?;
                    Ok(rows.into_iter().next().map(pyo3::Py::into_any))
                })
            }
        })
    }

    #[pyo3(signature = (stmt, params=Params::default()))]
    fn exec_drop(
        &self,
        py: Python<'_>,
        stmt: &Bound<'_, PyAny>,
        params: Params,
    ) -> PyResult<Py<PyroFuture>> {
        // Extract statement before async block (PreparedStatement is not Send)
        let stmt_input = if let Ok(prepared) = Bound::cast_exact::<PreparedStatement>(stmt) {
            StatementInput::Prepared(prepared.borrow().inner.clone())
        } else {
            StatementInput::Query(stmt.extract::<String>()?)
        };

        let inner = self.inner.clone();

        rust_future_into_py::<_, u64>(py, async move {
            let mut guard = inner.lock().await;
            let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

            let stmt_ref: ZeroPreparedStatement = match stmt_input {
                StatementInput::Query(query) => conn.prepare(&query).await?,
                StatementInput::Prepared(prepared) => prepared,
            };

            let mut handler = DropHandler::default();
            let params_adapter = ParamsAdapter::new(&params);
            conn.exec(&stmt_ref, params_adapter, &mut handler).await?;

            Ok(handler.rows_affected.unwrap_or(0))
        })
    }

    /// Execute a statement with multiple parameter sets in a batch.
    ///
    /// Uses pipeline mode internally for optimal performance.
    #[pyo3(signature = (stmt, params_list))]
    fn exec_batch(
        &self,
        py: Python<'_>,
        stmt: &Bound<'_, PyAny>,
        params_list: Vec<Py<PyAny>>,
    ) -> PyResult<Py<PyroFuture>> {
        let mut params_vec = Vec::new();
        for p in params_list {
            params_vec.push(p.extract::<Params>(py)?);
        }

        // Extract statement before async block (PreparedStatement is not Send)
        let stmt_input = if let Ok(prepared) = Bound::cast_exact::<PreparedStatement>(stmt) {
            StatementInput::Prepared(prepared.borrow().inner.clone())
        } else {
            StatementInput::Query(stmt.extract::<String>()?)
        };

        let inner = self.inner.clone();

        rust_future_into_py::<_, Option<Py<PyAny>>>(py, async move {
            let mut guard = inner.lock().await;
            let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

            let adapters: Vec<_> = params_vec.iter().map(ParamsAdapter::new).collect();
            match stmt_input {
                StatementInput::Query(query) => {
                    conn.exec_batch(query.as_str(), &adapters).await?;
                }
                StatementInput::Prepared(prepared) => {
                    conn.exec_batch(&prepared, &adapters).await?;
                }
            }
            Ok(None)
        })
    }

    /// Execute a statement and process rows iteratively via a callback.
    ///
    /// The callback receives an `UnnamedPortal` that can fetch rows in batches.
    /// Useful for processing large result sets that don't fit in memory.
    ///
    /// Note: The callback is synchronous - use `portal.fetch()` to get rows.
    ///
    /// ```python
    /// def process(portal):
    ///     while True:
    ///         rows, has_more = portal.fetch(1000)
    ///         for row in rows:
    ///             process_row(row)
    ///         if not has_more:
    ///             break
    ///     return total_count
    ///
    /// result = await conn.exec_iter("SELECT * FROM large_table", (), process)
    /// ```
    fn exec_iter(
        &self,
        py: Python<'_>,
        stmt: &Bound<'_, PyAny>,
        params: Params,
        callback: Py<PyAny>,
    ) -> PyResult<Py<PyroFuture>> {
        // Extract statement before async block (PreparedStatement is not Send)
        let stmt_input = if let Ok(prepared) = Bound::cast_exact::<PreparedStatement>(stmt) {
            StatementInput::Prepared(prepared.borrow().inner.clone())
        } else {
            StatementInput::Query(stmt.extract::<String>()?)
        };

        let inner = self.inner.clone();

        rust_future_into_py::<_, Py<PyAny>>(py, async move {
            let mut guard = inner.lock().await;
            let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

            let stmt_ref: ZeroPreparedStatement = match stmt_input {
                StatementInput::Query(query) => conn.prepare(&query).await?,
                StatementInput::Prepared(prepared) => prepared,
            };

            let params_adapter = ParamsAdapter::new(&params);

            let result = conn
                .exec_iter(&stmt_ref, params_adapter, |portal| {
                    // Create a channel for fetch requests from the Python callback
                    let (request_tx, request_rx) =
                        std::sync::mpsc::channel::<crate::r#async::unnamed_portal::FetchRequest>();

                    // Create the Python portal wrapper with the request channel
                    let py_portal = AsyncUnnamedPortal::new(request_tx);

                    // Spawn the Python callback on a blocking thread.
                    // This frees the tokio runtime to handle async fetch operations.
                    let callback_handle = std::thread::spawn(move || {
                        Python::attach(|py| {
                            let py_portal_obj = Py::new(py, py_portal)?;
                            callback.call1(py, (py_portal_obj,))
                        })
                    });

                    // SAFETY: The portal reference is valid for the lifetime of the exec_iter
                    // call. The future we return is awaited within exec_iter, so the portal
                    // remains valid for the entire duration of the async operation.
                    // We extend the lifetime to 'static to satisfy the borrow checker.
                    let portal_ptr = portal as *mut zero_postgres::tokio::UnnamedPortal<'_>;
                    let portal_static = unsafe {
                        &mut *(portal_ptr as *mut zero_postgres::tokio::UnnamedPortal<'static>)
                    };

                    // Return a future that handles fetch requests and waits for callback
                    handle_fetch_requests(portal_static, request_rx, callback_handle)
                })
                .await?;
            Ok(result)
        })
    }

    /// Prepare a statement for later execution.
    ///
    /// Returns a PreparedStatement that can be used with exec methods:
    ///
    /// ```python
    /// stmt = await conn.prepare("SELECT * FROM users WHERE id = $1")
    /// row1 = await conn.exec_first(stmt, (1,))
    /// row2 = await conn.exec_first(stmt, (2,))
    /// ```
    fn prepare(&self, py: Python<'_>, query: PyBackedStr) -> PyResult<Py<PyroFuture>> {
        let query_string = query.to_string();
        let inner = self.inner.clone();

        rust_future_into_py(py, async move {
            let mut guard = inner.lock().await;
            let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;
            let stmt = conn.prepare(&query_string).await?;
            Ok(PreparedStatement::new(stmt))
        })
    }

    /// Prepare multiple statements in a single round trip.
    ///
    /// ```python
    /// stmts = await conn.prepare_batch([
    ///     "SELECT * FROM users WHERE id = $1",
    ///     "INSERT INTO logs (msg) VALUES ($1)",
    /// ])
    /// ```
    fn prepare_batch(&self, py: Python<'_>, sqls: Vec<PyBackedStr>) -> PyResult<Py<PyroFuture>> {
        let inner = self.inner.clone();

        rust_future_into_py(py, async move {
            let mut guard = inner.lock().await;
            let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

            let sql_refs: Vec<&str> = sqls.iter().map(|s| &**s).collect();
            let statements = conn.prepare_batch(&sql_refs).await?;

            Python::attach(|py| {
                let list = PyList::new(py, statements.into_iter().map(PreparedStatement::new))?;
                Ok(list.unbind())
            })
        })
    }
}

// Public methods for internal use (not exposed to Python via #[pymethods])
impl AsyncConn {
    pub async fn query_drop_internal(&self, query: String) -> PyroResult<()> {
        let mut guard = self.inner.lock().await;
        let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

        let mut handler = DropHandler::default();
        conn.query(&query, &mut handler).await?;

        Ok(())
    }
}

/// Async helper to handle fetch requests from the Python callback thread.
///
/// This function runs on the tokio runtime and processes fetch requests
/// sent from the Python callback (running on a separate thread) via a channel.
async fn handle_fetch_requests(
    portal: &mut zero_postgres::tokio::UnnamedPortal<'static>,
    request_rx: std::sync::mpsc::Receiver<crate::r#async::unnamed_portal::FetchRequest>,
    callback_handle: std::thread::JoinHandle<PyResult<Py<PyAny>>>,
) -> Result<Py<PyAny>, zero_postgres::Error> {
    use crate::r#async::handler::{DictHandler, TupleHandler};
    use pyo3::types::{PyDict, PyList, PyTuple};

    // Process fetch requests until the callback finishes
    loop {
        // Check if there's a fetch request
        match request_rx.try_recv() {
            Ok(request) => {
                // Perform the async fetch
                let result = if request.as_dict {
                    let mut handler = DictHandler::new();
                    match portal.fetch(request.max_rows, &mut handler).await {
                        Ok(has_more) => Python::attach(|py| {
                            let rows: Vec<Py<PyDict>> = handler.rows_to_python(py)?;
                            let list = PyList::new(py, rows)?;
                            Ok((list.unbind(), has_more))
                        }),
                        Err(e) => Err(e.into()),
                    }
                } else {
                    let mut handler = TupleHandler::new();
                    match portal.fetch(request.max_rows, &mut handler).await {
                        Ok(has_more) => Python::attach(|py| {
                            let rows: Vec<Py<PyTuple>> = handler.rows_to_python(py)?;
                            let list = PyList::new(py, rows)?;
                            Ok((list.unbind(), has_more))
                        }),
                        Err(e) => Err(e.into()),
                    }
                };

                // Send the result back to the Python callback
                let _ = request.response_tx.send(result);
            }
            Err(std::sync::mpsc::TryRecvError::Empty) => {
                // Check if callback thread is done
                if callback_handle.is_finished() {
                    break;
                }
                // Yield to allow other async work
                tokio::task::yield_now().await;
            }
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                // Channel closed, callback must be done
                break;
            }
        }
    }

    // Get the callback result
    callback_handle
        .join()
        .map_err(|_| zero_postgres::Error::Protocol("callback thread panicked".into()))?
        .map_err(|e: PyErr| zero_postgres::Error::Protocol(e.to_string()))
}
