use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::{PyAny, PyDict, PyTuple};
use tokio::sync::Mutex;
use zero_postgres::state::extended::PreparedStatement;
use zero_postgres::tokio::Conn;

use crate::r#async::handler::{DictHandler, DropHandler, TupleHandler};
use crate::r#async::pipeline::AsyncPipeline;
use crate::r#async::transaction::AsyncTransaction;
use crate::error::{Error, PyroResult};
use crate::isolation_level::IsolationLevel;
use crate::opts::resolve_opts;
use crate::params::Params;
use crate::statement::Statement;
use crate::util::{PyroFuture, rust_future_into_py};
use crate::zero_params_adapter::ParamsAdapter;

#[pyclass(module = "pyro_postgres.async_", name = "Conn")]
pub struct AsyncConn {
    pub inner: Arc<Mutex<Option<Conn>>>,
    pub in_transaction: AtomicBool,
    pub stmt_cache: Arc<Mutex<HashMap<String, PreparedStatement>>>,
    tuple_handler: Arc<Mutex<TupleHandler>>,
    dict_handler: Arc<Mutex<DictHandler>>,
    affected_rows: Arc<Mutex<Option<u64>>>,
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
                stmt_cache: Arc::new(Mutex::new(HashMap::new())),
                tuple_handler: Arc::new(Mutex::new(TupleHandler::new())),
                dict_handler: Arc::new(Mutex::new(DictHandler::new())),
                affected_rows: Arc::new(Mutex::new(None)),
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

    fn affected_rows(&self, py: Python<'_>) -> PyResult<Py<PyroFuture>> {
        let affected_rows = self.affected_rows.clone();
        rust_future_into_py(py, async move { Ok(*affected_rows.lock().await) })
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
        let affected_rows_arc = self.affected_rows.clone();

        rust_future_into_py::<_, Vec<Py<PyAny>>>(py, async move {
            let mut guard = inner.lock().await;
            let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

            if as_dict {
                let mut handler = dict_handler.lock().await;
                handler.clear();
                conn.query(&query, &mut *handler).await?;
                *affected_rows_arc.lock().await = handler.rows_affected();
                Python::attach(|py| {
                    let rows: Vec<Py<PyDict>> = handler.rows_to_python(py)?;
                    Ok(rows.into_iter().map(pyo3::Py::into_any).collect())
                })
            } else {
                let mut handler = tuple_handler.lock().await;
                handler.clear();
                conn.query(&query, &mut *handler).await?;
                *affected_rows_arc.lock().await = handler.rows_affected();
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
        let affected_rows_arc = self.affected_rows.clone();

        rust_future_into_py::<_, Option<Py<PyAny>>>(py, async move {
            let mut guard = inner.lock().await;
            let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

            if as_dict {
                let mut handler = dict_handler.lock().await;
                handler.clear();
                conn.query(&query, &mut *handler).await?;
                *affected_rows_arc.lock().await = handler.rows_affected();
                Python::attach(|py| {
                    let rows = handler.rows_to_python(py)?;
                    Ok(rows.into_iter().next().map(pyo3::Py::into_any))
                })
            } else {
                let mut handler = tuple_handler.lock().await;
                handler.clear();
                conn.query(&query, &mut *handler).await?;
                *affected_rows_arc.lock().await = handler.rows_affected();
                Python::attach(|py| {
                    let rows = handler.rows_to_python(py)?;
                    Ok(rows.into_iter().next().map(pyo3::Py::into_any))
                })
            }
        })
    }

    fn query_drop(&self, py: Python<'_>, query: String) -> PyResult<Py<PyroFuture>> {
        let inner = self.inner.clone();
        let affected_rows_arc = self.affected_rows.clone();

        rust_future_into_py::<_, ()>(py, async move {
            let mut guard = inner.lock().await;
            let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

            let mut handler = DropHandler::default();
            conn.query(&query, &mut handler).await?;

            *affected_rows_arc.lock().await = handler.rows_affected;
            Ok(())
        })
    }

    // ─── Extended Query Protocol (Binary) ─────────────────────────────────────

    #[pyo3(signature = (query, params=None, *, as_dict=false))]
    fn exec(
        &self,
        py: Python<'_>,
        query: PyBackedStr,
        params: Option<Py<PyAny>>,
        as_dict: bool,
    ) -> PyResult<Py<PyroFuture>> {
        let params_obj: Params = params
            .map(|p| p.extract(py))
            .transpose()?
            .unwrap_or_default();
        let query_string = query.to_string();

        let inner = self.inner.clone();
        let stmt_cache = self.stmt_cache.clone();
        let tuple_handler = self.tuple_handler.clone();
        let dict_handler = self.dict_handler.clone();
        let affected_rows_arc = self.affected_rows.clone();

        rust_future_into_py::<_, Vec<Py<PyAny>>>(py, async move {
            let mut guard = inner.lock().await;
            let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

            let mut cache = stmt_cache.lock().await;
            if !cache.contains_key(&query_string) {
                let stmt = conn.prepare(&query_string).await?;
                cache.insert(query_string.clone(), stmt);
            }
            #[expect(clippy::unwrap_used)]
            let stmt = cache.get(&query_string).unwrap();

            let params_adapter = ParamsAdapter::new(&params_obj);
            if as_dict {
                let mut handler = dict_handler.lock().await;
                handler.clear();
                conn.exec(stmt, params_adapter, &mut *handler).await?;
                *affected_rows_arc.lock().await = handler.rows_affected();
                Python::attach(|py| {
                    let rows: Vec<Py<PyDict>> = handler.rows_to_python(py)?;
                    Ok(rows.into_iter().map(pyo3::Py::into_any).collect())
                })
            } else {
                let mut handler = tuple_handler.lock().await;
                handler.clear();
                conn.exec(stmt, params_adapter, &mut *handler).await?;
                *affected_rows_arc.lock().await = handler.rows_affected();
                Python::attach(|py| {
                    let rows: Vec<Py<PyTuple>> = handler.rows_to_python(py)?;
                    Ok(rows.into_iter().map(pyo3::Py::into_any).collect())
                })
            }
        })
    }

    #[pyo3(signature = (query, params=None, *, as_dict=false))]
    fn exec_first(
        &self,
        py: Python<'_>,
        query: PyBackedStr,
        params: Option<Py<PyAny>>,
        as_dict: bool,
    ) -> PyResult<Py<PyroFuture>> {
        let params_obj: Params = params
            .map(|p| p.extract(py))
            .transpose()?
            .unwrap_or_default();
        let query_string = query.to_string();

        let inner = self.inner.clone();
        let stmt_cache = self.stmt_cache.clone();
        let tuple_handler = self.tuple_handler.clone();
        let dict_handler = self.dict_handler.clone();
        let affected_rows_arc = self.affected_rows.clone();

        rust_future_into_py::<_, Option<Py<PyAny>>>(py, async move {
            let mut guard = inner.lock().await;
            let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

            let mut cache = stmt_cache.lock().await;
            if !cache.contains_key(&query_string) {
                let stmt = conn.prepare(&query_string).await?;
                cache.insert(query_string.clone(), stmt);
            }
            #[expect(clippy::unwrap_used)]
            let stmt = cache.get(&query_string).unwrap();

            let params_adapter = ParamsAdapter::new(&params_obj);
            if as_dict {
                let mut handler = dict_handler.lock().await;
                handler.clear();
                conn.exec(stmt, params_adapter, &mut *handler).await?;
                *affected_rows_arc.lock().await = handler.rows_affected();
                Python::attach(|py| {
                    let rows = handler.rows_to_python(py)?;
                    Ok(rows.into_iter().next().map(pyo3::Py::into_any))
                })
            } else {
                let mut handler = tuple_handler.lock().await;
                handler.clear();
                conn.exec(stmt, params_adapter, &mut *handler).await?;
                *affected_rows_arc.lock().await = handler.rows_affected();
                Python::attach(|py| {
                    let rows = handler.rows_to_python(py)?;
                    Ok(rows.into_iter().next().map(pyo3::Py::into_any))
                })
            }
        })
    }

    #[pyo3(signature = (query, params=None))]
    fn exec_drop(
        &self,
        py: Python<'_>,
        query: PyBackedStr,
        params: Option<Py<PyAny>>,
    ) -> PyResult<Py<PyroFuture>> {
        let params_obj: Params = params
            .map(|p| p.extract(py))
            .transpose()?
            .unwrap_or_default();
        let query_string = query.to_string();

        let inner = self.inner.clone();
        let stmt_cache = self.stmt_cache.clone();
        let affected_rows_arc = self.affected_rows.clone();

        rust_future_into_py::<_, ()>(py, async move {
            let mut guard = inner.lock().await;
            let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

            let mut cache = stmt_cache.lock().await;
            if !cache.contains_key(&query_string) {
                let stmt = conn.prepare(&query_string).await?;
                cache.insert(query_string.clone(), stmt);
            }
            #[expect(clippy::unwrap_used)]
            let stmt = cache.get(&query_string).unwrap();

            let mut handler = DropHandler::default();
            let params_adapter = ParamsAdapter::new(&params_obj);
            conn.exec(stmt, params_adapter, &mut handler).await?;

            *affected_rows_arc.lock().await = handler.rows_affected;
            Ok(())
        })
    }

    /// Execute a statement with multiple parameter sets in a batch.
    ///
    /// Uses pipeline mode internally for optimal performance.
    #[pyo3(signature = (query, params_list))]
    fn exec_batch(
        &self,
        py: Python<'_>,
        query: PyBackedStr,
        params_list: Vec<Py<PyAny>>,
    ) -> PyResult<Py<PyroFuture>> {
        let mut params_vec = Vec::new();
        for p in params_list {
            params_vec.push(p.extract::<Params>(py)?);
        }
        let query_string = query.to_string();

        let inner = self.inner.clone();

        rust_future_into_py::<_, ()>(py, async move {
            let mut guard = inner.lock().await;
            let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

            let adapters: Vec<_> = params_vec.iter().map(ParamsAdapter::new).collect();
            conn.exec_batch(query_string.as_str(), &adapters).await?;
            Ok(())
        })
    }

    /// Prepare a statement for later execution.
    ///
    /// Returns a Statement that can be used with `pipeline.exec()`:
    ///
    /// ```python
    /// stmt = await conn.prepare("INSERT INTO users (name) VALUES ($1)")
    /// async with conn.pipeline() as p:
    ///     t1 = await p.exec(stmt, ("Alice",))
    ///     t2 = await p.exec(stmt, ("Bob",))
    ///     await p.sync()
    ///     await p.claim_drop(t1)
    ///     await p.claim_drop(t2)
    /// ```
    fn prepare(&self, py: Python<'_>, query: PyBackedStr) -> PyResult<Py<PyroFuture>> {
        let query_string = query.to_string();
        let inner = self.inner.clone();
        let stmt_cache = self.stmt_cache.clone();

        rust_future_into_py(py, async move {
            let mut guard = inner.lock().await;
            let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

            let mut cache = stmt_cache.lock().await;
            if let Some(stmt) = cache.get(&query_string) {
                return Ok(Statement::new(stmt.clone()));
            }

            let stmt = conn.prepare(&query_string).await?;
            cache.insert(query_string, stmt.clone());
            Ok(Statement::new(stmt))
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

        *self.affected_rows.lock().await = handler.rows_affected;
        Ok(())
    }
}
