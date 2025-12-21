use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::types::PyList;
use zero_postgres::state::extended::PreparedStatement;
use zero_postgres::sync::Conn;

use crate::error::{Error, PyroResult};
use crate::isolation_level::IsolationLevel;
use crate::opts::resolve_opts;
use crate::params::Params;
use crate::sync::handler::{DictHandler, DropHandler, TupleHandler};
use crate::sync::pipeline::SyncPipeline;
use crate::sync::transaction::SyncTransaction;
use crate::zero_params_adapter::ParamsAdapter;

#[pyclass(module = "pyro_postgres.sync", name = "Conn")]
pub struct SyncConn {
    pub inner: Mutex<Option<Conn>>,
    pub in_transaction: AtomicBool,
    pub stmt_cache: Mutex<HashMap<String, PreparedStatement>>,
    affected_rows: Mutex<Option<u64>>,
}

#[pymethods]
impl SyncConn {
    #[new]
    #[pyo3(signature = (url_or_opts))]
    pub fn new(py: Python<'_>, url_or_opts: &Bound<'_, PyAny>) -> PyroResult<Self> {
        let opts = resolve_opts(py, url_or_opts)?;
        let conn = Conn::new(opts)?;

        Ok(Self {
            inner: Mutex::new(Some(conn)),
            in_transaction: AtomicBool::new(false),
            stmt_cache: Mutex::new(HashMap::new()),
            affected_rows: Mutex::new(None),
        })
    }

    #[pyo3(signature = (isolation_level=None, readonly=None))]
    fn start_transaction(
        slf: Py<Self>,
        isolation_level: Option<&IsolationLevel>,
        readonly: Option<bool>,
    ) -> SyncTransaction {
        let isolation_level_str: Option<String> = isolation_level.map(|l| l.as_str().to_string());
        SyncTransaction::new(slf, isolation_level_str, readonly)
    }

    /// Create a pipeline for batching multiple queries.
    ///
    /// Use as a context manager:
    /// ```python
    /// with conn.pipeline() as p:
    ///     t1 = p.exec("SELECT $1::int", (1,))
    ///     t2 = p.exec("SELECT $1::int", (2,))
    ///     p.sync()
    ///     result1 = p.claim_one(t1)
    ///     result2 = p.claim_collect(t2)
    /// ```
    fn pipeline(slf: Py<Self>) -> SyncPipeline {
        SyncPipeline::new(slf)
    }

    fn id(&self) -> PyroResult<u32> {
        let guard = self.inner.lock();
        let conn = guard.as_ref().ok_or(Error::ConnectionClosedError)?;
        Ok(conn.connection_id())
    }

    fn affected_rows(&self) -> PyResult<Option<u64>> {
        Ok(*self.affected_rows.lock())
    }

    fn ping(&self) -> PyroResult<()> {
        let mut guard = self.inner.lock();
        let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;
        conn.ping()?;
        Ok(())
    }

    // ─── Simple Query Protocol (Text) ───────────────────────────────────────

    #[pyo3(signature = (query, *, as_dict=false))]
    fn query(&self, py: Python<'_>, query: &str, as_dict: bool) -> PyroResult<Vec<Py<PyAny>>> {
        let mut guard = self.inner.lock();
        let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

        if as_dict {
            let mut handler = DictHandler::new(py);
            conn.query(&query, &mut handler)?;
            *self.affected_rows.lock() = handler.rows_affected();
            let rows = handler.into_rows();
            Ok(rows.bind(py).iter().map(pyo3::Bound::unbind).collect())
        } else {
            let mut handler = TupleHandler::new(py);
            conn.query(&query, &mut handler)?;
            *self.affected_rows.lock() = handler.rows_affected();
            let rows = handler.into_rows();
            Ok(rows.bind(py).iter().map(pyo3::Bound::unbind).collect())
        }
    }

    #[pyo3(signature = (query, *, as_dict=false))]
    fn query_first(
        &self,
        py: Python<'_>,
        query: &str,
        as_dict: bool,
    ) -> PyroResult<Option<Py<PyAny>>> {
        let mut guard = self.inner.lock();
        let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

        if as_dict {
            let mut handler = DictHandler::new(py);
            conn.query(&query, &mut handler)?;
            *self.affected_rows.lock() = handler.rows_affected();
            let rows = handler.into_rows();
            Ok(if rows.bind(py).len() > 0 {
                Some(rows.bind(py).get_item(0)?.unbind())
            } else {
                None
            })
        } else {
            let mut handler = TupleHandler::new(py);
            conn.query(&query, &mut handler)?;
            *self.affected_rows.lock() = handler.rows_affected();
            let rows = handler.into_rows();
            Ok(if rows.bind(py).len() > 0 {
                Some(rows.bind(py).get_item(0)?.unbind())
            } else {
                None
            })
        }
    }

    #[pyo3(signature = (query))]
    fn query_drop(&self, query: String) -> PyroResult<()> {
        let mut guard = self.inner.lock();
        let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

        let mut handler = DropHandler::default();
        conn.query(&query, &mut handler)?;

        *self.affected_rows.lock() = handler.rows_affected;
        Ok(())
    }

    // ─── Extended Query Protocol (Binary) ─────────────────────────────────────

    #[pyo3(signature = (query, params=Params::default(), *, as_dict=false))]
    fn exec(
        &self,
        py: Python<'_>,
        query: String,
        params: Params,
        as_dict: bool,
    ) -> PyroResult<Py<PyList>> {
        let mut guard = self.inner.lock();
        let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

        let mut cache = self.stmt_cache.lock();
        if !cache.contains_key(&query) {
            let stmt = conn.prepare(&query)?;
            cache.insert(query.clone(), stmt);
        }
        #[expect(clippy::unwrap_used)]
        let stmt = cache.get(&query).unwrap();

        let params_adapter = ParamsAdapter::new(&params);
        if as_dict {
            let mut handler = DictHandler::new(py);
            conn.exec(stmt, params_adapter, &mut handler)?;
            *self.affected_rows.lock() = handler.rows_affected();
            Ok(handler.into_rows())
        } else {
            let mut handler = TupleHandler::new(py);
            conn.exec(stmt, params_adapter, &mut handler)?;
            *self.affected_rows.lock() = handler.rows_affected();
            Ok(handler.into_rows())
        }
    }

    #[pyo3(signature = (query, params=Params::default(), *, as_dict=false))]
    fn exec_first(
        &self,
        py: Python<'_>,
        query: &str,
        params: Params,
        as_dict: bool,
    ) -> PyroResult<Option<Py<PyAny>>> {
        let mut guard = self.inner.lock();
        let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

        let mut cache = self.stmt_cache.lock();
        if !cache.contains_key(query) {
            let stmt = conn.prepare(&query)?;
            cache.insert(query.to_string(), stmt);
        }
        #[expect(clippy::unwrap_used)]
        let stmt = cache.get(query).unwrap();

        let params_adapter = ParamsAdapter::new(&params);
        if as_dict {
            let mut handler = DictHandler::new(py);
            conn.exec(stmt, params_adapter, &mut handler)?;
            *self.affected_rows.lock() = handler.rows_affected();
            let rows = handler.into_rows();
            Ok(if rows.bind(py).len() > 0 {
                Some(rows.bind(py).get_item(0)?.unbind())
            } else {
                None
            })
        } else {
            let mut handler = TupleHandler::new(py);
            conn.exec(stmt, params_adapter, &mut handler)?;
            *self.affected_rows.lock() = handler.rows_affected();
            let rows = handler.into_rows();
            Ok(if rows.bind(py).len() > 0 {
                Some(rows.bind(py).get_item(0)?.unbind())
            } else {
                None
            })
        }
    }

    #[pyo3(signature = (query, params=Params::default()))]
    fn exec_drop(&self, query: String, params: Params) -> PyroResult<()> {
        let mut guard = self.inner.lock();
        let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

        let mut cache = self.stmt_cache.lock();
        if !cache.contains_key(&query) {
            let stmt = conn.prepare(&query)?;
            cache.insert(query.clone(), stmt);
        }
        #[expect(clippy::unwrap_used)]
        let stmt = cache.get(&query).unwrap();

        let mut handler = DropHandler::default();
        let params_adapter = ParamsAdapter::new(&params);
        conn.exec(stmt, params_adapter, &mut handler)?;

        *self.affected_rows.lock() = handler.rows_affected;
        Ok(())
    }

    #[pyo3(signature = (query, params_list=vec![]))]
    fn exec_batch(&self, query: &str, params_list: Vec<Params>) -> PyroResult<()> {
        let mut guard = self.inner.lock();
        let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

        let mut cache = self.stmt_cache.lock();
        if !cache.contains_key(query) {
            let stmt = conn.prepare(&query)?;
            cache.insert(query.to_string(), stmt);
        }
        #[expect(clippy::unwrap_used)]
        let stmt = cache.get(query).unwrap();

        for params in params_list {
            let mut handler = DropHandler::default();
            let params_adapter = ParamsAdapter::new(&params);
            conn.exec(stmt, params_adapter, &mut handler)?;
            *self.affected_rows.lock() = handler.rows_affected;
        }
        Ok(())
    }

    pub fn close(&self) {
        *self.inner.lock() = None;
    }

    fn server_version(&self) -> PyroResult<String> {
        let guard = self.inner.lock();
        let conn = guard.as_ref().ok_or(Error::ConnectionClosedError)?;
        // Find server_version in params
        for (key, value) in conn.server_params() {
            if key == "server_version" {
                return Ok(value.clone());
            }
        }
        Ok(String::new())
    }
}

// Public methods for internal use (not exposed to Python via #[pymethods])
impl SyncConn {
    pub fn query_drop_internal(&self, query: String) -> PyroResult<()> {
        let mut guard = self.inner.lock();
        let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

        let mut handler = DropHandler::default();
        conn.query(&query, &mut handler)?;

        *self.affected_rows.lock() = handler.rows_affected;
        Ok(())
    }
}
