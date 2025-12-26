use std::sync::atomic::AtomicBool;

use either::Either;
use parking_lot::Mutex;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::PyList;
use zero_postgres::sync::Conn;

use crate::error::{Error, PyroResult};
use crate::isolation_level::IsolationLevel;
use crate::opts::resolve_opts;
use crate::params::Params;
use crate::statement::PreparedStatement;
use crate::sync::handler::{DictHandler, DropHandler, TupleHandler};
use crate::sync::pipeline::SyncPipeline;
use crate::sync::transaction::SyncTransaction;
use crate::sync::unnamed_portal::SyncUnnamedPortal;
use crate::zero_params_adapter::ParamsAdapter;

#[pyclass(module = "pyro_postgres.sync", name = "Conn")]
pub struct SyncConn {
    pub inner: Mutex<Option<Conn>>,
    pub in_transaction: AtomicBool,
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
        })
    }

    #[pyo3(signature = (isolation_level=None, readonly=None))]
    fn tx(
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
            let rows = handler.into_rows();
            Ok(rows.bind(py).iter().map(pyo3::Bound::unbind).collect())
        } else {
            let mut handler = TupleHandler::new(py);
            conn.query(&query, &mut handler)?;
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
            let rows = handler.into_rows();
            Ok(if rows.bind(py).len() > 0 {
                Some(rows.bind(py).get_item(0)?.unbind())
            } else {
                None
            })
        } else {
            let mut handler = TupleHandler::new(py);
            conn.query(&query, &mut handler)?;
            let rows = handler.into_rows();
            Ok(if rows.bind(py).len() > 0 {
                Some(rows.bind(py).get_item(0)?.unbind())
            } else {
                None
            })
        }
    }

    #[pyo3(signature = (query))]
    fn query_drop(&self, query: String) -> PyroResult<u64> {
        let mut guard = self.inner.lock();
        let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

        let mut handler = DropHandler::default();
        conn.query(&query, &mut handler)?;

        Ok(handler.rows_affected.unwrap_or(0))
    }

    // ─── Extended Query Protocol (Binary) ─────────────────────────────────────

    #[pyo3(signature = (stmt, params=Params::default(), *, as_dict=false))]
    fn exec(
        &self,
        py: Python<'_>,
        stmt: Either<PyBackedStr, Py<PreparedStatement>>,
        params: Params,
        as_dict: bool,
    ) -> PyroResult<Py<PyList>> {
        let mut guard = self.inner.lock();
        let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;
        let params_adapter = ParamsAdapter::new(&params);

        match stmt {
            Either::Left(query) => {
                let prepared = conn.prepare(&query)?;
                if as_dict {
                    let mut handler = DictHandler::new(py);
                    conn.exec(&prepared, params_adapter, &mut handler)?;
                    Ok(handler.into_rows())
                } else {
                    let mut handler = TupleHandler::new(py);
                    conn.exec(&prepared, params_adapter, &mut handler)?;
                    Ok(handler.into_rows())
                }
            }
            Either::Right(prepared) => {
                let stmt_ref = &prepared.borrow(py).inner;
                if as_dict {
                    let mut handler = DictHandler::new(py);
                    conn.exec(stmt_ref, params_adapter, &mut handler)?;
                    Ok(handler.into_rows())
                } else {
                    let mut handler = TupleHandler::new(py);
                    conn.exec(stmt_ref, params_adapter, &mut handler)?;
                    Ok(handler.into_rows())
                }
            }
        }
    }

    #[pyo3(signature = (stmt, params=Params::default(), *, as_dict=false))]
    fn exec_first(
        &self,
        py: Python<'_>,
        stmt: Either<PyBackedStr, Py<PreparedStatement>>,
        params: Params,
        as_dict: bool,
    ) -> PyroResult<Option<Py<PyAny>>> {
        let mut guard = self.inner.lock();
        let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;
        let params_adapter = ParamsAdapter::new(&params);

        match stmt {
            Either::Left(query) => {
                let prepared = conn.prepare(&query)?;
                if as_dict {
                    let mut handler = DictHandler::new(py);
                    conn.exec(&prepared, params_adapter, &mut handler)?;
                    let rows = handler.into_rows();
                    Ok(if rows.bind(py).len() > 0 {
                        Some(rows.bind(py).get_item(0)?.unbind())
                    } else {
                        None
                    })
                } else {
                    let mut handler = TupleHandler::new(py);
                    conn.exec(&prepared, params_adapter, &mut handler)?;
                    let rows = handler.into_rows();
                    Ok(if rows.bind(py).len() > 0 {
                        Some(rows.bind(py).get_item(0)?.unbind())
                    } else {
                        None
                    })
                }
            }
            Either::Right(prepared) => {
                let stmt_ref = &prepared.borrow(py).inner;
                if as_dict {
                    let mut handler = DictHandler::new(py);
                    conn.exec(stmt_ref, params_adapter, &mut handler)?;
                    let rows = handler.into_rows();
                    Ok(if rows.bind(py).len() > 0 {
                        Some(rows.bind(py).get_item(0)?.unbind())
                    } else {
                        None
                    })
                } else {
                    let mut handler = TupleHandler::new(py);
                    conn.exec(stmt_ref, params_adapter, &mut handler)?;
                    let rows = handler.into_rows();
                    Ok(if rows.bind(py).len() > 0 {
                        Some(rows.bind(py).get_item(0)?.unbind())
                    } else {
                        None
                    })
                }
            }
        }
    }

    #[pyo3(signature = (stmt, params=Params::default()))]
    fn exec_drop(
        &self,
        py: Python<'_>,
        stmt: Either<PyBackedStr, Py<PreparedStatement>>,
        params: Params,
    ) -> PyroResult<u64> {
        let mut guard = self.inner.lock();
        let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;
        let params_adapter = ParamsAdapter::new(&params);

        match stmt {
            Either::Left(query) => {
                let prepared = conn.prepare(&query)?;
                let mut handler = DropHandler::default();
                conn.exec(&prepared, params_adapter, &mut handler)?;
                Ok(handler.rows_affected.unwrap_or(0))
            }
            Either::Right(prepared) => {
                let stmt_ref = &prepared.borrow(py).inner;
                let mut handler = DropHandler::default();
                conn.exec(stmt_ref, params_adapter, &mut handler)?;
                Ok(handler.rows_affected.unwrap_or(0))
            }
        }
    }

    /// Execute a statement with multiple parameter sets in a batch.
    ///
    /// Uses pipeline mode internally for optimal performance.
    #[pyo3(signature = (stmt, params_list))]
    fn exec_batch(
        &self,
        py: Python<'_>,
        stmt: Either<PyBackedStr, Py<PreparedStatement>>,
        params_list: Vec<Params>,
    ) -> PyroResult<()> {
        let mut guard = self.inner.lock();
        let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;
        let adapters: Vec<_> = params_list.iter().map(ParamsAdapter::new).collect();

        match stmt {
            Either::Left(query) => {
                conn.exec_batch(&*query, &adapters)?;
            }
            Either::Right(prepared) => {
                let stmt_ref = &prepared.borrow(py).inner;
                conn.exec_batch(stmt_ref, &adapters)?;
            }
        }
        Ok(())
    }

    /// Execute a statement and process rows iteratively via a callback.
    ///
    /// The callback receives an `UnnamedPortal` that can fetch rows in batches.
    /// Useful for processing large result sets that don't fit in memory.
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
    /// result = conn.exec_iter("SELECT * FROM large_table", (), process)
    /// ```
    #[pyo3(signature = (stmt, params, callback))]
    fn exec_iter(
        &self,
        py: Python<'_>,
        stmt: Either<PyBackedStr, Py<PreparedStatement>>,
        params: Params,
        callback: Py<PyAny>,
    ) -> PyroResult<Py<PyAny>> {
        let mut guard = self.inner.lock();
        let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;
        let params_adapter = ParamsAdapter::new(&params);

        match stmt {
            Either::Left(query) => {
                let prepared = conn.prepare(&query)?;
                Ok(conn.exec_iter(&prepared, params_adapter, |portal| {
                    let py_portal = unsafe { SyncUnnamedPortal::new(portal) };
                    let py_portal_obj = Py::new(py, py_portal)
                        .map_err(|e| zero_postgres::Error::Protocol(e.to_string()))?;
                    let result = callback
                        .call1(py, (py_portal_obj,))
                        .map_err(|e| zero_postgres::Error::Protocol(e.to_string()))?;
                    Ok(result)
                })?)
            }
            Either::Right(prepared) => {
                let stmt_ref = &prepared.borrow(py).inner;
                Ok(conn.exec_iter(stmt_ref, params_adapter, |portal| {
                    let py_portal = unsafe { SyncUnnamedPortal::new(portal) };
                    let py_portal_obj = Py::new(py, py_portal)
                        .map_err(|e| zero_postgres::Error::Protocol(e.to_string()))?;
                    let result = callback
                        .call1(py, (py_portal_obj,))
                        .map_err(|e| zero_postgres::Error::Protocol(e.to_string()))?;
                    Ok(result)
                })?)
            }
        }
    }

    /// Prepare a statement for later execution.
    ///
    /// Returns a PreparedStatement that can be used with exec methods:
    ///
    /// ```python
    /// stmt = conn.prepare("SELECT * FROM users WHERE id = $1")
    /// row1 = conn.exec_first(stmt, (1,))
    /// row2 = conn.exec_first(stmt, (2,))
    /// ```
    fn prepare(&self, query: &str) -> PyroResult<PreparedStatement> {
        let mut guard = self.inner.lock();
        let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;
        let stmt = conn.prepare(query)?;
        Ok(PreparedStatement::new(stmt))
    }

    /// Prepare multiple statements in a single round trip.
    ///
    /// ```python
    /// stmts = conn.prepare_batch([
    ///     "SELECT * FROM users WHERE id = $1",
    ///     "INSERT INTO logs (msg) VALUES ($1)",
    /// ])
    /// ```
    fn prepare_batch(&self, py: Python<'_>, sqls: Vec<PyBackedStr>) -> PyroResult<Py<PyList>> {
        let mut guard = self.inner.lock();
        let conn = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

        let sql_refs: Vec<&str> = sqls.iter().map(|s| &**s).collect();
        let statements = conn.prepare_batch(&sql_refs)?;
        let list = PyList::new(py, statements.into_iter().map(PreparedStatement::new))?;
        Ok(list.unbind())
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

        Ok(())
    }
}
