//! Python wrapper for async NamedPortal.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::r#async::conn::AsyncConn;
use crate::r#async::handler::{DictHandler, TupleHandler};
use crate::error::Error;
use crate::util::{PyroFuture, rust_future_into_py};

/// Python wrapper for an async named portal.
///
/// Named portals allow interleaving multiple row streams. Unlike unnamed portals
/// (used in exec_iter), named portals can be executed multiple times and can
/// coexist with other portals.
///
/// Created by `Conn.exec_portal()`. Use `execute_collect()` to fetch rows,
/// `is_complete()` to check if all rows have been fetched, and `close()` to
/// clean up the portal.
#[pyclass(module = "pyro_postgres.async_", name = "NamedPortal")]
pub struct AsyncNamedPortal {
    /// The portal name on the server
    name: String,
    /// Whether all rows have been fetched
    complete: bool,
}

impl AsyncNamedPortal {
    /// Create a new named portal wrapper.
    pub fn new(name: String) -> Self {
        Self {
            name,
            complete: false,
        }
    }
}

#[pymethods]
impl AsyncNamedPortal {
    /// Execute the portal and collect up to `max_rows` rows.
    ///
    /// Returns a tuple of (rows, has_more) where:
    /// - rows: list of tuples (or dicts if as_dict=True)
    /// - has_more: True if more rows are available
    ///
    /// Use max_rows=0 to fetch all remaining rows at once.
    #[pyo3(signature = (conn, max_rows, *, as_dict=false))]
    fn execute_collect(
        &mut self,
        py: Python<'_>,
        conn: Py<AsyncConn>,
        max_rows: u32,
        as_dict: bool,
    ) -> PyResult<Py<PyroFuture>> {
        let name = self.name.clone();
        // Access inner through Python::attach pattern
        let inner = Python::attach(|py| conn.bind(py).borrow().inner.clone());

        rust_future_into_py::<_, (Py<PyList>, bool)>(py, async move {
            let mut guard = inner.lock().await;
            let conn_inner = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

            if as_dict {
                let mut handler = DictHandler::new();
                let has_more = conn_inner
                    .lowlevel_execute(&name, max_rows, &mut handler)
                    .await?;
                Python::attach(|py| {
                    let rows: Vec<Py<PyDict>> = handler.rows_to_python(py)?;
                    let list = PyList::new(py, rows)?;
                    Ok((list.unbind(), has_more))
                })
            } else {
                let mut handler = TupleHandler::new();
                let has_more = conn_inner
                    .lowlevel_execute(&name, max_rows, &mut handler)
                    .await?;
                Python::attach(|py| {
                    let rows: Vec<pyo3::Py<pyo3::types::PyTuple>> = handler.rows_to_python(py)?;
                    let list = PyList::new(py, rows)?;
                    Ok((list.unbind(), has_more))
                })
            }
        })
    }

    /// Check if all rows have been fetched from this portal.
    ///
    /// Returns True if the last `execute_collect()` call fetched all remaining rows.
    ///
    /// Note: For async, use the `has_more` return value from `execute_collect()` instead,
    /// as this property cannot be updated from async operations.
    fn is_complete(&self) -> bool {
        self.complete
    }

    /// Close the portal, releasing server resources.
    ///
    /// After closing, the portal cannot be used for further fetching.
    /// It's good practice to close portals when done, though they will
    /// also be closed when the transaction ends.
    fn close(&mut self, py: Python<'_>, conn: Py<AsyncConn>) -> PyResult<Py<PyroFuture>> {
        let name = self.name.clone();
        // Access inner through Python::attach pattern
        let inner = Python::attach(|py| conn.bind(py).borrow().inner.clone());

        rust_future_into_py::<_, ()>(py, async move {
            let mut guard = inner.lock().await;
            let conn_inner = guard.as_mut().ok_or(Error::ConnectionClosedError)?;

            conn_inner.lowlevel_close_portal(&name).await?;
            Ok(())
        })
    }
}
