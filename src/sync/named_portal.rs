//! Python wrapper for sync NamedPortal.

use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::error::PyroResult;
use crate::sync::conn::SyncConn;
use crate::sync::handler::{DictHandler, TupleHandler};

/// Python wrapper for a named portal.
///
/// Named portals allow interleaving multiple row streams. Unlike unnamed portals
/// (used in exec_iter), named portals can be executed multiple times and can
/// coexist with other portals.
///
/// Created by `Conn.exec_portal()`. Use `execute_collect()` to fetch rows,
/// `is_complete()` to check if all rows have been fetched, and `close()` to
/// clean up the portal.
#[pyclass(module = "pyro_postgres.sync", name = "NamedPortal")]
pub struct SyncNamedPortal {
    /// The portal name on the server
    name: String,
    /// Whether all rows have been fetched
    complete: bool,
}

impl SyncNamedPortal {
    /// Create a new named portal wrapper.
    pub fn new(name: String) -> Self {
        Self {
            name,
            complete: false,
        }
    }
}

#[pymethods]
impl SyncNamedPortal {
    /// Execute the portal and collect up to `max_rows` rows.
    ///
    /// Returns a list of tuples (or dicts if as_dict=True).
    /// Use max_rows=0 to fetch all remaining rows at once.
    ///
    /// After this call, check `is_complete()` to see if more rows are available.
    #[pyo3(signature = (conn, max_rows, *, as_dict=false))]
    fn execute_collect(
        &mut self,
        py: Python<'_>,
        conn: &SyncConn,
        max_rows: u32,
        as_dict: bool,
    ) -> PyroResult<Py<PyList>> {
        let mut guard = conn.inner.lock();
        let inner = guard
            .as_mut()
            .ok_or(crate::error::Error::ConnectionClosedError)?;

        if as_dict {
            let mut handler = DictHandler::new(py);
            let has_more = inner.lowlevel_execute(&self.name, max_rows, &mut handler)?;
            self.complete = !has_more;
            Ok(handler.into_rows())
        } else {
            let mut handler = TupleHandler::new(py);
            let has_more = inner.lowlevel_execute(&self.name, max_rows, &mut handler)?;
            self.complete = !has_more;
            Ok(handler.into_rows())
        }
    }

    /// Check if all rows have been fetched from this portal.
    ///
    /// Returns True if the last `execute_collect()` call fetched all remaining rows.
    fn is_complete(&self) -> bool {
        self.complete
    }

    /// Close the portal, releasing server resources.
    ///
    /// After closing, the portal cannot be used for further fetching.
    /// It's good practice to close portals when done, though they will
    /// also be closed when the transaction ends.
    fn close(&mut self, conn: &SyncConn) -> PyroResult<()> {
        let mut guard = conn.inner.lock();
        let inner = guard
            .as_mut()
            .ok_or(crate::error::Error::ConnectionClosedError)?;

        inner.lowlevel_close_portal(&self.name)?;
        Ok(())
    }
}
