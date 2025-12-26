//! Python wrapper for async UnnamedPortal.
//!
//! Uses a channel-based approach to communicate between the Python callback
//! (running on a blocking thread) and the tokio runtime (handling async fetch).

use std::sync::mpsc;

use pyo3::prelude::*;
use pyo3::types::PyList;
use tokio::sync::oneshot;

use crate::error::PyroResult;

/// Request sent from Python callback to the async runtime
pub struct FetchRequest {
    pub max_rows: u32,
    pub as_dict: bool,
    pub response_tx: oneshot::Sender<PyroResult<(Py<PyList>, bool)>>,
}

/// Python wrapper for an async unnamed portal.
///
/// This allows iterative row fetching within an `exec_iter` callback.
/// The portal communicates with the async runtime via channels.
#[pyclass(module = "pyro_postgres.async_", name = "UnnamedPortal", unsendable)]
pub struct AsyncUnnamedPortal {
    /// Channel to send fetch requests to the async runtime
    request_tx: mpsc::Sender<FetchRequest>,
}

impl AsyncUnnamedPortal {
    /// Create a new wrapper with a request channel.
    pub fn new(request_tx: mpsc::Sender<FetchRequest>) -> Self {
        Self { request_tx }
    }
}

#[pymethods]
impl AsyncUnnamedPortal {
    /// Fetch up to `max_rows` rows from the portal.
    ///
    /// Returns a tuple of (rows, has_more) where:
    /// - rows: list of tuples (or dicts if as_dict=True)
    /// - has_more: True if more rows are available
    ///
    /// Use max_rows=0 to fetch all remaining rows at once.
    #[pyo3(signature = (max_rows, *, as_dict=false))]
    fn fetch(
        &mut self,
        py: Python<'_>,
        max_rows: u32,
        as_dict: bool,
    ) -> PyroResult<(Py<PyList>, bool)> {
        // Create a oneshot channel for the response
        let (response_tx, response_rx) = oneshot::channel();

        // Send the request to the async runtime
        let request = FetchRequest {
            max_rows,
            as_dict,
            response_tx,
        };

        self.request_tx
            .send(request)
            .map_err(|_| crate::error::Error::ConnectionClosedError)?;

        // Release the GIL and wait for the response
        py.detach(|| {
            response_rx
                .blocking_recv()
                .map_err(|_| crate::error::Error::ConnectionClosedError)?
        })
    }
}
