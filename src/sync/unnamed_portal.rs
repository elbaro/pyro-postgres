//! Python wrapper for sync UnnamedPortal.

use std::ptr::NonNull;

use pyo3::prelude::*;
use pyo3::types::PyList;
use zero_postgres::sync::UnnamedPortal;

use crate::error::PyroResult;
use crate::sync::handler::{DictHandler, TupleHandler};

/// Python wrapper for an unnamed portal.
///
/// This allows iterative row fetching within an `exec_iter` callback.
/// The portal is only valid during the callback execution.
#[pyclass(module = "pyro_postgres.sync", name = "UnnamedPortal", unsendable)]
pub struct SyncUnnamedPortal {
    /// Raw pointer to the underlying portal.
    /// SAFETY: This is only valid during the exec_iter callback.
    portal: NonNull<UnnamedPortal<'static>>,
}

impl SyncUnnamedPortal {
    /// Create a new wrapper from a mutable reference to a portal.
    ///
    /// # Safety
    /// The caller must ensure that:
    /// - The portal reference remains valid for the lifetime of this wrapper
    /// - The wrapper is not used after the exec_iter callback returns
    pub unsafe fn new(portal: &mut UnnamedPortal<'_>) -> Self {
        // Cast away the lifetime - safe as long as we only use this within the callback
        let portal_ptr = portal as *mut UnnamedPortal<'_> as *mut UnnamedPortal<'static>;
        // SAFETY: portal_ptr is derived from a valid mutable reference, so it's non-null
        Self {
            portal: unsafe { NonNull::new_unchecked(portal_ptr) },
        }
    }
}

#[pymethods]
impl SyncUnnamedPortal {
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
        // SAFETY: This is only called during the exec_iter callback,
        // so the portal pointer is still valid.
        let portal = unsafe { self.portal.as_mut() };

        if as_dict {
            let mut handler = DictHandler::new(py);
            let has_more = portal.fetch(max_rows, &mut handler)?;
            Ok((handler.into_rows(), has_more))
        } else {
            let mut handler = TupleHandler::new(py);
            let has_more = portal.fetch(max_rows, &mut handler)?;
            Ok((handler.into_rows(), has_more))
        }
    }
}
