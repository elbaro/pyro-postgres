//! Shared pipeline ticket type.

use pyo3::prelude::*;
use zero_postgres::Ticket;

/// Python wrapper for pipeline Ticket.
///
/// SAFETY: We transmute the Ticket lifetime to 'static because the Pipeline
/// stores Py<PreparedStatement> objects that keep the PreparedStatements alive.
#[pyclass(module = "pyro_postgres", name = "Ticket", frozen)]
#[derive(Clone, Copy)]
pub struct PyTicket {
    pub(crate) inner: Ticket<'static>,
}

impl PyTicket {
    /// Create a new PyTicket from a Ticket.
    ///
    /// # Safety
    /// The caller must ensure that the PreparedStatement referenced by the Ticket
    /// (if any) is kept alive for the ticket's lifetime (e.g., stored in Pipeline state).
    pub unsafe fn new(ticket: Ticket<'_>) -> Self {
        Self {
            inner: unsafe { std::mem::transmute(ticket) },
        }
    }
}
