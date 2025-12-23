#![allow(async_fn_in_trait)]

pub mod r#async;
pub mod error;
pub mod from_wire_value;
pub mod isolation_level;
pub mod opts;
pub mod params;
pub mod py_imports;
pub mod statement;
pub mod sync;
pub mod ticket;
pub mod tokio_thread;
pub mod util;
pub mod value;
pub mod zero_params_adapter;

use pyo3::prelude::*;

use crate::{
    isolation_level::IsolationLevel,
    opts::Opts,
    r#async::{conn::AsyncConn, pipeline::AsyncPipeline, transaction::AsyncTransaction},
    statement::Statement,
    sync::{conn::SyncConn, pipeline::SyncPipeline, transaction::SyncTransaction},
    ticket::PyTicket,
    util::PyroFuture,
    value::{PyJson, PyJsonb},
};

#[pyfunction]
/// Initialize the Tokio runtime thread.
/// This is called automatically when the module is loaded.
/// Note: `worker_threads` and `thread_name` parameters are ignored since we use a single-threaded runtime.
#[pyo3(signature = (worker_threads=None, thread_name=None))]
fn init(worker_threads: Option<usize>, thread_name: Option<&str>) {
    // Initialize the global TokioThread
    let _ = tokio_thread::get_tokio_thread();

    // Suppress unused variable warnings
    let _ = worker_threads;
    let _ = thread_name;
}

/// A Python module implemented in Rust.
#[pymodule(gil_used = false)]
mod pyro_postgres {
    use super::*;

    #[pymodule_export]
    use super::init;

    #[pymodule_export]
    use super::IsolationLevel;

    #[pymodule_export]
    use super::Opts;

    #[pymodule_export]
    use super::PyroFuture;

    #[pymodule_export]
    use super::PyJson;

    #[pymodule_export]
    use super::PyJsonb;

    #[pymodule_export]
    use super::Statement;

    #[pymodule_export]
    use super::PyTicket;

    #[pymodule]
    mod error {
        use crate::error as error_types;

        #[pymodule_export]
        use error_types::IncorrectApiUsageError;

        #[pymodule_export]
        use error_types::UrlError;

        #[pymodule_export]
        use error_types::PostgresError;

        #[pymodule_export]
        use error_types::ConnectionClosedError;

        #[pymodule_export]
        use error_types::TransactionClosedError;

        #[pymodule_export]
        use error_types::DecodeError;

        #[pymodule_export]
        use error_types::PoisonError;

        #[pymodule_export]
        use error_types::PythonObjectCreationError;
    }

    #[pymodule]
    mod async_ {
        #[pymodule_export]
        use crate::r#async::conn::AsyncConn;

        #[pymodule_export]
        use crate::r#async::pipeline::AsyncPipeline;

        #[pymodule_export]
        use crate::r#async::transaction::AsyncTransaction;
    }

    #[pymodule]
    mod sync {
        #[pymodule_export]
        use crate::sync::conn::SyncConn;

        #[pymodule_export]
        use crate::sync::pipeline::SyncPipeline;

        #[pymodule_export]
        use crate::sync::transaction::SyncTransaction;
    }

    #[pymodule_init]
    fn module_init(m: &Bound<'_, PyModule>) -> PyResult<()> {
        pyo3_log::init();

        if cfg!(debug_assertions) {
            log::debug!("Running in Debug mode.");
        } else {
            log::debug!("Running in Release mode.");
        }

        super::init(None, None);

        // Alias
        Python::attach(|py| {
            m.add("Opts", py.get_type::<Opts>())?;
            m.add("Ticket", py.get_type::<PyTicket>())?;
            m.add("AsyncConn", py.get_type::<AsyncConn>())?;
            m.add("AsyncPipeline", py.get_type::<AsyncPipeline>())?;
            m.add("AsyncTransaction", py.get_type::<AsyncTransaction>())?;
            m.add("SyncConn", py.get_type::<SyncConn>())?;
            m.add("SyncPipeline", py.get_type::<SyncPipeline>())?;
            m.add("SyncTransaction", py.get_type::<SyncTransaction>())?;
            m.add("Json", py.get_type::<PyJson>())?;
            m.add("Jsonb", py.get_type::<PyJsonb>())?;
            m.add("Statement", py.get_type::<Statement>())?;
            PyResult::Ok(())
        })?;

        let py = m.py();
        let sys_modules = py.import("sys")?.getattr("modules")?;
        for name in ["error", "sync", "async_"] {
            let module = m.getattr(name)?;
            module.setattr("__name__", format!("pyro_postgres.{name}"))?;
            sys_modules.set_item(format!("pyro_postgres.{module}"), module)?;
        }

        Ok(())
    }
}
