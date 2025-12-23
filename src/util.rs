use std::future::Future;

use pyo3::IntoPyObjectExt;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

use crate::error::PyroResult;

pub type PyroFuture = PyAny;

/// Iterator wrapper that keeps `RaiiFuture` alive during iteration
#[pyclass]
struct PyroFutureIterator {
    iterator: Py<PyAny>,
    _future: Py<PyroFuture>, // Keep the future alive
}

#[pymethods]
impl PyroFutureIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.iterator.bind(py).call_method0("__next__")
    }

    fn send<'py>(&self, py: Python<'py>, value: Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        self.iterator.bind(py).call_method1("send", (value,))
    }

    #[pyo3(signature = (*args))]
    fn throw<'py>(
        &self,
        py: Python<'py>,
        args: &'py Bound<'_, PyTuple>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.iterator.bind(py).call_method1("throw", args)
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        match self.iterator.bind(py).call_method0("close") {
            Ok(_) => Ok(()),
            Err(e) if e.is_instance_of::<pyo3::exceptions::PyAttributeError>(py) => Ok(()),
            Err(e) => Err(e),
        }
    }
}

/// Convert a Rust future into a Python awaitable.
pub fn rust_future_into_py<F, T>(py: Python<'_>, fut: F) -> PyResult<Py<PyroFuture>>
where
    F: Future<Output = PyroResult<T>> + Send + 'static,
    T: for<'py> IntoPyObject<'py> + Send + 'static,
{
    let event_loop = pyo3_async_runtimes::get_running_loop(py)?;

    // Because the event loop can be changed, these attributes are not cached.
    let create_future = event_loop.getattr(intern!(py, "create_future"))?.unbind();
    let call_soon_threadsafe = event_loop
        .getattr(intern!(py, "call_soon_threadsafe"))?
        .unbind();

    let py_future = create_future.call0(py)?;
    {
        let py_future = py_future.clone_ref(py);
        crate::tokio_thread::get_tokio_thread().spawn(async move {
            let result = fut.await;

            Python::attach(|py| {
                let bound_future = py_future.bind(py);
                match result {
                    Ok(value) => {
                        call_soon_threadsafe
                            .call1(
                                py,
                                (
                                    bound_future
                                        .getattr(intern!(py, "set_result"))
                                        .expect("set_result"),
                                    value.into_py_any(py).expect("into_py_any"),
                                ),
                            )
                            .expect("call_soon_threadsafe");
                    }
                    Err(err) => {
                        call_soon_threadsafe
                            .call1(
                                py,
                                (
                                    bound_future
                                        .getattr(intern!(py, "set_exception"))
                                        .expect("set_exception"),
                                    pyo3::PyErr::from(err)
                                        .into_bound_py_any(py)
                                        .expect("into_bound_py_any"),
                                ),
                            )
                            .expect("call_soon_threadsafe");
                    }
                }
            });
        });
    }

    Ok(py_future)
}

pub struct PyTupleBuilder {
    ptr: *mut pyo3::ffi::PyObject,
}

impl PyTupleBuilder {
    pub fn new(_py: Python, len: usize) -> Self {
        let ptr = unsafe { pyo3::ffi::PyTuple_New(len as isize) };
        Self { ptr }
    }

    pub fn set(&self, index: usize, value: Bound<'_, PyAny>) {
        unsafe {
            pyo3::ffi::PyTuple_SetItem(self.ptr, index as pyo3::ffi::Py_ssize_t, value.into_ptr());
        }
    }

    pub fn build(self, py: Python<'_>) -> Bound<'_, PyTuple> {
        unsafe { Bound::from_owned_ptr(py, self.ptr).cast_into_unchecked() }
    }
}
