use pyo3::prelude::*;
use pyo3::sync::GILOnceCell;
use pyo3::types::PyType;

static DATE_CLASS: GILOnceCell<Py<PyType>> = GILOnceCell::new();
static DATETIME_CLASS: GILOnceCell<Py<PyType>> = GILOnceCell::new();
static TIME_CLASS: GILOnceCell<Py<PyType>> = GILOnceCell::new();
static TIMEDELTA_CLASS: GILOnceCell<Py<PyType>> = GILOnceCell::new();
static DECIMAL_CLASS: GILOnceCell<Py<PyType>> = GILOnceCell::new();
static UUID_CLASS: GILOnceCell<Py<PyType>> = GILOnceCell::new();
static JSON_MODULE: GILOnceCell<Py<PyModule>> = GILOnceCell::new();

pub fn get_date_class(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    DATE_CLASS
        .get_or_try_init(py, || {
            let datetime = py.import("datetime")?;
            let cls = datetime.getattr("date")?.downcast_into::<PyType>()?;
            Ok(cls.unbind())
        })
        .map(|cls| cls.bind(py))
}

pub fn get_datetime_class(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    DATETIME_CLASS
        .get_or_try_init(py, || {
            let datetime = py.import("datetime")?;
            let cls = datetime.getattr("datetime")?.downcast_into::<PyType>()?;
            Ok(cls.unbind())
        })
        .map(|cls| cls.bind(py))
}

pub fn get_time_class(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    TIME_CLASS
        .get_or_try_init(py, || {
            let datetime = py.import("datetime")?;
            let cls = datetime.getattr("time")?.downcast_into::<PyType>()?;
            Ok(cls.unbind())
        })
        .map(|cls| cls.bind(py))
}

pub fn get_timedelta_class(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    TIMEDELTA_CLASS
        .get_or_try_init(py, || {
            let datetime = py.import("datetime")?;
            let cls = datetime.getattr("timedelta")?.downcast_into::<PyType>()?;
            Ok(cls.unbind())
        })
        .map(|cls| cls.bind(py))
}

pub fn get_decimal_class(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    DECIMAL_CLASS
        .get_or_try_init(py, || {
            let decimal = py.import("decimal")?;
            let cls = decimal.getattr("Decimal")?.downcast_into::<PyType>()?;
            Ok(cls.unbind())
        })
        .map(|cls| cls.bind(py))
}

pub fn get_uuid_class(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    UUID_CLASS
        .get_or_try_init(py, || {
            let uuid = py.import("uuid")?;
            let cls = uuid.getattr("UUID")?.downcast_into::<PyType>()?;
            Ok(cls.unbind())
        })
        .map(|cls| cls.bind(py))
}

pub fn get_json_module(py: Python<'_>) -> PyResult<&Bound<'_, PyModule>> {
    JSON_MODULE
        .get_or_try_init(py, || {
            let json = py.import("json")?;
            Ok(json.unbind())
        })
        .map(|m| m.bind(py))
}
