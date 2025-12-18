use pyo3::exceptions::PyException;
use pyo3::prelude::*;

// Define Python exception types
pyo3::create_exception!(pyro_postgres.error, IncorrectApiUsageError, PyException);
pyo3::create_exception!(pyro_postgres.error, UrlError, PyException);
pyo3::create_exception!(pyro_postgres.error, PostgresError, PyException);
pyo3::create_exception!(pyro_postgres.error, ConnectionClosedError, PyException);
pyo3::create_exception!(pyro_postgres.error, TransactionClosedError, PyException);
pyo3::create_exception!(pyro_postgres.error, DecodeError, PyException);
pyo3::create_exception!(pyro_postgres.error, PoisonError, PyException);
pyo3::create_exception!(pyro_postgres.error, PythonObjectCreationError, PyException);

/// Internal error type for pyro-postgres
#[derive(Debug)]
pub enum Error {
    IncorrectApiUsageError(&'static str),
    UrlError(String),
    PostgresError(String),
    ConnectionClosedError,
    TransactionClosedError,
    DecodeError(String),
    PoisonError(String),
    PythonObjectCreationError(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::IncorrectApiUsageError(msg) => write!(f, "Incorrect API usage: {msg}"),
            Error::UrlError(msg) => write!(f, "URL error: {msg}"),
            Error::PostgresError(msg) => write!(f, "Postgres error: {msg}"),
            Error::ConnectionClosedError => write!(f, "Connection is closed"),
            Error::TransactionClosedError => write!(f, "Transaction is closed"),
            Error::DecodeError(msg) => write!(f, "Decode error: {msg}"),
            Error::PoisonError(msg) => write!(f, "Poison error: {msg}"),
            Error::PythonObjectCreationError(msg) => {
                write!(f, "Python object creation error: {msg}")
            }
        }
    }
}

impl std::error::Error for Error {}

impl From<Error> for PyErr {
    fn from(err: Error) -> Self {
        match err {
            Error::IncorrectApiUsageError(msg) => IncorrectApiUsageError::new_err(msg),
            Error::UrlError(msg) => UrlError::new_err(msg),
            Error::PostgresError(msg) => PostgresError::new_err(msg),
            Error::ConnectionClosedError => ConnectionClosedError::new_err("Connection is closed"),
            Error::TransactionClosedError => {
                TransactionClosedError::new_err("Transaction is closed")
            }
            Error::DecodeError(msg) => DecodeError::new_err(msg),
            Error::PoisonError(msg) => PoisonError::new_err(msg),
            Error::PythonObjectCreationError(msg) => PythonObjectCreationError::new_err(msg),
        }
    }
}

impl From<zero_postgres::Error> for Error {
    fn from(err: zero_postgres::Error) -> Self {
        Error::PostgresError(err.to_string())
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(err: std::str::Utf8Error) -> Self {
        Error::DecodeError(err.to_string())
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(err: std::num::ParseIntError) -> Self {
        Error::DecodeError(err.to_string())
    }
}

impl From<std::num::ParseFloatError> for Error {
    fn from(err: std::num::ParseFloatError) -> Self {
        Error::DecodeError(err.to_string())
    }
}

impl From<PyErr> for Error {
    fn from(err: PyErr) -> Self {
        Error::PythonObjectCreationError(err.to_string())
    }
}

/// Result type alias for pyro-postgres
pub type PyroResult<T> = Result<T, Error>;
