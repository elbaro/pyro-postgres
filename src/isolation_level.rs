use pyo3::prelude::*;

/// `PostgreSQL` transaction isolation levels.
#[pyclass(module = "pyro_postgres", name = "IsolationLevel", eq)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

#[pymethods]
impl IsolationLevel {
    #[new]
    fn new(level: &str) -> PyResult<Self> {
        match level.to_lowercase().as_str() {
            "read uncommitted" | "read_uncommitted" | "readuncommitted" => {
                Ok(IsolationLevel::ReadUncommitted)
            }
            "read committed" | "read_committed" | "readcommitted" => {
                Ok(IsolationLevel::ReadCommitted)
            }
            "repeatable read" | "repeatable_read" | "repeatableread" => {
                Ok(IsolationLevel::RepeatableRead)
            }
            "serializable" => Ok(IsolationLevel::Serializable),
            _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Unknown isolation level: {level}"
            ))),
        }
    }

    #[staticmethod]
    fn read_uncommitted() -> Self {
        IsolationLevel::ReadUncommitted
    }

    #[staticmethod]
    fn read_committed() -> Self {
        IsolationLevel::ReadCommitted
    }

    #[staticmethod]
    fn repeatable_read() -> Self {
        IsolationLevel::RepeatableRead
    }

    #[staticmethod]
    fn serializable() -> Self {
        IsolationLevel::Serializable
    }

    fn __repr__(&self) -> &'static str {
        match self {
            IsolationLevel::ReadUncommitted => "IsolationLevel.ReadUncommitted",
            IsolationLevel::ReadCommitted => "IsolationLevel.ReadCommitted",
            IsolationLevel::RepeatableRead => "IsolationLevel.RepeatableRead",
            IsolationLevel::Serializable => "IsolationLevel.Serializable",
        }
    }
}

impl IsolationLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            IsolationLevel::ReadUncommitted => "READ UNCOMMITTED",
            IsolationLevel::ReadCommitted => "READ COMMITTED",
            IsolationLevel::RepeatableRead => "REPEATABLE READ",
            IsolationLevel::Serializable => "SERIALIZABLE",
        }
    }
}
