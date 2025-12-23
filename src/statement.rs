use pyo3::prelude::*;
use zero_postgres::state::extended::PreparedStatement;

/// Python wrapper for a prepared statement.
///
/// Created via `conn.prepare()` and used with `pipeline.exec()`:
///
/// ```python
/// stmt = conn.prepare("INSERT INTO users (name) VALUES ($1)")
/// with conn.pipeline() as p:
///     t1 = p.exec(stmt, ("Alice",))
///     t2 = p.exec(stmt, ("Bob",))
///     p.sync()
///     p.claim_drop(t1)
///     p.claim_drop(t2)
/// ```
#[pyclass(module = "pyro_postgres", name = "Statement", frozen)]
#[derive(Clone)]
pub struct Statement {
    pub inner: PreparedStatement,
}

impl Statement {
    pub fn new(inner: PreparedStatement) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl Statement {
    fn __repr__(&self) -> String {
        format!("Statement(name='{}')", self.inner.wire_name())
    }
}
