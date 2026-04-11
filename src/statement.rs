use pyo3::prelude::*;
use zero_postgres::state::extended::PreparedStatement as ZeroPreparedStatement;

/// Python wrapper for a prepared statement.
///
/// Created via `conn.prepare()` and used with `pipeline.exec()`:
///
/// ```python
/// prepared = conn.prepare("INSERT INTO users (name) VALUES ($1)")
/// with conn.pipeline() as p:
///     t1 = p.exec(prepared, ("Alice",))
///     t2 = p.exec(prepared, ("Bob",))
///     p.sync()
///     p.claim_drop(t1)
///     p.claim_drop(t2)
/// ```
#[pyclass(module = "pyro_postgres", name = "PreparedStatement", frozen, from_py_object)]
#[derive(Clone)]
pub struct PreparedStatement {
    pub inner: ZeroPreparedStatement,
}

#[pymethods]
impl PreparedStatement {
    fn __repr__(&self) -> String {
        format!("PreparedStatement(name='{}')", self.inner.wire_name())
    }
}
