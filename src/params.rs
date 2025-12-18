//! Python parameter handling.

use pyo3::prelude::*;

use crate::value::Value;

/// A collection of parameter values for SQL queries.
#[derive(Debug, Default)]
pub struct Params(pub Vec<Value>);

impl FromPyObject<'_, '_> for Params {
    type Error = PyErr;

    fn extract(ob: Borrowed<PyAny>) -> Result<Self, Self::Error> {
        // Accept None, tuple, or list
        if ob.is_none() {
            return Ok(Params(Vec::new()));
        }

        // Try to extract as a sequence
        let seq = ob.downcast::<pyo3::types::PySequence>()?;
        let len = seq.len()?;
        let mut values = Vec::with_capacity(len);

        for i in 0..len {
            let item = seq.get_item(i)?;
            let value = item.extract::<Value>()?;
            values.push(value);
        }

        Ok(Params(values))
    }
}

impl Params {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Value> {
        self.0.iter()
    }
}
