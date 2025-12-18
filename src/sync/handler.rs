//! PostgreSQL result handlers for Python conversion.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use zero_postgres::Result;
use zero_postgres::handler::{BinaryHandler, TextHandler};
use zero_postgres::protocol::backend::query::{CommandComplete, DataRow, RowDescription};

use crate::from_wire_value::{decode_binary_to_python, decode_text_to_python};
use crate::util::PyTupleBuilder;

/// Handler that collects rows as Python tuples.
pub struct TupleHandler<'py> {
    py: Python<'py>,
    rows: Py<PyList>,
    rows_affected: Option<u64>,
}

impl<'py> TupleHandler<'py> {
    pub fn new(py: Python<'py>) -> Self {
        Self {
            py,
            rows: PyList::empty(py).unbind(),
            rows_affected: None,
        }
    }

    pub fn into_rows(self) -> Py<PyList> {
        self.rows
    }

    pub fn rows_affected(&self) -> Option<u64> {
        self.rows_affected
    }
}

impl<'py> TextHandler for TupleHandler<'py> {
    fn row(&mut self, cols: RowDescription<'_>, row: DataRow<'_>) -> Result<()> {
        let fields = cols.fields();
        let tuple = PyTupleBuilder::new(self.py, fields.len());

        for (i, (field, value)) in fields.iter().zip(row.iter()).enumerate() {
            let py_value = match value {
                None => self.py.None().into_bound(self.py),
                Some(bytes) => decode_text_to_python(self.py, field.type_oid(), bytes)
                    .map_err(|e| zero_postgres::Error::Protocol(e.to_string()))?
                    .into_bound(self.py),
            };
            tuple.set(i, py_value);
        }

        self.rows
            .bind(self.py)
            .append(tuple.build(self.py))
            .expect("append");
        Ok(())
    }

    fn result_end(&mut self, complete: CommandComplete<'_>) -> Result<()> {
        self.rows_affected = complete.rows_affected();
        Ok(())
    }
}

impl<'py> BinaryHandler for TupleHandler<'py> {
    fn row(&mut self, cols: RowDescription<'_>, row: DataRow<'_>) -> Result<()> {
        let fields = cols.fields();
        let tuple = PyTupleBuilder::new(self.py, fields.len());

        for (i, (field, value)) in fields.iter().zip(row.iter()).enumerate() {
            let py_value = match value {
                None => self.py.None().into_bound(self.py),
                Some(bytes) => decode_binary_to_python(self.py, field.type_oid(), bytes)
                    .map_err(|e| zero_postgres::Error::Protocol(e.to_string()))?
                    .into_bound(self.py),
            };
            tuple.set(i, py_value);
        }

        self.rows
            .bind(self.py)
            .append(tuple.build(self.py))
            .expect("append");
        Ok(())
    }

    fn result_end(&mut self, complete: CommandComplete<'_>) -> Result<()> {
        self.rows_affected = complete.rows_affected();
        Ok(())
    }
}

/// Handler that collects rows as Python dicts.
pub struct DictHandler<'py> {
    py: Python<'py>,
    rows: Py<PyList>,
    rows_affected: Option<u64>,
}

impl<'py> DictHandler<'py> {
    pub fn new(py: Python<'py>) -> Self {
        Self {
            py,
            rows: PyList::empty(py).unbind(),
            rows_affected: None,
        }
    }

    pub fn into_rows(self) -> Py<PyList> {
        self.rows
    }

    pub fn rows_affected(&self) -> Option<u64> {
        self.rows_affected
    }
}

impl<'py> TextHandler for DictHandler<'py> {
    fn row(&mut self, cols: RowDescription<'_>, row: DataRow<'_>) -> Result<()> {
        let dict = PyDict::new(self.py);

        for (field, value) in cols.iter().zip(row.iter()) {
            let py_value = match value {
                None => self.py.None().into_bound(self.py),
                Some(bytes) => decode_text_to_python(self.py, field.type_oid(), bytes)
                    .map_err(|e| zero_postgres::Error::Protocol(e.to_string()))?
                    .into_bound(self.py),
            };
            dict.set_item(field.name, py_value).expect("set_item");
        }

        self.rows.bind(self.py).append(dict).expect("append");
        Ok(())
    }

    fn result_end(&mut self, complete: CommandComplete<'_>) -> Result<()> {
        self.rows_affected = complete.rows_affected();
        Ok(())
    }
}

impl<'py> BinaryHandler for DictHandler<'py> {
    fn row(&mut self, cols: RowDescription<'_>, row: DataRow<'_>) -> Result<()> {
        let dict = PyDict::new(self.py);

        for (field, value) in cols.iter().zip(row.iter()) {
            let py_value = match value {
                None => self.py.None().into_bound(self.py),
                Some(bytes) => decode_binary_to_python(self.py, field.type_oid(), bytes)
                    .map_err(|e| zero_postgres::Error::Protocol(e.to_string()))?
                    .into_bound(self.py),
            };
            dict.set_item(field.name, py_value).expect("set_item");
        }

        self.rows.bind(self.py).append(dict).expect("append");
        Ok(())
    }

    fn result_end(&mut self, complete: CommandComplete<'_>) -> Result<()> {
        self.rows_affected = complete.rows_affected();
        Ok(())
    }
}

/// Handler that discards all results.
#[derive(Default)]
pub struct DropHandler {
    pub rows_affected: Option<u64>,
}

impl TextHandler for DropHandler {
    fn row(&mut self, _cols: RowDescription<'_>, _row: DataRow<'_>) -> Result<()> {
        Ok(())
    }

    fn result_end(&mut self, complete: CommandComplete<'_>) -> Result<()> {
        self.rows_affected = complete.rows_affected();
        Ok(())
    }
}

impl BinaryHandler for DropHandler {
    fn row(&mut self, _cols: RowDescription<'_>, _row: DataRow<'_>) -> Result<()> {
        Ok(())
    }

    fn result_end(&mut self, complete: CommandComplete<'_>) -> Result<()> {
        self.rows_affected = complete.rows_affected();
        Ok(())
    }
}
