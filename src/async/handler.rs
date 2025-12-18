//! Async PostgreSQL result handlers for Python conversion.
//!
//! These handlers collect row data without holding GIL, then convert to Python
//! objects when needed.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use zero_postgres::Result;
use zero_postgres::handler::{BinaryHandler, TextHandler};
use zero_postgres::protocol::backend::query::{CommandComplete, DataRow, RowDescription};

use crate::from_wire_value::{decode_binary_to_python, decode_text_to_python};

/// A single row of raw data
struct RawRow {
    /// (oid, bytes or None for NULL)
    columns: Vec<(u32, Option<Vec<u8>>)>,
    /// Column names
    names: Vec<String>,
}

/// Handler that collects rows as raw data for later Python conversion.
#[derive(Default)]
pub struct TupleHandler {
    rows: Vec<RawRow>,
    rows_affected: Option<u64>,
}

impl TupleHandler {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn clear(&mut self) {
        self.rows.clear();
        self.rows_affected = None;
    }

    pub fn rows_affected(&self) -> Option<u64> {
        self.rows_affected
    }

    /// Convert collected rows to Python tuples
    pub fn rows_to_python(&self, py: Python<'_>) -> PyResult<Vec<Py<PyTuple>>> {
        let mut result = Vec::with_capacity(self.rows.len());

        for row in &self.rows {
            let tuple = PyTuple::new(
                py,
                row.columns.iter().map(|(oid, data)| {
                    match data {
                        None => py.None().into_bound(py),
                        Some(bytes) => {
                            // Use binary decoding since we stored raw bytes
                            decode_binary_to_python(py, *oid, bytes)
                                .unwrap_or_else(|_| py.None())
                                .into_bound(py)
                        }
                    }
                }),
            )?;
            result.push(tuple.unbind());
        }

        Ok(result)
    }
}

impl TextHandler for TupleHandler {
    fn row(&mut self, cols: RowDescription<'_>, row: DataRow<'_>) -> Result<()> {
        let fields = cols.fields();
        let mut columns = Vec::with_capacity(fields.len());
        let mut names = Vec::with_capacity(fields.len());

        for (field, value) in fields.iter().zip(row.iter()) {
            names.push(field.name.to_string());
            columns.push((field.type_oid(), value.map(|b| b.to_vec())));
        }

        self.rows.push(RawRow { columns, names });
        Ok(())
    }

    fn result_end(&mut self, complete: CommandComplete<'_>) -> Result<()> {
        self.rows_affected = complete.rows_affected();
        Ok(())
    }
}

impl BinaryHandler for TupleHandler {
    fn row(&mut self, cols: RowDescription<'_>, row: DataRow<'_>) -> Result<()> {
        let fields = cols.fields();
        let mut columns = Vec::with_capacity(fields.len());
        let mut names = Vec::with_capacity(fields.len());

        for (field, value) in fields.iter().zip(row.iter()) {
            names.push(field.name.to_string());
            columns.push((field.type_oid(), value.map(|b| b.to_vec())));
        }

        self.rows.push(RawRow { columns, names });
        Ok(())
    }

    fn result_end(&mut self, complete: CommandComplete<'_>) -> Result<()> {
        self.rows_affected = complete.rows_affected();
        Ok(())
    }
}

/// Handler that collects rows as raw data for later Python dict conversion.
#[derive(Default)]
pub struct DictHandler {
    rows: Vec<RawRow>,
    rows_affected: Option<u64>,
}

impl DictHandler {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn clear(&mut self) {
        self.rows.clear();
        self.rows_affected = None;
    }

    pub fn rows_affected(&self) -> Option<u64> {
        self.rows_affected
    }

    /// Convert collected rows to Python dicts
    pub fn rows_to_python(&self, py: Python<'_>) -> PyResult<Vec<Py<PyDict>>> {
        let mut result = Vec::with_capacity(self.rows.len());

        for row in &self.rows {
            let dict = PyDict::new(py);

            for ((oid, data), name) in row.columns.iter().zip(row.names.iter()) {
                let py_value = match data {
                    None => py.None().into_bound(py),
                    Some(bytes) => decode_binary_to_python(py, *oid, bytes)
                        .unwrap_or_else(|_| py.None())
                        .into_bound(py),
                };
                dict.set_item(name, py_value)?;
            }

            result.push(dict.unbind());
        }

        Ok(result)
    }
}

impl TextHandler for DictHandler {
    fn row(&mut self, cols: RowDescription<'_>, row: DataRow<'_>) -> Result<()> {
        let fields = cols.fields();
        let mut columns = Vec::with_capacity(fields.len());
        let mut names = Vec::with_capacity(fields.len());

        for (field, value) in fields.iter().zip(row.iter()) {
            names.push(field.name.to_string());
            columns.push((field.type_oid(), value.map(|b| b.to_vec())));
        }

        self.rows.push(RawRow { columns, names });
        Ok(())
    }

    fn result_end(&mut self, complete: CommandComplete<'_>) -> Result<()> {
        self.rows_affected = complete.rows_affected();
        Ok(())
    }
}

impl BinaryHandler for DictHandler {
    fn row(&mut self, cols: RowDescription<'_>, row: DataRow<'_>) -> Result<()> {
        let fields = cols.fields();
        let mut columns = Vec::with_capacity(fields.len());
        let mut names = Vec::with_capacity(fields.len());

        for (field, value) in fields.iter().zip(row.iter()) {
            names.push(field.name.to_string());
            columns.push((field.type_oid(), value.map(|b| b.to_vec())));
        }

        self.rows.push(RawRow { columns, names });
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
