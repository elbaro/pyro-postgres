use pyo3::types::PyByteArray;
use pyo3::{
    prelude::*,
    pybacked::{PyBackedBytes, PyBackedStr},
    types::PyBytes,
};

use crate::py_imports::get_json_module;

/// Zero-copy `PostgreSQL` value type using `PyBackedStr` and `PyBackedBytes`
///
/// This enum is similar to the pyro-mysql Value but uses `PyO3`'s zero-copy types
/// for string and byte data, avoiding unnecessary allocations when converting
/// from Python to Rust.
///
/// Note: This type does not implement Clone because `PyBackedBytes` and `PyBackedStr`
/// are non-cloneable zero-copy views into Python objects.
#[derive(Debug)]
pub enum Value {
    /// NULL value
    NULL,

    /// Byte data (zero-copy from Python bytes/bytearray)
    Bytes(PyBackedBytes),

    /// String data (zero-copy from Python str)
    Str(PyBackedStr),

    /// Signed 64-bit integer
    Int(i64),

    /// Unsigned 64-bit integer
    UInt(u64),

    /// 32-bit floating point
    Float(f32),

    /// 64-bit floating point
    Double(f64),

    /// Date: year, month, day
    Date(i32, u8, u8),

    /// Time: hour, minute, second, microsecond
    Time(u8, u8, u8, u32),

    /// Timestamp: year, month, day, hour, minute, second, microsecond
    Timestamp(i32, u8, u8, u8, u8, u8, u32),

    /// Interval/Duration: months, days, microseconds
    Interval(i32, i32, i64),
}

impl FromPyObject<'_, '_> for Value {
    type Error = PyErr;

    fn extract(ob: Borrowed<PyAny>) -> Result<Self, Self::Error> {
        let py = ob.py();

        // Get the type object and its name
        let type_obj = ob.get_type();
        let type_name = type_obj.name()?;

        // Match on type name
        match type_name.to_str()? {
            "NoneType" => Ok(Value::NULL),

            "bool" => {
                let v = ob.extract::<bool>()?;
                Ok(Value::Int(i64::from(v)))
            }

            "int" => {
                // Try to fit in i64 first, then u64, otherwise convert to string
                if let Ok(v) = ob.extract::<i64>() {
                    Ok(Value::Int(v))
                } else if let Ok(v) = ob.extract::<u64>() {
                    Ok(Value::UInt(v))
                } else {
                    // Integer too large for i64/u64, store as zero-copy string
                    let int_str = ob.str()?;
                    let backed_str = int_str.extract::<PyBackedStr>()?;
                    Ok(Value::Str(backed_str))
                }
            }

            "float" => {
                let v = ob.extract::<f64>()?;
                Ok(Value::Double(v))
            }

            "str" => {
                // Zero-copy string extraction
                let backed_str = ob.extract::<PyBackedStr>()?;
                Ok(Value::Str(backed_str))
            }

            "bytes" => {
                // Zero-copy bytes extraction
                let backed_bytes = ob.extract::<PyBackedBytes>()?;
                Ok(Value::Bytes(backed_bytes))
            }

            "bytearray" => {
                // Extract from bytearray (requires a copy since PyBackedBytes doesn't support bytearray)
                let v = ob.cast::<PyByteArray>()?;
                // We need to create bytes from bytearray
                let bytes_obj = PyBytes::new(py, &v.to_vec());
                let backed_bytes = bytes_obj.extract::<PyBackedBytes>()?;
                Ok(Value::Bytes(backed_bytes))
            }

            "tuple" | "list" | "set" | "frozenset" | "dict" => {
                // Serialize collections to JSON as zero-copy string
                let json_module = get_json_module(py)?;
                let json_str = json_module
                    .call_method1("dumps", (ob,))?
                    .extract::<PyBackedStr>()?;
                Ok(Value::Str(json_str))
            }

            "datetime" => {
                // datetime.datetime
                let year = ob.getattr("year")?.extract::<i32>()?;
                let month = ob.getattr("month")?.extract::<u8>()?;
                let day = ob.getattr("day")?.extract::<u8>()?;
                let hour = ob.getattr("hour")?.extract::<u8>()?;
                let minute = ob.getattr("minute")?.extract::<u8>()?;
                let second = ob.getattr("second")?.extract::<u8>()?;
                let microsecond = ob.getattr("microsecond")?.extract::<u32>()?;
                Ok(Value::Timestamp(
                    year,
                    month,
                    day,
                    hour,
                    minute,
                    second,
                    microsecond,
                ))
            }

            "date" => {
                // datetime.date
                let year = ob.getattr("year")?.extract::<i32>()?;
                let month = ob.getattr("month")?.extract::<u8>()?;
                let day = ob.getattr("day")?.extract::<u8>()?;
                Ok(Value::Date(year, month, day))
            }

            "time" => {
                // datetime.time
                let hour = ob.getattr("hour")?.extract::<u8>()?;
                let minute = ob.getattr("minute")?.extract::<u8>()?;
                let second = ob.getattr("second")?.extract::<u8>()?;
                let microsecond = ob.getattr("microsecond")?.extract::<u32>()?;
                Ok(Value::Time(hour, minute, second, microsecond))
            }

            "timedelta" => {
                // datetime.timedelta -> PostgreSQL interval
                let days = ob.getattr("days")?.extract::<i32>()?;
                let seconds = ob.getattr("seconds")?.extract::<i64>()?;
                let microseconds = ob.getattr("microseconds")?.extract::<i64>()?;
                let total_micros = seconds * 1_000_000 + microseconds;
                Ok(Value::Interval(0, days, total_micros))
            }

            "Decimal" => {
                // decimal.Decimal - convert to zero-copy string
                let decimal_str = ob.str()?.extract::<PyBackedStr>()?;
                Ok(Value::Str(decimal_str))
            }

            "UUID" => {
                // uuid.UUID - convert to string representation
                let uuid_str = ob.str()?.extract::<PyBackedStr>()?;
                Ok(Value::Str(uuid_str))
            }

            _ => Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                "Unsupported value type: {:?}",
                type_obj.fully_qualified_name()
            ))),
        }
    }
}

impl Value {
    /// Get a reference to the bytes (if this is a Bytes or Str variant)
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(b) => {
                let bytes_ref: &[u8] = b.as_ref();
                Some(bytes_ref)
            }
            Value::Str(s) => {
                let str_ref: &str = s.as_ref();
                Some(str_ref.as_bytes())
            }
            _ => None,
        }
    }

    /// Get a reference to the string (if this is a Str variant)
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(s) => {
                let str_ref: &str = s.as_ref();
                Some(str_ref)
            }
            Value::Bytes(b) => {
                let bytes_ref: &[u8] = b.as_ref();
                std::str::from_utf8(bytes_ref).ok()
            }
            _ => None,
        }
    }

    /// Check if this value is NULL
    pub fn is_null(&self) -> bool {
        matches!(self, Value::NULL)
    }
}
