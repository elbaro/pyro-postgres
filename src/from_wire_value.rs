//! Convert `PostgreSQL` wire format values to Python objects.

use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};
use time::{Date, Month};

use crate::py_imports::{
    get_date_class, get_datetime_class, get_decimal_class, get_time_class, get_timedelta_class,
    get_uuid_class,
};

/// PostgreSQL epoch (2000-01-01)
const PG_EPOCH: Date = match Date::from_calendar_date(2000, Month::January, 1) {
    Ok(d) => d,
    Err(_) => unreachable!(),
};

// PostgreSQL OIDs for common types
pub const OID_BOOL: u32 = 16;
pub const OID_BYTEA: u32 = 17;
pub const OID_INT8: u32 = 20;
pub const OID_INT2: u32 = 21;
pub const OID_INT4: u32 = 23;
pub const OID_TEXT: u32 = 25;
pub const OID_OID: u32 = 26;
pub const OID_FLOAT4: u32 = 700;
pub const OID_FLOAT8: u32 = 701;
pub const OID_VARCHAR: u32 = 1043;
pub const OID_DATE: u32 = 1082;
pub const OID_TIME: u32 = 1083;
pub const OID_TIMESTAMP: u32 = 1114;
pub const OID_TIMESTAMPTZ: u32 = 1184;
pub const OID_INTERVAL: u32 = 1186;
pub const OID_TIMETZ: u32 = 1266;
pub const OID_NUMERIC: u32 = 1700;
pub const OID_UUID: u32 = 2950;
pub const OID_JSON: u32 = 114;
pub const OID_JSONB: u32 = 3802;
pub const OID_CHAR: u32 = 18;
pub const OID_BPCHAR: u32 = 1042;
pub const OID_NAME: u32 = 19;

/// Decode a text-format `PostgreSQL` value to a Python object.
///
/// Text format is used for simple queries. Values are UTF-8 encoded strings.
pub fn decode_text_to_python(py: Python<'_>, oid: u32, bytes: &[u8]) -> PyResult<Py<PyAny>> {
    let s = std::str::from_utf8(bytes)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

    match oid {
        OID_BOOL => {
            let v = s == "t";
            v.into_py_any(py)
        }

        OID_INT2 => {
            let v: i16 = s.parse().map_err(|e: std::num::ParseIntError| {
                pyo3::exceptions::PyValueError::new_err(e.to_string())
            })?;
            v.into_py_any(py)
        }

        OID_INT4 | OID_OID => {
            let v: i32 = s.parse().map_err(|e: std::num::ParseIntError| {
                pyo3::exceptions::PyValueError::new_err(e.to_string())
            })?;
            v.into_py_any(py)
        }

        OID_INT8 => {
            let v: i64 = s.parse().map_err(|e: std::num::ParseIntError| {
                pyo3::exceptions::PyValueError::new_err(e.to_string())
            })?;
            v.into_py_any(py)
        }

        OID_FLOAT4 => {
            let v: f32 = s.parse().map_err(|e: std::num::ParseFloatError| {
                pyo3::exceptions::PyValueError::new_err(e.to_string())
            })?;
            // Convert via ryu to avoid precision loss
            let mut buffer = ryu::Buffer::new();
            let f64_val: f64 =
                buffer
                    .format(v)
                    .parse()
                    .map_err(|e: std::num::ParseFloatError| {
                        pyo3::exceptions::PyValueError::new_err(e.to_string())
                    })?;
            f64_val.into_py_any(py)
        }

        OID_FLOAT8 => {
            let v: f64 = s.parse().map_err(|e: std::num::ParseFloatError| {
                pyo3::exceptions::PyValueError::new_err(e.to_string())
            })?;
            v.into_py_any(py)
        }

        OID_TEXT | OID_VARCHAR | OID_CHAR | OID_BPCHAR | OID_NAME => {
            Ok(PyString::new(py, s).into_any().unbind())
        }

        OID_BYTEA => {
            let decoded = decode_bytea_text(s)?;
            Ok(PyBytes::new(py, &decoded).into_any().unbind())
        }

        OID_DATE => {
            let date_class = get_date_class(py)?;
            let (year, month, day) = parse_date(s)?;
            let date = date_class.call1((year, month, day))?;
            date.into_py_any(py)
        }

        OID_TIME | OID_TIMETZ => {
            let time_class = get_time_class(py)?;
            let (hour, minute, second, micro) = parse_time(s)?;
            let time = time_class.call1((hour, minute, second, micro))?;
            time.into_py_any(py)
        }

        OID_TIMESTAMP | OID_TIMESTAMPTZ => {
            let datetime_class = get_datetime_class(py)?;
            let (year, month, day, hour, minute, second, micro) = parse_timestamp(s)?;
            let dt = datetime_class.call1((year, month, day, hour, minute, second, micro))?;
            dt.into_py_any(py)
        }

        OID_INTERVAL => {
            let timedelta_class = get_timedelta_class(py)?;
            let (days, seconds, microseconds) = parse_interval(s)?;
            let td = timedelta_class.call1((days, seconds, microseconds))?;
            td.into_py_any(py)
        }

        OID_NUMERIC => {
            let decimal_class = get_decimal_class(py)?;
            let decimal = decimal_class.call1((s,))?;
            decimal.into_py_any(py)
        }

        OID_UUID => {
            let uuid_class = get_uuid_class(py)?;
            let uuid = uuid_class.call1((s,))?;
            uuid.into_py_any(py)
        }

        OID_JSON | OID_JSONB => {
            // Return JSON as string - let Python parse it if needed
            Ok(PyString::new(py, s).into_any().unbind())
        }

        _ => {
            // Unknown type - return as string
            Ok(PyString::new(py, s).into_any().unbind())
        }
    }
}

/// Decode a binary-format `PostgreSQL` value to a Python object.
///
/// Binary format uses `PostgreSQL`'s internal representation.
pub fn decode_binary_to_python(py: Python<'_>, oid: u32, bytes: &[u8]) -> PyResult<Py<PyAny>> {
    match oid {
        OID_BOOL => {
            let v = !bytes.is_empty() && bytes[0] != 0;
            v.into_py_any(py)
        }

        OID_INT2 => {
            let arr: [u8; 2] = bytes
                .try_into()
                .map_err(|_| pyo3::exceptions::PyValueError::new_err("Invalid INT2 binary data"))?;
            let v = i16::from_be_bytes(arr);
            v.into_py_any(py)
        }

        OID_INT4 | OID_OID => {
            let arr: [u8; 4] = bytes
                .try_into()
                .map_err(|_| pyo3::exceptions::PyValueError::new_err("Invalid INT4 binary data"))?;
            let v = i32::from_be_bytes(arr);
            v.into_py_any(py)
        }

        OID_INT8 => {
            let arr: [u8; 8] = bytes
                .try_into()
                .map_err(|_| pyo3::exceptions::PyValueError::new_err("Invalid INT8 binary data"))?;
            let v = i64::from_be_bytes(arr);
            v.into_py_any(py)
        }

        OID_FLOAT4 => {
            let arr: [u8; 4] = bytes.try_into().map_err(|_| {
                pyo3::exceptions::PyValueError::new_err("Invalid FLOAT4 binary data")
            })?;
            let v = f32::from_be_bytes(arr);
            // Convert via ryu to avoid precision loss
            let mut buffer = ryu::Buffer::new();
            let f64_val: f64 =
                buffer
                    .format(v)
                    .parse()
                    .map_err(|e: std::num::ParseFloatError| {
                        pyo3::exceptions::PyValueError::new_err(e.to_string())
                    })?;
            f64_val.into_py_any(py)
        }

        OID_FLOAT8 => {
            let arr: [u8; 8] = bytes.try_into().map_err(|_| {
                pyo3::exceptions::PyValueError::new_err("Invalid FLOAT8 binary data")
            })?;
            let v = f64::from_be_bytes(arr);
            v.into_py_any(py)
        }

        OID_TEXT | OID_VARCHAR | OID_CHAR | OID_BPCHAR | OID_NAME => {
            let s = std::str::from_utf8(bytes)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            Ok(PyString::new(py, s).into_any().unbind())
        }

        OID_BYTEA => Ok(PyBytes::new(py, bytes).into_any().unbind()),

        OID_DATE => {
            // PostgreSQL binary date: i32 days since 2000-01-01
            let arr: [u8; 4] = bytes
                .try_into()
                .map_err(|_| pyo3::exceptions::PyValueError::new_err("Invalid DATE binary data"))?;
            let days = i32::from_be_bytes(arr);
            let date_class = get_date_class(py)?;
            // PostgreSQL epoch is 2000-01-01
            let (year, month, day) = days_since_pg_epoch_to_ymd(days);
            let date = date_class.call1((year, month, day))?;
            date.into_py_any(py)
        }

        OID_TIME => {
            // PostgreSQL binary time: i64 microseconds since midnight
            let arr: [u8; 8] = bytes
                .try_into()
                .map_err(|_| pyo3::exceptions::PyValueError::new_err("Invalid TIME binary data"))?;
            let micros = i64::from_be_bytes(arr);
            let time_class = get_time_class(py)?;
            let (hour, minute, second, micro) = micros_to_time(micros);
            let time = time_class.call1((hour, minute, second, micro))?;
            time.into_py_any(py)
        }

        OID_TIMESTAMP | OID_TIMESTAMPTZ => {
            // PostgreSQL binary timestamp: i64 microseconds since 2000-01-01 00:00:00
            let arr: [u8; 8] = bytes.try_into().map_err(|_| {
                pyo3::exceptions::PyValueError::new_err("Invalid TIMESTAMP binary data")
            })?;
            let micros = i64::from_be_bytes(arr);
            let datetime_class = get_datetime_class(py)?;
            let (year, month, day, hour, minute, second, micro) =
                micros_since_pg_epoch_to_datetime(micros);
            let dt = datetime_class.call1((year, month, day, hour, minute, second, micro))?;
            dt.into_py_any(py)
        }

        OID_INTERVAL => {
            // PostgreSQL binary interval: 8 bytes microseconds + 4 bytes days + 4 bytes months
            if bytes.len() != 16 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "Invalid INTERVAL binary data",
                ));
            }
            let micros = i64::from_be_bytes(bytes[0..8].try_into().expect("8 bytes"));
            let days = i32::from_be_bytes(bytes[8..12].try_into().expect("4 bytes"));
            let _months = i32::from_be_bytes(bytes[12..16].try_into().expect("4 bytes"));

            let timedelta_class = get_timedelta_class(py)?;
            let seconds = (micros / 1_000_000) as i32;
            let microseconds = (micros % 1_000_000) as i32;
            let td = timedelta_class.call1((days, seconds, microseconds))?;
            td.into_py_any(py)
        }

        OID_NUMERIC => {
            // Numeric binary format is complex, decode to string then to Decimal
            let s = decode_numeric_binary(bytes)?;
            let decimal_class = get_decimal_class(py)?;
            let decimal = decimal_class.call1((s,))?;
            decimal.into_py_any(py)
        }

        OID_UUID => {
            // UUID is 16 bytes
            if bytes.len() != 16 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "Invalid UUID binary data",
                ));
            }
            let uuid_class = get_uuid_class(py)?;
            // Build hex string and use the proper constructor
            let hex_str = bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
            let uuid = uuid_class.call1((hex_str,))?;
            uuid.into_py_any(py)
        }

        OID_JSON | OID_JSONB => {
            // JSONB has a version byte prefix
            let data = if oid == OID_JSONB && !bytes.is_empty() {
                &bytes[1..] // Skip version byte
            } else {
                bytes
            };
            let s = std::str::from_utf8(data)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            Ok(PyString::new(py, s).into_any().unbind())
        }

        _ => {
            // Unknown type - return as bytes
            Ok(PyBytes::new(py, bytes).into_any().unbind())
        }
    }
}

/// Decode `PostgreSQL` text-format bytea (hex or escape format)
fn decode_bytea_text(s: &str) -> PyResult<Vec<u8>> {
    if let Some(hex) = s.strip_prefix("\\x") {
        // Hex format
        (0..hex.len())
            .step_by(2)
            .map(|i| {
                u8::from_str_radix(&hex[i..i + 2], 16).map_err(|e| {
                    pyo3::exceptions::PyValueError::new_err(format!("Invalid hex: {e}"))
                })
            })
            .collect()
    } else {
        // Escape format
        let mut result = Vec::new();
        let mut chars = s.chars();
        while let Some(c) = chars.next() {
            if c == '\\' {
                match chars.next() {
                    Some('\\') => result.push(b'\\'),
                    Some(c1) if c1.is_ascii_digit() => {
                        let c2 = chars.next().ok_or_else(|| {
                            pyo3::exceptions::PyValueError::new_err("Invalid escape sequence")
                        })?;
                        let c3 = chars.next().ok_or_else(|| {
                            pyo3::exceptions::PyValueError::new_err("Invalid escape sequence")
                        })?;
                        let oct = format!("{c1}{c2}{c3}");
                        let byte = u8::from_str_radix(&oct, 8).map_err(|e| {
                            pyo3::exceptions::PyValueError::new_err(format!("Invalid octal: {e}"))
                        })?;
                        result.push(byte);
                    }
                    _ => {
                        return Err(pyo3::exceptions::PyValueError::new_err(
                            "Invalid escape sequence",
                        ));
                    }
                }
            } else {
                result.push(c as u8);
            }
        }
        Ok(result)
    }
}

/// Parse `PostgreSQL` text date format: YYYY-MM-DD
fn parse_date(s: &str) -> PyResult<(i32, u32, u32)> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "Invalid date format: {s}"
        )));
    }
    let year: i32 = parts[0].parse().map_err(|e: std::num::ParseIntError| {
        pyo3::exceptions::PyValueError::new_err(e.to_string())
    })?;
    let month: u32 = parts[1].parse().map_err(|e: std::num::ParseIntError| {
        pyo3::exceptions::PyValueError::new_err(e.to_string())
    })?;
    let day: u32 = parts[2].parse().map_err(|e: std::num::ParseIntError| {
        pyo3::exceptions::PyValueError::new_err(e.to_string())
    })?;
    Ok((year, month, day))
}

/// Parse `PostgreSQL` text time format: HH:MM:SS[.microseconds][+/-TZ]
fn parse_time(s: &str) -> PyResult<(u32, u32, u32, u32)> {
    // Strip timezone if present
    let time_part = s.split(['+', '-']).next().unwrap_or(s);
    let parts: Vec<&str> = time_part.split(':').collect();
    if parts.len() < 2 {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "Invalid time format: {s}"
        )));
    }
    let hour: u32 = parts[0].parse().map_err(|e: std::num::ParseIntError| {
        pyo3::exceptions::PyValueError::new_err(e.to_string())
    })?;
    let minute: u32 = parts[1].parse().map_err(|e: std::num::ParseIntError| {
        pyo3::exceptions::PyValueError::new_err(e.to_string())
    })?;

    let (second, micro) = if parts.len() > 2 {
        let sec_parts: Vec<&str> = parts[2].split('.').collect();
        let second: u32 = sec_parts[0].parse().map_err(|e: std::num::ParseIntError| {
            pyo3::exceptions::PyValueError::new_err(e.to_string())
        })?;
        let micro: u32 = if sec_parts.len() > 1 {
            let frac = sec_parts[1];
            // Pad to 6 digits
            let padded = format!("{frac:0<6}");
            padded[..6].parse().map_err(|e: std::num::ParseIntError| {
                pyo3::exceptions::PyValueError::new_err(e.to_string())
            })?
        } else {
            0
        };
        (second, micro)
    } else {
        (0, 0)
    };

    Ok((hour, minute, second, micro))
}

/// Parse `PostgreSQL` text timestamp format: YYYY-MM-DD HH:MM:SS[.microseconds][+/-TZ]
fn parse_timestamp(s: &str) -> PyResult<(i32, u32, u32, u32, u32, u32, u32)> {
    let parts: Vec<&str> = s.split(' ').collect();
    if parts.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "Invalid timestamp format: {s}"
        )));
    }

    let (year, month, day) = parse_date(parts[0])?;
    let (hour, minute, second, micro) = if parts.len() > 1 {
        parse_time(parts[1])?
    } else {
        (0, 0, 0, 0)
    };

    Ok((year, month, day, hour, minute, second, micro))
}

/// Parse `PostgreSQL` text interval format (simplified)
fn parse_interval(s: &str) -> PyResult<(i32, i32, i32)> {
    // This is a simplified parser - PostgreSQL interval format is complex
    // For now, return as zero interval - proper parsing would need more work
    let _ = s;
    Ok((0, 0, 0))
}

/// Convert days since `PostgreSQL` epoch (2000-01-01) to (year, month, day)
fn days_since_pg_epoch_to_ymd(days: i32) -> (i32, u32, u32) {
    let julian_day = PG_EPOCH.to_julian_day() + days;
    let date = Date::from_julian_day(julian_day).unwrap_or(PG_EPOCH);
    (date.year(), date.month() as u32, date.day() as u32)
}

/// Convert microseconds since midnight to (hour, minute, second, microsecond)
fn micros_to_time(micros: i64) -> (u32, u32, u32, u32) {
    let total_seconds = micros / 1_000_000;
    let micro = (micros % 1_000_000) as u32;
    let second = (total_seconds % 60) as u32;
    let total_minutes = total_seconds / 60;
    let minute = (total_minutes % 60) as u32;
    let hour = (total_minutes / 60) as u32;
    (hour, minute, second, micro)
}

/// Convert microseconds since `PostgreSQL` epoch to datetime components
fn micros_since_pg_epoch_to_datetime(micros: i64) -> (i32, u32, u32, u32, u32, u32, u32) {
    let days = (micros / 86_400_000_000) as i32;
    let day_micros = micros % 86_400_000_000;

    let (year, month, day) = days_since_pg_epoch_to_ymd(days);
    let (hour, minute, second, micro) = micros_to_time(day_micros);

    (year, month, day, hour, minute, second, micro)
}

/// Decode `PostgreSQL` binary numeric format to string
fn decode_numeric_binary(bytes: &[u8]) -> PyResult<String> {
    if bytes.len() < 8 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "Invalid NUMERIC binary data",
        ));
    }

    let ndigits = i16::from_be_bytes([bytes[0], bytes[1]]) as usize;
    let weight = i16::from_be_bytes([bytes[2], bytes[3]]);
    let sign = i16::from_be_bytes([bytes[4], bytes[5]]);
    let dscale = i16::from_be_bytes([bytes[6], bytes[7]]) as usize;

    if bytes.len() < 8 + ndigits * 2 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "Invalid NUMERIC binary data length",
        ));
    }

    // Special cases - NaN is represented by sign = 0xC000
    if sign == 0xC000u16 as i16 {
        return Ok("NaN".to_string());
    }

    // Collect digits (each is 0-9999 representing 4 decimal digits)
    let mut digits: Vec<i16> = Vec::with_capacity(ndigits);
    for i in 0..ndigits {
        let d = i16::from_be_bytes([bytes[8 + i * 2], bytes[9 + i * 2]]);
        digits.push(d);
    }

    // Build string
    let mut result = String::new();
    if sign == 0x4000 {
        result.push('-');
    }

    // Integer part
    let int_ndigits = (weight + 1) as usize;
    if int_ndigits > 0 {
        for i in 0..int_ndigits {
            let d = if i < digits.len() { digits[i] } else { 0 };
            if i == 0 {
                result.push_str(&d.to_string());
            } else {
                result.push_str(&format!("{d:04}"));
            }
        }
    } else {
        result.push('0');
    }

    // Fractional part
    if dscale > 0 {
        result.push('.');
        let frac_start = int_ndigits;
        let mut frac_digits = String::new();
        for i in frac_start..digits.len() {
            frac_digits.push_str(&format!("{:04}", digits[i]));
        }
        // Pad with zeros if needed
        while frac_digits.len() < dscale {
            frac_digits.push('0');
        }
        result.push_str(&frac_digits[..dscale]);
    }

    Ok(result)
}
