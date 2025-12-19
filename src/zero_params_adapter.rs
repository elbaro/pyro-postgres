//! Adapter to convert Python parameter values to `PostgreSQL` wire format.

use zero_postgres::conversion::ToParams;
use zero_postgres::protocol::types::{oid, Oid};

use crate::params::Params;
use crate::value::Value;

/// Adapter that wraps Python params for use with zero-postgres
pub struct ParamsAdapter<'a> {
    params: &'a Params,
}

impl<'a> ParamsAdapter<'a> {
    pub fn new(params: &'a Params) -> Self {
        Self { params }
    }
}

impl ToParams for ParamsAdapter<'_> {
    fn param_count(&self) -> usize {
        self.params.0.len()
    }

    fn natural_oids(&self) -> Vec<Oid> {
        self.params.0.iter().map(natural_oid).collect()
    }

    fn to_binary(&self, target_oids: &[Oid], buf: &mut Vec<u8>) -> zero_postgres::Result<()> {
        for (value, &target_oid) in self.params.0.iter().zip(target_oids.iter()) {
            encode_value_to_binary(value, target_oid, buf)?;
        }
        Ok(())
    }
}

/// Get the natural OID for a Python value.
fn natural_oid(value: &Value) -> Oid {
    match value {
        Value::NULL => 0, // Unknown/NULL
        Value::Int(_) => oid::INT8,
        Value::UInt(_) => oid::INT8,
        Value::Float(_) => oid::FLOAT4,
        Value::Double(_) => oid::FLOAT8,
        Value::Str(_) => oid::TEXT,
        Value::Bytes(_) => oid::BYTEA,
        Value::Date(_, _, _) => oid::DATE,
        Value::Time(_, _, _, _) => oid::TIME,
        Value::Timestamp(_, _, _, _, _, _, _) => oid::TIMESTAMP,
        Value::Interval(_, _, _) => oid::INTERVAL,
    }
}

/// Encode a Python Value to `PostgreSQL` binary format for a target OID.
///
/// Format: Int32 length followed by bytes, or Int32 -1 for NULL.
///
/// This function supports flexible encoding: an i64 can encode as INT2, INT4, or INT8
/// depending on what the server expects (with overflow checking).
fn encode_value_to_binary(
    value: &Value,
    target_oid: Oid,
    buf: &mut Vec<u8>,
) -> zero_postgres::Result<()> {
    match value {
        Value::NULL => {
            buf.extend_from_slice(&(-1_i32).to_be_bytes());
            Ok(())
        }

        Value::Int(v) => encode_int(*v, target_oid, buf),

        Value::UInt(v) => encode_uint(*v, target_oid, buf),

        Value::Float(v) => encode_float(*v, target_oid, buf),

        Value::Double(v) => encode_double(*v, target_oid, buf),

        Value::Str(s) => {
            let s_ref: &str = s.as_ref();
            let bytes = s_ref.as_bytes();
            buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            buf.extend_from_slice(bytes);
            Ok(())
        }

        Value::Bytes(b) => {
            let bytes: &[u8] = b.as_ref();
            buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            buf.extend_from_slice(bytes);
            Ok(())
        }

        Value::Date(year, month, day) => {
            // PostgreSQL binary date: i32 days since 2000-01-01
            let days = ymd_to_days_since_pg_epoch(*year, *month, *day);
            buf.extend_from_slice(&4_i32.to_be_bytes());
            buf.extend_from_slice(&days.to_be_bytes());
            Ok(())
        }

        Value::Time(hour, minute, second, micro) => {
            // PostgreSQL binary time: i64 microseconds since midnight
            let micros = time_to_micros(*hour, *minute, *second, *micro);
            buf.extend_from_slice(&8_i32.to_be_bytes());
            buf.extend_from_slice(&micros.to_be_bytes());
            Ok(())
        }

        Value::Timestamp(year, month, day, hour, minute, second, micro) => {
            // PostgreSQL binary timestamp: i64 microseconds since 2000-01-01 00:00:00
            let days = ymd_to_days_since_pg_epoch(*year, *month, *day);
            let time_micros = time_to_micros(*hour, *minute, *second, *micro);
            let total_micros = i64::from(days) * 86_400_000_000 + time_micros;
            buf.extend_from_slice(&8_i32.to_be_bytes());
            buf.extend_from_slice(&total_micros.to_be_bytes());
            Ok(())
        }

        Value::Interval(months, days, micros) => {
            // PostgreSQL binary interval: 8 bytes microseconds + 4 bytes days + 4 bytes months
            buf.extend_from_slice(&16_i32.to_be_bytes());
            buf.extend_from_slice(&micros.to_be_bytes());
            buf.extend_from_slice(&days.to_be_bytes());
            buf.extend_from_slice(&months.to_be_bytes());
            Ok(())
        }
    }
}

/// Encode an i64 value with flexible integer type encoding.
fn encode_int(v: i64, target_oid: Oid, buf: &mut Vec<u8>) -> zero_postgres::Result<()> {
    match target_oid {
        oid::INT2 => {
            let v16 = i16::try_from(v)
                .map_err(|_| zero_postgres::Error::overflow("i64", "INT2"))?;
            buf.extend_from_slice(&2_i32.to_be_bytes());
            buf.extend_from_slice(&v16.to_be_bytes());
        }
        oid::INT4 => {
            let v32 = i32::try_from(v)
                .map_err(|_| zero_postgres::Error::overflow("i64", "INT4"))?;
            buf.extend_from_slice(&4_i32.to_be_bytes());
            buf.extend_from_slice(&v32.to_be_bytes());
        }
        oid::INT8 | 0 => {
            // INT8 or unknown (0) - use natural i64
            buf.extend_from_slice(&8_i32.to_be_bytes());
            buf.extend_from_slice(&v.to_be_bytes());
        }
        _ => {
            return Err(zero_postgres::Error::type_mismatch(oid::INT8, target_oid));
        }
    }
    Ok(())
}

/// Encode a u64 value with flexible integer type encoding.
fn encode_uint(v: u64, target_oid: Oid, buf: &mut Vec<u8>) -> zero_postgres::Result<()> {
    match target_oid {
        oid::INT2 => {
            let v16 = i16::try_from(v)
                .map_err(|_| zero_postgres::Error::overflow("u64", "INT2"))?;
            buf.extend_from_slice(&2_i32.to_be_bytes());
            buf.extend_from_slice(&v16.to_be_bytes());
        }
        oid::INT4 => {
            let v32 = i32::try_from(v)
                .map_err(|_| zero_postgres::Error::overflow("u64", "INT4"))?;
            buf.extend_from_slice(&4_i32.to_be_bytes());
            buf.extend_from_slice(&v32.to_be_bytes());
        }
        oid::INT8 | 0 => {
            // INT8 or unknown (0) - convert to i64
            let v64 = i64::try_from(v)
                .map_err(|_| zero_postgres::Error::overflow("u64", "INT8"))?;
            buf.extend_from_slice(&8_i32.to_be_bytes());
            buf.extend_from_slice(&v64.to_be_bytes());
        }
        _ => {
            return Err(zero_postgres::Error::type_mismatch(oid::INT8, target_oid));
        }
    }
    Ok(())
}

/// Encode an f32 value with flexible float type encoding.
fn encode_float(v: f32, target_oid: Oid, buf: &mut Vec<u8>) -> zero_postgres::Result<()> {
    match target_oid {
        oid::FLOAT4 | 0 => {
            buf.extend_from_slice(&4_i32.to_be_bytes());
            buf.extend_from_slice(&v.to_bits().to_be_bytes());
        }
        oid::FLOAT8 => {
            buf.extend_from_slice(&8_i32.to_be_bytes());
            buf.extend_from_slice(&(f64::from(v)).to_bits().to_be_bytes());
        }
        _ => {
            return Err(zero_postgres::Error::type_mismatch(oid::FLOAT4, target_oid));
        }
    }
    Ok(())
}

/// Encode an f64 value with flexible float type encoding.
fn encode_double(v: f64, target_oid: Oid, buf: &mut Vec<u8>) -> zero_postgres::Result<()> {
    match target_oid {
        oid::FLOAT4 => {
            // Note: potential precision loss
            buf.extend_from_slice(&4_i32.to_be_bytes());
            buf.extend_from_slice(&(v as f32).to_bits().to_be_bytes());
        }
        oid::FLOAT8 | 0 => {
            buf.extend_from_slice(&8_i32.to_be_bytes());
            buf.extend_from_slice(&v.to_bits().to_be_bytes());
        }
        _ => {
            return Err(zero_postgres::Error::type_mismatch(oid::FLOAT8, target_oid));
        }
    }
    Ok(())
}

/// Convert (year, month, day) to days since `PostgreSQL` epoch (2000-01-01)
fn ymd_to_days_since_pg_epoch(year: i32, month: u8, day: u8) -> i32 {
    // Algorithm from Howard Hinnant
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let m = i32::from(month);
    let d = i32::from(day);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let julian = era * 146_097 + doe;

    // PostgreSQL epoch is 2000-01-01 which is Julian day 2451545
    julian - 2_451_545
}

/// Convert (hour, minute, second, microsecond) to microseconds since midnight
fn time_to_micros(hour: u8, minute: u8, second: u8, micro: u32) -> i64 {
    let h = i64::from(hour);
    let m = i64::from(minute);
    let s = i64::from(second);
    let us = i64::from(micro);
    h * 3_600_000_000 + m * 60_000_000 + s * 1_000_000 + us
}
