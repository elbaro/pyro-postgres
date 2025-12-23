//! Adapter to convert Python parameter values to `PostgreSQL` wire format.

use time::{Date, Month, Time};
use zero_postgres::conversion::ToParams;
use zero_postgres::protocol::types::{Oid, oid};

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

    fn encode(&self, target_oids: &[Oid], buf: &mut Vec<u8>) -> zero_postgres::Result<()> {
        for (value, &target_oid) in self.params.0.iter().zip(target_oids.iter()) {
            encode_value(value, target_oid, buf)?;
        }
        Ok(())
    }
}

/// Get the natural OID for a Python value.
fn natural_oid(value: &Value) -> Oid {
    match value {
        Value::NULL => 0, // Unknown/NULL
        Value::Bool(_) => oid::BOOL,
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
        Value::Uuid(_) => oid::UUID,
        Value::Json(_) => oid::JSON,
        Value::Jsonb(_) => oid::JSONB,
        Value::Decimal(_) => oid::NUMERIC,
    }
}

/// Encode a Python Value for a target OID.
///
/// Format: Int32 length followed by bytes, or Int32 -1 for NULL.
///
/// This function supports flexible encoding: an i64 can encode as INT2, INT4, or INT8
/// depending on what the server expects (with overflow checking).
fn encode_value(value: &Value, target_oid: Oid, buf: &mut Vec<u8>) -> zero_postgres::Result<()> {
    match value {
        Value::NULL => {
            buf.extend_from_slice(&(-1_i32).to_be_bytes());
            Ok(())
        }

        Value::Bool(v) => encode_bool(*v, target_oid, buf),

        Value::Int(v) => encode_int(*v, target_oid, buf),

        Value::UInt(v) => encode_uint(*v, target_oid, buf),

        Value::Float(v) => encode_float(*v, target_oid, buf),

        Value::Double(v) => encode_double(*v, target_oid, buf),

        Value::Str(s) => {
            let s_ref: &str = s.as_ref();
            encode_str(s_ref, target_oid, buf)
        }

        Value::Bytes(b) => {
            let bytes: &[u8] = b.as_ref();
            buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            buf.extend_from_slice(bytes);
            Ok(())
        }

        Value::Date(year, month, day) => {
            // PostgreSQL binary date: i32 days since 2000-01-01
            let days = days_since_pg_epoch(*year, *month, *day)?;
            buf.extend_from_slice(&4_i32.to_be_bytes());
            buf.extend_from_slice(&days.to_be_bytes());
            Ok(())
        }

        Value::Time(hour, minute, second, micro) => {
            // PostgreSQL binary time: i64 microseconds since midnight
            let micros = micros_since_midnight(*hour, *minute, *second, *micro)?;
            buf.extend_from_slice(&8_i32.to_be_bytes());
            buf.extend_from_slice(&micros.to_be_bytes());
            Ok(())
        }

        Value::Timestamp(year, month, day, hour, minute, second, micro) => {
            // PostgreSQL binary timestamp: i64 microseconds since 2000-01-01 00:00:00
            let days = days_since_pg_epoch(*year, *month, *day)?;
            let time_micros = micros_since_midnight(*hour, *minute, *second, *micro)?;
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

        Value::Uuid(v) => encode_uuid(*v, target_oid, buf),

        Value::Json(s) => encode_json(s, target_oid, buf),

        Value::Jsonb(s) => encode_jsonb(s, target_oid, buf),

        Value::Decimal(s) => encode_decimal(s.as_ref(), target_oid, buf),
    }
}

/// Encode a bool value with flexible type encoding.
fn encode_bool(v: bool, target_oid: Oid, buf: &mut Vec<u8>) -> zero_postgres::Result<()> {
    match target_oid {
        oid::BOOL | 0 => {
            buf.extend_from_slice(&1_i32.to_be_bytes());
            buf.push(if v { 1 } else { 0 });
        }
        oid::INT2 => {
            buf.extend_from_slice(&2_i32.to_be_bytes());
            buf.extend_from_slice(&(i16::from(v)).to_be_bytes());
        }
        oid::INT4 => {
            buf.extend_from_slice(&4_i32.to_be_bytes());
            buf.extend_from_slice(&(i32::from(v)).to_be_bytes());
        }
        oid::INT8 => {
            buf.extend_from_slice(&8_i32.to_be_bytes());
            buf.extend_from_slice(&(i64::from(v)).to_be_bytes());
        }
        _ => {
            return Err(zero_postgres::Error::type_mismatch(oid::BOOL, target_oid));
        }
    }
    Ok(())
}

/// Encode an i64 value with flexible integer type encoding.
fn encode_int(v: i64, target_oid: Oid, buf: &mut Vec<u8>) -> zero_postgres::Result<()> {
    match target_oid {
        oid::INT2 => {
            let v16 =
                i16::try_from(v).map_err(|_| zero_postgres::Error::overflow("i64", "INT2"))?;
            buf.extend_from_slice(&2_i32.to_be_bytes());
            buf.extend_from_slice(&v16.to_be_bytes());
        }
        oid::INT4 => {
            let v32 =
                i32::try_from(v).map_err(|_| zero_postgres::Error::overflow("i64", "INT4"))?;
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
            let v16 =
                i16::try_from(v).map_err(|_| zero_postgres::Error::overflow("u64", "INT2"))?;
            buf.extend_from_slice(&2_i32.to_be_bytes());
            buf.extend_from_slice(&v16.to_be_bytes());
        }
        oid::INT4 => {
            let v32 =
                i32::try_from(v).map_err(|_| zero_postgres::Error::overflow("u64", "INT4"))?;
            buf.extend_from_slice(&4_i32.to_be_bytes());
            buf.extend_from_slice(&v32.to_be_bytes());
        }
        oid::INT8 | 0 => {
            // INT8 or unknown (0) - convert to i64
            let v64 =
                i64::try_from(v).map_err(|_| zero_postgres::Error::overflow("u64", "INT8"))?;
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

/// Encode a string value with flexible type encoding.
fn encode_str(s: &str, target_oid: Oid, buf: &mut Vec<u8>) -> zero_postgres::Result<()> {
    let bytes = s.as_bytes();
    match target_oid {
        oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME | oid::JSON | 0 => {
            // Text types and JSON: raw bytes
            buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            buf.extend_from_slice(bytes);
        }
        oid::JSONB => {
            // JSONB binary format: version byte (0x01) + JSON text
            buf.extend_from_slice(&(bytes.len() as i32 + 1).to_be_bytes());
            buf.push(0x01);
            buf.extend_from_slice(bytes);
        }
        _ => {
            return Err(zero_postgres::Error::type_mismatch(oid::TEXT, target_oid));
        }
    }
    Ok(())
}

/// Encode a UUID value.
fn encode_uuid(v: u128, target_oid: Oid, buf: &mut Vec<u8>) -> zero_postgres::Result<()> {
    match target_oid {
        oid::UUID | 0 => {
            // PostgreSQL binary UUID: 16 bytes (big-endian u128)
            buf.extend_from_slice(&16_i32.to_be_bytes());
            buf.extend_from_slice(&v.to_be_bytes());
        }
        _ => {
            return Err(zero_postgres::Error::type_mismatch(oid::UUID, target_oid));
        }
    }
    Ok(())
}

/// Encode a JSON value.
fn encode_json(s: &str, target_oid: Oid, buf: &mut Vec<u8>) -> zero_postgres::Result<()> {
    let bytes = s.as_bytes();
    match target_oid {
        oid::JSON | 0 => {
            buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            buf.extend_from_slice(bytes);
        }
        oid::JSONB => {
            buf.extend_from_slice(&(bytes.len() as i32 + 1).to_be_bytes());
            buf.push(0x01);
            buf.extend_from_slice(bytes);
        }
        _ => {
            return Err(zero_postgres::Error::type_mismatch(oid::JSON, target_oid));
        }
    }
    Ok(())
}

/// Encode a JSONB value.
fn encode_jsonb(s: &str, target_oid: Oid, buf: &mut Vec<u8>) -> zero_postgres::Result<()> {
    let bytes = s.as_bytes();
    match target_oid {
        oid::JSONB | 0 => {
            buf.extend_from_slice(&(bytes.len() as i32 + 1).to_be_bytes());
            buf.push(0x01);
            buf.extend_from_slice(bytes);
        }
        oid::JSON => {
            buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            buf.extend_from_slice(bytes);
        }
        _ => {
            return Err(zero_postgres::Error::type_mismatch(oid::JSONB, target_oid));
        }
    }
    Ok(())
}

/// Encode a Decimal value (text format for NUMERIC).
fn encode_decimal(s: &str, target_oid: Oid, buf: &mut Vec<u8>) -> zero_postgres::Result<()> {
    match target_oid {
        oid::NUMERIC | 0 => {
            // Text format: just the string representation
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            buf.extend_from_slice(bytes);
            Ok(())
        }
        _ => Err(zero_postgres::Error::type_mismatch(
            oid::NUMERIC,
            target_oid,
        )),
    }
}

/// PostgreSQL epoch (2000-01-01)
const PG_EPOCH: Date = match Date::from_calendar_date(2000, Month::January, 1) {
    Ok(d) => d,
    Err(_) => unreachable!(),
};

/// Convert (year, month, day) to days since `PostgreSQL` epoch (2000-01-01)
fn days_since_pg_epoch(year: i32, month: u8, day: u8) -> zero_postgres::Result<i32> {
    let month = Month::try_from(month)
        .map_err(|_| zero_postgres::Error::InvalidUsage(format!("invalid month: {month}")))?;
    let date = Date::from_calendar_date(year, month, day)
        .map_err(|e| zero_postgres::Error::InvalidUsage(format!("invalid date: {e}")))?;
    Ok(date.to_julian_day() - PG_EPOCH.to_julian_day())
}

/// Convert (hour, minute, second, microsecond) to microseconds since midnight
fn micros_since_midnight(
    hour: u8,
    minute: u8,
    second: u8,
    micro: u32,
) -> zero_postgres::Result<i64> {
    let time = Time::from_hms_micro(hour, minute, second, micro)
        .map_err(|e| zero_postgres::Error::InvalidUsage(format!("invalid time: {e}")))?;
    let (h, m, s, us) = time.as_hms_micro();
    Ok(i64::from(h) * 3_600_000_000
        + i64::from(m) * 60_000_000
        + i64::from(s) * 1_000_000
        + i64::from(us))
}
