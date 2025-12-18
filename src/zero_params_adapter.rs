//! Adapter to convert Python parameter values to `PostgreSQL` wire format.

use zero_postgres::conversion::ToParams;

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

    fn to_binary(&self, buf: &mut Vec<u8>) {
        for value in &self.params.0 {
            encode_value_to_binary(value, buf);
        }
    }
}

/// Encode a Python Value to `PostgreSQL` binary format.
///
/// Format: Int32 length followed by bytes, or Int32 -1 for NULL.
fn encode_value_to_binary(value: &Value, buf: &mut Vec<u8>) {
    match value {
        Value::NULL => {
            buf.extend_from_slice(&(-1_i32).to_be_bytes());
        }

        Value::Int(v) => {
            buf.extend_from_slice(&8_i32.to_be_bytes());
            buf.extend_from_slice(&v.to_be_bytes());
        }

        Value::UInt(v) => {
            // Send as i64 (may overflow for very large u64)
            buf.extend_from_slice(&8_i32.to_be_bytes());
            buf.extend_from_slice(&(*v as i64).to_be_bytes());
        }

        Value::Float(v) => {
            buf.extend_from_slice(&4_i32.to_be_bytes());
            buf.extend_from_slice(&v.to_be_bytes());
        }

        Value::Double(v) => {
            buf.extend_from_slice(&8_i32.to_be_bytes());
            buf.extend_from_slice(&v.to_be_bytes());
        }

        Value::Str(s) => {
            let s_ref: &str = s.as_ref();
            let bytes = s_ref.as_bytes();
            buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            buf.extend_from_slice(bytes);
        }

        Value::Bytes(b) => {
            let bytes: &[u8] = b.as_ref();
            buf.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            buf.extend_from_slice(bytes);
        }

        Value::Date(year, month, day) => {
            // PostgreSQL binary date: i32 days since 2000-01-01
            let days = ymd_to_days_since_pg_epoch(*year, *month, *day);
            buf.extend_from_slice(&4_i32.to_be_bytes());
            buf.extend_from_slice(&days.to_be_bytes());
        }

        Value::Time(hour, minute, second, micro) => {
            // PostgreSQL binary time: i64 microseconds since midnight
            let micros = time_to_micros(*hour, *minute, *second, *micro);
            buf.extend_from_slice(&8_i32.to_be_bytes());
            buf.extend_from_slice(&micros.to_be_bytes());
        }

        Value::Timestamp(year, month, day, hour, minute, second, micro) => {
            // PostgreSQL binary timestamp: i64 microseconds since 2000-01-01 00:00:00
            let days = ymd_to_days_since_pg_epoch(*year, *month, *day);
            let time_micros = time_to_micros(*hour, *minute, *second, *micro);
            let total_micros = i64::from(days) * 86_400_000_000 + time_micros;
            buf.extend_from_slice(&8_i32.to_be_bytes());
            buf.extend_from_slice(&total_micros.to_be_bytes());
        }

        Value::Interval(months, days, micros) => {
            // PostgreSQL binary interval: 8 bytes microseconds + 4 bytes days + 4 bytes months
            buf.extend_from_slice(&16_i32.to_be_bytes());
            buf.extend_from_slice(&micros.to_be_bytes());
            buf.extend_from_slice(&days.to_be_bytes());
            buf.extend_from_slice(&months.to_be_bytes());
        }
    }
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
