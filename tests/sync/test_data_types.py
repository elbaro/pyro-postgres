"""Tests for sync data type handling."""

import datetime
from decimal import Decimal
from uuid import UUID

import pytest

from pyro_postgres.sync import Conn

from ..conftest import get_test_db_url


class TestSyncIntegerTypes:
    """Test sync integer type handling."""

    def test_smallint(self):
        """Test SMALLINT (int2)."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT 32767::smallint")
        assert result[0] == 32767
        conn.close()

    def test_integer(self):
        """Test INTEGER (int4)."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT 2147483647::integer")
        assert result[0] == 2147483647
        conn.close()

    def test_bigint(self):
        """Test BIGINT (int8)."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT 9223372036854775807::bigint")
        assert result[0] == 9223372036854775807
        conn.close()

    def test_negative_integers(self):
        """Test negative integers."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT -42::integer")
        assert result[0] == -42
        conn.close()

    def test_integer_param(self):
        """Test integer as parameter."""
        conn = Conn(get_test_db_url())
        result = conn.exec_first("SELECT $1::integer", (42,))
        assert result[0] == 42
        conn.close()

    def test_bigint_param(self):
        """Test bigint as parameter."""
        conn = Conn(get_test_db_url())
        result = conn.exec_first("SELECT $1::bigint", (9223372036854775807,))
        assert result[0] == 9223372036854775807
        conn.close()


class TestSyncFloatingPointTypes:
    """Test sync floating point type handling."""

    def test_real(self):
        """Test REAL (float4)."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT 3.14::real")
        assert abs(result[0] - 3.14) < 0.001
        conn.close()

    def test_double_precision(self):
        """Test DOUBLE PRECISION (float8)."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT 3.141592653589793::double precision")
        assert abs(result[0] - 3.141592653589793) < 0.0000001
        conn.close()

    def test_float_param(self):
        """Test float as parameter."""
        conn = Conn(get_test_db_url())
        result = conn.exec_first("SELECT $1::double precision", (3.14,))
        assert abs(result[0] - 3.14) < 0.001
        conn.close()

    def test_negative_float(self):
        """Test negative float."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT -3.14::double precision")
        assert abs(result[0] - (-3.14)) < 0.001
        conn.close()


class TestSyncStringTypes:
    """Test sync string type handling."""

    def test_text(self):
        """Test TEXT type."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT 'hello world'::text")
        assert result[0] == "hello world"
        conn.close()

    def test_varchar(self):
        """Test VARCHAR type."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT 'hello'::varchar(255)")
        assert result[0] == "hello"
        conn.close()

    def test_char(self):
        """Test CHAR type."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT 'a'::char(1)")
        assert result[0] == "a"
        conn.close()

    def test_empty_string(self):
        """Test empty string."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT ''::text")
        assert result[0] == ""
        conn.close()

    def test_unicode_string(self):
        """Test Unicode string."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT 'こんにちは'::text")
        assert result[0] == "こんにちは"
        conn.close()

    def test_string_param(self):
        """Test string as parameter."""
        conn = Conn(get_test_db_url())
        result = conn.exec_first("SELECT $1::text", ("hello",))
        assert result[0] == "hello"
        conn.close()

    def test_unicode_param(self):
        """Test Unicode as parameter."""
        conn = Conn(get_test_db_url())
        result = conn.exec_first("SELECT $1::text", ("日本語",))
        assert result[0] == "日本語"
        conn.close()


class TestSyncBooleanType:
    """Test sync boolean type handling."""

    def test_true(self):
        """Test TRUE value."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT TRUE")
        assert result[0] is True
        conn.close()

    def test_false(self):
        """Test FALSE value."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT FALSE")
        assert result[0] is False
        conn.close()

    def test_bool_param_true(self):
        """Test boolean True as parameter."""
        conn = Conn(get_test_db_url())
        result = conn.exec_first("SELECT $1::boolean", (True,))
        assert result[0] is True
        conn.close()

    def test_bool_param_false(self):
        """Test boolean False as parameter."""
        conn = Conn(get_test_db_url())
        result = conn.exec_first("SELECT $1::boolean", (False,))
        assert result[0] is False
        conn.close()


class TestSyncBinaryTypes:
    """Test sync binary type handling."""

    def test_bytea(self):
        """Test BYTEA type."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT '\\x48454c4c4f'::bytea")
        assert result[0] == b"HELLO"
        conn.close()

    def test_bytea_param(self):
        """Test bytes as parameter."""
        conn = Conn(get_test_db_url())
        result = conn.exec_first("SELECT $1::bytea", (b"hello",))
        assert result[0] == b"hello"
        conn.close()

    def test_empty_bytea(self):
        """Test empty bytea."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT ''::bytea")
        assert result[0] == b""
        conn.close()


class TestSyncDateTimeTypes:
    """Test sync date/time type handling."""

    def test_date(self):
        """Test DATE type."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT '2024-01-15'::date")
        assert result[0] == datetime.date(2024, 1, 15)
        conn.close()

    def test_time(self):
        """Test TIME type."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT '14:30:00'::time")
        assert result[0] == datetime.time(14, 30, 0)
        conn.close()

    def test_time_with_microseconds(self):
        """Test TIME with microseconds."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT '14:30:00.123456'::time")
        assert result[0] == datetime.time(14, 30, 0, 123456)
        conn.close()

    def test_timestamp(self):
        """Test TIMESTAMP type."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT '2024-01-15 14:30:00'::timestamp")
        assert result[0] == datetime.datetime(2024, 1, 15, 14, 30, 0)
        conn.close()

    def test_timestamp_with_microseconds(self):
        """Test TIMESTAMP with microseconds."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT '2024-01-15 14:30:00.123456'::timestamp")
        assert result[0] == datetime.datetime(2024, 1, 15, 14, 30, 0, 123456)
        conn.close()

    def test_date_param(self):
        """Test date as parameter."""
        conn = Conn(get_test_db_url())
        d = datetime.date(2024, 1, 15)
        result = conn.exec_first("SELECT $1::date", (d,))
        assert result[0] == d
        conn.close()

    def test_time_param(self):
        """Test time as parameter."""
        conn = Conn(get_test_db_url())
        t = datetime.time(14, 30, 0)
        result = conn.exec_first("SELECT $1::time", (t,))
        assert result[0] == t
        conn.close()

    def test_datetime_param(self):
        """Test datetime as parameter."""
        conn = Conn(get_test_db_url())
        dt = datetime.datetime(2024, 1, 15, 14, 30, 0)
        result = conn.exec_first("SELECT $1::timestamp", (dt,))
        assert result[0] == dt
        conn.close()

    def test_timedelta_param(self):
        """Test timedelta as parameter (interval)."""
        conn = Conn(get_test_db_url())
        td = datetime.timedelta(days=5, hours=3, minutes=30)
        result = conn.exec_first("SELECT $1::interval", (td,))
        # Result is a timedelta
        assert isinstance(result[0], datetime.timedelta)
        conn.close()


class TestSyncNumericTypes:
    """Test sync numeric/decimal type handling."""

    def test_numeric(self):
        """Test NUMERIC type."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT 123.456::numeric")
        assert result[0] == Decimal("123.456")
        conn.close()

    def test_numeric_high_precision(self):
        """Test NUMERIC with high precision."""
        conn = Conn(get_test_db_url())
        result = conn.query_first(
            "SELECT 12345678901234567890.12345678901234567890::numeric"
        )
        assert isinstance(result[0], Decimal)
        conn.close()

    def test_decimal_param(self):
        """Test Decimal as parameter."""
        conn = Conn(get_test_db_url())
        d = Decimal("123.456")
        result = conn.exec_first("SELECT $1::numeric", (d,))
        assert result[0] == d
        conn.close()


class TestSyncUUIDType:
    """Test sync UUID type handling."""

    def test_uuid(self):
        """Test UUID type."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid")
        assert result[0] == UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
        conn.close()

    def test_uuid_param(self):
        """Test UUID as parameter."""
        conn = Conn(get_test_db_url())
        u = UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
        result = conn.exec_first("SELECT $1::uuid", (u,))
        assert result[0] == u
        conn.close()


class TestSyncJSONTypes:
    """Test sync JSON type handling."""

    def test_json(self):
        """Test JSON type returns string."""
        conn = Conn(get_test_db_url())
        result = conn.query_first('SELECT \'{"key": "value"}\'::json')
        assert isinstance(result[0], str)
        assert "key" in result[0]
        conn.close()

    def test_jsonb(self):
        """Test JSONB type returns string."""
        conn = Conn(get_test_db_url())
        result = conn.query_first('SELECT \'{"key": "value"}\'::jsonb')
        assert isinstance(result[0], str)
        assert "key" in result[0]
        conn.close()

    def test_json_array_param(self):
        """Test list as JSON parameter using Jsonb wrapper."""
        from pyro_postgres import Jsonb

        conn = Conn(get_test_db_url())
        result = conn.exec_first("SELECT $1::jsonb", (Jsonb([1, 2, 3]),))
        assert isinstance(result[0], str)
        conn.close()

    def test_json_dict_param(self):
        """Test dict as JSON parameter using Jsonb wrapper."""
        from pyro_postgres import Jsonb

        conn = Conn(get_test_db_url())
        result = conn.exec_first("SELECT $1::jsonb", (Jsonb({"key": "value"}),))
        assert isinstance(result[0], str)
        conn.close()


class TestSyncNullValues:
    """Test sync NULL value handling."""

    def test_null_result(self):
        """Test NULL in result."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT NULL::integer")
        assert result[0] is None
        conn.close()

    def test_null_param(self):
        """Test NULL as parameter."""
        conn = Conn(get_test_db_url())
        result = conn.exec_first("SELECT $1::integer IS NULL", (None,))
        assert result[0] is True
        conn.close()

    def test_null_in_different_types(self):
        """Test NULL for different types."""
        conn = Conn(get_test_db_url())
        result = conn.query_first(
            "SELECT NULL::text, NULL::integer, NULL::boolean, NULL::date"
        )
        assert result[0] is None
        assert result[1] is None
        assert result[2] is None
        assert result[3] is None
        conn.close()


class TestSyncSpecialValues:
    """Test sync special value handling."""

    def test_oid(self):
        """Test OID type."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT 12345::oid")
        assert result[0] == 12345
        conn.close()

    def test_name(self):
        """Test NAME type."""
        conn = Conn(get_test_db_url())
        result = conn.query_first("SELECT 'pg_catalog'::name")
        assert result[0] == "pg_catalog"
        conn.close()
