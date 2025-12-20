"""Tests for async data type handling."""

import datetime
from decimal import Decimal
from uuid import UUID

import pytest

from pyro_postgres.async_ import Conn

from ..conftest import get_test_db_url


class TestAsyncIntegerTypes:
    """Test async integer type handling."""

    @pytest.mark.asyncio
    async def test_smallint(self):
        """Test SMALLINT (int2)."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first("SELECT 32767::smallint")
        assert result[0] == 32767
        await conn.close()

    @pytest.mark.asyncio
    async def test_integer(self):
        """Test INTEGER (int4)."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first("SELECT 2147483647::integer")
        assert result[0] == 2147483647
        await conn.close()

    @pytest.mark.asyncio
    async def test_bigint(self):
        """Test BIGINT (int8)."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first("SELECT 9223372036854775807::bigint")
        assert result[0] == 9223372036854775807
        await conn.close()

    @pytest.mark.asyncio
    async def test_negative_integers(self):
        """Test negative integers."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first("SELECT -42::integer")
        assert result[0] == -42
        await conn.close()

    @pytest.mark.asyncio
    async def test_integer_param(self):
        """Test integer as parameter."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.exec_first("SELECT $1::integer", (42,))
        assert result[0] == 42
        await conn.close()

    @pytest.mark.asyncio
    async def test_bigint_param(self):
        """Test bigint as parameter."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.exec_first("SELECT $1::bigint", (9223372036854775807,))
        assert result[0] == 9223372036854775807
        await conn.close()


class TestAsyncFloatingPointTypes:
    """Test async floating point type handling."""

    @pytest.mark.asyncio
    async def test_real(self):
        """Test REAL (float4)."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first("SELECT 3.14::real")
        assert abs(result[0] - 3.14) < 0.001
        await conn.close()

    @pytest.mark.asyncio
    async def test_double_precision(self):
        """Test DOUBLE PRECISION (float8)."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first("SELECT 3.141592653589793::double precision")
        assert abs(result[0] - 3.141592653589793) < 0.0000001
        await conn.close()

    @pytest.mark.asyncio
    async def test_float_param(self):
        """Test float as parameter."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.exec_first("SELECT $1::double precision", (3.14,))
        assert abs(result[0] - 3.14) < 0.001
        await conn.close()

    @pytest.mark.asyncio
    async def test_negative_float(self):
        """Test negative float."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first("SELECT -3.14::double precision")
        assert abs(result[0] - (-3.14)) < 0.001
        await conn.close()


class TestAsyncStringTypes:
    """Test async string type handling."""

    @pytest.mark.asyncio
    async def test_text(self):
        """Test TEXT type."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first("SELECT 'hello world'::text")
        assert result[0] == "hello world"
        await conn.close()

    @pytest.mark.asyncio
    async def test_varchar(self):
        """Test VARCHAR type."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first("SELECT 'hello'::varchar(255)")
        assert result[0] == "hello"
        await conn.close()

    @pytest.mark.asyncio
    async def test_char(self):
        """Test CHAR type."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first("SELECT 'a'::char(1)")
        assert result[0] == "a"
        await conn.close()

    @pytest.mark.asyncio
    async def test_empty_string(self):
        """Test empty string."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first("SELECT ''::text")
        assert result[0] == ""
        await conn.close()

    @pytest.mark.asyncio
    async def test_unicode_string(self):
        """Test Unicode string."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first("SELECT 'こんにちは'::text")
        assert result[0] == "こんにちは"
        await conn.close()

    @pytest.mark.asyncio
    async def test_string_param(self):
        """Test string as parameter."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.exec_first("SELECT $1::text", ("hello",))
        assert result[0] == "hello"
        await conn.close()

    @pytest.mark.asyncio
    async def test_unicode_param(self):
        """Test Unicode as parameter."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.exec_first("SELECT $1::text", ("日本語",))
        assert result[0] == "日本語"
        await conn.close()


class TestAsyncBooleanType:
    """Test async boolean type handling."""

    @pytest.mark.asyncio
    async def test_true(self):
        """Test TRUE value."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first("SELECT TRUE")
        assert result[0] is True
        await conn.close()

    @pytest.mark.asyncio
    async def test_false(self):
        """Test FALSE value."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first("SELECT FALSE")
        assert result[0] is False
        await conn.close()

    @pytest.mark.asyncio
    async def test_bool_param_true(self):
        """Test boolean True as parameter."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.exec_first("SELECT $1::boolean", (True,))
        assert result[0] is True
        await conn.close()

    @pytest.mark.asyncio
    async def test_bool_param_false(self):
        """Test boolean False as parameter."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.exec_first("SELECT $1::boolean", (False,))
        assert result[0] is False
        await conn.close()


class TestAsyncBinaryTypes:
    """Test async binary type handling."""

    @pytest.mark.asyncio
    async def test_bytea(self):
        """Test BYTEA type."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first("SELECT '\\x48454c4c4f'::bytea")
        assert result[0] == b"HELLO"
        await conn.close()

    @pytest.mark.asyncio
    async def test_bytea_param(self):
        """Test bytes as parameter."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.exec_first("SELECT $1::bytea", (b"hello",))
        assert result[0] == b"hello"
        await conn.close()

    @pytest.mark.asyncio
    async def test_empty_bytea(self):
        """Test empty bytea."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first("SELECT ''::bytea")
        assert result[0] == b""
        await conn.close()


class TestAsyncDateTimeTypes:
    """Test async date/time type handling."""

    @pytest.mark.asyncio
    async def test_date(self):
        """Test DATE type."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first("SELECT '2024-01-15'::date")
        assert result[0] == datetime.date(2024, 1, 15)
        await conn.close()

    @pytest.mark.asyncio
    async def test_time(self):
        """Test TIME type."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first("SELECT '14:30:00'::time")
        assert result[0] == datetime.time(14, 30, 0)
        await conn.close()

    @pytest.mark.asyncio
    async def test_time_with_microseconds(self):
        """Test TIME with microseconds."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first("SELECT '14:30:00.123456'::time")
        assert result[0] == datetime.time(14, 30, 0, 123456)
        await conn.close()

    @pytest.mark.asyncio
    async def test_timestamp(self):
        """Test TIMESTAMP type."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first("SELECT '2024-01-15 14:30:00'::timestamp")
        assert result[0] == datetime.datetime(2024, 1, 15, 14, 30, 0)
        await conn.close()

    @pytest.mark.asyncio
    async def test_timestamp_with_microseconds(self):
        """Test TIMESTAMP with microseconds."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first(
            "SELECT '2024-01-15 14:30:00.123456'::timestamp"
        )
        assert result[0] == datetime.datetime(2024, 1, 15, 14, 30, 0, 123456)
        await conn.close()

    @pytest.mark.asyncio
    async def test_date_param(self):
        """Test date as parameter."""
        conn = await Conn.new(get_test_db_url())
        d = datetime.date(2024, 1, 15)
        result = await conn.exec_first("SELECT $1::date", (d,))
        assert result[0] == d
        await conn.close()

    @pytest.mark.asyncio
    async def test_time_param(self):
        """Test time as parameter."""
        conn = await Conn.new(get_test_db_url())
        t = datetime.time(14, 30, 0)
        result = await conn.exec_first("SELECT $1::time", (t,))
        assert result[0] == t
        await conn.close()

    @pytest.mark.asyncio
    async def test_datetime_param(self):
        """Test datetime as parameter."""
        conn = await Conn.new(get_test_db_url())
        dt = datetime.datetime(2024, 1, 15, 14, 30, 0)
        result = await conn.exec_first("SELECT $1::timestamp", (dt,))
        assert result[0] == dt
        await conn.close()

    @pytest.mark.asyncio
    async def test_timedelta_param(self):
        """Test timedelta as parameter (interval)."""
        conn = await Conn.new(get_test_db_url())
        td = datetime.timedelta(days=5, hours=3, minutes=30)
        result = await conn.exec_first("SELECT $1::interval", (td,))
        # Result is a timedelta
        assert isinstance(result[0], datetime.timedelta)
        await conn.close()


class TestAsyncNumericTypes:
    """Test async numeric/decimal type handling."""

    @pytest.mark.asyncio
    async def test_numeric(self):
        """Test NUMERIC type."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first("SELECT 123.456::numeric")
        assert result[0] == Decimal("123.456")
        await conn.close()

    @pytest.mark.asyncio
    async def test_numeric_high_precision(self):
        """Test NUMERIC with high precision."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first(
            "SELECT 12345678901234567890.12345678901234567890::numeric"
        )
        assert isinstance(result[0], Decimal)
        await conn.close()

    @pytest.mark.asyncio
    async def test_decimal_param(self):
        """Test Decimal as parameter."""
        conn = await Conn.new(get_test_db_url())
        d = Decimal("123.456")
        result = await conn.exec_first("SELECT $1::numeric", (d,))
        assert result[0] == d
        await conn.close()


class TestAsyncUUIDType:
    """Test async UUID type handling."""

    @pytest.mark.asyncio
    async def test_uuid(self):
        """Test UUID type."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first(
            "SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid"
        )
        assert result[0] == UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
        await conn.close()

    @pytest.mark.asyncio
    async def test_uuid_param(self):
        """Test UUID as parameter."""
        conn = await Conn.new(get_test_db_url())
        u = UUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
        result = await conn.exec_first("SELECT $1::uuid", (u,))
        assert result[0] == u
        await conn.close()


class TestAsyncJSONTypes:
    """Test async JSON type handling."""

    @pytest.mark.asyncio
    async def test_json(self):
        """Test JSON type returns string."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first("SELECT '{\"key\": \"value\"}'::json")
        assert isinstance(result[0], str)
        assert "key" in result[0]
        await conn.close()

    @pytest.mark.asyncio
    async def test_jsonb(self):
        """Test JSONB type returns string."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first("SELECT '{\"key\": \"value\"}'::jsonb")
        assert isinstance(result[0], str)
        assert "key" in result[0]
        await conn.close()

    @pytest.mark.asyncio
    async def test_json_array_param(self):
        """Test list as JSON parameter using Jsonb wrapper."""
        from pyro_postgres import Jsonb

        conn = await Conn.new(get_test_db_url())
        result = await conn.exec_first("SELECT $1::jsonb", (Jsonb([1, 2, 3]),))
        assert isinstance(result[0], str)
        await conn.close()

    @pytest.mark.asyncio
    async def test_json_dict_param(self):
        """Test dict as JSON parameter using Jsonb wrapper."""
        from pyro_postgres import Jsonb

        conn = await Conn.new(get_test_db_url())
        result = await conn.exec_first("SELECT $1::jsonb", (Jsonb({"key": "value"}),))
        assert isinstance(result[0], str)
        await conn.close()


class TestAsyncNullValues:
    """Test async NULL value handling."""

    @pytest.mark.asyncio
    async def test_null_result(self):
        """Test NULL in result."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first("SELECT NULL::integer")
        assert result[0] is None
        await conn.close()

    @pytest.mark.asyncio
    async def test_null_param(self):
        """Test NULL as parameter."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.exec_first("SELECT $1::integer IS NULL", (None,))
        assert result[0] is True
        await conn.close()

    @pytest.mark.asyncio
    async def test_null_in_different_types(self):
        """Test NULL for different types."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first(
            "SELECT NULL::text, NULL::integer, NULL::boolean, NULL::date"
        )
        assert result[0] is None
        assert result[1] is None
        assert result[2] is None
        assert result[3] is None
        await conn.close()


class TestAsyncSpecialValues:
    """Test async special value handling."""

    @pytest.mark.asyncio
    async def test_oid(self):
        """Test OID type."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first("SELECT 12345::oid")
        assert result[0] == 12345
        await conn.close()

    @pytest.mark.asyncio
    async def test_name(self):
        """Test NAME type."""
        conn = await Conn.new(get_test_db_url())
        result = await conn.query_first("SELECT 'pg_catalog'::name")
        assert result[0] == "pg_catalog"
        await conn.close()
