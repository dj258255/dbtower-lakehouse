"""offload 경계 고정 — dt 파싱·하루 창·parquet 스키마 계약 (Phase 8, F11).

스키마는 docs/CONTRACT.md의 계약이다. 필드가 추가·삭제·타입 변경되면
다운스트림(dbt·DuckLake) 전부가 영향을 받으므로 테스트로 못박는다.
"""
from datetime import date, datetime

import pyarrow as pa
import pytest

from extract.offload import SNAPSHOT_SCHEMA, day_window, parse_logical_date


class TestParseLogicalDate:
    def test_iso_string(self):
        assert parse_logical_date("2026-07-05") == date(2026, 7, 5)

    def test_date_passthrough(self):
        d = date(2026, 7, 5)
        assert parse_logical_date(d) is d

    def test_bad_format_fails_loudly(self):
        with pytest.raises(ValueError):
            parse_logical_date("2026/07/05")

    def test_nonsense_date_fails(self):
        with pytest.raises(ValueError):
            parse_logical_date("2026-13-40")


class TestDayWindow:
    def test_half_open_utc_day(self):
        start, end = day_window(date(2026, 7, 5))
        assert start == datetime(2026, 7, 5, 0, 0, 0)
        assert end == datetime(2026, 7, 6, 0, 0, 0)

    def test_month_boundary(self):
        start, end = day_window(date(2026, 6, 30))
        assert end == datetime(2026, 7, 1, 0, 0, 0)


class TestSnapshotSchemaContract:
    def test_field_names_and_order(self):
        assert SNAPSHOT_SCHEMA.names == [
            "id", "instance_id", "captured_at", "query_id",
            "query_text", "calls", "total_time_ms", "rows_examined",
        ]

    def test_field_types(self):
        expected = {
            "id": pa.int64(),
            "instance_id": pa.int64(),
            "captured_at": pa.timestamp("us"),
            "query_id": pa.string(),
            "query_text": pa.string(),
            "calls": pa.int64(),
            "total_time_ms": pa.float64(),
            "rows_examined": pa.int64(),
        }
        for name, typ in expected.items():
            assert SNAPSHOT_SCHEMA.field(name).type == typ

    def test_only_query_text_nullable(self):
        for f in SNAPSHOT_SCHEMA:
            assert f.nullable == (f.name == "query_text")
