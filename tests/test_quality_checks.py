"""품질 게이트 판정 로직(순수 함수) 고정 — Phase 8 (F11).

게이트의 세 판정(reconciliation/completeness/freshness)과 Phase 8에 추가된
schema_drift는 입력이 dict/리스트인 순수 함수다. 원천 PG도 S3도 없이
경계 사례를 고정한다 — 판정 기준이 조용히 바뀌면 여기서 깨진다.
"""
from datetime import datetime

from extract.quality import (
    FAIL,
    OK,
    WARN,
    DtReport,
    check_completeness,
    check_freshness,
    check_reconciliation,
    check_schema_drift,
)

EXPECTED = {
    "id": "bigint",
    "captured_at": "timestamp without time zone",
}


class TestReconciliation:
    def test_ok_when_counts_match(self):
        r = check_reconciliation({1: 100, 2: 50}, {1: 100, 2: 50})
        assert r.status == OK

    def test_fail_on_any_instance_mismatch(self):
        r = check_reconciliation({1: 100, 2: 50}, {1: 100, 2: 49})
        assert r.status == FAIL
        assert "inst 2" in r.detail

    def test_fail_when_parquet_has_instance_pg_lacks(self):
        # 원천엔 없는데 parquet에만 있는 인스턴스 = 유령 적재 — 역방향도 잡는다.
        r = check_reconciliation({1: 100}, {1: 100, 9: 10})
        assert r.status == FAIL

    def test_both_empty_is_ok(self):
        # 0 == 0 정합. (이 상황 자체는 completeness가 따로 잡는다.)
        assert check_reconciliation({}, {}).status == OK


class TestCompleteness:
    def test_ok_when_all_registry_instances_present(self):
        assert check_completeness([1, 2], {1: 5, 2: 7}).status == OK

    def test_fail_on_missing_instance(self):
        r = check_completeness([1, 2, 3], {1: 5, 2: 7})
        assert r.status == FAIL
        assert "[3]" in r.detail

    def test_zero_row_instance_counts_as_missing(self):
        # 파일은 있는데 0행 = 존재 아님. n > 0만 존재로 친다.
        assert check_completeness([1], {1: 0}).status == FAIL


class TestFreshness:
    def test_ok_near_day_boundary(self):
        r = check_freshness(datetime(2026, 7, 5, 23, 59, 30), "2026-07-05")
        assert r.status == OK

    def test_warn_between_thresholds(self):
        # 경계까지 4h — WARN(>3h) 구간, FAIL(>12h) 미만.
        r = check_freshness(datetime(2026, 7, 5, 20, 0, 0), "2026-07-05")
        assert r.status == WARN

    def test_fail_when_half_day_empty(self):
        r = check_freshness(datetime(2026, 7, 5, 9, 0, 0), "2026-07-05")
        assert r.status == FAIL

    def test_fail_when_partition_empty(self):
        assert check_freshness(None, "2026-07-05").status == FAIL

    def test_accepts_iso_string(self):
        r = check_freshness("2026-07-05T23:59:00", "2026-07-05")
        assert r.status == OK


class TestSchemaDrift:
    def test_ok_when_types_match(self):
        assert check_schema_drift(dict(EXPECTED), EXPECTED).status == OK

    def test_fail_on_missing_column(self):
        r = check_schema_drift({"id": "bigint"}, EXPECTED)
        assert r.status == FAIL
        assert "captured_at" in r.detail

    def test_fail_on_type_change(self):
        actual = dict(EXPECTED, id="integer")
        r = check_schema_drift(actual, EXPECTED)
        assert r.status == FAIL
        assert "bigint" in r.detail

    def test_warn_on_extra_source_column(self):
        actual = dict(EXPECTED, new_metric="bigint")
        r = check_schema_drift(actual, EXPECTED)
        assert r.status == WARN
        assert "new_metric" in r.detail


class TestVerdict:
    def test_fail_blocks_and_wins_over_warn(self):
        rep = DtReport(dt="2026-07-05")
        rep.checks.append(check_freshness(datetime(2026, 7, 5, 20, 0), "2026-07-05"))  # WARN
        rep.checks.append(check_reconciliation({1: 1}, {1: 2}))  # FAIL
        assert rep.status == FAIL
        assert rep.blocked is True

    def test_warn_does_not_block(self):
        rep = DtReport(dt="2026-07-05")
        rep.checks.append(check_freshness(datetime(2026, 7, 5, 20, 0), "2026-07-05"))  # WARN
        assert rep.status == WARN
        assert rep.blocked is False
