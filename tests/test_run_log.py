"""pipeline_run_log 레코드 조립 로직 고정 — Phase 10.

build_record는 게이트 축 dict + 상태 + 소요시간을 pipeline_run_log 행으로 접는
순수 함수다(DuckLake·S3 없이 판정을 고정). 운영 대시보드가 이 행을 읽으므로
필드 이름·매핑이 조용히 바뀌면 여기서 깨진다.
"""
from datetime import UTC, datetime

from extract.run_log import _axes_from_reports, build_record


class TestBuildRecord:
    def test_maps_gate_axes_to_columns(self):
        axes = {"reconciliation": "OK", "completeness": "OK",
                "freshness": "WARN", "schema_drift": "OK"}
        rec = build_record(
            dt="2026-07-05", gate_axes=axes, gate_status="WARN",
            duration_sec=1.2345, run_id="r1", offload_rows=149259,
            published_rows=1749,
        )
        assert rec["dt"] == "2026-07-05"
        assert rec["gate_status"] == "WARN"
        assert rec["gate_reconciliation"] == "OK"
        assert rec["gate_freshness"] == "WARN"
        assert rec["gate_schema_drift"] == "OK"
        assert rec["offload_rows"] == 149259
        assert rec["published_rows"] == 1749
        assert rec["duration_sec"] == 1.234  # 소수 3자리 반올림

    def test_missing_axis_stays_none(self):
        # 계약 밖 상태를 지어내지 않는다 — 없는 축은 None.
        rec = build_record(
            dt="2026-07-08", gate_axes={"reconciliation": "OK"},
            gate_status="FAIL", duration_sec=0.5,
        )
        assert rec["gate_completeness"] is None
        assert rec["gate_freshness"] is None
        assert rec["offload_rows"] is None

    def test_run_at_defaults_to_now(self):
        before = datetime.now(UTC)
        rec = build_record(dt="2026-07-05", gate_axes={}, gate_status="OK",
                           duration_sec=0.0)
        assert rec["run_at"] >= before


class _FakeCheck:
    def __init__(self, name, status):
        self.name, self.status = name, status


class _FakeReport:
    def __init__(self, checks, status):
        self.checks = checks
        self.status = status


class TestAxesFromReports:
    def test_extracts_axis_status_and_verdict(self):
        rep = _FakeReport(
            [_FakeCheck("reconciliation", "OK"), _FakeCheck("freshness", "FAIL")],
            "FAIL",
        )
        axes, status = _axes_from_reports([rep])
        assert axes == {"reconciliation": "OK", "freshness": "FAIL"}
        assert status == "FAIL"
