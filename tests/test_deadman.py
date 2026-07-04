"""deadman 감시 로직 고정 — Phase 9.

heartbeat 신선도 판정(순수 함수)과 파싱을 원천 PG 없이 못박는다. deadman은
'성공의 부재'를 잡는 역방향 감시라, 경계(기한 직전/직후·heartbeat 없음)를 특히
정확히 고정해야 한다 — 여기가 조용히 틀리면 침묵을 놓친다.
"""
from datetime import UTC, datetime, timedelta

import pytest

from extract.deadman import (
    check_stale,
    evaluate,
    parse_duration,
    parse_watch,
)

NOW = datetime(2026, 7, 9, 12, 0, 0, tzinfo=UTC)


class TestParseDuration:
    def test_hours(self):
        assert parse_duration("26h") == 26 * 3600

    def test_minutes(self):
        assert parse_duration("90m") == 90 * 60

    def test_days(self):
        assert parse_duration("8d") == 8 * 86400

    def test_seconds(self):
        assert parse_duration("30s") == 30

    def test_bare_number_is_hours(self):
        assert parse_duration("2") == 2 * 3600

    def test_empty_fails(self):
        with pytest.raises(ValueError):
            parse_duration("")


class TestParseWatch:
    def test_single(self):
        assert parse_watch("snapshot_offload:26h") == {"snapshot_offload": 26 * 3600}

    def test_multiple(self):
        got = parse_watch("a:1h, b:8d")
        assert got == {"a": 3600, "b": 8 * 86400}

    def test_default_when_no_duration(self):
        assert parse_watch("a") == {"a": 26 * 3600}


class TestCheckStale:
    def test_fresh_within_deadline(self):
        v = check_stale("d", NOW - timedelta(hours=10), 26 * 3600, now=NOW)
        assert v.stale is False
        assert round(v.age_hours, 1) == 10.0

    def test_stale_past_deadline(self):
        v = check_stale("d", NOW - timedelta(hours=30), 26 * 3600, now=NOW)
        assert v.stale is True
        assert round(v.age_hours, 1) == 30.0

    def test_missing_heartbeat_is_stale(self):
        # 한 번도 성공 못 함 / 테이블 비어 있음 → 낡음으로 본다(가장 위험한 상태).
        v = check_stale("d", None, 26 * 3600, now=NOW)
        assert v.stale is True
        assert v.age_seconds is None
        assert v.age_hours is None

    def test_exactly_at_deadline_is_not_stale(self):
        # 경계: age == max_age는 아직 통과(> 만 낡음).
        v = check_stale("d", NOW - timedelta(hours=26), 26 * 3600, now=NOW)
        assert v.stale is False

    def test_naive_timestamp_treated_as_utc(self):
        naive = (NOW - timedelta(hours=30)).replace(tzinfo=None)
        v = check_stale("d", naive, 26 * 3600, now=NOW)
        assert v.stale is True


class TestEvaluate:
    def test_mix_of_healthy_and_stale(self):
        watch = {"good": 26 * 3600, "bad": 26 * 3600, "never": 26 * 3600}
        heartbeats = {
            "good": NOW - timedelta(hours=2),
            "bad": NOW - timedelta(hours=40),
            # "never"는 heartbeat 없음.
        }
        verdicts = {v.dag_id: v.stale for v in evaluate(watch, heartbeats, now=NOW)}
        assert verdicts == {"good": False, "bad": True, "never": True}


class TestRunFiresWebhook:
    """run()이 낡은 heartbeat마다 정확히 한 번 webhook을 쏘는가(PG·수신기 없이 페이크)."""

    def _patch(self, monkeypatch, heartbeats):
        import extract.deadman as dm

        sent: list[dict] = []
        monkeypatch.setattr(dm, "read_heartbeats", lambda: heartbeats)
        monkeypatch.setattr(dm, "post_webhook", lambda payload: sent.append(payload) or True)
        return dm, sent

    def test_stale_fires_one_alert(self, monkeypatch):
        dm, sent = self._patch(monkeypatch, {"snapshot_offload": NOW - timedelta(hours=40)})
        fired = dm.run("snapshot_offload:26h", now=NOW)
        assert fired == 1
        assert len(sent) == 1
        assert sent[0]["event"] == "pipeline_deadman"
        assert sent[0]["dag_id"] == "snapshot_offload"

    def test_healthy_fires_nothing(self, monkeypatch):
        dm, sent = self._patch(monkeypatch, {"snapshot_offload": NOW - timedelta(hours=1)})
        fired = dm.run("snapshot_offload:26h", now=NOW)
        assert fired == 0
        assert sent == []

    def test_missing_heartbeat_fires(self, monkeypatch):
        dm, sent = self._patch(monkeypatch, {})
        fired = dm.run("snapshot_offload:26h", now=NOW)
        assert fired == 1
        assert sent[0]["last_success_at"] is None
