"""F3 발행 원자성 고정 — 마트 전체가 단일 트랜잭션으로 나가는가 (Phase 8).

DuckLake 없이 페이크 커넥션으로 SQL 시퀀스를 고정한다:
- 정상: BEGIN … 마트 전부 … COMMIT (ROLLBACK 없음)
- 중간 실패: ROLLBACK이 반드시 나가고 COMMIT은 없다 — 혼합 버전 노출 차단.
(실제 DuckLake에서의 전/후 스냅샷 실측은 docs/VERIFICATION.md Phase 8 절.)
"""
import pytest

import extract.publish_marts as pm


class FakeLakeCon:
    """execute된 SQL을 기록하고, n번째 CREATE에서 장애를 주입할 수 있는 커넥션."""

    def __init__(self, fail_on_create=None):
        self.sqls = []
        self.creates = 0
        self.fail_on_create = fail_on_create
        self._last = None

    def execute(self, sql, *args):
        self.sqls.append(sql.strip())
        s = sql.lstrip().upper()
        if s.startswith("CREATE OR REPLACE TABLE"):
            self.creates += 1
            if self.creates == self.fail_on_create:
                raise RuntimeError("주입 장애: 발행 중 사망")
        self._last = (42,) if s.startswith("SELECT COUNT") else None
        return self

    def fetchone(self):
        return self._last

    def close(self):
        pass


@pytest.fixture
def lake(monkeypatch):
    def _lake(fail_on_create=None):
        con = FakeLakeCon(fail_on_create)
        monkeypatch.setattr(pm, "open_lake", lambda cfg, sink: con)
        return con

    return _lake


def test_happy_path_single_transaction(lake):
    con = lake()
    published = pm.publish_marts(duckdb_path="ignored.duckdb")
    assert published == {t: 42 for t in pm.MART_TABLES}
    assert "BEGIN" in con.sqls
    assert "COMMIT" in con.sqls
    assert "ROLLBACK" not in con.sqls
    # 마트 발행 전부가 BEGIN과 COMMIT 사이에 있어야 한다(단일 커밋).
    begin, commit = con.sqls.index("BEGIN"), con.sqls.index("COMMIT")
    creates = [i for i, s in enumerate(con.sqls) if s.startswith("CREATE OR REPLACE")]
    assert len(creates) == len(pm.MART_TABLES)
    assert all(begin < i < commit for i in creates)


def test_mid_failure_rolls_back_no_commit(lake):
    con = lake(fail_on_create=2)
    with pytest.raises(RuntimeError, match="주입 장애"):
        pm.publish_marts(duckdb_path="ignored.duckdb")
    assert "ROLLBACK" in con.sqls
    assert "COMMIT" not in con.sqls
