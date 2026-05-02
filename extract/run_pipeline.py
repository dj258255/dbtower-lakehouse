"""오케스트레이션 — 품질 게이트를 dbt 앞에 세운 fail-closed 파이프라인(호스트용).

    load(이미 적재됨) → quality gate → (통과 시에만) dbt run

Airflow 없이도 fail-closed를 그대로 재현한다. 게이트가 FAIL이면 dbt를 아예 호출하지
않고 종료코드 2로 빠진다 — 조용히 틀린 데이터 위에 마트를 짓지 않는다.

    python -m extract.run_pipeline 2026-07-05 2026-07-06 2026-07-07
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from extract.quality import evaluate, print_report

DBT_DIR = Path(__file__).resolve().parent.parent / "dbt" / "dbtower_lakehouse"


def run(days: list[str]) -> int:
    print("=== 1) 품질 게이트 ===")
    reports = evaluate(days)
    print_report(reports)

    if any(r.blocked for r in reports):
        print("\n=== 2) dbt ===")
        print("SKIPPED — 게이트 FAIL. dbt를 실행하지 않는다(fail-closed).")
        return 2

    print("\n=== 2) dbt run (게이트 통과) ===")
    sys.stdout.flush()  # 버퍼 비우고 dbt 서브프로세스 출력이 뒤에 오도록 순서 보장.
    proc = subprocess.run(
        [sys.executable, "-m", "dbt.cli.main", "run", "--profiles-dir", "."],
        cwd=DBT_DIR,
    )
    return proc.returncode


if __name__ == "__main__":
    argv = sys.argv[1:] or ["2026-07-05", "2026-07-06", "2026-07-07"]
    raise SystemExit(run(argv))
