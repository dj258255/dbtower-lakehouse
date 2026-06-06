"""추출 파이프라인 설정 — 전부 환경변수로 주입한다.

기본값은 '호스트에서 직접 실행'(로컬 e2e 검증)에 맞춰져 있다.
Airflow 컨테이너 안에서 돌 때는 docker-compose가 컨테이너 호스트명
(dbtower-postgres:5432 / dbtower-minio:9000)으로 덮어쓴다.

원천은 반드시 DBTower 메타 PG(관측 전용)다. 운영 대상 DB는 건드리지 않는다.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class SourceConfig:
    """DBTower 메타 PG(query_snapshot이 사는 곳). 읽기 전용으로만 쓴다."""

    host: str = os.getenv("SRC_PG_HOST", "localhost")
    port: int = int(os.getenv("SRC_PG_PORT", "15432"))
    dbname: str = os.getenv("SRC_PG_DB", "dbtower")
    user: str = os.getenv("SRC_PG_USER", "postgres")
    password: str = os.getenv("SRC_PG_PASSWORD", "dbtower1234")

    def dsn(self) -> str:
        # connect_timeout: 원천이 죽어 있으면 무한 대기 대신 5초 안에 실패시킨다
        # (Phase 6 — 걸려서 멈춘 태스크는 재시도도 알림도 못 탄다. 빨리 죽어야 산다).
        return (
            f"host={self.host} port={self.port} dbname={self.dbname} "
            f"user={self.user} password={self.password} connect_timeout=5"
        )


@dataclass(frozen=True)
class SinkConfig:
    """MinIO(S3 호환). DBTower 데모 스택의 dbtower-minio 재사용."""

    endpoint: str = os.getenv("S3_ENDPOINT", "http://localhost:19000")
    access_key: str = os.getenv("S3_ACCESS_KEY", "dbtower")
    secret_key: str = os.getenv("S3_SECRET_KEY", "dbtower1234")
    bucket: str = os.getenv("S3_BUCKET", "lakehouse")
    region: str = os.getenv("S3_REGION", "us-east-1")


@dataclass(frozen=True)
class DuckLakeConfig:
    """DuckLake 테이블 포맷 설정(Phase 5).

    카탈로그는 PostgreSQL, 데이터 파일은 MinIO(S3)에 둔다. 카탈로그 DB는
    DBTower 메타 DB(dbtower)와 **분리된 별도 DB**(ducklake_catalog)를 쓴다 —
    같은 PG 인스턴스를 재사용하되 관측 데이터를 오염시키지 않는다.
    """

    # 카탈로그 PG 접속 — SourceConfig와 같은 인스턴스, DB만 다르다.
    catalog_host: str = os.getenv("SRC_PG_HOST", "localhost")
    catalog_port: int = int(os.getenv("SRC_PG_PORT", "15432"))
    catalog_db: str = os.getenv("DUCKLAKE_CATALOG_DB", "ducklake_catalog")
    catalog_user: str = os.getenv("SRC_PG_USER", "postgres")
    catalog_password: str = os.getenv("SRC_PG_PASSWORD", "dbtower1234")

    # 데이터 파일이 사는 곳(S3). 카탈로그는 메타데이터만, 실제 컬럼나 파일은 여기.
    data_path: str = os.getenv("DUCKLAKE_DATA_PATH", "s3://lakehouse/ducklake/")
    lake_alias: str = os.getenv("DUCKLAKE_ALIAS", "lh")

    def catalog_dsn(self) -> str:
        """DuckDB ATTACH 'ducklake:postgres:...' 에 넘길 PG DSN."""
        return (
            f"dbname={self.catalog_db} host={self.catalog_host} "
            f"port={self.catalog_port} user={self.catalog_user} "
            f"password={self.catalog_password} connect_timeout=5"
        )

    def admin_dsn(self) -> str:
        """카탈로그 DB 생성/조회용 접속(postgres 기본 DB로 접속)."""
        return (
            f"host={self.catalog_host} port={self.catalog_port} dbname=postgres "
            f"user={self.catalog_user} password={self.catalog_password} connect_timeout=5"
        )


# 추출 배치 크기 상한 — 메타 PG를 한 번에 짓누르지 않도록 서버 커서로 나눠 읽는다.
FETCH_BATCH_SIZE = int(os.getenv("EXTRACT_FETCH_BATCH", "50000"))

# 원천 테이블·raw 경로 규약(계약은 docs/CONTRACT.md).
SOURCE_TABLE = "query_snapshot"
RAW_PREFIX = "raw/query_snapshot"
