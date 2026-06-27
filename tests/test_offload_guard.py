"""F1 아카이브 자기파괴 가드 고정 — Phase 8.

원천 보존(7일) 밖 dt를 재실행하면 원천은 0행이고, 이때 delete-first 멱등
덮어쓰기는 아카이브 유일본을 지운 뒤 아무것도 안 쓴다(감사에서 실측).
가드의 결정 로직(순수)과 run_offload 통합 경로(얇은 페이크 PG/S3)를 고정한다.
"""
import pytest

import extract.offload as offload
from extract.offload import (
    ArchiveSelfDestructError,
    decide_partition_action,
)


class TestDecidePartitionAction:
    def test_source_rows_present_overwrites(self):
        assert decide_partition_action(100, True) == "overwrite"
        assert decide_partition_action(100, False) == "overwrite"

    def test_empty_source_no_partition_skips(self):
        assert decide_partition_action(0, False) == "skip"

    def test_empty_source_existing_partition_refuses_loudly(self):
        # 핵심: 유일본일 수 있는 파티션 앞에서 조용한 삭제 대신 시끄러운 실패.
        with pytest.raises(ArchiveSelfDestructError):
            decide_partition_action(0, True)


# ---------------------------------------------------------------------------
# 얇은 페이크 — 실제 PG/S3 없이 run_offload의 가드 경로를 끝까지 태운다.
# ---------------------------------------------------------------------------

class FakeCursor:
    """_list_instance_ids(fetchall)와 _fetch_partition(iteration) 둘 다 흉내낸다."""

    def __init__(self, instance_ids, rows_by_instance):
        self._instance_ids = instance_ids
        self._rows_by_instance = rows_by_instance
        self._rows = []
        self.itersize = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        if "database_instance" in sql:
            self._rows = [(iid,) for iid in self._instance_ids]
        else:  # _fetch_partition — params[0]이 instance_id.
            self._rows = self._rows_by_instance.get(params[0], [])

    def fetchall(self):
        return self._rows

    def __iter__(self):
        return iter(self._rows)


class FakeConn:
    def __init__(self, instance_ids, rows_by_instance):
        self._instance_ids = instance_ids
        self._rows_by_instance = rows_by_instance

    def cursor(self, name=None):
        return FakeCursor(self._instance_ids, self._rows_by_instance)

    def set_session(self, **kw):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


class FakeS3:
    """오브젝트 dict 하나로 list/delete/put을 흉내내는 최소 S3."""

    def __init__(self, objects=None):
        self.objects = dict(objects or {})  # key -> bytes

    def head_bucket(self, Bucket):
        pass

    def list_objects_v2(self, Bucket, Prefix, MaxKeys=1000):
        keys = [k for k in self.objects if k.startswith(Prefix)]
        return {"KeyCount": len(keys[:MaxKeys]),
                "Contents": [{"Key": k} for k in keys[:MaxKeys]]}

    def get_paginator(self, name):
        s3 = self

        class Paginator:
            def paginate(self, Bucket, Prefix):
                keys = [k for k in s3.objects if k.startswith(Prefix)]
                yield {"Contents": [{"Key": k} for k in keys]}

        return Paginator()

    def delete_objects(self, Bucket, Delete):
        for o in Delete["Objects"]:
            self.objects.pop(o["Key"], None)

    def put_object(self, Bucket, Key, Body):
        self.objects[Key] = Body


def _row(instance_id, i=1):
    from datetime import datetime
    return (i, instance_id, datetime(2026, 6, 1, 12, 0, 0),
            f"q{i}", "SELECT 1", 10, 1.5, 100)


PARTITION_KEY = "raw/query_snapshot/dt=2026-06-01/instance_id=1/part-000.parquet"


@pytest.fixture
def wire(monkeypatch):
    """run_offload의 PG/S3 배선을 페이크로 갈아끼우는 조립기."""

    def _wire(rows_by_instance, s3_objects):
        s3 = FakeS3(s3_objects)
        monkeypatch.setattr(offload, "_s3_client", lambda sink: s3)
        monkeypatch.setattr(
            offload.psycopg2, "connect",
            lambda dsn: FakeConn([1], rows_by_instance),
        )
        return s3

    return _wire


class TestRunOffloadGuard:
    def test_empty_source_existing_partition_raises_and_preserves(self, wire):
        s3 = wire(rows_by_instance={}, s3_objects={PARTITION_KEY: b"precious"})
        with pytest.raises(ArchiveSelfDestructError):
            offload.run_offload("2026-06-01")
        # 유일본이 그대로 남아 있어야 한다 — 수정 전에는 여기서 사라졌다.
        assert s3.objects == {PARTITION_KEY: b"precious"}

    def test_empty_source_no_partition_skips_quietly(self, wire):
        wire(rows_by_instance={}, s3_objects={})
        result = offload.run_offload("2026-06-01")
        assert result["total_rows"] == 0
        assert result["instances"] == {}

    def test_normal_flow_still_overwrites(self, wire):
        s3 = wire(
            rows_by_instance={1: [_row(1, 1), _row(1, 2)]},
            s3_objects={PARTITION_KEY: b"stale"},
        )
        result = offload.run_offload("2026-06-01")
        assert result["instances"][1] == 2
        # delete→write 멱등 경로: 새 parquet가 옛 오브젝트를 대체.
        assert list(s3.objects) == [PARTITION_KEY]
        assert s3.objects[PARTITION_KEY] != b"stale"
