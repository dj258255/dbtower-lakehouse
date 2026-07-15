-- 어플라이언스 카탈로그 PG 초기화 (첫 부팅 1회, docker-entrypoint-initdb.d).
--
-- ducklake_catalog : DuckLake 카탈로그. 앱(ensure_catalog_db)도 멱등 생성하지만
--                    어플라이언스의 DB 목록을 명시적으로 남긴다.
-- metabase_app     : Metabase 앱 DB(H2 대신). Metabase는 테이블은 만들지만
--                    데이터베이스 자체는 안 만들므로 여기서 미리 만든다.
CREATE DATABASE ducklake_catalog OWNER catalog;
CREATE DATABASE metabase_app OWNER catalog;
