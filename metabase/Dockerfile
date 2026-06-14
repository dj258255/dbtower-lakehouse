# Metabase + DuckDB 드라이버 (Phase 7 대시보드).
#
# 왜 공식 이미지가 아닌가: metabase/metabase 공식 이미지는 Alpine(musl) 기반이라
# DuckDB JDBC의 네이티브 라이브러리(glibc 링크)가 로드되지 않는다. 실측 에러:
#   UnsatisfiedLinkError: libduckdb_java....so: Error loading shared library
#   libstdc++.so.6: No such file or directory
# 드라이버 저장소(motherduckdb/metabase_duckdb_driver)가 권장하는 Debian 계열
# (eclipse-temurin jammy) 위에 metabase.jar + 드라이버 jar를 직접 얹는다.
#
# 버전은 짝으로 고정한다 — 드라이버는 Metabase 메이저·DuckDB 버전에 맞춰 릴리스된다:
#   드라이버 1.5.3.0 = Metabase 59 + DuckDB 1.5.3 (dbt의 duckdb 1.5.4와 같은 1.5 계열,
#   파일·DuckLake 포맷 호환 실측 — docs/VERIFICATION.md 8절)
FROM eclipse-temurin:21-jre-jammy

ARG METABASE_VERSION=0.59.16
ARG METABASE_DUCKDB_DRIVER_VERSION=1.5.3.0

ENV MB_PLUGINS_DIR=/home/metabase/plugins/

RUN groupadd -r metabase && useradd -r -g metabase metabase

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# /metabase-data: 앱 DB(H2) 영속용 — compose가 named volume을 여기에 건다.
RUN mkdir -p /home/metabase/plugins /metabase-data && \
    chown -R metabase:metabase /home/metabase /metabase-data

WORKDIR /home/metabase
ADD --chown=metabase:metabase https://downloads.metabase.com/v${METABASE_VERSION}/metabase.jar /home/metabase/
ADD --chown=metabase:metabase https://github.com/motherduckdb/metabase_duckdb_driver/releases/download/${METABASE_DUCKDB_DRIVER_VERSION}/duckdb.metabase-driver.jar /home/metabase/plugins/

RUN chmod 755 /home/metabase/metabase.jar && \
    chmod 755 /home/metabase/plugins/duckdb.metabase-driver.jar

EXPOSE 3000

USER metabase

CMD ["java", "-jar", "/home/metabase/metabase.jar"]
