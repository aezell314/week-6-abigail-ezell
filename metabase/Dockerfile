# Metabase + the DuckDB driver, on a Debian/glibc base. **Provided — don't change.**
#
# Why not the official metabase/metabase image? It's built on Alpine (musl libc),
# and DuckDB's driver ships a native library compiled against glibc — on Alpine it
# fails to load at connection time. The driver maintainers' own fix is to run
# Metabase on Debian instead. That's all this image is: the same Metabase, on a
# glibc base where the native library loads. docker compose BUILDS it locally, so
# it builds for whatever machine you're on — arm64 (Apple Silicon) or amd64.
#
# The DuckDB "driver" embeds a whole DuckDB engine inside the Metabase container:
# Metabase isn't talking to a database server, it's opening your serving copy of
# warehouse.duckdb directly, as one more DuckDB client.
FROM eclipse-temurin:21-jre-jammy

# glibc is already on this base. libstdc++6 is the piece DuckDB's native lib needs.
RUN apt-get update \
 && apt-get install -y --no-install-recommends libstdc++6 curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ARG METABASE_VERSION=0.59.12
# The DuckDB community driver. Pinned to 1.5.4.0, which bundles DuckDB 1.5.4 —
# the exact version dbt used to build your warehouse. That match matters: DuckDB's
# storage format is version-locked, so a mismatched driver can't open your file.
ARG DUCKDB_DRIVER_VERSION=1.5.4.0

RUN curl -fSL -o metabase.jar https://downloads.metabase.com/v${METABASE_VERSION}/metabase.jar

RUN mkdir -p /plugins \
 && curl -fSL -o /plugins/duckdb.metabase-driver.jar \
      https://github.com/motherduckdb/metabase_duckdb_driver/releases/download/${DUCKDB_DRIVER_VERSION}/duckdb.metabase-driver.jar

ENV MB_PLUGINS_DIR=/plugins

EXPOSE 3000
CMD ["java", "-jar", "metabase.jar"]
