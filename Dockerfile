# Development build
FROM rust:1-slim-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

# Runtime image
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/mallardb /usr/local/bin/mallardb

# Default database path (follows PostgreSQL PGDATA convention)
ENV MALLARDB_DATABASE=/var/lib/mallardb/data/mallard.db
# Store DuckDB extensions alongside data for persistence
ENV MALLARDB_EXTENSION_DIRECTORY=/var/lib/mallardb/extensions
VOLUME /var/lib/mallardb

# Init script directories
VOLUME /docker-entrypoint-initdb.d
VOLUME /docker-entrypoint-startdb.d

EXPOSE 5432

HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 \
    CMD pg_isready -h localhost -p 5432 -U ${POSTGRES_USER:-postgres} || exit 1

ENTRYPOINT ["mallardb"]
