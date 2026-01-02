# Development build
FROM rust:1-alpine AS builder

RUN apk add --no-cache musl-dev g++

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

# Runtime image
FROM alpine:3.23

RUN apk add --no-cache postgresql-client

COPY --from=builder /app/target/release/mallardb /usr/local/bin/mallardb

# Default data directory
ENV MALLARDB_DATA_DIR=/var/lib/mallardb/data
VOLUME /var/lib/mallardb/data

# Init script directories
VOLUME /docker-entrypoint-initdb.d
VOLUME /docker-entrypoint-startdb.d

EXPOSE 5432

HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 \
    CMD pg_isready -h localhost -p 5432 -U ${POSTGRES_USER:-postgres} || exit 1

ENTRYPOINT ["mallardb"]
