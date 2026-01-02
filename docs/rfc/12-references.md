# Section 12: References

## 12.1 Normative References

References that implementations MUST follow.

- **[RFC2119]** Bradner, S., "Key words for use in RFCs to Indicate Requirement Levels", BCP 14, RFC 2119, March 1997.
  https://www.rfc-editor.org/rfc/rfc2119

- **[PGWIRE]** PostgreSQL Global Development Group, "Frontend/Backend Protocol", PostgreSQL Documentation.
  https://www.postgresql.org/docs/current/protocol.html

- **[PGWIRE-MSG]** PostgreSQL Global Development Group, "Message Formats", PostgreSQL Documentation.
  https://www.postgresql.org/docs/current/protocol-message-formats.html

- **[PG-CATALOG]** PostgreSQL Global Development Group, "System Catalogs", PostgreSQL Documentation.
  https://www.postgresql.org/docs/current/catalogs.html

- **[PG-TYPES]** PostgreSQL Global Development Group, "Data Types", PostgreSQL Documentation.
  https://www.postgresql.org/docs/current/datatype.html

- **[PG-ERRCODES]** PostgreSQL Global Development Group, "PostgreSQL Error Codes", PostgreSQL Documentation.
  https://www.postgresql.org/docs/current/errcodes-appendix.html

- **[DUCKDB]** DuckDB Foundation, "DuckDB Documentation".
  https://duckdb.org/docs/

## 12.2 Informative References

References for additional context (not required for conformance).

- **[DUCKDB-API]** DuckDB Foundation, "DuckDB Rust API".
  https://docs.rs/duckdb/latest/duckdb/

- **[PGWIRE-RS]** pgwire-rs Contributors, "pgwire - PostgreSQL wire protocol implementation in Rust".
  https://github.com/sunng87/pgwire
  https://docs.rs/pgwire/latest/pgwire/

- **[TOKIO]** Tokio Contributors, "Tokio - An asynchronous Rust runtime".
  https://tokio.rs/

- **[PG-DOCKER]** PostgreSQL Docker Community, "Official PostgreSQL Docker Image".
  https://hub.docker.com/_/postgres

- **[SQLALCHEMY]** SQLAlchemy Authors, "SQLAlchemy Documentation".
  https://docs.sqlalchemy.org/

- **[PRISMA]** Prisma Data, Inc., "Prisma Documentation".
  https://www.prisma.io/docs/

- **[SCRAM]** Newman, C., et al., "Salted Challenge Response Authentication Mechanism (SCRAM)", RFC 5802, July 2010.
  https://www.rfc-editor.org/rfc/rfc5802

## 12.3 Rust Crate Dependencies

Primary crates used in implementation:

| Crate | Purpose | Version |
|-------|---------|---------|
| `pgwire` | PostgreSQL wire protocol | 0.x |
| `duckdb` | DuckDB Rust bindings | 0.x |
| `tokio` | Async runtime | 1.x |
| `tracing` | Logging/instrumentation | 0.1.x |
| `rustls` | TLS implementation | 0.21.x |

## 12.4 Related Projects

- **pg_duckdb** - PostgreSQL extension that embeds DuckDB
  https://github.com/duckdb/pg_duckdb

- **duckdb-wasm** - DuckDB compiled to WebAssembly
  https://github.com/duckdb/duckdb-wasm

- **CockroachDB** - Distributed SQL database with PostgreSQL compatibility
  https://www.cockroachlabs.com/

- **Materialize** - Streaming database with PostgreSQL compatibility
  https://materialize.com/

- **QuestDB** - Time-series database with PostgreSQL wire protocol
  https://questdb.io/
