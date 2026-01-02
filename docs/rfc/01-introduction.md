# Section 1: Introduction

## 1.1 Background

DuckDB is an embedded analytical database with exceptional performance for OLAP workloads, Parquet/CSV ingestion, and complex analytical queries. However, its native API requires direct library integration, limiting its accessibility from standard database tooling.

PostgreSQL's wire protocol (pgwire) is the de facto standard for relational database communication. Virtually every programming language, ORM, and database tool supports PostgreSQL connections. By implementing pgwire over DuckDB, mallardb bridges this gap—enabling DuckDB's analytical capabilities through familiar PostgreSQL interfaces.

## 1.2 Scope

This specification defines:

- The architecture for multiplexing PostgreSQL connections over shared DuckDB instances
- Role-based connection routing (read-write vs read-only)
- PostgreSQL wire protocol implementation requirements
- Authentication using PostgreSQL-compatible methods
- Catalog system emulation (pg_catalog, information_schema)
- Type OID mapping between DuckDB and PostgreSQL
- Transaction handling semantics
- Configuration and deployment model

This specification does NOT define:

- PostgreSQL replication protocol support
- Logical decoding or CDC
- LISTEN/NOTIFY channels
- Foreign data wrappers
- PostgreSQL procedural languages (PL/pgSQL, etc.)
- Multi-database support (single database per instance)
- Advisory locks
- Two-phase commit (prepared transactions)

## 1.3 Goals and Requirements

mallardb MUST:

1. Accept connections from standard PostgreSQL clients without modification
2. Authenticate users via PostgreSQL-compatible password authentication
3. Execute SQL queries against DuckDB and return PostgreSQL-formatted results
4. Provide catalog views that satisfy ORM introspection queries (SQLAlchemy, Prisma, TypeORM)
5. Support prepared statements with parameter binding (`$1`, `$2`, etc.)
6. Map DuckDB types to PostgreSQL type OIDs in wire protocol responses
7. Return PostgreSQL SQLSTATE error codes
8. Support concurrent read connections with dedicated DuckDB readers
9. Serialize write operations through a shared writer

mallardb SHOULD:

1. Support TLS connections
2. Expose DuckDB-specific features (parquet, httpfs, etc.) transparently
3. Provide PostgreSQL-compatible health check endpoints
4. Support the COPY protocol for bulk data transfer

mallardb MAY:

1. Implement additional PostgreSQL authentication methods (SCRAM-SHA-256, certificate)
2. Support multiple databases in future versions
3. Implement role-based access control beyond read/write separation

## 1.4 Document Organization

- [Section 2](./02-terminology.md) defines terminology and conventions
- [Section 3](./03-architecture.md) describes the system architecture
- [Section 4](./04-wire-protocol.md) specifies wire protocol implementation
- [Section 5](./05-authentication.md) covers authentication methods
- [Section 6](./06-catalog.md) defines catalog system emulation
- [Section 7](./07-query-execution.md) details query routing and execution
- [Section 8](./08-type-system.md) specifies type mapping
- [Section 9](./09-transactions.md) covers transaction semantics
- [Section 10](./10-configuration.md) defines configuration options
- [Section 11](./11-security.md) addresses security considerations
- [Section 12](./12-references.md) lists normative and informative references
