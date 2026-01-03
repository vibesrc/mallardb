# Abstract

mallardb is a PostgreSQL wire protocol-compatible database server powered by DuckDB. It presents a fully PostgreSQL-compatible interface to clients—including authentication, catalog queries, type OIDs, and SQLSTATE error codes—while internally executing all queries against DuckDB.

This specification defines the architecture, protocol implementation, catalog emulation, and operational characteristics required for mallardb to function as a drop-in PostgreSQL replacement for applications using standard PostgreSQL client libraries, ORMs (SQLAlchemy, Prisma, etc.), and tools (psql, pgAdmin, DBeaver).

## Status of This Document

**Status:** Draft
**Version:** 0.1.0-draft
**Last Updated:** 2026-01-03
**Obsoletes:** None
**Updates:** None

This document specifies the mallardb database server for implementers and integrators. Distribution of this document is unlimited.

## Design Philosophy

mallardb follows these core principles:

1. **PostgreSQL on the outside**: Any PostgreSQL client SHOULD connect without modification
2. **DuckDB on the inside**: Full access to DuckDB features, extensions, and performance
3. **Role-based access**: Connection role determines read/write access
4. **Minimal interception**: Leverage DuckDB's native pg_catalog; only intercept what's necessary
5. **Per-client connections**: Each client maintains a persistent DuckDB connection
6. **Operational simplicity**: Docker-native with PostgreSQL-compatible environment variables
