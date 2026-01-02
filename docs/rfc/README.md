# mallardb Specification

**Version:** 0.1.0-draft  
**Status:** Draft  
**Last Updated:** 2026-01-01

## Abstract

mallardb is a PostgreSQL-compatible database server that uses DuckDB as its storage and query engine. It implements the PostgreSQL wire protocol (pgwire), authentication, and catalog system, allowing standard PostgreSQL tools and ORMs to connect seamlessly while benefiting from DuckDB's analytical performance and features.

## Table of Contents

1. [Abstract](./00-abstract.md)
2. [Introduction](./01-introduction.md)
3. [Terminology](./02-terminology.md)
4. [Architecture](./03-architecture.md)
5. [Wire Protocol](./04-wire-protocol.md)
6. [Authentication](./05-authentication.md)
7. [Catalog System](./06-catalog.md)
8. [Query Execution](./07-query-execution.md)
9. [Type System](./08-type-system.md)
10. [Transactions](./09-transactions.md)
11. [Configuration](./10-configuration.md)
12. [Security Considerations](./11-security.md)
13. [References](./12-references.md)

## Document Index

| Section | File | Description | Lines |
|---------|------|-------------|-------|
| Abstract | [00-abstract.md](./00-abstract.md) | Status and summary | ~40 |
| Introduction | [01-introduction.md](./01-introduction.md) | Background, scope, goals | ~120 |
| Terminology | [02-terminology.md](./02-terminology.md) | Definitions and conventions | ~80 |
| Architecture | [03-architecture.md](./03-architecture.md) | System design, connection model | ~200 |
| Wire Protocol | [04-wire-protocol.md](./04-wire-protocol.md) | pgwire implementation | ~150 |
| Authentication | [05-authentication.md](./05-authentication.md) | Auth methods, roles | ~100 |
| Catalog System | [06-catalog.md](./06-catalog.md) | pg_catalog, information_schema | ~300 |
| Query Execution | [07-query-execution.md](./07-query-execution.md) | Routing, serialization | ~150 |
| Type System | [08-type-system.md](./08-type-system.md) | OID mapping, type conversion | ~120 |
| Transactions | [09-transactions.md](./09-transactions.md) | Transaction handling | ~100 |
| Configuration | [10-configuration.md](./10-configuration.md) | Env vars, Docker | ~100 |
| Security | [11-security.md](./11-security.md) | Threat model, mitigations | ~100 |
| References | [12-references.md](./12-references.md) | Normative and informative refs | ~50 |

## Quick Navigation

- **Implementers**: Start with [Architecture](./03-architecture.md)
- **ORM developers**: See [Catalog System](./06-catalog.md) and [Type System](./08-type-system.md)
- **DevOps**: See [Configuration](./10-configuration.md)
- **Security review**: See [Security Considerations](./11-security.md)
