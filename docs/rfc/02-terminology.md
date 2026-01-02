# Section 2: Terminology and Conventions

## 2.1 Requirements Language

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be interpreted as described in RFC 2119.

## 2.2 Definitions

**Client**
: A PostgreSQL-compatible application, tool, or library that connects to mallardb using the PostgreSQL wire protocol.

**Connection**
: A TCP connection from a client that has completed the startup handshake and authentication.

**Session**
: The stateful context associated with a connection, including transaction state, prepared statements, and session variables.

**Writer**
: The shared DuckDB connection used for all write operations (INSERT, UPDATE, DELETE, DDL). Only one writer exists per mallardb instance.

**Reader**
: A dedicated DuckDB connection used for read-only operations. Each read-only client connection gets its own reader.

**Write Role**
: The PostgreSQL role (configured via `POSTGRES_USER`) that has full read-write access, routed through the shared writer.

**Read Role**
: The PostgreSQL role (configured via `POSTGRES_READONLY_USER`) restricted to read-only operations, allocated a dedicated reader.

**Catalog**
: The system tables and views (`pg_catalog`, `information_schema`) that describe database objects.

**OID**
: Object Identifier. A 32-bit integer used by PostgreSQL to identify types, functions, and other database objects.

**pgwire**
: The PostgreSQL wire protocol, a message-based protocol for client-server communication.

## 2.3 Abbreviations

| Abbreviation | Expansion |
|--------------|-----------|
| API | Application Programming Interface |
| DDL | Data Definition Language (CREATE, ALTER, DROP) |
| DML | Data Manipulation Language (INSERT, UPDATE, DELETE) |
| OID | Object Identifier |
| ORM | Object-Relational Mapping |
| SQL | Structured Query Language |
| TLS | Transport Layer Security |
| WAL | Write-Ahead Log |

## 2.4 Notation Conventions

SQL keywords are written in UPPERCASE: `SELECT`, `CREATE TABLE`.

Configuration variables are written in monospace with underscores: `POSTGRES_USER`.

Wire protocol message types are written in PascalCase: `StartupMessage`, `Query`, `RowDescription`.

DuckDB and PostgreSQL type names are written in lowercase: `integer`, `varchar`, `timestamp`.
