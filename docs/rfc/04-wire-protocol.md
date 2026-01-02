# Section 4: Wire Protocol

## 4.1 Overview

mallardb implements PostgreSQL wire protocol version 3.0 as defined in the PostgreSQL documentation. This section specifies the required protocol features and any deviations from standard PostgreSQL behavior.

## 4.2 Protocol Implementation

### 4.2.1 Startup Phase

The startup phase MUST follow this sequence:

1. Client connects via TCP
2. Client MAY send `SSLRequest` to negotiate TLS
3. Client sends `StartupMessage` with protocol version and parameters
4. Server responds with authentication request
5. Client provides credentials
6. Server responds with `AuthenticationOk` or error
7. Server sends `ParameterStatus` messages for session parameters
8. Server sends `BackendKeyData` with process ID and secret key
9. Server sends `ReadyForQuery` indicating idle state

### 4.2.2 Required Startup Parameters

mallardb MUST accept and handle these startup parameters:

| Parameter | Handling |
|-----------|----------|
| user | Used for authentication and role routing |
| database | Accepted but ignored (single database) |
| application_name | Stored in session, returned in `ParameterStatus` |
| client_encoding | MUST accept "UTF8", reject others |
| options | Parsed for runtime parameters |

mallardb MUST send these `ParameterStatus` messages after authentication:

| Parameter | Value |
|-----------|-------|
| server_version | "15.0" (or configured version string) |
| server_encoding | "UTF8" |
| client_encoding | "UTF8" |
| DateStyle | "ISO, MDY" |
| TimeZone | System timezone or configured value |
| integer_datetimes | "on" |
| standard_conforming_strings | "on" |

## 4.3 Simple Query Protocol

The simple query protocol handles unparameterized queries sent via the `Query` message.

### 4.3.1 Query Message Processing

Upon receiving a `Query` message, mallardb MUST:

1. Check if the query targets system catalogs (route to catalog emulator if so)
2. Route the query to the appropriate DuckDB connection (writer or reader)
3. Execute the query against DuckDB
4. Translate results to PostgreSQL wire format
5. Send response messages

### 4.3.2 Response Sequence

For a successful SELECT query:

```
Server → RowDescription (column metadata with OIDs)
Server → DataRow (one per result row)
Server → CommandComplete ("SELECT n" where n is row count)
Server → ReadyForQuery
```

For a successful INSERT/UPDATE/DELETE:

```
Server → CommandComplete ("INSERT 0 n" / "UPDATE n" / "DELETE n")
Server → ReadyForQuery
```

For DDL (CREATE, ALTER, DROP):

```
Server → CommandComplete ("CREATE TABLE" / etc.)
Server → ReadyForQuery
```

## 4.4 Extended Query Protocol

The extended query protocol supports prepared statements and parameter binding.

### 4.4.1 Parse

The `Parse` message creates a prepared statement. mallardb MUST:

1. Parse the SQL and extract parameter placeholders (`$1`, `$2`, etc.)
2. Store the prepared statement with the given name (empty string = unnamed)
3. Send `ParseComplete` on success

### 4.4.2 Bind

The `Bind` message binds parameters to a prepared statement. mallardb MUST:

1. Locate the prepared statement by name
2. Bind provided parameter values
3. Create a portal with the given name
4. Send `BindComplete` on success

Parameter format codes:
- 0 = text format (default)
- 1 = binary format

mallardb MUST support text format for all types. Binary format support is OPTIONAL.

### 4.4.3 Execute

The `Execute` message executes a portal. mallardb MUST:

1. Locate the portal by name
2. Execute the bound query against DuckDB
3. Send result rows (respecting row limit if specified)
4. Send `CommandComplete` when done

### 4.4.4 Describe

The `Describe` message requests metadata. mallardb MUST:

1. For statements ('S'): Send `ParameterDescription` and `RowDescription`
2. For portals ('P'): Send `RowDescription`

### 4.4.5 Sync

The `Sync` message marks an extended query sequence boundary. mallardb MUST:

1. Complete any pending operations
2. Send `ReadyForQuery`

## 4.5 COPY Protocol

mallardb SHOULD support the COPY protocol for bulk data transfer.

### 4.5.1 COPY TO STDOUT

For `COPY ... TO STDOUT`:

```
Server → CopyOutResponse (format, column count, column formats)
Server → CopyData (one per chunk of data)
Server → CopyDone
Server → CommandComplete
Server → ReadyForQuery
```

### 4.5.2 COPY FROM STDIN

For `COPY ... FROM STDIN`:

```
Server → CopyInResponse (format, column count, column formats)
Client → CopyData (one per chunk of data)
Client → CopyDone
Server → CommandComplete
Server → ReadyForQuery
```

## 4.6 Error Handling

### 4.6.1 ErrorResponse Format

All errors MUST be returned as `ErrorResponse` messages with these fields:

| Field | Code | Required | Description |
|-------|------|----------|-------------|
| Severity | 'S' | Yes | ERROR, FATAL, PANIC |
| SQLSTATE | 'C' | Yes | 5-character error code |
| Message | 'M' | Yes | Human-readable message |
| Detail | 'D' | No | Additional detail |
| Hint | 'H' | No | Suggestion for resolution |
| Position | 'P' | No | Cursor position in query |

### 4.6.2 SQLSTATE Mapping

mallardb MUST map DuckDB errors to appropriate PostgreSQL SQLSTATE codes:

| DuckDB Error Category | SQLSTATE | Class |
|-----------------------|----------|-------|
| Syntax error | 42601 | Syntax Error |
| Undefined table | 42P01 | Undefined Table |
| Undefined column | 42703 | Undefined Column |
| Duplicate key | 23505 | Unique Violation |
| Foreign key violation | 23503 | Foreign Key Violation |
| Not null violation | 23502 | Not Null Violation |
| Division by zero | 22012 | Division By Zero |
| Invalid input | 22P02 | Invalid Text Representation |
| Out of memory | 53200 | Out Of Memory |
| Connection failure | 08006 | Connection Failure |
| Read-only violation | 25006 | Read Only SQL Transaction |
| Unknown/Other | 42000 | Syntax Error or Access Rule Violation |

### 4.6.3 Read-Only Violation

When a read role attempts a write operation, mallardb MUST:

1. Return ErrorResponse with SQLSTATE `25006`
2. Message: "cannot execute {statement} in a read-only transaction"
3. NOT execute the statement against DuckDB

## 4.7 Cancellation

mallardb SHOULD support query cancellation via the cancel request protocol:

1. Client opens new connection
2. Client sends `CancelRequest` with process ID and secret key from `BackendKeyData`
3. Server cancels in-flight query for matching session
4. Server closes cancel connection immediately (no response)

Implementation is OPTIONAL for initial release.
