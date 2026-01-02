# Section 8: Type System

## 8.1 Overview

PostgreSQL clients expect specific Object Identifiers (OIDs) for data types in wire protocol responses. mallardb maps DuckDB types to PostgreSQL OIDs to ensure compatibility with client libraries and ORMs.

## 8.2 Type Mapping Table

### Table 8-1: DuckDB to PostgreSQL Type Mapping

| DuckDB Type | PostgreSQL Type | OID | Type Length | Description |
|-------------|-----------------|-----|-------------|-------------|
| BOOLEAN | bool | 16 | 1 | Boolean |
| TINYINT | int2 | 21 | 2 | 8-bit integer (widened) |
| SMALLINT | int2 | 21 | 2 | 16-bit integer |
| INTEGER | int4 | 23 | 4 | 32-bit integer |
| BIGINT | int8 | 20 | 8 | 64-bit integer |
| HUGEINT | numeric | 1700 | -1 | 128-bit integer |
| UTINYINT | int2 | 21 | 2 | Unsigned (widened) |
| USMALLINT | int4 | 23 | 4 | Unsigned (widened) |
| UINTEGER | int8 | 20 | 8 | Unsigned (widened) |
| UBIGINT | numeric | 1700 | -1 | Unsigned (widened) |
| FLOAT | float4 | 700 | 4 | 32-bit float |
| DOUBLE | float8 | 701 | 8 | 64-bit float |
| DECIMAL(p,s) | numeric | 1700 | -1 | Arbitrary precision |
| VARCHAR | varchar | 1043 | -1 | Variable character |
| CHAR(n) | bpchar | 1042 | -1 | Fixed character |
| TEXT | text | 25 | -1 | Unlimited text |
| BLOB | bytea | 17 | -1 | Binary data |
| DATE | date | 1082 | 4 | Calendar date |
| TIME | time | 1083 | 8 | Time of day |
| TIMESTAMP | timestamp | 1114 | 8 | Date and time |
| TIMESTAMP WITH TIME ZONE | timestamptz | 1184 | 8 | Date/time with TZ |
| INTERVAL | interval | 1186 | 16 | Time interval |
| UUID | uuid | 2950 | 16 | UUID |
| JSON | jsonb | 3802 | -1 | JSON binary |
| LIST | array | varies | -1 | Array type |
| STRUCT | record | 2249 | -1 | Composite type |
| MAP | jsonb | 3802 | -1 | Key-value (as JSON) |
| ENUM | enum | varies | 4 | Enumeration |
| BIT | bit | 1560 | -1 | Bit string |
| NULL | unknown | 705 | -1 | Unknown type |

## 8.3 Array Types

PostgreSQL array types have their own OIDs. mallardb maps DuckDB LIST types to PostgreSQL arrays:

### Table 8-2: Array Type OIDs

| Element Type | Array OID | Element OID |
|--------------|-----------|-------------|
| bool | 1000 | 16 |
| int2 | 1005 | 21 |
| int4 | 1007 | 23 |
| int8 | 1016 | 20 |
| float4 | 1021 | 700 |
| float8 | 1022 | 701 |
| text | 1009 | 25 |
| varchar | 1015 | 1043 |
| date | 1182 | 1082 |
| timestamp | 1115 | 1114 |
| timestamptz | 1185 | 1184 |
| uuid | 2951 | 2950 |
| jsonb | 3807 | 3802 |

## 8.4 Type Modifiers

Type modifiers (typmods) encode precision, scale, and length information:

### 8.4.1 VARCHAR/CHAR

For `VARCHAR(n)` and `CHAR(n)`:
```
typmod = n + 4  (PostgreSQL adds 4 for header)
```

Example: `VARCHAR(255)` → typmod = 259

### 8.4.2 NUMERIC

For `NUMERIC(precision, scale)`:
```
typmod = ((precision << 16) | scale) + 4
```

Example: `NUMERIC(10,2)` → typmod = ((10 << 16) | 2) + 4 = 655366

### 8.4.3 TIME/TIMESTAMP

For `TIME(p)` and `TIMESTAMP(p)`:
```
typmod = p  (fractional seconds precision 0-6)
```

## 8.5 Wire Format Encoding

### 8.5.1 Text Format

All types MUST support text format encoding. Values are transmitted as UTF-8 strings:

| Type | Text Format Example |
|------|---------------------|
| bool | `t` / `f` |
| int4 | `12345` |
| float8 | `3.14159` |
| text | `hello world` |
| date | `2024-01-15` |
| timestamp | `2024-01-15 10:30:00` |
| timestamptz | `2024-01-15 10:30:00+00` |
| uuid | `550e8400-e29b-41d4-a716-446655440000` |
| bytea | `\x48656c6c6f` (hex format) |
| array | `{1,2,3}` |
| json | `{"key": "value"}` |
| interval | `1 year 2 mons 3 days 04:05:06` |

### 8.5.2 Binary Format

Binary format support is OPTIONAL. If implemented:

| Type | Binary Format |
|------|---------------|
| bool | 1 byte: 0x00 or 0x01 |
| int2 | 2 bytes, big-endian |
| int4 | 4 bytes, big-endian |
| int8 | 8 bytes, big-endian |
| float4 | 4 bytes, IEEE 754 |
| float8 | 8 bytes, IEEE 754 |
| uuid | 16 bytes, raw |

## 8.6 RowDescription Encoding

When sending `RowDescription` messages, mallardb MUST provide accurate type information:

```
For each column:
  - Field name (string)
  - Table OID (int32): 0 if not from a table
  - Column number (int16): 0 if not from a table
  - Type OID (int32): from mapping table
  - Type size (int16): from mapping table (-1 for variable)
  - Type modifier (int32): -1 if not applicable
  - Format code (int16): 0=text, 1=binary
```

### 8.6.1 Example

For query `SELECT id, name, created_at FROM users`:

```
RowDescription:
  Field count: 3
  
  Field 1:
    Name: "id"
    Table OID: 16384 (users table OID)
    Column: 1
    Type OID: 23 (int4)
    Type size: 4
    Type modifier: -1
    Format: 0 (text)
    
  Field 2:
    Name: "name"
    Table OID: 16384
    Column: 2
    Type OID: 1043 (varchar)
    Type size: -1
    Type modifier: 259 (varchar(255))
    Format: 0 (text)
    
  Field 3:
    Name: "created_at"
    Table OID: 16384
    Column: 3
    Type OID: 1184 (timestamptz)
    Type size: 8
    Type modifier: -1
    Format: 0 (text)
```

## 8.7 Type Coercion

### 8.7.1 Parameter Binding

When clients bind parameters, mallardb handles type coercion:

1. Client specifies parameter type OIDs in `Bind` message
2. mallardb maps OIDs to DuckDB types
3. Values are converted before passing to DuckDB

If type OID is 0 (unknown), mallardb infers type from:
- The parameter value's text representation
- The expected type from the prepared statement

### 8.7.2 Implicit Casts

mallardb relies on DuckDB's implicit casting. PostgreSQL-specific cast behaviors are NOT emulated.

## 8.8 Special Types

### 8.8.1 ENUM Types

DuckDB enums are mapped to PostgreSQL enums:
- Each enum gets a unique OID (generated at runtime)
- Enum values returned as text
- Enum metadata available in `pg_enum` catalog

### 8.8.2 Composite Types (STRUCT)

DuckDB STRUCTs are mapped to PostgreSQL composite types:
- OID 2249 (record) for anonymous structs
- Named structs get unique OIDs
- Wire format uses record text encoding: `(field1,field2,...)`

### 8.8.3 JSON

Both DuckDB JSON and MAP types map to PostgreSQL jsonb (OID 3802):
- JSON is passed through unchanged
- MAP is serialized to JSON object format

## 8.9 pg_type Catalog

The `pg_catalog.pg_type` view MUST include entries for all mapped types. See [Section 6.3.4](./06-catalog.md#634-pg_type-required) for the full catalog specification.

Clients query `pg_type` to:
- Resolve type names to OIDs
- Discover array types
- Determine type properties

## 8.10 Unsupported Types

When DuckDB returns a type with no PostgreSQL mapping:

1. Use OID 25 (text) as fallback
2. Convert value to string representation
3. Log warning for debugging

This ensures queries never fail due to unmapped types, though precision may be lost.
