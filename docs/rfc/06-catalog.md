# Section 6: Catalog System

## 6.1 Overview

mallardb emulates PostgreSQL's catalog system to support standard tools and ORMs. This includes both the `pg_catalog` schema (PostgreSQL system catalogs) and the `information_schema` (SQL standard).

The catalog emulator operates by:
1. Intercepting queries to catalog schemas
2. Translating to equivalent DuckDB queries where possible
3. Synthesizing responses for PostgreSQL-specific catalogs
4. Caching static catalog data for performance

## 6.2 Implementation Strategy

### 6.2.1 Query Interception

mallardb MUST detect catalog queries by:
1. Parsing the query to identify referenced tables/views
2. Checking if any reference matches `pg_catalog.*` or `information_schema.*`
3. Routing matched queries to the catalog emulator

### 6.2.2 Translation vs Synthesis

| Catalog | Strategy |
|---------|----------|
| `information_schema.tables` | Translate to DuckDB `information_schema` |
| `information_schema.columns` | Translate to DuckDB `information_schema` |
| `pg_catalog.pg_type` | Synthesize from known type mappings |
| `pg_catalog.pg_class` | Hybrid (DuckDB tables + synthetic entries) |
| `pg_catalog.pg_namespace` | Hybrid (DuckDB schemas + `pg_catalog`) |
| `pg_catalog.pg_attribute` | Translate + augment with OID info |

## 6.3 pg_catalog Tables

### 6.3.1 pg_namespace (Required)

Emulates PostgreSQL schemas.

```sql
CREATE VIEW pg_catalog.pg_namespace AS
SELECT 
    oid,
    nspname,
    nspowner,
    nspacl
FROM (
    -- DuckDB schemas
    SELECT 
        hash(schema_name)::int AS oid,
        schema_name AS nspname,
        10 AS nspowner,  -- postgres superuser
        NULL AS nspacl
    FROM information_schema.schemata
    
    UNION ALL
    
    -- Synthetic pg_catalog
    SELECT 11 AS oid, 'pg_catalog' AS nspname, 10 AS nspowner, NULL AS nspacl
);
```

Required columns:

| Column | Type | Description |
|--------|------|-------------|
| oid | oid | Namespace OID |
| nspname | name | Schema name |
| nspowner | oid | Owner role OID |
| nspacl | aclitem[] | Access privileges (NULL) |

### 6.3.2 pg_class (Required)

Emulates tables, views, indexes, and sequences.

```sql
CREATE VIEW pg_catalog.pg_class AS
SELECT
    hash(table_schema || '.' || table_name)::int AS oid,
    table_name AS relname,
    (SELECT oid FROM pg_namespace WHERE nspname = table_schema) AS relnamespace,
    CASE table_type
        WHEN 'BASE TABLE' THEN 'r'  -- ordinary table
        WHEN 'VIEW' THEN 'v'        -- view
        WHEN 'LOCAL TEMPORARY' THEN 'r'
    END AS relkind,
    10 AS relowner,
    0 AS relam,
    0 AS reltuples,
    0 AS relpages,
    -- ... additional columns
FROM information_schema.tables
WHERE table_schema NOT IN ('pg_catalog', 'information_schema');
```

Required columns for ORM compatibility:

| Column | Type | Description |
|--------|------|-------------|
| oid | oid | Relation OID |
| relname | name | Table/view name |
| relnamespace | oid | Schema OID |
| relkind | char | r=table, v=view, i=index, S=sequence |
| relowner | oid | Owner role OID |
| relam | oid | Access method (0 for tables) |
| reltuples | float4 | Estimated row count |
| relpages | int4 | Estimated page count |
| relhasindex | bool | Has indexes |
| relispopulated | bool | True for tables |
| relreplident | char | Replica identity |

### 6.3.3 pg_attribute (Required)

Emulates column metadata.

```sql
CREATE VIEW pg_catalog.pg_attribute AS
SELECT
    (SELECT oid FROM pg_class WHERE relname = c.table_name 
     AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = c.table_schema)) AS attrelid,
    c.column_name AS attname,
    (SELECT oid FROM pg_type WHERE typname = map_type(c.data_type)) AS atttypid,
    -1 AS attstattarget,
    CASE WHEN c.character_maximum_length IS NOT NULL 
         THEN c.character_maximum_length + 4
         ELSE -1 END AS attlen,
    c.ordinal_position AS attnum,
    0 AS attndims,
    -1 AS attcacheoff,
    CASE WHEN c.numeric_precision IS NOT NULL
         THEN ((c.numeric_precision::int << 16) | c.numeric_scale::int)
         ELSE -1 END AS atttypmod,
    NOT c.is_nullable AS attnotnull,
    c.column_default IS NOT NULL AS atthasdef,
    FALSE AS attisdropped,
    '' AS attidentity,
    '' AS attgenerated
FROM information_schema.columns c;
```

Required columns:

| Column | Type | Description |
|--------|------|-------------|
| attrelid | oid | Table OID |
| attname | name | Column name |
| atttypid | oid | Type OID |
| attlen | int2 | Type length |
| attnum | int2 | Column number (1-based) |
| atttypmod | int4 | Type modifier |
| attnotnull | bool | NOT NULL constraint |
| atthasdef | bool | Has default value |
| attisdropped | bool | Dropped column |

### 6.3.4 pg_type (Required)

Emulates type metadata. This is primarily synthetic based on the type mapping table in [Section 8](./08-type-system.md).

```sql
CREATE VIEW pg_catalog.pg_type AS
SELECT * FROM (VALUES
    (16, 'bool', 11, 1, 'b', 'boolean'),
    (20, 'int8', 11, 8, 'b', 'bigint'),
    (21, 'int2', 11, 2, 'b', 'smallint'),
    (23, 'int4', 11, 4, 'b', 'integer'),
    (25, 'text', 11, -1, 'b', 'text'),
    (700, 'float4', 11, 4, 'b', 'real'),
    (701, 'float8', 11, 8, 'b', 'double precision'),
    (1043, 'varchar', 11, -1, 'b', 'character varying'),
    (1082, 'date', 11, 4, 'b', 'date'),
    (1083, 'time', 11, 8, 'b', 'time without time zone'),
    (1114, 'timestamp', 11, 8, 'b', 'timestamp without time zone'),
    (1184, 'timestamptz', 11, 8, 'b', 'timestamp with time zone'),
    (1700, 'numeric', 11, -1, 'b', 'numeric'),
    (2950, 'uuid', 11, 16, 'b', 'uuid'),
    (3802, 'jsonb', 11, -1, 'b', 'jsonb'),
    -- ... additional types
) AS t(oid, typname, typnamespace, typlen, typtype, typname_long);
```

Required columns:

| Column | Type | Description |
|--------|------|-------------|
| oid | oid | Type OID |
| typname | name | Type name |
| typnamespace | oid | Schema OID (11 = pg_catalog) |
| typlen | int2 | Storage size (-1 = variable) |
| typtype | char | b=base, c=composite, d=domain |
| typbasetype | oid | Base type for domains |
| typtypmod | int4 | Type modifier |
| typnotnull | bool | NOT NULL constraint |
| typarray | oid | Array type OID |

### 6.3.5 pg_constraint (Required for ORMs)

Emulates constraints (primary keys, foreign keys, unique, check).

Required columns:

| Column | Type | Description |
|--------|------|-------------|
| oid | oid | Constraint OID |
| conname | name | Constraint name |
| connamespace | oid | Schema OID |
| contype | char | p=PK, f=FK, u=unique, c=check |
| conrelid | oid | Table OID |
| confrelid | oid | Referenced table (FK only) |
| conkey | int2[] | Constrained columns |
| confkey | int2[] | Referenced columns (FK only) |

### 6.3.6 pg_index (Required for ORMs)

Emulates index metadata.

Required columns:

| Column | Type | Description |
|--------|------|-------------|
| indexrelid | oid | Index OID |
| indrelid | oid | Table OID |
| indnatts | int2 | Number of columns |
| indisunique | bool | Unique index |
| indisprimary | bool | Primary key |
| indkey | int2vector | Column numbers |

### 6.3.7 pg_roles / pg_user (Required)

Emulates role/user metadata.

```sql
CREATE VIEW pg_catalog.pg_roles AS
SELECT
    10 AS oid,
    current_user AS rolname,
    TRUE AS rolsuper,
    TRUE AS rolinherit,
    TRUE AS rolcreaterole,
    TRUE AS rolcreatedb,
    TRUE AS rolcanlogin,
    -1 AS rolconnlimit,
    '********' AS rolpassword,
    NULL AS rolvaliduntil;
```

### 6.3.8 pg_database (Required)

Emulates database metadata (single database).

```sql
CREATE VIEW pg_catalog.pg_database AS
SELECT
    1 AS oid,
    current_database() AS datname,
    10 AS datdba,
    6 AS encoding,  -- UTF8
    'C' AS datcollate,
    'C' AS datctype,
    TRUE AS datistemplate,
    TRUE AS datallowconn,
    -1 AS datconnlimit;
```

## 6.4 information_schema Views

DuckDB provides native `information_schema` support. mallardb SHOULD pass through most information_schema queries directly, augmenting only where PostgreSQL-specific columns are expected.

### 6.4.1 tables

Pass through to DuckDB, ensure these columns exist:

| Column | Source |
|--------|--------|
| table_catalog | current_database() |
| table_schema | DuckDB |
| table_name | DuckDB |
| table_type | DuckDB (map if needed) |

### 6.4.2 columns

Pass through to DuckDB, ensure these columns exist:

| Column | Source |
|--------|--------|
| table_catalog | current_database() |
| table_schema | DuckDB |
| table_name | DuckDB |
| column_name | DuckDB |
| ordinal_position | DuckDB |
| column_default | DuckDB |
| is_nullable | DuckDB |
| data_type | DuckDB (may need mapping) |
| character_maximum_length | DuckDB |
| numeric_precision | DuckDB |
| numeric_scale | DuckDB |
| udt_name | Map to PostgreSQL type name |

### 6.4.3 schemata

Pass through to DuckDB plus synthetic `pg_catalog` and `information_schema`.

## 6.5 psql Command Support

mallardb MUST support catalog queries generated by these psql commands:

| Command | Description | Key Catalogs |
|---------|-------------|--------------|
| `\dt` | List tables | pg_class, pg_namespace |
| `\dt+` | List tables with size | pg_class, pg_namespace, pg_total_relation_size |
| `\d tablename` | Describe table | pg_attribute, pg_type, pg_constraint, pg_index |
| `\dn` | List schemas | pg_namespace |
| `\du` | List roles | pg_roles |
| `\df` | List functions | pg_proc (minimal) |
| `\di` | List indexes | pg_index, pg_class |
| `\dv` | List views | pg_class (relkind='v') |
| `\l` | List databases | pg_database |
| `\conninfo` | Connection info | Session state |

### 6.5.1 Size Functions

psql size commands require these functions:

```sql
-- Return 0 for now; accurate sizing is future work
CREATE FUNCTION pg_total_relation_size(oid) RETURNS bigint AS 0;
CREATE FUNCTION pg_table_size(oid) RETURNS bigint AS 0;
CREATE FUNCTION pg_indexes_size(oid) RETURNS bigint AS 0;
CREATE FUNCTION pg_size_pretty(bigint) RETURNS text AS 
    CASE 
        WHEN $1 < 1024 THEN $1 || ' bytes'
        WHEN $1 < 1048576 THEN ($1 / 1024) || ' kB'
        WHEN $1 < 1073741824 THEN ($1 / 1048576) || ' MB'
        ELSE ($1 / 1073741824) || ' GB'
    END;
```

## 6.6 ORM Compatibility

### 6.6.1 SQLAlchemy

SQLAlchemy introspection queries:

```sql
-- Table existence
SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1);

-- Column introspection  
SELECT * FROM information_schema.columns WHERE table_name = $1;

-- Primary keys
SELECT a.attname FROM pg_index i
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
WHERE i.indrelid = $1::regclass AND i.indisprimary;

-- Foreign keys
SELECT ... FROM pg_constraint WHERE contype = 'f' AND conrelid = $1::regclass;
```

### 6.6.2 Prisma

Prisma introspection queries include:

```sql
-- Database version
SELECT version();

-- Schema list
SELECT nspname FROM pg_namespace WHERE nspname NOT LIKE 'pg_%';

-- Enum types
SELECT t.typname, e.enumlabel FROM pg_type t 
JOIN pg_enum e ON t.oid = e.enumtypid;
```

### 6.6.3 TypeORM

TypeORM introspection patterns similar to SQLAlchemy.

## 6.7 System Functions

mallardb MUST implement these PostgreSQL system functions:

| Function | Return | Implementation |
|----------|--------|----------------|
| `current_database()` | name | Return configured database name |
| `current_schema()` | name | Return 'public' or first in search_path |
| `current_user` | name | Return authenticated username |
| `session_user` | name | Return authenticated username |
| `version()` | text | Return mallardb version string |
| `pg_backend_pid()` | int | Return connection's process ID |
| `pg_typeof(any)` | regtype | Return type name of argument |
| `has_table_privilege(...)` | bool | Return TRUE (permissive) |
| `has_schema_privilege(...)` | bool | Return TRUE (permissive) |

### 6.7.1 version() Format

The `version()` function MUST return a string parseable by PostgreSQL clients:

```
PostgreSQL 15.0 (mallardb 0.1.0, DuckDB 1.0.0)
```

Format: `PostgreSQL {compat_version} (mallardb {version}, DuckDB {duckdb_version})`

## 6.8 Search Path

mallardb MUST support `search_path` for schema resolution:

```sql
SET search_path TO public, analytics;
SHOW search_path;
```

Default search_path: `"$user", public`

The search_path affects:
- Unqualified table name resolution
- `current_schema()` return value
- New object creation schema
