//! Type system mapping between DuckDB and PostgreSQL
//!
//! Maps DuckDB types to PostgreSQL type OIDs for wire protocol compatibility.

use pgwire::api::Type;

/// PostgreSQL type OIDs
pub mod oid {
    pub const BOOL: u32 = 16;
    pub const BYTEA: u32 = 17;
    pub const INT8: u32 = 20;
    pub const INT2: u32 = 21;
    pub const INT4: u32 = 23;
    pub const TEXT: u32 = 25;
    pub const FLOAT4: u32 = 700;
    pub const FLOAT8: u32 = 701;
    pub const UNKNOWN: u32 = 705;
    pub const BPCHAR: u32 = 1042;
    pub const VARCHAR: u32 = 1043;
    pub const DATE: u32 = 1082;
    pub const TIME: u32 = 1083;
    pub const TIMESTAMP: u32 = 1114;
    pub const TIMESTAMPTZ: u32 = 1184;
    pub const INTERVAL: u32 = 1186;
    pub const BIT: u32 = 1560;
    pub const NUMERIC: u32 = 1700;
    pub const UUID: u32 = 2950;
    pub const JSONB: u32 = 3802;

    // Array types
    pub const BOOL_ARRAY: u32 = 1000;
    pub const INT2_ARRAY: u32 = 1005;
    pub const INT4_ARRAY: u32 = 1007;
    pub const TEXT_ARRAY: u32 = 1009;
    pub const VARCHAR_ARRAY: u32 = 1015;
    pub const INT8_ARRAY: u32 = 1016;
    pub const FLOAT4_ARRAY: u32 = 1021;
    pub const FLOAT8_ARRAY: u32 = 1022;
    pub const TIMESTAMP_ARRAY: u32 = 1115;
    pub const DATE_ARRAY: u32 = 1182;
    pub const TIMESTAMPTZ_ARRAY: u32 = 1185;
    pub const UUID_ARRAY: u32 = 2951;
    pub const JSONB_ARRAY: u32 = 3807;
}

/// Map DuckDB type name to pgwire Type
pub fn duckdb_type_to_pgwire(type_name: &str) -> Type {
    let upper = type_name.to_uppercase();

    // Handle parameterized types
    let base_type = if upper.contains('(') {
        upper.split('(').next().unwrap_or(&upper)
    } else {
        &upper
    };

    match base_type.trim() {
        "BOOLEAN" | "BOOL" => Type::BOOL,

        "TINYINT" | "INT1" => Type::INT2, // Widen to int2
        "SMALLINT" | "INT2" => Type::INT2,
        "INTEGER" | "INT" | "INT4" | "SIGNED" => Type::INT4,
        "BIGINT" | "INT8" | "LONG" => Type::INT8,
        "HUGEINT" | "INT128" => Type::NUMERIC,

        "UTINYINT" | "UINT1" => Type::INT2,
        "USMALLINT" | "UINT2" => Type::INT4,
        "UINTEGER" | "UINT4" => Type::INT8,
        "UBIGINT" | "UINT8" => Type::NUMERIC,

        "FLOAT" | "FLOAT4" | "REAL" => Type::FLOAT4,
        "DOUBLE" | "FLOAT8" => Type::FLOAT8,
        "DECIMAL" | "NUMERIC" => Type::NUMERIC,

        "VARCHAR" | "STRING" => Type::VARCHAR,
        "CHAR" | "BPCHAR" => Type::CHAR,
        "TEXT" => Type::TEXT,
        "BLOB" | "BYTEA" | "BINARY" | "VARBINARY" => Type::BYTEA,

        "DATE" => Type::DATE,
        "TIME" => Type::TIME,
        "TIMESTAMP" => Type::TIMESTAMP,
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => Type::TIMESTAMPTZ,
        "INTERVAL" => Type::INTERVAL,

        "UUID" => Type::UUID,
        "JSON" | "JSONB" => Type::JSONB,

        "BIT" | "BITSTRING" => Type::BIT,

        // Array/list types
        t if t.ends_with("[]") => {
            let element = t.trim_end_matches("[]");
            match element {
                "INTEGER" | "INT" | "INT4" => Type::INT4_ARRAY,
                "BIGINT" | "INT8" => Type::INT8_ARRAY,
                "TEXT" | "VARCHAR" | "STRING" => Type::TEXT_ARRAY,
                "BOOLEAN" | "BOOL" => Type::BOOL_ARRAY,
                "FLOAT" | "FLOAT4" | "REAL" => Type::FLOAT4_ARRAY,
                "DOUBLE" | "FLOAT8" => Type::FLOAT8_ARRAY,
                _ => Type::TEXT_ARRAY, // Fallback
            }
        }

        // DuckDB LIST type
        "LIST" => Type::TEXT_ARRAY,
        "MAP" => Type::JSONB,
        "STRUCT" => Type::JSONB, // Serialize as JSON

        _ => Type::TEXT, // Fallback to text
    }
}

/// Type information for pg_type catalog
pub struct PgTypeInfo {
    pub oid: u32,
    pub typname: &'static str,
    pub typlen: i16,
    pub typtype: char,
}

/// Get the pg_type entries for catalog emulation
pub fn get_pg_types() -> Vec<PgTypeInfo> {
    vec![
        PgTypeInfo {
            oid: oid::BOOL,
            typname: "bool",
            typlen: 1,
            typtype: 'b',
        },
        PgTypeInfo {
            oid: oid::BYTEA,
            typname: "bytea",
            typlen: -1,
            typtype: 'b',
        },
        PgTypeInfo {
            oid: oid::INT8,
            typname: "int8",
            typlen: 8,
            typtype: 'b',
        },
        PgTypeInfo {
            oid: oid::INT2,
            typname: "int2",
            typlen: 2,
            typtype: 'b',
        },
        PgTypeInfo {
            oid: oid::INT4,
            typname: "int4",
            typlen: 4,
            typtype: 'b',
        },
        PgTypeInfo {
            oid: oid::TEXT,
            typname: "text",
            typlen: -1,
            typtype: 'b',
        },
        PgTypeInfo {
            oid: oid::FLOAT4,
            typname: "float4",
            typlen: 4,
            typtype: 'b',
        },
        PgTypeInfo {
            oid: oid::FLOAT8,
            typname: "float8",
            typlen: 8,
            typtype: 'b',
        },
        PgTypeInfo {
            oid: oid::VARCHAR,
            typname: "varchar",
            typlen: -1,
            typtype: 'b',
        },
        PgTypeInfo {
            oid: oid::DATE,
            typname: "date",
            typlen: 4,
            typtype: 'b',
        },
        PgTypeInfo {
            oid: oid::TIME,
            typname: "time",
            typlen: 8,
            typtype: 'b',
        },
        PgTypeInfo {
            oid: oid::TIMESTAMP,
            typname: "timestamp",
            typlen: 8,
            typtype: 'b',
        },
        PgTypeInfo {
            oid: oid::TIMESTAMPTZ,
            typname: "timestamptz",
            typlen: 8,
            typtype: 'b',
        },
        PgTypeInfo {
            oid: oid::INTERVAL,
            typname: "interval",
            typlen: 16,
            typtype: 'b',
        },
        PgTypeInfo {
            oid: oid::NUMERIC,
            typname: "numeric",
            typlen: -1,
            typtype: 'b',
        },
        PgTypeInfo {
            oid: oid::UUID,
            typname: "uuid",
            typlen: 16,
            typtype: 'b',
        },
        PgTypeInfo {
            oid: oid::JSONB,
            typname: "jsonb",
            typlen: -1,
            typtype: 'b',
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oid_values() {
        assert_eq!(oid::BOOL, 16);
        assert_eq!(oid::INT4, 23);
        assert_eq!(oid::TEXT, 25);
        assert_eq!(oid::TIMESTAMP, 1114);
    }

    #[test]
    fn test_boolean_types() {
        assert_eq!(duckdb_type_to_pgwire("BOOLEAN"), Type::BOOL);
        assert_eq!(duckdb_type_to_pgwire("BOOL"), Type::BOOL);
        assert_eq!(duckdb_type_to_pgwire("boolean"), Type::BOOL);
    }

    #[test]
    fn test_integer_types() {
        assert_eq!(duckdb_type_to_pgwire("TINYINT"), Type::INT2);
        assert_eq!(duckdb_type_to_pgwire("SMALLINT"), Type::INT2);
        assert_eq!(duckdb_type_to_pgwire("INT2"), Type::INT2);
        assert_eq!(duckdb_type_to_pgwire("INTEGER"), Type::INT4);
        assert_eq!(duckdb_type_to_pgwire("INT"), Type::INT4);
        assert_eq!(duckdb_type_to_pgwire("INT4"), Type::INT4);
        assert_eq!(duckdb_type_to_pgwire("BIGINT"), Type::INT8);
        assert_eq!(duckdb_type_to_pgwire("INT8"), Type::INT8);
        assert_eq!(duckdb_type_to_pgwire("HUGEINT"), Type::NUMERIC);
    }

    #[test]
    fn test_unsigned_integer_types() {
        assert_eq!(duckdb_type_to_pgwire("UTINYINT"), Type::INT2);
        assert_eq!(duckdb_type_to_pgwire("USMALLINT"), Type::INT4);
        assert_eq!(duckdb_type_to_pgwire("UINTEGER"), Type::INT8);
        assert_eq!(duckdb_type_to_pgwire("UBIGINT"), Type::NUMERIC);
    }

    #[test]
    fn test_float_types() {
        assert_eq!(duckdb_type_to_pgwire("FLOAT"), Type::FLOAT4);
        assert_eq!(duckdb_type_to_pgwire("FLOAT4"), Type::FLOAT4);
        assert_eq!(duckdb_type_to_pgwire("REAL"), Type::FLOAT4);
        assert_eq!(duckdb_type_to_pgwire("DOUBLE"), Type::FLOAT8);
        assert_eq!(duckdb_type_to_pgwire("FLOAT8"), Type::FLOAT8);
    }

    #[test]
    fn test_decimal_types() {
        assert_eq!(duckdb_type_to_pgwire("DECIMAL"), Type::NUMERIC);
        assert_eq!(duckdb_type_to_pgwire("NUMERIC"), Type::NUMERIC);
        assert_eq!(duckdb_type_to_pgwire("DECIMAL(10,2)"), Type::NUMERIC);
    }

    #[test]
    fn test_string_types() {
        assert_eq!(duckdb_type_to_pgwire("VARCHAR"), Type::VARCHAR);
        assert_eq!(duckdb_type_to_pgwire("STRING"), Type::VARCHAR);
        assert_eq!(duckdb_type_to_pgwire("VARCHAR(255)"), Type::VARCHAR);
        assert_eq!(duckdb_type_to_pgwire("TEXT"), Type::TEXT);
        assert_eq!(duckdb_type_to_pgwire("CHAR"), Type::CHAR);
    }

    #[test]
    fn test_binary_types() {
        assert_eq!(duckdb_type_to_pgwire("BLOB"), Type::BYTEA);
        assert_eq!(duckdb_type_to_pgwire("BYTEA"), Type::BYTEA);
        assert_eq!(duckdb_type_to_pgwire("BINARY"), Type::BYTEA);
        assert_eq!(duckdb_type_to_pgwire("VARBINARY"), Type::BYTEA);
    }

    #[test]
    fn test_datetime_types() {
        assert_eq!(duckdb_type_to_pgwire("DATE"), Type::DATE);
        assert_eq!(duckdb_type_to_pgwire("TIME"), Type::TIME);
        assert_eq!(duckdb_type_to_pgwire("TIMESTAMP"), Type::TIMESTAMP);
        assert_eq!(duckdb_type_to_pgwire("TIMESTAMPTZ"), Type::TIMESTAMPTZ);
        assert_eq!(duckdb_type_to_pgwire("INTERVAL"), Type::INTERVAL);
    }

    #[test]
    fn test_special_types() {
        assert_eq!(duckdb_type_to_pgwire("UUID"), Type::UUID);
        assert_eq!(duckdb_type_to_pgwire("JSON"), Type::JSONB);
        assert_eq!(duckdb_type_to_pgwire("JSONB"), Type::JSONB);
        assert_eq!(duckdb_type_to_pgwire("BIT"), Type::BIT);
    }

    #[test]
    fn test_array_types() {
        assert_eq!(duckdb_type_to_pgwire("INTEGER[]"), Type::INT4_ARRAY);
        assert_eq!(duckdb_type_to_pgwire("INT[]"), Type::INT4_ARRAY);
        assert_eq!(duckdb_type_to_pgwire("BIGINT[]"), Type::INT8_ARRAY);
        assert_eq!(duckdb_type_to_pgwire("TEXT[]"), Type::TEXT_ARRAY);
        assert_eq!(duckdb_type_to_pgwire("BOOLEAN[]"), Type::BOOL_ARRAY);
        assert_eq!(duckdb_type_to_pgwire("FLOAT[]"), Type::FLOAT4_ARRAY);
        assert_eq!(duckdb_type_to_pgwire("DOUBLE[]"), Type::FLOAT8_ARRAY);
    }

    #[test]
    fn test_complex_types() {
        assert_eq!(duckdb_type_to_pgwire("LIST"), Type::TEXT_ARRAY);
        assert_eq!(duckdb_type_to_pgwire("MAP"), Type::JSONB);
        assert_eq!(duckdb_type_to_pgwire("STRUCT"), Type::JSONB);
    }

    #[test]
    fn test_unknown_type_fallback() {
        assert_eq!(duckdb_type_to_pgwire("UNKNOWN_TYPE"), Type::TEXT);
        assert_eq!(duckdb_type_to_pgwire("CUSTOM"), Type::TEXT);
    }

    #[test]
    fn test_case_insensitivity() {
        assert_eq!(duckdb_type_to_pgwire("integer"), Type::INT4);
        assert_eq!(duckdb_type_to_pgwire("Integer"), Type::INT4);
        assert_eq!(duckdb_type_to_pgwire("INTEGER"), Type::INT4);
    }

    #[test]
    fn test_parameterized_types() {
        assert_eq!(duckdb_type_to_pgwire("VARCHAR(100)"), Type::VARCHAR);
        assert_eq!(duckdb_type_to_pgwire("DECIMAL(18,4)"), Type::NUMERIC);
        assert_eq!(duckdb_type_to_pgwire("NUMERIC(10)"), Type::NUMERIC);
    }

    #[test]
    fn test_get_pg_types() {
        let types = get_pg_types();
        assert!(!types.is_empty());

        // Check for some expected types
        assert!(
            types
                .iter()
                .any(|t| t.typname == "bool" && t.oid == oid::BOOL)
        );
        assert!(
            types
                .iter()
                .any(|t| t.typname == "int4" && t.oid == oid::INT4)
        );
        assert!(
            types
                .iter()
                .any(|t| t.typname == "text" && t.oid == oid::TEXT)
        );
        assert!(
            types
                .iter()
                .any(|t| t.typname == "timestamp" && t.oid == oid::TIMESTAMP)
        );
    }

    #[test]
    fn test_pg_type_info_structure() {
        let types = get_pg_types();

        // Check bool type info
        let bool_type = types.iter().find(|t| t.typname == "bool").unwrap();
        assert_eq!(bool_type.oid, oid::BOOL);
        assert_eq!(bool_type.typlen, 1);
        assert_eq!(bool_type.typtype, 'b');

        // Check variable-length type
        let text_type = types.iter().find(|t| t.typname == "text").unwrap();
        assert_eq!(text_type.typlen, -1); // Variable length

        // Check fixed-length type
        let int4_type = types.iter().find(|t| t.typname == "int4").unwrap();
        assert_eq!(int4_type.typlen, 4);
    }
}
