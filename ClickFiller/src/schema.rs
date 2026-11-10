//! The ClickHouse table and the row projection, from one column table.
//!
//! The DDL sent to ClickHouse and the JSONEachRow rows built from
//! Elasticsearch documents both derive from [`COLUMNS`], so they cannot
//! drift apart: add a column there and both the table and the projected
//! rows pick it up.
//!
//! Column semantics (mirroring elastic-filler's document shape):
//!
//! * `ts` replaces `@timestamp` — friendlier in SQL, same nanosecond
//!   ISO-8601 value (kept verbatim in `_source` even though the ES
//!   `date` field itself truncates to millis).
//! * `es_id` carries the Elasticsearch document id (`file:packet:n`) —
//!   provenance, and half of the dedup key.
//! * Numeric fields default to `0`, string fields to `''`, matching
//!   "not applicable for this msg_type" — which `msg_type` already
//!   answers. Only fields where Elasticsearch stores an explicit `null`
//!   with meaning (`symbol_index` on non-symbol messages, the refresh
//!   header's sequence numbers "absent on some channels") are
//!   `Nullable`.
//! * `price` stays the raw integer numerator the feed carries; scaling
//!   by `price_scale_code` remains the analysis layer's job (same
//!   convention as elastic-filler).
//!
//! [`ReplacingMergeTree`] with `ORDER BY (ts, es_id)` makes the whole
//! transfer idempotent: a rerun (or the HTTP client's transparent
//! reconnect-resend, or a resume that re-walks a boundary) re-inserts
//! identical rows, and merges collapse them — point queries use `FINAL`.

use crate::json::Json;

/// One column: its ClickHouse name, the `_source` key it comes from
/// (empty for injected columns like `es_id`), the ClickHouse type, and
/// the two knobs that decide DDL and row rendering.
pub struct Column {
    pub name: &'static str,
    pub es: &'static str,
    pub ch_type: &'static str,
    /// Numbers must be JSON number tokens (a string is a document bug).
    pub numeric: bool,
    /// Rendered as `Nullable(T)`; null/absent values become NULL.
    pub nullable: bool,
}

/// Table columns in declaration order — also the row field order, so
/// inserted lines are stable and diffable across runs.
pub const COLUMNS: &[Column] = &[
    Column { name: "ts", es: "@timestamp", ch_type: "DateTime64(9, 'UTC')", numeric: false, nullable: false },
    Column { name: "es_id", es: "", ch_type: "String", numeric: false, nullable: false },
    Column { name: "file", es: "file", ch_type: "LowCardinality(String)", numeric: false, nullable: false },
    Column { name: "msg_type", es: "msg_type", ch_type: "LowCardinality(String)", numeric: false, nullable: false },
    Column { name: "msg_type_id", es: "msg_type_id", ch_type: "UInt16", numeric: true, nullable: false },
    Column { name: "symbol", es: "symbol", ch_type: "LowCardinality(String)", numeric: false, nullable: false },
    Column { name: "symbol_index", es: "symbol_index", ch_type: "UInt32", numeric: true, nullable: true },
    Column { name: "source_time", es: "source_time", ch_type: "UInt32", numeric: true, nullable: false },
    Column { name: "source_time_ns", es: "source_time_ns", ch_type: "UInt32", numeric: true, nullable: false },
    Column { name: "product_id", es: "product_id", ch_type: "UInt8", numeric: true, nullable: false },
    Column { name: "channel_id", es: "channel_id", ch_type: "UInt8", numeric: true, nullable: false },
    Column { name: "partition_id", es: "partition_id", ch_type: "UInt32", numeric: true, nullable: false },
    Column { name: "symbol_seq_num", es: "symbol_seq_num", ch_type: "UInt32", numeric: true, nullable: false },
    Column { name: "security_status", es: "security_status", ch_type: "LowCardinality(String)", numeric: false, nullable: false },
    Column { name: "halt_condition", es: "halt_condition", ch_type: "LowCardinality(String)", numeric: false, nullable: false },
    Column { name: "current_refresh_pkt", es: "current_refresh_pkt", ch_type: "UInt16", numeric: true, nullable: false },
    Column { name: "total_refresh_pkts", es: "total_refresh_pkts", ch_type: "UInt16", numeric: true, nullable: false },
    Column { name: "last_seq_num", es: "last_seq_num", ch_type: "UInt32", numeric: true, nullable: true },
    Column { name: "last_symbol_seq_num", es: "last_symbol_seq_num", ch_type: "UInt32", numeric: true, nullable: true },
    Column { name: "order_id", es: "order_id", ch_type: "UInt64", numeric: true, nullable: false },
    Column { name: "new_order_id", es: "new_order_id", ch_type: "UInt64", numeric: true, nullable: false },
    Column { name: "trade_id", es: "trade_id", ch_type: "UInt32", numeric: true, nullable: false },
    Column { name: "price", es: "price", ch_type: "UInt32", numeric: true, nullable: false },
    Column { name: "volume", es: "volume", ch_type: "UInt32", numeric: true, nullable: false },
    Column { name: "side", es: "side", ch_type: "LowCardinality(String)", numeric: false, nullable: false },
    Column { name: "firm_id", es: "firm_id", ch_type: "LowCardinality(String)", numeric: false, nullable: false },
    Column { name: "position_change", es: "position_change", ch_type: "UInt8", numeric: true, nullable: false },
    Column { name: "printable", es: "printable", ch_type: "UInt8", numeric: true, nullable: false },
    Column { name: "trade_conditions", es: "trade_conditions", ch_type: "LowCardinality(String)", numeric: false, nullable: false },
    Column { name: "reference_price", es: "reference_price", ch_type: "UInt32", numeric: true, nullable: false },
    Column { name: "paired_qty", es: "paired_qty", ch_type: "UInt32", numeric: true, nullable: false },
];

/// The `CREATE TABLE IF NOT EXISTS` statement, derived from [`COLUMNS`].
/// Idempotent: every run ensures the table before transferring.
pub fn create_table_ddl(table: &str) -> String {
    let lines = COLUMNS
        .iter()
        .map(|col| {
            let kind = if col.nullable {
                format!("Nullable({})", col.ch_type)
            } else if col.numeric {
                format!("{} DEFAULT 0", col.ch_type)
            } else if col.ch_type.contains("String") {
                // String-family columns: absent -> empty string.
                format!("{} DEFAULT ''", col.ch_type)
            } else {
                // ts: fixed precision + UTC, no default.
                col.ch_type.to_string()
            };
            format!("    {} {}", col.name, kind)
        })
        .collect::<Vec<_>>()
        .join(",\n");
    format!(
        "CREATE TABLE IF NOT EXISTS {table}\n(\n{lines}\n) \
         ENGINE = ReplacingMergeTree\nPARTITION BY toDate(ts)\nORDER BY (ts, es_id)"
    )
}

/// Project one Elasticsearch `_source` (with its `_id`) into a
/// JSONEachRow row: fields renamed (`@timestamp` -> `ts`), `es_id`
/// injected, unknown fields dropped, absent fields left to the column
/// defaults. Fails loudly on shapes the schema cannot represent (a
/// missing timestamp, a string where a number belongs) — a silently
/// partial transfer is worse than a stopped one.
pub fn project_row(es_id: &str, source: &Json) -> Result<String, String> {
    // The timeline position is not defaultable: without it the row would
    // land nowhere (or silently at the epoch).
    let ts = source
        .get("@timestamp")
        .ok_or_else(|| "document has no `@timestamp`".to_string())?;
    if !matches!(ts, Json::Str(_)) {
        return Err(format!("`@timestamp` wants a string, got {}", kind_of(ts)));
    }

    let mut row = String::from("{");
    let mut first = true;
    for col in COLUMNS {
        let value = if col.name == "es_id" {
            Some(&Json::Str(es_id.to_string()))
        } else {
            source.get(col.es)
        };
        let rendered = match value {
            // Absent: non-nullable columns take their DEFAULT, nullable
            // ones NULL — no key at all.
            None => None,
            // Explicit null is only meaningful on nullable columns.
            Some(Json::Null) if col.nullable => Some("null".to_string()),
            Some(Json::Null) => None,
            Some(value) => Some(render_value(col, value)?),
        };
        if let Some(text) = rendered {
            if !first {
                row.push(',');
            }
            first = false;
            // Column names are static ASCII identifiers: plain quoting.
            row.push('"');
            row.push_str(col.name);
            row.push_str("\":");
            row.push_str(&text);
        }
    }
    row.push('}');
    Ok(row)
}

/// Render one `_source` value for its column: numbers stay raw digit
/// text (u64-safe), strings stay quoted and escaped. A numeric-looking
/// value in a string column is coerced to its quoted form (dynamic ES
/// mapping can store a numeric-looking keyword as a number); anything
/// else is a document bug worth stopping for.
fn render_value(col: &Column, value: &Json) -> Result<String, String> {
    if col.numeric {
        return match value {
            Json::Num(text) => Ok(text.clone()),
            other => Err(format!(
                "field `{}` wants {}, got {}",
                col.name,
                col.ch_type,
                kind_of(other)
            )),
        };
    }
    match value {
        Json::Str(_) => Ok(value.to_compact()),
        Json::Num(text) => Ok(Json::Str(text.clone()).to_compact()),
        other => Err(format!(
            "field `{}` wants {}, got {}",
            col.name,
            col.ch_type,
            kind_of(other)
        )),
    }
}

fn kind_of(value: &Json) -> &'static str {
    match value {
        Json::Num(_) => "number",
        Json::Str(_) => "string",
        Json::Bool(_) => "bool",
        Json::Arr(_) => "array",
        Json::Obj(_) => "object",
        Json::Null => "null",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json;

    #[test]
    fn ddl_has_engine_and_every_column() {
        let ddl = create_table_ddl("xdp_messages");
        assert!(ddl.starts_with("CREATE TABLE IF NOT EXISTS xdp_messages"));
        assert!(ddl.contains("ENGINE = ReplacingMergeTree"));
        assert!(ddl.contains("PARTITION BY toDate(ts)"));
        assert!(ddl.contains("ORDER BY (ts, es_id)"));
        for col in COLUMNS {
            assert!(ddl.contains(col.name), "missing column {}", col.name);
        }
        assert!(ddl.contains("ts DateTime64(9, 'UTC')"));
        assert!(ddl.contains("symbol_index Nullable(UInt32)"));
        assert!(ddl.contains("price UInt32 DEFAULT 0"));
        assert!(ddl.contains("side LowCardinality(String) DEFAULT ''"));
        assert!(ddl.contains("es_id String"));
        assert!(!ddl.contains("DEFAULT NULL"));
        assert!(ddl.ends_with("ORDER BY (ts, es_id)"));
    }

    #[test]
    fn add_order_doc_projects_to_column_order() {
        let source = json::parse(
            r#"{"@timestamp":"2023-08-22T13:30:00.000000500Z","file":"cap.pcap.zst",
                "msg_type":"add_order","msg_type_id":100,"symbol_index":100,"symbol":"WSM",
                "source_time_ns":500,"symbol_seq_num":7,"order_id":4242,"price":123456,
                "volume":300,"side":"B","firm_id":"MPI","kibana_only":true}"#,
        )
        .unwrap();
        let row = project_row("cap.pcap.zst:9:0", &source).unwrap();
        // Column order, ts rename, es_id injection, unknown field dropped.
        assert_eq!(
            row,
            "{\"ts\":\"2023-08-22T13:30:00.000000500Z\",\"es_id\":\"cap.pcap.zst:9:0\",\
             \"file\":\"cap.pcap.zst\",\"msg_type\":\"add_order\",\"msg_type_id\":100,\
             \"symbol\":\"WSM\",\"symbol_index\":100,\"source_time_ns\":500,\
             \"symbol_seq_num\":7,\"order_id\":4242,\"price\":123456,\"volume\":300,\
             \"side\":\"B\",\"firm_id\":\"MPI\"}"
        );
    }

    #[test]
    fn absent_and_null_fields_render_per_nullability() {
        let source = json::parse(
            r#"{"@timestamp":"2023-08-22T13:30:01.000000001Z","file":"f","msg_type":"refresh_header",
                "msg_type_id":35,"current_refresh_pkt":1,"total_refresh_pkts":3,
                "last_seq_num":null,"last_symbol_seq_num":123}"#,
        )
        .unwrap();
        let row = project_row("f:1:0", &source).unwrap();
        // Explicit null survives on nullable columns; the other null stays
        // dropped; numeric defaults are simply absent keys.
        assert!(row.contains("\"last_seq_num\":null"));
        assert!(row.contains("\"last_symbol_seq_num\":123"));
        assert!(!row.contains("symbol_index"));
        assert!(!row.contains("price"));
        assert!(!row.contains("\"ts\":null"));
    }

    #[test]
    fn numeric_looking_values_coerce_into_string_columns() {
        // Dynamic ES mapping stores numeric-looking keywords as numbers;
        // string columns take the quoted form rather than failing.
        let source = json::parse(
            r#"{"@timestamp":"2023-08-22T13:30:00Z","file":"f","side":66}"#,
        )
        .unwrap();
        let row = project_row("f:2:0", &source).unwrap();
        assert!(row.contains("\"side\":\"66\""));
    }

    #[test]
    fn bad_shapes_are_errors_not_garbage_rows() {
        // Missing timestamp: the row cannot be placed on the timeline.
        let no_ts = json::parse(r#"{"file":"f","msg_type":"add_order"}"#).unwrap();
        assert!(project_row("f:3:0", &no_ts)
            .unwrap_err()
            .contains("@timestamp"));

        // A string in a numeric column is a document bug worth stopping for.
        let str_price = json::parse(
            r#"{"@timestamp":"2023-08-22T13:30:00Z","file":"f","price":"12.5"}"#,
        )
        .unwrap();
        assert!(project_row("f:4:0", &str_price).unwrap_err().contains("price"));

        // An object in a string column too.
        let obj_file = json::parse(
            r#"{"@timestamp":"2023-08-22T13:30:00Z","file":{"a":1}}"#,
        )
        .unwrap();
        assert!(project_row("f:5:0", &obj_file).unwrap_err().contains("file"));
    }

    #[test]
    fn numbers_stay_exact_through_projection() {
        let source = json::parse(
            r#"{"@timestamp":"2023-08-22T13:30:00Z","file":"f","order_id":4242424242424242}"#,
        )
        .unwrap();
        let row = project_row("f:6:0", &source).unwrap();
        assert!(row.contains("\"order_id\":4242424242424242"));
    }
}
