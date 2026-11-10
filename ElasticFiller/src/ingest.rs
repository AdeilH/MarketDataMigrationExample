//! Message ingestion: turn decoded XDP messages into Elasticsearch
//! documents, one per message, in feed order ("stored linearly").
//!
//! The document shape is deliberately flat — one object per message with
//! the common envelope (`@timestamp`, source file, message type) plus the
//! variant's own fields, with the symbol name joined in from the
//! in-memory table ([`crate`]'s populate pass / Redis). Flat beats nested
//! here: Kibana Discover columns, aggregations, and field mappings all
//! stay trivial.
//!
//! Prices are stored as the raw integer numerators the feed carries (see
//! the xdp module docs); scaling by `price_scale_code` is left to the
//! analysis layer, which knows the per-symbol scale.

use crate::feeds::xdp;

/// Fraction-of-second resolution of the pcap magic currently being read:
/// microseconds in classic files, nanoseconds in modern ones.
#[derive(Clone, Copy, PartialEq)]
pub enum TsUnit {
    Microseconds,
    Nanoseconds,
}

/// Capture timestamp as an ISO-8601 UTC string Kibana parses natively
/// (`2023-08-22T13:30:00.123456789Z`), from the pcap record header.
pub fn format_timestamp(ts_sec: u32, ts_frac: u32, unit: TsUnit) -> String {
    let days = (ts_sec / 86_400) as i64;
    let sod = ts_sec % 86_400;
    let (y, m, d) = civil_from_days(days);
    let ns = match unit {
        TsUnit::Microseconds => u64::from(ts_frac) * 1_000,
        TsUnit::Nanoseconds => u64::from(ts_frac),
    };
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{ns:09}Z",
        sod / 3_600,
        sod % 3_600 / 60,
        sod % 60
    )
}

/// Days since the Unix epoch to (year, month, day) — Howard Hinnant's
/// `civil_from_days`, so the tool needs no date library.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Escape the two characters that matter inside JSON strings. All fields
/// this tool emits come from the feed's ASCII parser, so this is a
/// belt-and-braces guard, not a full JSON escaper.
fn esc(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

/// The message types, as short lowercase names for documents and filters.
pub fn type_name(msg: &xdp::Message<'_>) -> &'static str {
    match msg {
        xdp::Message::SequenceNumberReset { .. } => "sequence_number_reset",
        xdp::Message::SourceTimeReference { .. } => "source_time_reference",
        xdp::Message::SymbolIndexMapping { .. } => "symbol_index_mapping",
        xdp::Message::SecurityStatus { .. } => "security_status",
        xdp::Message::RefreshHeader { .. } => "refresh_header",
        xdp::Message::AddOrder { .. } => "add_order",
        xdp::Message::ModifyOrder { .. } => "modify_order",
        xdp::Message::DeleteOrder { .. } => "delete_order",
        xdp::Message::Execution { .. } => "execution",
        xdp::Message::ReplaceOrder { .. } => "replace_order",
        xdp::Message::Imbalance { .. } => "imbalance",
        xdp::Message::AddOrderRefresh { .. } => "add_order_refresh",
        xdp::Message::NonDisplayedTrade { .. } => "non_displayed_trade",
        xdp::Message::Other { .. } => "other",
    }
}

/// One JSON document per non-symbol message; `None` for the messages this
/// pipeline excludes (`SymbolIndexMapping` — the symbol pass already
/// stores those in Redis).
pub fn message_json(
    msg: &xdp::Message<'_>,
    ts: &str,
    file: &str,
    symbol_of: impl Fn(u32) -> Option<String>,
) -> Option<String> {
    if matches!(msg, xdp::Message::SymbolIndexMapping { .. }) {
        return None;
    }
    let type_name = type_name(msg);
    let type_id = msg.msg_type();

    // Per-variant payload; every arm sets these the same way so the
    // envelope below stays written once. (`sym_index`, not `symbol_index`:
    // the match patterns bind the messages' own field with that name.)
    let mut extra = String::new();
    let mut sym_index = None;
    match msg {
        xdp::Message::SequenceNumberReset { source_time, source_time_ns, product_id, channel_id } => {
            extra = format!(
                r#","source_time":{source_time},"source_time_ns":{source_time_ns},"product_id":{product_id},"channel_id":{channel_id}"#
            );
        }
        xdp::Message::SourceTimeReference { id, source_time } => {
            extra = format!(r#","partition_id":{id},"source_time":{source_time}"#);
        }
        xdp::Message::SecurityStatus { source_time, source_time_ns, symbol_index, symbol_seq_num, security_status, halt_condition } => {
            sym_index = Some(*symbol_index);
            extra = format!(
                r#","source_time":{source_time},"source_time_ns":{source_time_ns},"symbol_seq_num":{symbol_seq_num},"security_status":"{}","halt_condition":"{}""#,
                *security_status as char,
                *halt_condition as char
            );
        }
        xdp::Message::RefreshHeader { current_refresh_pkt, total_refresh_pkts, last_seq_num, last_symbol_seq_num } => {
            extra = format!(
                r#","current_refresh_pkt":{current_refresh_pkt},"total_refresh_pkts":{total_refresh_pkts},"last_seq_num":{},"last_symbol_seq_num":{}"#,
                opt_num(*last_seq_num),
                opt_num(*last_symbol_seq_num)
            );
        }
        xdp::Message::AddOrder { source_time_ns, symbol_index, symbol_seq_num, order_id, price, volume, side, firm_id } => {
            sym_index = Some(*symbol_index);
            extra = format!(
                r#","source_time_ns":{source_time_ns},"symbol_seq_num":{symbol_seq_num},"order_id":{order_id},"price":{price},"volume":{volume},"side":"{}","firm_id":"{}""#,
                *side as char,
                esc(firm_id)
            );
        }
        xdp::Message::ModifyOrder { source_time_ns, symbol_index, symbol_seq_num, order_id, price, volume, position_change, side } => {
            sym_index = Some(*symbol_index);
            extra = format!(
                r#","source_time_ns":{source_time_ns},"symbol_seq_num":{symbol_seq_num},"order_id":{order_id},"price":{price},"volume":{volume},"position_change":{position_change},"side":"{}""#,
                *side as char
            );
        }
        xdp::Message::DeleteOrder { source_time_ns, symbol_index, symbol_seq_num, order_id } => {
            sym_index = Some(*symbol_index);
            extra = format!(
                r#","source_time_ns":{source_time_ns},"symbol_seq_num":{symbol_seq_num},"order_id":{order_id}"#
            );
        }
        xdp::Message::Execution { source_time_ns, symbol_index, symbol_seq_num, order_id, trade_id, price, volume, printable, trade_conditions } => {
            sym_index = Some(*symbol_index);
            extra = format!(
                r#","source_time_ns":{source_time_ns},"symbol_seq_num":{symbol_seq_num},"order_id":{order_id},"trade_id":{trade_id},"price":{price},"volume":{volume},"printable":{printable},"trade_conditions":"{}""#,
                esc(&String::from_utf8_lossy(trade_conditions))
            );
        }
        xdp::Message::ReplaceOrder { source_time_ns, symbol_index, symbol_seq_num, order_id, new_order_id, price, volume, side } => {
            sym_index = Some(*symbol_index);
            extra = format!(
                r#","source_time_ns":{source_time_ns},"symbol_seq_num":{symbol_seq_num},"order_id":{order_id},"new_order_id":{new_order_id},"price":{price},"volume":{volume},"side":"{}""#,
                *side as char
            );
        }
        xdp::Message::Imbalance { source_time, source_time_ns, symbol_index, symbol_seq_num, reference_price, paired_qty } => {
            sym_index = Some(*symbol_index);
            extra = format!(
                r#","source_time":{source_time},"source_time_ns":{source_time_ns},"symbol_seq_num":{symbol_seq_num},"reference_price":{reference_price},"paired_qty":{paired_qty}"#
            );
        }
        xdp::Message::AddOrderRefresh { source_time, source_time_ns, symbol_index, symbol_seq_num, order_id, price, volume, side, firm_id } => {
            sym_index = Some(*symbol_index);
            extra = format!(
                r#","source_time":{source_time},"source_time_ns":{source_time_ns},"symbol_seq_num":{symbol_seq_num},"order_id":{order_id},"price":{price},"volume":{volume},"side":"{}","firm_id":"{}""#,
                *side as char,
                esc(firm_id)
            );
        }
        xdp::Message::NonDisplayedTrade { source_time_ns, symbol_index, symbol_seq_num, trade_id, price, volume, printable, trade_conditions } => {
            sym_index = Some(*symbol_index);
            extra = format!(
                r#","source_time_ns":{source_time_ns},"symbol_seq_num":{symbol_seq_num},"trade_id":{trade_id},"price":{price},"volume":{volume},"printable":{printable},"trade_conditions":"{}""#,
                esc(&String::from_utf8_lossy(trade_conditions))
            );
        }
        xdp::Message::Other { .. } => {}
        xdp::Message::SymbolIndexMapping { .. } => unreachable!("excluded above"),
    }

    let symbol_field = match sym_index {
        Some(i) => match symbol_of(i) {
            Some(s) => format!(r#","symbol_index":{i},"symbol":"{}""#, esc(&s)),
            None => format!(r#","symbol_index":{i}"#),
        },
        None => String::new(),
    };

    Some(format!(
        r#"{{"@timestamp":"{ts}","file":"{}","msg_type":"{type_name}","msg_type_id":{type_id}{symbol_field}{extra}}}"#,
        esc(file)
    ))
}

/// `Option<u32>` as a JSON number or `null` (refresh-header sequence
/// numbers are absent on some channels).
fn opt_num(v: Option<u32>) -> String {
    v.map_or_else(|| "null".to_string(), |n| n.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_formats_micro_and_nano() {
        // 2023-08-22 13:30:00 UTC.
        let sec: u32 = 1_692_711_000;
        assert_eq!(
            format_timestamp(sec, 123_456_789, TsUnit::Nanoseconds),
            "2023-08-22T13:30:00.123456789Z"
        );
        assert_eq!(
            format_timestamp(sec, 123_456, TsUnit::Microseconds),
            "2023-08-22T13:30:00.123456000Z"
        );
    }

    #[test]
    fn timestamp_leap_day() {
        // 2024-02-29 00:00:00 UTC (leap year, civil_from_days edge).
        let sec: u32 = 1_709_164_800;
        assert_eq!(
            format_timestamp(sec, 0, TsUnit::Nanoseconds),
            "2024-02-29T00:00:00.000000000Z"
        );
    }

    fn sym(i: u32) -> Option<String> {
        match i {
            100 => Some("WSM".to_string()),
            _ => None,
        }
    }

    #[test]
    fn add_order_doc_has_envelope_and_payload() {
        let msg = xdp::Message::AddOrder {
            source_time_ns: 500,
            symbol_index: 100,
            symbol_seq_num: 7,
            order_id: 4242,
            price: 123_456,
            volume: 300,
            side: b'B',
            firm_id: "MPI",
        };
        let doc = message_json(&msg, "2023-08-22T13:30:00.000000500Z", "cap.pcap.zst", sym)
            .unwrap();
        assert!(doc.contains(r#""@timestamp":"2023-08-22T13:30:00.000000500Z""#));
        assert!(doc.contains(r#""file":"cap.pcap.zst""#));
        assert!(doc.contains(r#""msg_type":"add_order""#));
        assert!(doc.contains(r#""msg_type_id":"#));
        assert!(doc.contains(r#""symbol_index":100,"symbol":"WSM""#));
        assert!(doc.contains(r#""order_id":4242"#));
        assert!(doc.contains(r#""price":123456,"volume":300,"side":"B","firm_id":"MPI""#));
    }

    #[test]
    fn symbol_mapping_is_excluded_and_unknown_index_omits_symbol() {
        let mapping = xdp::Message::SymbolIndexMapping {
            symbol_index: 100,
            symbol: "WSM",
            market_id: 9,
            system_id: 1,
            exchange_code: b'A',
            price_scale_code: 4,
            security_type: b'C',
            lot_size: 100,
            prev_close_price: 0,
            prev_close_volume: 0,
        };
        assert!(message_json(&mapping, "t", "f", sym).is_none());

        let del = xdp::Message::DeleteOrder {
            source_time_ns: 1,
            symbol_index: 999, // not in the table
            symbol_seq_num: 2,
            order_id: 3,
        };
        let doc = message_json(&del, "t", "f", sym).unwrap();
        assert!(doc.contains(r#""symbol_index":999"#));
        assert!(!doc.contains(r#""symbol":"#));
    }
}
