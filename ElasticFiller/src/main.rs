//! elastic-filler: build the XDP symbol index table in parallel.
//!
//! A market-data consumer (e.g. an order-book builder) needs the
//! `symbol_index -> symbol` mapping before it can interpret any AddOrder,
//! Execution, ... message, because the later messages reference symbols by
//! index only. The mappings are broadcast on the feed itself
//! ([`SymbolIndexMapping`]), so the table can be reconstructed from any set
//! of captures that contains them.
//!
//! This tool does exactly that pass, and nothing else: every `.zst` capture
//! in a directory is decompressed and dissected in parallel (one scoped
//! thread per worker, work handed out file-by-file through an atomic
//! cursor), and the only events looked at are the symbol mappings.
//!
//! Usage:
//!
//! ```text
//! elastic-filler [--populate-symbols] [--store-messages] [--threads N]
//!                [--redis-host H] [--redis-port P]
//!                [--es-host H] [--es-port P] [--es-index NAME]
//!                [--es-user U] [--es-pass P] [--max-files N]
//!                [data-dir-or-file]
//! ```
//!
//! The data directory defaults to `./data` (where `make data` downloads
//! and extracts the Databento samples). Files are processed by all cores;
//! each worker owns its own feed parser (parsers are stateful). Every file
//! builds its own symbol map, and the maps are merged in the main thread
//! in sorted file order — deterministic across runs, unlike worker
//! completion order. A re-mapping of an index to a different symbol is
//! reported as a conflict rather than silently overwritten.
//!
//! With `--populate-symbols` (the pre-market warm-up run) the merged table
//! is uploaded to Redis instead of printed: the staging hash
//! `xdp:symbols:incoming` is filled with chunked HSETs, then atomically
//! RENAMEd over `xdp:symbols`, so a consumer polling the live key sees
//! either the previous table or the complete new one — never a partial
//! upload. Point the connection at the `docker compose up -d` Redis with
//! `--redis-host`/`--redis-port` (default `127.0.0.1:6379`).
//!
//! With `--store-messages` (the replay pass) the tool first pulls the
//! whole table into memory with one `HGETALL`, then walks the captures
//! with a pool of workers — one Elasticsearch connection per worker,
//! files handed out from the shared queue in chronological order —
//! posting every non-symbol message as one flat document (`_bulk`,
//! NDJSON), enriched with the symbol name from that table. Document ids
//! are deterministic per file (`file:packet:n`), so arrival order across
//! workers is irrelevant: Kibana sorts by `@timestamp`, and re-running
//! any subset of files overwrites exactly its own documents.
//!
//! [`SymbolIndexMapping`]: pcap_parser::feeds::xdp::Message::SymbolIndexMapping

#![forbid(unsafe_code)]

mod es;
mod ingest;
mod redis;

use self::es::Es;
use self::redis::Reply;
use pcap_parser::pcap::PcapMagic;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::thread;
use std::time::Instant;

use pcap_parser::feeds::xdp;
use pcap_parser::feeds::{self, Event};
use pcap_parser::pcap::GlobalHeader;
use pcap_parser::zstd::frame;

/// Feed parsed by this tool. The symbol table is an XDP concept; extend
/// [`PARSER_NAMES`] and this match if another feed ever needs the same pass.
const FEED: &str = "xdp";

/// Redis hash holding the live symbol table (field: index, value: symbol).
const SYMBOLS_KEY: &str = "xdp:symbols";
/// Staging hash the table is built in before the atomic RENAME swap.
const SYMBOLS_STAGING_KEY: &str = "xdp:symbols:incoming";
/// Freshness metadata for whoever checks the table before market.
const META_KEY: &str = "xdp:symbols:meta";
/// Field-value pairs per HSET command. The protocol allows far more; this
/// keeps each command small and a stuck upload observable in chunks.
const HSET_CHUNK: usize = 500;

/// Elasticsearch index template name (covers `<index>*`).
const ES_TEMPLATE: &str = "xdp-messages";
/// Bulk request thresholds: flush after this many documents or bytes,
/// whichever comes first. ~4 MB keeps individual requests comfortably
/// inside Elasticsearch's `http.max_content_length` (100 MB default).
const BULK_MAX_DOCS: usize = 5_000;
const BULK_MAX_BYTES: usize = 4 * 1024 * 1024;

/// One file's contribution to the table: the symbol mappings found in that
/// capture and its packet count. Every file builds its own map; the merge
/// happens once, in the main thread, in sorted file order.
#[derive(Debug, Default)]
struct FileSymbols {
    symbols: HashMap<u32, String>,
    packets: u64,
}

/// Shared work queue: an atomic cursor over the file list, plus the count
/// of files popped but not yet finished (for the progress line). Capture
/// sizes vary wildly (a few MB overnight vs. ~80 MB at the market open),
/// so hand-out has to be dynamic: whoever finishes first claims the next
/// file — no fixed chunks, no worker ever waits for another to catch up.
struct Queue<'a> {
    files: &'a [PathBuf],
    next: AtomicUsize,
    /// Files currently being processed (popped, not yet completed).
    in_flight: AtomicUsize,
}

impl<'a> Queue<'a> {
    fn new(files: &'a [PathBuf]) -> Self {
        Queue {
            files,
            next: AtomicUsize::new(0),
            in_flight: AtomicUsize::new(0),
        }
    }

    /// Take the next unclaimed file (marking it in flight), or `None` when
    /// the directory is done.
    fn pop(&self) -> Option<&'a PathBuf> {
        let file = self.files.get(self.next.fetch_add(1, Ordering::Relaxed))?;
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        Some(file)
    }
}

/// Progress bar: files completed so far (atomic, bumped by every worker).
static DONE: AtomicUsize = AtomicUsize::new(0);

/// Read + decompress + dissect one capture, keeping only symbol mappings.
fn scan_file(path: &Path) -> Result<FileSymbols, String> {
    let raw = std::fs::read(path).map_err(|e| format!("cannot read: {e}"))?;
    let decompressed =
        frame::decompress(&raw).map_err(|e| format!("zstd decompression failed: {e}"))?;
    let header = GlobalHeader::parse(&decompressed)
        .map_err(|e| format!("invalid pcap global header: {e}"))?;

    let mut parser = feeds
        ::parser_by_name(FEED)
        .ok_or_else(|| format!("unknown feed `{FEED}` (available: {})", feeds::PARSER_NAMES.join(", ")))?;
    parser.begin(&header);

    let mut symbols = HashMap::new();
    let mut packets = 0u64;
    for record in header.records(&decompressed) {
        let rec = record.map_err(|e| format!("bad pcap record: {e}"))?;
        packets += 1;
        let events = parser
            .parse_packet(rec.data)
            .map_err(|e| format!("packet {}: {} failed: {e}", rec.header.ts_sec, parser.name()))?;
        for event in events {
            // Heartbeats carry no symbols; skip them.
            let Event::Xdp(msg) = event else { continue };
            if let xdp::Message::SymbolIndexMapping { symbol_index, symbol, .. } = msg {
                symbols.insert(symbol_index, symbol.to_string());
            }
        }
    }
    Ok(FileSymbols { symbols, packets })
}

/// Merge one file's symbols into the combined table, flagging a re-mapped
/// index (same index, different symbol) instead of silently overwriting.
fn merge_symbols(combined: &mut HashMap<u32, String>, new: HashMap<u32, String>) {
    for (index, symbol) in new {
        match combined.entry(index) {
            Entry::Occupied(prev) if *prev.get() != symbol => {
                eprintln!("warning: symbol index {index} maps to both {:?} and {:?}", prev.get(), symbol);
            }
            _ => {
                combined.insert(index, symbol);
            }
        }
    }
}

/// Upload the merged table to Redis (the `--populate-symbols` pass).
///
/// The table is built in a staging hash and swapped in with an atomic
/// RENAME, so a consumer reading `xdp:symbols` sees either the previous
/// table or the complete new one — never a half-uploaded hash. Any error
/// mid-upload leaves the live key untouched (only a stale staging key
/// remains, which the next run clears first).
fn populate_redis(
    combined: &HashMap<u32, String>,
    host: &str,
    port: u16,
) -> Result<(), String> {
    let mut conn = redis::Redis::connect(host, port).map_err(|e| {
        format!("cannot connect to redis://{host}:{port}: {e} — is the container up? (`docker compose up -d`)")
    })?;
    match conn.command(&["PING"])? {
        Reply::Simple(s) if s == "PONG" => {}
        _ => return Err(format!("unexpected PING reply from redis://{host}:{port}")),
    }

    // Fresh staging area (clears junk from a crashed previous run).
    conn.command(&["DEL", SYMBOLS_STAGING_KEY])?;
    // Byte-sorted pairs: uploads are reproducible and diffable run to run.
    let mut pairs: Vec<(String, String)> =
        combined.iter().map(|(k, v)| (k.to_string(), v.clone())).collect();
    pairs.sort_unstable();
    for chunk in pairs.chunks(HSET_CHUNK) {
        let mut args = vec!["HSET", SYMBOLS_STAGING_KEY];
        args.extend(chunk.iter().flat_map(|(k, v)| [k.as_str(), v.as_str()]));
        conn.command(&args)?; // :N — number of new fields
    }
    // Atomic swap: readers of SYMBOLS_KEY go old-table-or-new-table.
    conn.command(&["RENAME", SYMBOLS_STAGING_KEY, SYMBOLS_KEY])?;

    // Freshness metadata for whoever checks the table before market.
    let populated_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    conn.command(&[
        "HSET",
        META_KEY,
        "symbols",
        &combined.len().to_string(),
        "populated_at",
        &populated_at.to_string(),
    ])?;
    Ok(())
}

/// Advisory pre-flight check for non-populate runs: warn up front (before
/// the scan) when Redis has no symbol table to consume, so an empty table
/// is discovered here rather than at game time. Never fatal — the run
/// itself doesn't need Redis; the warning goes to stderr.
fn warn_if_redis_empty(host: &str, port: u16) {
    let mut conn = match redis::Redis::connect(host, port) {
        Ok(conn) => conn,
        Err(e) => {
            eprintln!(
                "warning: cannot reach redis://{host}:{port}: {e} — cannot verify the symbol table; is the container up? (`docker compose up -d`)"
            );
            return;
        }
    };
    match conn.command(&["EXISTS", SYMBOLS_KEY]) {
        Ok(Reply::Integer(0)) => eprintln!(
            "warning: Redis at redis://{host}:{port} is empty — no symbol table (`{SYMBOLS_KEY}`). Populate it first: cargo run --release -- --populate-symbols"
        ),
        Ok(Reply::Integer(_)) => {} // table present, nothing to say
        Ok(_) => eprintln!(
            "warning: unexpected EXISTS reply from redis://{host}:{port} — cannot verify the symbol table"
        ),
        Err(e) => eprintln!(
            "warning: redis://{host}:{port} query failed: {e} — cannot verify the symbol table"
        ),
    }
}

/// Pull the whole symbol table into process memory with one HGETALL — the
/// first step of every `--store-messages` run (messages are enriched with
/// symbol names from this table).
fn load_symbols_from_redis(host: &str, port: u16) -> Result<HashMap<u32, String>, String> {
    let mut conn = redis::Redis::connect(host, port).map_err(|e| {
        format!("cannot connect to redis://{host}:{port}: {e} — is the container up? (`docker compose up -d`)")
    })?;
    let reply = conn
        .command(&["HGETALL", SYMBOLS_KEY])
        .map_err(|e| format!("HGETALL {SYMBOLS_KEY} failed: {e}"))?;
    let Reply::Array(items) = reply else {
        return Err(format!("unexpected HGETALL reply from redis://{host}:{port}"));
    };
    if items.is_empty() {
        return Err(format!(
            "`{SYMBOLS_KEY}` in Redis is empty — populate the symbol table first: cargo run --release -- --populate-symbols"
        ));
    }
    let mut symbols = HashMap::with_capacity(items.len() / 2);
    let mut pairs = items.into_iter();
    while let (Some(field), Some(value)) = (pairs.next(), pairs.next()) {
        let (Reply::Bulk(field), Reply::Bulk(value)) = (field, value) else {
            return Err("HGETALL reply contained non-bulk entries".to_string());
        };
        let index: u32 = std::str::from_utf8(&field)
            .ok()
            .and_then(|f| f.parse().ok())
            .ok_or_else(|| format!("non-numeric symbol index `{}` in Redis", String::from_utf8_lossy(&field)))?;
        symbols.insert(index, String::from_utf8_lossy(&value).into_owned());
    }
    Ok(symbols)
}

/// Elasticsearch health ping: `GET /` must answer 200 with cluster info.
fn ping_es(es: &mut Es) -> Result<(), String> {
    let (code, body) = es
        .request("GET", "/", "application/json", b"")
        .map_err(|e| format!("elasticsearch request failed: {e}"))?;
    if code == 200 && body.contains("cluster_name") {
        Ok(())
    } else {
        Err(format!(
            "elasticsearch health check returned HTTP {code}: {}",
            body.chars().take(200).collect::<String>()
        ))
    }
}

/// One-time index template: single shard / no replicas (single-node dev
/// cluster) and explicit types for the fields Kibana filters on;
/// everything else is dynamically mapped on first document.
fn ensure_index_template(es: &mut Es, index: &str) -> Result<(), String> {
    let tpl = format!(
        r#"{{"index_patterns":["{index}*"],"template":{{"settings":{{"number_of_shards":1,"number_of_replicas":0,"refresh_interval":"5s"}},"mappings":{{"properties":{{"@timestamp":{{"type":"date"}},"file":{{"type":"keyword"}},"msg_type":{{"type":"keyword"}},"msg_type_id":{{"type":"integer"}},"side":{{"type":"keyword"}},"symbol":{{"type":"keyword"}},"firm_id":{{"type":"keyword"}},"trade_conditions":{{"type":"keyword"}}}}}}}}}}"#
    );
    let (code, body) = es
        .request("PUT", &format!("/_index_template/{ES_TEMPLATE}"), "application/json", tpl.as_bytes())
        .map_err(|e| format!("index template request failed: {e}"))?;
    if code == 200 {
        Ok(())
    } else {
        Err(format!(
            "index template PUT returned HTTP {code}: {}",
            body.chars().take(200).collect::<String>()
        ))
    }
}

/// Send one bulk request: NDJSON `index` action per buffered document.
/// Each document carries a deterministic `_id` (`file:packet:n`), so
/// re-running the same capture set overwrites instead of duplicating —
/// a rerun after a crash (or a transparent reconnect-resend in
/// [`es`]) is always safe.
///
/// A per-item `errors:true` response is fatal here — a silently partial
/// linear store is worse than a stopped one.
fn flush_bulk(es: &mut Es, index: &str, docs: &mut Vec<(String, String)>) -> Result<(), String> {
    if docs.is_empty() {
        return Ok(());
    }
    let mut body = String::with_capacity(
        docs.iter().map(|(id, doc)| id.len() + doc.len()).sum::<usize>() + docs.len() * 64,
    );
    for (id, doc) in docs.drain(..) {
        body.push_str(&format!("{{\"index\":{{\"_index\":\"{index}\",\"_id\":\"{id}\"}}}}\n{doc}\n"));
    }
    let (code, resp) = es
        .request("POST", "/_bulk", "application/x-ndjson", body.as_bytes())
        .map_err(|e| format!("bulk request failed: {e}"))?;
    if code != 200 {
        return Err(format!(
            "bulk request returned HTTP {code}: {}",
            resp.chars().take(300).collect::<String>()
        ));
    }
    // Top-level response key: `"errors":true` means some items failed even
    // though the request as a whole was accepted.
    if resp.contains("\"errors\":true") {
        return Err(format!(
            "bulk response reported per-item errors: {}",
            resp.chars().take(300).collect::<String>()
        ));
    }
    Ok(())
}

/// The `--store-messages` pass: Redis table -> memory, then every
/// non-symbol message into Elasticsearch.
///
/// Worker threads pull files off the shared queue (chronological start
/// order) and each owns an ES connection plus a bulk buffer — the ES
/// round trip is the bottleneck, so parallel connections are the lever.
/// Ordering is safe by construction: ids are derived per file
/// (`file:packet:n`), so nothing depends on which worker finished what.
fn store_messages_mode(
    data_dir: &str,
    redis_host: &str,
    redis_port: u16,
    es_host: &str,
    es_port: u16,
    es_index: &str,
    es_user: &str,
    es_pass: &str,
    max_files: Option<usize>,
    threads_override: Option<usize>,
) -> ExitCode {
    // 1. Symbols into memory (the reason the populate pass runs first).
    let symbols = match load_symbols_from_redis(redis_host, redis_port) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::FAILURE;
        }
    };
    println!("loaded {} symbols from redis://{redis_host}:{redis_port} ({SYMBOLS_KEY})", symbols.len());

    // 2. Elasticsearch session, health, mapping.
    let mut es = match es::Es::connect(es_host, es_port, es_user, es_pass) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: cannot connect to http://{es_host}:{es_port}: {e}");
            return ExitCode::FAILURE;
        }
    };
    if let Err(e) = ping_es(&mut es) {
        eprintln!("error: {e} — check the URL and credentials (--es-user/--es-pass)");
        return ExitCode::FAILURE;
    }
    if let Err(e) = ensure_index_template(&mut es, es_index) {
        eprintln!("error: {e}");
        return ExitCode::FAILURE;
    }

    // 3. Captures: a single file or a directory, chronological order.
    //    (Name order == time order for these timestamped capture names.)
    let path = Path::new(data_dir);
    let mut files: Vec<PathBuf> = if path.is_file() {
        vec![path.to_path_buf()]
    } else {
        match collect_zst(path) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("error: cannot list {data_dir}: {e}");
                return ExitCode::FAILURE;
            }
        }
    };
    if let Some(n) = max_files {
        files.truncate(n);
    }
    if files.is_empty() {
        eprintln!("error: no .zst captures found for {data_dir}");
        return ExitCode::FAILURE;
    }

    let threads = threads_override
        .or_else(|| thread::available_parallelism().ok().map(|n| n.get()))
        .unwrap_or(1)
        .min(files.len())
        .max(1);
    println!(
        "storing messages from {} file(s) -> http://{es_host}:{es_port}, index `{es_index}`, {} worker(s)",
        files.len(),
        threads
    );

    let start = Instant::now();
    let queue = Queue::new(&files);
    let docs_total = AtomicU64::new(0);
    let bytes_total = AtomicU64::new(0);
    let packets_total = AtomicU64::new(0);
    let files_done = AtomicUsize::new(0);
    let per_worker: Mutex<Vec<u64>> = Mutex::new(vec![0; threads]);
    let failures: Mutex<Vec<String>> = Mutex::new(Vec::new());

    thread::scope(|scope| {
        for worker in 0..threads {
            // Shared state enters each worker as a copy of a shared
            // reference; nothing is consumed by the first spawn.
            let queue = &queue;
            let symbols = &symbols;
            let docs_total = &docs_total;
            let bytes_total = &bytes_total;
            let packets_total = &packets_total;
            let files_done = &files_done;
            let per_worker = &per_worker;
            let failures = &failures;
            let files = &files;
            scope.spawn(move || {
                // One ES connection per worker: bulk posts are the
                // bottleneck, parallel connections are the whole point.
                let mut es = match es::Es::connect(es_host, es_port, es_user, es_pass) {
                    Ok(c) => c,
                    Err(e) => {
                        failures.lock().unwrap().push(format!(
                            "worker {worker}: cannot connect to http://{es_host}:{es_port}: {e}"
                        ));
                        return;
                    }
                };
                let mut bulk: Vec<(String, String)> = Vec::with_capacity(BULK_MAX_DOCS);
                let mut bulk_bytes = 0usize;
                while let Some(path) = queue.pop() {
                    let name = path.display().to_string();
                    let result = ingest_file(
                        path, &name, symbols, &mut es, es_index, &mut bulk, &mut bulk_bytes,
                    );
                    match result {
                        Ok((packets, docs, bytes)) => {
                            packets_total.fetch_add(packets, Ordering::Relaxed);
                            docs_total.fetch_add(docs, Ordering::Relaxed);
                            bytes_total.fetch_add(bytes, Ordering::Relaxed);
                            per_worker.lock().unwrap()[worker] += 1;
                            let done = files_done.fetch_add(1, Ordering::Relaxed) + 1;
                            eprint!(
                                "\r{}/{} files, {} msgs, {} workers",
                                done,
                                files.len(),
                                docs_total.load(Ordering::Relaxed),
                                threads
                            );
                        }
                        Err(e) => {
                            // This worker stops pulling files; the rest
                            // keep going. A rerun repairs the gap (ids).
                            failures.lock().unwrap().push(format!("{name}: {e}"));
                            break;
                        }
                    }
                }
                if let Err(e) = flush_bulk(&mut es, es_index, &mut bulk) {
                    failures.lock().unwrap().push(format!("worker {worker}: final flush: {e}"));
                }
            });
        }
    });
    eprintln!();

    let elapsed = start.elapsed();
    let docs = docs_total.load(Ordering::Relaxed);
    let failures = failures.into_inner().unwrap();
    for f in &failures {
        eprintln!("error: {f}");
    }
    println!(
        "{} messages ({:.0} bytes/doc) from {} packets in {:.1?} ({:.0} msgs/s, {} workers); worker files: {:?}; indexed into `{es_index}`",
        docs,
        bytes_total.load(Ordering::Relaxed) as f64 / docs.max(1) as f64,
        packets_total.load(Ordering::Relaxed),
        elapsed,
        docs as f64 / elapsed.as_secs_f64().max(1e-9),
        threads,
        per_worker.into_inner().unwrap(),
    );

    if failures.is_empty() {
        ExitCode::SUCCESS
    } else {
        eprintln!("error: {} file(s)/worker(s) failed — rerun the same command to repair", failures.len());
        ExitCode::FAILURE
    }
}

/// Decompress + dissect one capture, posting message documents through the
/// worker's bulk buffer (flushed at the bulk thresholds inside).
///
/// Packet numbering restarts per file on purpose: the document id
/// `{file}:{packet_in_file}:{n}` is then deterministic per file,
/// independent of which worker ingested it or when.
fn ingest_file(
    path: &Path,
    name: &str,
    symbols: &HashMap<u32, String>,
    es: &mut Es,
    es_index: &str,
    bulk: &mut Vec<(String, String)>,
    bulk_bytes: &mut usize,
) -> Result<(u64, u64, u64), String> {
    let raw = std::fs::read(path).map_err(|e| format!("cannot read: {e}"))?;
    let decompressed =
        frame::decompress(&raw).map_err(|e| format!("zstd decompression failed: {e}"))?;
    let header =
        GlobalHeader::parse(&decompressed).map_err(|e| format!("invalid pcap global header: {e}"))?;
    let unit = match header.magic {
        PcapMagic::NanosecondsLe | PcapMagic::NanosecondsBe => ingest::TsUnit::Nanoseconds,
        _ => ingest::TsUnit::Microseconds,
    };
    let mut parser = feeds
        ::parser_by_name(FEED)
        .ok_or_else(|| format!("unknown feed `{FEED}`"))?;
    parser.begin(&header);

    let mut packet_in_file = 0u64;
    let mut docs = 0u64;
    let mut bytes = 0u64;
    for record in header.records(&decompressed) {
        let rec = record.map_err(|e| format!("bad pcap record: {e}"))?;
        packet_in_file += 1;
        let events = parser
            .parse_packet(rec.data)
            .map_err(|e| format!("{} failed on a packet: {e}", parser.name()))?;
        let ts = ingest::format_timestamp(rec.header.ts_sec, rec.header.ts_frac, unit);
        // Position within the current packet's document stream — part of
        // the deterministic id below.
        let mut doc_in_packet = 0u64;
        for event in events {
            let Event::Xdp(msg) = event else { continue };
            // Heartbeats carry no message; symbol mappings live in Redis.
            // Everything else becomes one linear document.
            if let Some(doc) = ingest::message_json(&msg, &ts, name, |i| symbols.get(&i).cloned()) {
                doc_in_packet += 1;
                let id = format!("{name}:{packet_in_file}:{doc_in_packet}");
                let len = doc.len() as u64;
                *bulk_bytes += len as usize;
                bytes += len;
                bulk.push((id, doc));
                docs += 1;
                if bulk.len() >= BULK_MAX_DOCS || *bulk_bytes >= BULK_MAX_BYTES {
                    flush_bulk(es, es_index, bulk)?;
                    *bulk_bytes = 0;
                }
            }
        }
    }
    Ok((packet_in_file, docs, bytes))
}

/// Collect the `.zst` captures in `dir`, sorted (stable merge order and
/// reproducible output).
fn collect_zst(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_file() && path.extension().is_some_and(|e| e == "zst") {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

fn main() -> ExitCode {
    // args: [--populate-symbols] [--threads N] [--redis-host H]
    //       [--redis-port P] [data-dir]
    let mut data_dir = "data".to_string();
    let mut threads_override = None;
    let mut populate = false;
    let mut store = false;
    let mut redis_host = "127.0.0.1".to_string();
    let mut redis_port: u16 = 6379;
    let mut es_host = "127.0.0.1".to_string();
    let mut es_port: u16 = 9200;
    let mut es_index = "xdp-messages".to_string();
    let mut es_user = "elastic-admin".to_string();
    let mut es_pass = "elastic-password".to_string();
    let mut max_files: Option<usize> = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        // If/else rather than matching on `arg.as_str()` so the positional
        // arm can move `arg` without fighting the match borrow.
        if arg == "--populate-symbols" {
            populate = true;
        } else if arg == "--store-messages" {
            store = true;
        } else if arg == "--threads" {
            match args.next().and_then(|v| v.parse::<usize>().ok()) {
                Some(n) if n >= 1 => threads_override = Some(n),
                _ => {
                    eprintln!("error: --threads needs a positive number");
                    return ExitCode::FAILURE;
                }
            }
        } else if arg == "--redis-host" {
            match args.next() {
                Some(h) => redis_host = h,
                None => {
                    eprintln!("error: --redis-host needs a value");
                    return ExitCode::FAILURE;
                }
            }
        } else if arg == "--redis-port" {
            match args.next().and_then(|v| v.parse::<u16>().ok()) {
                Some(p) => redis_port = p,
                _ => {
                    eprintln!("error: --redis-port needs a port number");
                    return ExitCode::FAILURE;
                }
            }
        } else if arg == "--es-host" {
            match args.next() {
                Some(h) => es_host = h,
                None => {
                    eprintln!("error: --es-host needs a value");
                    return ExitCode::FAILURE;
                }
            }
        } else if arg == "--es-port" {
            match args.next().and_then(|v| v.parse::<u16>().ok()) {
                Some(p) => es_port = p,
                _ => {
                    eprintln!("error: --es-port needs a port number");
                    return ExitCode::FAILURE;
                }
            }
        } else if arg == "--es-index" {
            match args.next() {
                Some(s) => es_index = s,
                None => {
                    eprintln!("error: --es-index needs a value");
                    return ExitCode::FAILURE;
                }
            }
        } else if arg == "--es-user" {
            match args.next() {
                Some(s) => es_user = s,
                None => {
                    eprintln!("error: --es-user needs a value");
                    return ExitCode::FAILURE;
                }
            }
        } else if arg == "--es-pass" {
            match args.next() {
                Some(s) => es_pass = s,
                None => {
                    eprintln!("error: --es-pass needs a value");
                    return ExitCode::FAILURE;
                }
            }
        } else if arg == "--max-files" {
            match args.next().and_then(|v| v.parse::<usize>().ok()) {
                Some(n) if n >= 1 => max_files = Some(n),
                _ => {
                    eprintln!("error: --max-files needs a positive number");
                    return ExitCode::FAILURE;
                }
            }
        } else {
            data_dir = arg;
        }
    }

    if populate && store {
        eprintln!("error: pick one mode per run: --populate-symbols or --store-messages");
        return ExitCode::FAILURE;
    }

    // Advisory only for runs that merely look at Redis; populate is about
    // to fill the table and store does its own mandatory load.
    if !populate && !store {
        warn_if_redis_empty(&redis_host, redis_port);
    }

    if store {
        return store_messages_mode(
            &data_dir,
            &redis_host,
            redis_port,
            &es_host,
            es_port,
            &es_index,
            &es_user,
            &es_pass,
            max_files,
            threads_override,
        );
    }

    let mut files = match collect_zst(Path::new(&data_dir)) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("error: cannot list {data_dir}: {e}");
            return ExitCode::FAILURE;
        }
    };
    // Longest-first scheduling: put the big market-hours captures in flight
    // at t=0 and let small files fill the tail, instead of one ~80 MB
    // capture sitting at the end and single-handedly setting the critical
    // path. Compressed size tracks packet count, the dominant cost. The
    // merge is ordered by path, not by completion, so output is unchanged.
    files.sort_by_key(|p| std::cmp::Reverse(p.metadata().map_or(0, |m| m.len())));
    if files.is_empty() {
        eprintln!("error: no .zst captures in {data_dir}");
        return ExitCode::FAILURE;
    }

    let threads = threads_override
        .or_else(|| thread::available_parallelism().ok().map(|n| n.get()))
        .unwrap_or(1)
        .min(files.len())
        .max(1);
    println!(
        "populating XDP symbol table from {} file(s) in {data_dir}, {} thread(s){}",
        files.len(),
        threads,
        if populate {
            format!(" -> redis://{redis_host}:{redis_port}")
        } else {
            String::new()
        }
    );

    let start = Instant::now();
    let queue = Queue::new(&files);
    // Each file produces its own map (plus its error, if any); the merge
    // happens once here, after all workers finish.
    let results: Mutex<Vec<(PathBuf, Result<FileSymbols, String>)>> = Mutex::new(Vec::new());
    // Files completed per worker, printed at the end so the parallel spread
    // is visible rather than assumed.
    let per_worker: Mutex<Vec<usize>> = Mutex::new(vec![0; threads]);

    thread::scope(|scope| {
        for worker in 0..threads {
            // Shared state enters each worker as a copy of a shared
            // reference (the `move` moves those and `worker`, nothing
            // else), so no spawn consumes state another worker needs.
            let queue = &queue;
            let results = &results;
            let per_worker = &per_worker;
            let files = &files;
            let start = &start;
            scope.spawn(move || {
                while let Some(path) = queue.pop() {
                    // Failures stay per-file: one bad capture must not lose
                    // the mappings recovered from the rest.
                    let result =
                        scan_file(path).map_err(|e| format!("{}: {e}", path.display()));
                    results.lock().unwrap().push((path.to_path_buf(), result));
                    queue.in_flight.fetch_sub(1, Ordering::Relaxed);
                    per_worker.lock().unwrap()[worker] += 1;
                    let done = DONE.fetch_add(1, Ordering::Relaxed) + 1;
                    // The counter ticks per finished file: with captures
                    // ranging from ~3 MB to ~80 MB, a quiet line usually
                    // means big files are mid-decompress, not a stalled
                    // worker — the in-flight count and clock show the pool
                    // still grinding.
                    eprint!(
                        "\r{done}/{} files, {} in flight, {:>3}s",
                        files.len(),
                        queue.in_flight.load(Ordering::Relaxed),
                        start.elapsed().as_secs()
                    );
                }
            });
        }
    });
    eprintln!();

    let elapsed = start.elapsed();
    println!("worker files: {:?}", per_worker.into_inner().unwrap());
    // Merge in sorted file order: the capture names are timestamped, so
    // "first seen wins" means the earliest capture that carries the mapping
    // — deterministic across runs, unlike worker completion order.
    let mut results = results.into_inner().unwrap();
    results.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    let mut combined = HashMap::new();
    let mut packets = 0u64;
    let mut failures = 0usize;
    for (_, file) in results {
        match file {
            Ok(file) => {
                packets += file.packets;
                merge_symbols(&mut combined, file.symbols);
            }
            Err(e) => {
                failures += 1;
                eprintln!("error: {e}");
            }
        }
    }

    println!(
        "{} packets in {:.1?} ({:.0} files/s); {} unique symbol indices",
        packets,
        elapsed,
        files.len() as f64 / elapsed.as_secs_f64(),
        combined.len()
    );

    if populate {
        // A table built from a partial directory is not safe to publish:
        // refuse rather than let a pre-market run upload a truncated set.
        if failures > 0 {
            eprintln!("error: not uploading to Redis: {failures} file(s) failed");
            return ExitCode::FAILURE;
        }
        let upload_start = Instant::now();
        match populate_redis(&combined, &redis_host, redis_port) {
            Ok(()) => println!(
                "uploaded {} symbols to redis://{redis_host}:{redis_port} ({SYMBOLS_KEY}) in {:.1?}",
                combined.len(),
                upload_start.elapsed()
            ),
            Err(e) => {
                eprintln!("error: {e}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        let mut rows: Vec<_> = combined.into_iter().collect();
        rows.sort_unstable();
        for (index, symbol) in rows {
            println!("{index:>7}  {symbol}");
        }
    }

    if failures > 0 {
        eprintln!("error: {failures} file(s) failed");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_keeps_first_and_flags_conflicts() {
        let mut combined = HashMap::from([(1u32, "AAA".to_string()), (2, "BBB".to_string())]);
        // Same index + same symbol (a re-broadcast mapping): no-op, no warning path.
        merge_symbols(&mut combined, HashMap::from([(1, "AAA".to_string())]));
        assert_eq!(combined[&1], "AAA");
        // Same index + different symbol: merged value stays the first seen.
        merge_symbols(&mut combined, HashMap::from([(2, "ZZZ".to_string())]));
        assert_eq!(combined[&2], "BBB");
        // A genuinely new index lands in the table.
        merge_symbols(&mut combined, HashMap::from([(3, "CCC".to_string())]));
        assert_eq!(combined[&3], "CCC");
    }

    #[test]
    fn queue_hands_out_each_file_exactly_once() {
        let files: Vec<PathBuf> = ["a", "b", "c"].iter().map(PathBuf::from).collect();
        let queue = Queue::new(&files);
        let names: Vec<_> = std::iter::from_fn(|| queue.pop().map(|p| p.display().to_string())).collect();
        assert_eq!(names, ["a", "b", "c"]);
        assert!(queue.pop().is_none());
    }
}
