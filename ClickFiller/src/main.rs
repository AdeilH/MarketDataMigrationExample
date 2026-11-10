//! click-filler: move the XDP message index from Elasticsearch into
//! ClickHouse, as fast as the two servers allow.
//!
//! elastic-filler replays the captures into Elasticsearch (`xdp-messages`,
//! one flat document per message); this tool moves that index into
//! ClickHouse, where the analysis queries live. It is a pure mover: no
//! transformation beyond renaming `@timestamp` to `ts` and attaching the
//! document id — every value travels byte-exact (numbers keep their
//! original digits, timestamps keep their nanoseconds). The table and the
//! row projection share one column table ([`schema`]), and the
//! ReplacingMergeTree it generates makes reruns and restarts safe.
//!
//! Usage:
//!
//! ```text
//! click-filler [--es-host H] [--es-port P] [--es-index NAME]
//!              [--es-user U] [--es-pass P]
//!              [--ch-host H] [--ch-port P] [--ch-table NAME]
//!              [--ch-user U] [--ch-pass P]
//!              [--workers W] [--ranges R] [--fetch-page N]
//!              [--insert-batch N] [--report-secs N]
//!              [--checkpoint FILE] [--limit N]
//!              [--resume] [--verify] [--create-table-only]
//! ```
//!
//! # How the transfer runs
//!
//! One `date_histogram` over the index gives the exact document count
//! per minute bucket; the buckets are cut into R contiguous ranges of
//! near-equal document count (R auto: ~4 M docs per range, `--ranges`
//! overrides). Disjoint ranges cannot skip documents, and even overlap
//! would be absorbed by the ReplacingMergeTree.
//!
//! W worker threads (`--workers`, default 8) then drain the range queue,
//! each with its own Elasticsearch connection, its own point-in-time
//! snapshot and `search_after` walk sorted by `[@timestamp, _shard_doc]`
//! (sorting on `_id` is refused by Elasticsearch — fielddata is
//! disallowed and enabling it on a 60 M-document index would blow the
//! heap), and its own ClickHouse connection. While one worker inserts,
//! the others fetch: the ES read latency is what the parallelism hides.
//!
//! Rows are buffered per worker and inserted in big batches (~100k rows
//! / 32 MB) — many small parallel inserts would pile up thousands of
//! MergeTree parts per partition and throttle ClickHouse.
//!
//! The checkpoint file holds every range's walk position and is
//! rewritten after each inserted batch; `--resume` continues exactly the
//! unfinished ranges (seeking one millisecond before each boundary —
//! `_shard_doc` values are segment-relative and meaningless across
//! snapshots, but the timestamp is stable, and the refetched boundary
//! collapses in ClickHouse). Resuming also appends fresh ranges for
//! documents that arrived after the snapshot was cut, so a growing
//! index (elastic-filler still ingesting) is never left behind.
//!
//! # Timing
//!
//! A reporter thread prints one line per `--report-secs` (default 1s):
//! documents done/total, instantaneous and average docs/s, the docs/min
//! and docs/hour projections, and the ETA. The final summary adds the
//! per-minute breakdown and the fetch/insert/project time split — where
//! the wall-clock actually went.

#![forbid(unsafe_code)]

mod clickhouse;
mod es;
mod http;
mod json;
mod schema;

use self::clickhouse::ClickHouse;
use self::es::{Es, Hit};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Where the walk positions live between runs (`--checkpoint`).
const CHECKPOINT_DEFAULT: &str = "clickfiller-checkpoint.json";
/// PIT refresh window: every page search re-arms it, so a slow insert
/// loop cannot let the snapshot lapse mid-run.
const PIT_KEEP_ALIVE: &str = "5m";
/// Rows one worker buffers before an insert. Big batches keep
/// ClickHouse's part count low — the classic failure mode of parallel
/// loading is thousands of tiny parts per partition.
const INSERT_BATCH_DEFAULT: usize = 100_000;
/// Flush earlier than `--insert-batch` when rows are fat.
const INSERT_MAX_BYTES: usize = 32 * 1024 * 1024;
/// Ranges aim for this many documents each (auto mode).
const TARGET_DOCS_PER_RANGE: u64 = 4_000_000;
/// Ceiling on auto-created ranges, protecting the ES heap from too many
/// concurrent search contexts (`--ranges` can override).
const MAX_RANGES: usize = 128;
const DEFAULT_WORKERS: usize = 8;
const FETCH_PAGE_DEFAULT: usize = 10_000;
const REPORT_SECS_DEFAULT: u64 = 1;

/// What a run should do.
#[derive(Clone, Copy)]
enum Mode {
    /// Default: transfer everything (or continue a checkpoint).
    Transfer,
    /// `--verify`: counts + time ranges on both sides.
    Verify,
    /// `--create-table-only`: ensure the ClickHouse table, nothing else.
    CreateTable,
}

#[derive(Clone)]
struct Config {
    es_host: String,
    es_port: u16,
    es_index: String,
    es_user: String,
    es_pass: String,
    ch_host: String,
    ch_port: u16,
    ch_table: String,
    ch_user: String,
    ch_pass: String,
    workers: usize,
    /// `--ranges R`: fixed range count (0 = auto from the histogram).
    ranges: usize,
    fetch_page: usize,
    insert_batch: usize,
    report_secs: u64,
    checkpoint: PathBuf,
    /// `--limit N`: stop after N documents total (cumulative across
    /// resumes). The checkpoint stays on disk — `--resume` continues.
    limit: Option<u64>,
    resume: bool,
    mode: Mode,
}

impl Default for Config {
    fn default() -> Self {
        // The local defaults: the elastic-filler ES stack, and this
        // repo's ClickHouse container (published on 8124 next to a
        // possible host server; `default` / `clickhouse-password`).
        Config {
            es_host: "127.0.0.1".into(),
            es_port: 9200,
            es_index: "xdp-messages".into(),
            es_user: "elastic-admin".into(),
            es_pass: "elastic-password".into(),
            ch_host: "127.0.0.1".into(),
            ch_port: 8124,
            ch_table: "xdp_messages".into(),
            ch_user: "default".into(),
            ch_pass: "clickhouse-password".into(),
            workers: DEFAULT_WORKERS,
            ranges: 0,
            fetch_page: FETCH_PAGE_DEFAULT,
            insert_batch: INSERT_BATCH_DEFAULT,
            report_secs: REPORT_SECS_DEFAULT,
            checkpoint: CHECKPOINT_DEFAULT.into(),
            limit: None,
            resume: false,
            mode: Mode::Transfer,
        }
    }
}

const USAGE: &str = "\
click-filler: move the XDP message index from Elasticsearch into ClickHouse

  click-filler [flags]

connection:
  --es-host H        elasticsearch host      (default 127.0.0.1)
  --es-port P        elasticsearch port      (default 9200)
  --es-index NAME    index to read           (default xdp-messages)
  --es-user U        basic-auth user         (default elastic-admin)
  --es-pass P        basic-auth password     (default elastic-password)
  --ch-host H        clickhouse host         (default 127.0.0.1)
  --ch-port P        clickhouse HTTP port    (default 8124)
  --ch-table NAME    table to fill           (default xdp_messages)
  --ch-user U        X-ClickHouse-User       (default default)
  --ch-pass P        X-ClickHouse-Key        (default clickhouse-password)

throughput:
  --workers W        parallel range walkers  (default 8)
  --ranges R         time ranges to cut the index into (0 = auto ~4M docs)
  --fetch-page N     documents per ES search (default 10000)
  --insert-batch N   rows per CH insert      (default 100000)
  --report-secs N    seconds between progress lines (default 1)

state:
  --checkpoint FILE  walk-position file      (default clickfiller-checkpoint.json)
  --limit N          stop after N documents (cumulative); checkpoint is kept
  --resume           continue from the checkpoint file
  --verify           compare counts + time ranges on both sides
  --create-table-only  ensure the ClickHouse table, transfer nothing
  -h, --help         this text
";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let cfg = match parse_args(&args) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("error: {e}\n\n{USAGE}");
            return ExitCode::FAILURE;
        }
    };
    match cfg.mode {
        Mode::Verify => verify_mode(&cfg),
        Mode::CreateTable => create_table_mode(&cfg),
        Mode::Transfer => transfer_mode(&cfg),
    }
}

/// Hand-rolled argv walk — same style as elastic-filler, no clap.
fn parse_args(args: &[String]) -> Result<Config, String> {
    let mut cfg = Config::default();
    let mut i = 0;
    while i < args.len() {
        let flag = args[i].as_str();
        let mut value = || -> Result<String, String> {
            i += 1;
            args.get(i)
                .cloned()
                .ok_or_else(|| format!("{flag} needs a value"))
        };
        match flag {
            "--es-host" => cfg.es_host = value()?,
            "--es-port" => cfg.es_port = value()?.parse().map_err(|_| "--es-port wants a number")?,
            "--es-index" => cfg.es_index = value()?,
            "--es-user" => cfg.es_user = value()?,
            "--es-pass" => cfg.es_pass = value()?,
            "--ch-host" => cfg.ch_host = value()?,
            "--ch-port" => cfg.ch_port = value()?.parse().map_err(|_| "--ch-port wants a number")?,
            "--ch-table" => cfg.ch_table = value()?,
            "--ch-user" => cfg.ch_user = value()?,
            "--ch-pass" => cfg.ch_pass = value()?,
            "--workers" => cfg.workers = value()?.parse().map_err(|_| "--workers wants a number")?,
            "--ranges" => cfg.ranges = value()?.parse().map_err(|_| "--ranges wants a number")?,
            "--fetch-page" => {
                cfg.fetch_page = value()?.parse().map_err(|_| "--fetch-page wants a number")?
            }
            "--insert-batch" => {
                cfg.insert_batch = value()?.parse().map_err(|_| "--insert-batch wants a number")?
            }
            "--report-secs" => {
                cfg.report_secs = value()?.parse().map_err(|_| "--report-secs wants a number")?
            }
            "--checkpoint" => cfg.checkpoint = value()?.into(),
            "--limit" => cfg.limit = Some(value()?.parse().map_err(|_| "--limit wants a number")?),
            "--resume" => cfg.resume = true,
            "--verify" => cfg.mode = Mode::Verify,
            "--create-table-only" => cfg.mode = Mode::CreateTable,
            "-h" | "--help" => {
                print!("{USAGE}");
                std::process::exit(0);
            }
            other => return Err(format!("unknown flag `{other}` (see --help)")),
        }
        i += 1;
    }
    if cfg.workers == 0 {
        return Err("--workers must be at least 1".to_string());
    }
    if cfg.fetch_page == 0 || cfg.insert_batch == 0 {
        return Err("--fetch-page and --insert-batch must be at least 1".to_string());
    }
    Ok(cfg)
}

/// Connect to both sides with operator-friendly failure messages.
fn connect(cfg: &Config) -> Result<(Es, ClickHouse), String> {
    let es = Es::connect(&cfg.es_host, cfg.es_port, &cfg.es_index, &cfg.es_user, &cfg.es_pass)
        .map_err(|e| {
            format!(
                "cannot reach elasticsearch at {}:{}: {e} — is the stack up? (`cd ../ElasticFiller && docker compose up -d`)",
                cfg.es_host, cfg.es_port
            )
        })?;
    let ch = ClickHouse::connect(&cfg.ch_host, cfg.ch_port, &cfg.ch_user, &cfg.ch_pass).map_err(|e| {
        format!(
            "cannot reach clickhouse at {}:{}: {e} — is the server up?",
            cfg.ch_host, cfg.ch_port
        )
    })?;
    Ok((es, ch))
}

/// `--verify`: document counts and time ranges, side by side.
fn verify_mode(cfg: &Config) -> ExitCode {
    finish(verify(cfg))
}

fn verify(cfg: &Config) -> Result<(), String> {
    let (mut es, mut ch) = connect(cfg)?;
    es.ping()?;
    ch.ping()?;

    let es_count = es.count()?;
    let (es_first, es_last) = if es_count == 0 {
        (None, None)
    } else {
        (Some(es.extreme_ts("asc")?), Some(es.extreme_ts("desc")?))
    };
    let (ch_count, ch_first, ch_last) = ch.stats(&cfg.ch_table)?;

    let range = |first: Option<String>, last: Option<String>| match (first, last) {
        (Some(f), Some(l)) => format!("{f} .. {l}"),
        _ => "empty".to_string(),
    };
    println!(
        "elasticsearch {index}: {n} docs · {range}",
        index = cfg.es_index,
        n = es_count,
        range = range(es_first.map(iso_millis), es_last.map(iso_millis)),
    );
    println!(
        "clickhouse    {table}: {n} rows · {range}",
        table = cfg.ch_table,
        n = ch_count,
        range = range(ch_first, ch_last),
    );
    if es_count == ch_count {
        println!("counts match");
    } else {
        println!(
            "counts differ (elasticsearch {es}, clickhouse {ch}) — a transfer may still be running, or one never finished (`make resume`)",
            es = es_count,
            ch = ch_count
        );
    }
    Ok(())
}

/// `--create-table-only`: ensure the table, show it, done.
fn create_table_mode(cfg: &Config) -> ExitCode {
    finish(create_table(cfg))
}

fn create_table(cfg: &Config) -> Result<(), String> {
    let (_, mut ch) = connect(cfg)?;
    ch.ping()?;
    ch.ensure_table(&cfg.ch_table)?;
    let shown = ch.exec(&format!("SHOW CREATE TABLE {}", cfg.ch_table))?;
    println!("table `{}` ready:\n{}", cfg.ch_table, shown.trim_end());
    Ok(())
}

/// The transfer itself: cut ranges, walk them in parallel, report speed.
fn transfer_mode(cfg: &Config) -> ExitCode {
    let started = Instant::now();
    match run_transfer(cfg, started) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            eprintln!(
                "the checkpoint holds every range's position — rerun with --resume to continue"
            );
            ExitCode::FAILURE
        }
    }
}

/// One contiguous, disjoint slice of the index timeline, in epoch-millis
/// bounds on `@timestamp` (`gte` inclusive, `lt` exclusive).
#[derive(Clone, Copy, Debug)]
struct RangeJob {
    gte: u64,
    lt: u64,
}

/// Shared work queue: an atomic cursor over the range list (same pattern
/// as elastic-filler's file queue — whoever finishes first claims the
/// next range, no fixed worker assignment).
struct Queue {
    jobs: Vec<RangeJob>,
    next: AtomicUsize,
}

impl Queue {
    fn next(&self) -> Option<(usize, RangeJob)> {
        let i = self.next.fetch_add(1, Ordering::SeqCst);
        self.jobs.get(i).map(|j| (i, *j))
    }
}

/// Per-range walk position: `ts` is the last transferred `@timestamp`
/// (epoch millis, 0 = untouched), `docs` the rows already inserted for
/// this range, `complete` set once its walk hit the empty page.
#[derive(Clone)]
struct RangePos {
    gte: u64,
    lt: u64,
    ts: u64,
    docs: u64,
    complete: bool,
}

#[derive(Clone, Default)]
struct Checkpoint {
    ranges: Vec<RangePos>,
    /// Document total at cut time (histogram sum) — the header line and
    /// the reporter's ETA denominator.
    estimate: u64,
}

/// Shared counters: what the reporter prints, what the limit stops on,
/// and where the wall-clock went.
struct Stats {
    docs: AtomicU64,
    bytes: AtomicU64,
    /// Nanoseconds spent in ES page fetches / CH inserts / projection —
    /// the summary's time split. Summed per worker, so these can exceed
    /// wall time; they answer "which activity dominates", not total.
    fetch_ns: AtomicU64,
    insert_ns: AtomicU64,
    project_ns: AtomicU64,
    /// Sum of per-range totals once each walk's first page reports —
    /// drives the ETA.
    total: AtomicU64,
    /// An error or the `--limit` fired: workers finish their current
    /// page (checkpointing it) and claim nothing new.
    stop: AtomicBool,
    /// The stop was the `--limit`, not an error (the summary says which).
    limit_hit: AtomicBool,
    /// Reporter thread exit flag.
    finished: AtomicBool,
    started: Instant,
    /// Documents inserted, bucketed by wall-clock minute.
    per_minute: Mutex<Vec<u64>>,
}

impl Stats {
    fn new(started: Instant) -> Self {
        Stats {
            docs: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            fetch_ns: AtomicU64::new(0),
            insert_ns: AtomicU64::new(0),
            project_ns: AtomicU64::new(0),
            total: AtomicU64::new(0),
            stop: AtomicBool::new(false),
            limit_hit: AtomicBool::new(false),
            finished: AtomicBool::new(false),
            started,
            per_minute: Mutex::new(Vec::new()),
        }
    }

    fn add_docs(&self, n: u64) {
        self.docs.fetch_add(n, Ordering::SeqCst);
        let minute = (self.started.elapsed().as_secs() / 60) as usize;
        let mut minutes = self.poison_tolerant_minutes();
        if minutes.len() <= minute {
            minutes.resize(minute + 1, 0);
        }
        minutes[minute] += n;
    }

    fn poison_tolerant_minutes(&self) -> std::sync::MutexGuard<'_, Vec<u64>> {
        self.per_minute
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn run_transfer(cfg: &Config, started: Instant) -> Result<(), String> {
    let (mut es, mut ch) = connect(cfg)?;
    es.ping()?;
    ch.ping()?;

    // The table exists before the first row moves — a transfer never
    // runs against a missing schema.
    ch.ensure_table(&cfg.ch_table)?;

    // Positions: resume the stored snapshot, or cut a fresh one (which
    // also drops any stale checkpoint).
    let mut cp = if cfg.resume {
        match load_checkpoint(&cfg.checkpoint) {
            Some(cp) => {
                let done: u64 = cp.ranges.iter().map(|r| r.docs).sum();
                let complete = cp.ranges.iter().filter(|r| r.complete).count();
                println!(
                    "resuming from {}: {complete}/{total} ranges complete, {done} documents were already transferred",
                    cfg.checkpoint.display(),
                    complete = complete,
                    total = cp.ranges.len(),
                    done = commas(done),
                );
                cp
            }
            None => {
                println!(
                    "no checkpoint at {} — cutting fresh ranges",
                    cfg.checkpoint.display()
                );
                cut_ranges(&mut es, cfg)?
            }
        }
    } else {
        let _ = std::fs::remove_file(&cfg.checkpoint);
        cut_ranges(&mut es, cfg)?
    };

    // The index grows while we run (elastic-filler may still be
    // ingesting): a resumed walk appends ranges for the tail.
    if cfg.resume {
        extend_tail(&mut es, cfg, &mut cp)?;
    }
    if cp.ranges.is_empty() {
        println!("index {} holds no documents — nothing to transfer", cfg.es_index);
        return Ok(());
    }
    save_checkpoint(&cfg.checkpoint, &cp)?;

    let workers = cfg.workers.min(cp.ranges.len()).max(1);
    println!(
        "{index}: ~{total} documents in {ranges} ranges · {workers} workers · page {page} · insert batch {batch}",
        index = cfg.es_index,
        total = commas(cp.estimate),
        ranges = cp.ranges.len(),
        workers = workers,
        page = cfg.fetch_page,
        batch = cfg.insert_batch,
    );

    let stats = Arc::new(Stats::new(started));
    let reporter = spawn_reporter(Arc::clone(&stats), Duration::from_secs(cfg.report_secs.max(1)));

    let queue = Arc::new(Queue {
        jobs: cp.ranges.iter().map(|r| RangeJob { gte: r.gte, lt: r.lt }).collect(),
        next: AtomicUsize::new(0),
    });
    let ckpt = Arc::new(Mutex::new(cp));

    let mut handles = Vec::with_capacity(workers);
    for w in 0..workers {
        let cfg = cfg.clone();
        let es_creds = (
            cfg.es_host.clone(),
            cfg.es_port,
            cfg.es_index.clone(),
            cfg.es_user.clone(),
            cfg.es_pass.clone(),
        );
        let ch_creds = (cfg.ch_host.clone(), cfg.ch_port, cfg.ch_user.clone(), cfg.ch_pass.clone());
        let stats = Arc::clone(&stats);
        let queue = Arc::clone(&queue);
        let ckpt = Arc::clone(&ckpt);
        handles.push(thread::spawn(move || {
            worker_loop(w, &cfg, &es_creds, &ch_creds, &queue, &stats, &ckpt)
        }));
    }

    // A worker error stops the others (they wind down at their next
    // checkpointed page); the first error wins the report.
    let mut first_error = None;
    for handle in handles {
        let outcome = handle.join().unwrap_or_else(|panic| {
            Err(format!("worker panicked: {panic}", panic = panic_downcast(panic)))
        });
        if let Err(e) = outcome {
            stats.stop.store(true, Ordering::SeqCst);
            first_error.get_or_insert(e);
        }
    }
    stats.finished.store(true, Ordering::SeqCst);
    let _ = reporter.join();

    match first_error {
        Some(e) => Err(e),
        None => {
            // The walk reached the end of every range: the position file
            // has no purpose anymore. A limit-stopped run keeps it.
            let all_done = ckpt
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .ranges
                .iter()
                .all(|r| r.complete);
            if all_done {
                let _ = std::fs::remove_file(&cfg.checkpoint);
            }
            if stats.limit_hit.load(Ordering::SeqCst) {
                println!(
                    "stopped at --limit {limit}: {docs} documents transferred this run, checkpoint kept — continue with `--resume` (drop the --limit flag for the rest)",
                    limit = cfg.limit.expect("limit hit implies a limit"),
                    docs = commas(stats.docs.load(Ordering::SeqCst)),
                );
            }
            print_summary(cfg, &stats);
            Ok(())
        }
    }
}

/// A panic payload as readable text, without knowing its type.
fn panic_downcast(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(text) = payload.downcast_ref::<&str>() {
        (*text).to_string()
    } else if let Some(text) = payload.downcast_ref::<String>() {
        text.clone()
    } else {
        "unknown panic".to_string()
    }
}

/// Cut a fresh snapshot: histogram the whole index, split into ranges.
fn cut_ranges(es: &mut Es, cfg: &Config) -> Result<Checkpoint, String> {
    let first = es.extreme_ts("asc")?;
    if first == 0 {
        return Ok(Checkpoint::default());
    }
    let last = es.extreme_ts("desc")?;
    let (interval, interval_ms) = pick_interval(last.saturating_sub(first) + 60_000);
    let buckets = es.ts_histogram(interval)?;
    let total: u64 = buckets.iter().map(|b| b.1).sum();
    let n_ranges = if cfg.ranges > 0 {
        cfg.ranges
    } else {
        (total.div_ceil(TARGET_DOCS_PER_RANGE) as usize).clamp(1, MAX_RANGES)
    };
    let jobs = split_ranges(&buckets, n_ranges, interval_ms);
    let target = (total / jobs.len().max(1) as u64).max(1);
    println!(
        "{index}: {total} documents from {from} to {to} → {ranges} ranges of ~{target} docs (histogram {interval})",
        index = cfg.es_index,
        total = commas(total),
        from = iso_millis(first),
        to = iso_millis(last),
        ranges = jobs.len(),
        target = commas(target),
        interval = interval,
    );
    Ok(Checkpoint {
        estimate: total,
        ranges: jobs
            .into_iter()
            .map(|j| RangePos { gte: j.gte, lt: j.lt, ts: 0, docs: 0, complete: false })
            .collect(),
    })
}

/// Documents that arrived after the snapshot was cut: histogram the tail
/// past the last range's upper bound and append ranges for it. The first
/// tail range starts exactly at the old bound (disjoint from the
/// snapshot, which ends there).
fn extend_tail(es: &mut Es, cfg: &Config, cp: &mut Checkpoint) -> Result<(), String> {
    let Some(last_lt) = cp.ranges.last().map(|r| r.lt) else {
        return Ok(());
    };
    let max = es.extreme_ts("desc")?;
    if max < last_lt {
        return Ok(()); // nothing new
    }
    let (interval, interval_ms) = pick_interval(max.saturating_sub(last_lt) + 60_000);
    // A bucket overlaps the tail when it extends past the old bound —
    // even if it starts before it (a mid-bucket cut). The snapshot's own
    // ranges already cover everything strictly before `last_lt`.
    let buckets: Vec<(u64, u64)> = es
        .ts_histogram(interval)?
        .into_iter()
        .filter(|(key, _)| *key + interval_ms > last_lt)
        .collect();
    let tail_total: u64 = buckets.iter().map(|b| b.1).sum();
    if tail_total == 0 {
        return Ok(());
    }
    let n_ranges = if cfg.ranges > 0 {
        cfg.ranges
    } else {
        (tail_total.div_ceil(TARGET_DOCS_PER_RANGE) as usize).clamp(1, MAX_RANGES)
    };
    let mut jobs = split_ranges(&buckets, n_ranges, interval_ms);
    if let Some(first) = jobs.first_mut() {
        first.gte = last_lt;
    }
    let n = jobs.len();
    cp.ranges
        .extend(jobs.into_iter().map(|j| RangePos { gte: j.gte, lt: j.lt, ts: 0, docs: 0, complete: false }));
    cp.estimate += tail_total;
    println!(
        "index grew: {total} new documents past {bound} — added {n} tail ranges",
        total = commas(tail_total),
        bound = iso_millis(last_lt),
        n = n,
    );
    Ok(())
}

/// Split populated histogram buckets into `n` contiguous ranges of
/// near-equal document count. Cuts fall on bucket boundaries; the final
/// bound extends one interval past the last bucket (a bucket covers its
/// whole span, empty buckets included via `min_doc_count: 0`).
fn split_ranges(buckets: &[(u64, u64)], n: usize, interval_ms: u64) -> Vec<RangeJob> {
    if buckets.is_empty() {
        return Vec::new();
    }
    let n = n.max(1).min(buckets.len());
    let total: u64 = buckets.iter().map(|b| b.1).sum();
    let target = total.div_ceil(n as u64);
    let mut jobs = Vec::with_capacity(n);
    let mut start = buckets[0].0;
    let mut acc: u64 = 0;
    for (i, &(key, count)) in buckets.iter().enumerate() {
        let last_bucket = i + 1 == buckets.len();
        acc += count;
        // Cut here only while at least one range remains for the rest.
        if acc >= target && jobs.len() + 1 < n && !last_bucket {
            jobs.push(RangeJob { gte: start, lt: key + interval_ms });
            start = key + interval_ms;
            acc = 0;
        }
    }
    jobs.push(RangeJob { gte: start, lt: buckets[buckets.len() - 1].0 + interval_ms });
    jobs
}

/// Histogram interval for a time span: the finest candidate that still
/// keeps the bucket count manageable (≤ ~5000 — one cheap aggregation
/// with fine enough granularity to balance ranges precisely).
fn pick_interval(span_ms: u64) -> (&'static str, u64) {
    const CANDIDATES: [(&str, u64); 5] = [
        ("60s", 60_000),
        ("10m", 600_000),
        ("1h", 3_600_000),
        ("6h", 21_600_000),
        ("1d", 86_400_000),
    ];
    CANDIDATES
        .iter()
        .find(|(_, ms)| span_ms / ms <= 5_000)
        .copied()
        .unwrap_or(*CANDIDATES.last().unwrap())
}

/// Connection credentials cloned per worker (threads need owned data).
type EsCreds = (String, u16, String, String, String);
type ChCreds = (String, u16, String, String);

fn worker_loop(
    w: usize,
    cfg: &Config,
    es_creds: &EsCreds,
    ch_creds: &ChCreds,
    queue: &Queue,
    stats: &Stats,
    ckpt: &Mutex<Checkpoint>,
) -> Result<(), String> {
    let mut es = Es::connect(&es_creds.0, es_creds.1, &es_creds.2, &es_creds.3, &es_creds.4)
        .map_err(|e| format!("worker {w}: {e}"))?;
    let mut ch = ClickHouse::connect(&ch_creds.0, ch_creds.1, &ch_creds.2, &ch_creds.3)
        .map_err(|e| format!("worker {w}: {e}"))?;

    // Drain the queue until it is empty or another worker's error/limit
    // told everyone to wind down.
    while let Some((idx, job)) = queue.next() {
        if stats.stop.load(Ordering::SeqCst) {
            return Ok(());
        }
        walk_range(w, idx, job, cfg, &mut es, &mut ch, stats, ckpt)?;
    }
    Ok(())
}

/// Walk one range: PIT snapshot -> `search_after` pages -> project ->
/// buffer -> big insert -> checkpoint. Returns when the range is done or
/// the global stop fired (both fine); errors bubble up and stop the run.
fn walk_range(
    w: usize,
    idx: usize,
    job: RangeJob,
    cfg: &Config,
    es: &mut Es,
    ch: &mut ClickHouse,
    stats: &Stats,
    ckpt: &Mutex<Checkpoint>,
) -> Result<(), String> {
    // Resume position for this range: seek one millisecond before the
    // stored boundary — `_shard_doc` values are PIT-internal, but the
    // timestamp is stable, and the refetched boundary collapses in
    // ClickHouse's ReplacingMergeTree.
    let start_after = {
        let cp = ckpt.lock().unwrap_or_else(|p| p.into_inner());
        let pos = &cp.ranges[idx];
        if pos.complete {
            return Ok(());
        }
        if pos.ts > 0 {
            Some(format!("[{},0]", pos.ts.saturating_sub(1)))
        } else {
            None
        }
    };
    let filter = format!(r#"{{"range":{{"@timestamp":{{"gte":{},"lt":{}}}}}}}"#, job.gte, job.lt);
    let pit = es
        .open_pit(PIT_KEEP_ALIVE)
        .map_err(|e| format!("worker {w}: {e}"))?;

    let mut after = start_after;
    let mut buffer: Vec<String> = Vec::new();
    let mut buf_bytes: usize = 0;
    let mut pending_ts: u64 = 0;
    let mut range_reported = false;

    let result: Result<(), String> = loop {
        if stats.stop.load(Ordering::SeqCst) {
            break Ok(());
        }
        let t = Instant::now();
        let page = match es.search_page(&pit, PIT_KEEP_ALIVE, cfg.fetch_page, after.as_deref(), Some(&filter)) {
            Ok(page) => page,
            Err(e) => break Err(format!("worker {w}: {e}")),
        };
        stats.fetch_ns.fetch_add(t.elapsed().as_nanos() as u64, Ordering::SeqCst);
        if !range_reported {
            stats.total.fetch_add(page.total, Ordering::SeqCst);
            range_reported = true;
        }
        if page.hits.is_empty() {
            if let Err(e) = flush_buffer(ch, cfg, stats, ckpt, idx, w, &mut buffer, &mut buf_bytes, pending_ts) {
                break Err(e);
            }
            // Mark the range done and persist, all under the checkpoint
            // lock so concurrent flushes cannot interleave snapshots.
            let outcome = {
                let mut cp = ckpt.lock().unwrap_or_else(|p| p.into_inner());
                cp.ranges[idx].complete = true;
                save_checkpoint(&cfg.checkpoint, &cp)
            };
            if let Err(e) = outcome {
                break Err(e);
            }
            break Ok(());
        }

        // Project first: a bad document stops the run before any of the
        // page is stored half-transformed.
        let t = Instant::now();
        let page_rows = match project_all(&page.hits) {
            Ok(rows) => rows,
            Err(e) => break Err(format!("worker {w}: {e}")),
        };
        for row in page_rows {
            buf_bytes += row.len() + 1;
            buffer.push(row);
        }
        stats.project_ns.fetch_add(t.elapsed().as_nanos() as u64, Ordering::SeqCst);
        let last = page.hits.last().expect("non-empty page has a last hit");
        pending_ts = last.ts_text.parse().unwrap_or(pending_ts);
        after = Some(last.sort.clone());

        if buffer.len() >= cfg.insert_batch || buf_bytes >= INSERT_MAX_BYTES {
            if let Err(e) = flush_buffer(ch, cfg, stats, ckpt, idx, w, &mut buffer, &mut buf_bytes, pending_ts) {
                break Err(e);
            }
        }
        if let Some(limit) = cfg.limit {
            if stats.docs.load(Ordering::SeqCst) >= limit {
                stats.limit_hit.store(true, Ordering::SeqCst);
                stats.stop.store(true, Ordering::SeqCst);
                break Ok(());
            }
        }
    };
    es.close_pit(&pit);
    result
}

/// Project a whole page: a bad document names itself, the run stops
/// before any of the page is stored half-transformed.
fn project_all(hits: &[Hit]) -> Result<Vec<String>, String> {
    hits.iter()
        .map(|hit| {
            schema::project_row(&hit.id, &hit.source)
                .map_err(|e| format!("document `{}`: {e}", hit.id))
        })
        .collect()
}

/// Insert the buffered rows as one batch, then — and only then — advance
/// the range's checkpointed position and the shared counters.
#[allow(clippy::too_many_arguments)]
fn flush_buffer(
    ch: &mut ClickHouse,
    cfg: &Config,
    stats: &Stats,
    ckpt: &Mutex<Checkpoint>,
    idx: usize,
    w: usize,
    buffer: &mut Vec<String>,
    buf_bytes: &mut usize,
    ts: u64,
) -> Result<(), String> {
    if buffer.is_empty() {
        return Ok(());
    }
    let t = Instant::now();
    let outcome = ch.insert_rows(&cfg.ch_table, buffer);
    stats.insert_ns.fetch_add(t.elapsed().as_nanos() as u64, Ordering::SeqCst);
    if let Err(e) = outcome {
        stats.stop.store(true, Ordering::SeqCst);
        return Err(format!("worker {w}: {e}"));
    }
    let n = buffer.len() as u64;
    stats.bytes.fetch_add(*buf_bytes as u64, Ordering::SeqCst);
    stats.add_docs(n);
    buffer.clear();
    *buf_bytes = 0;
    // Position advances only after the batch is stored: a crash between
    // insert and checkpoint re-inserts (and ClickHouse collapses it), a
    // crash the other way around would lose rows. Update + write happen
    // under one lock so concurrent flushes cannot interleave file
    // snapshots.
    {
        let mut cp = ckpt.lock().unwrap_or_else(|p| p.into_inner());
        cp.ranges[idx].ts = ts;
        cp.ranges[idx].docs += n;
        save_checkpoint(&cfg.checkpoint, &cp)?;
    }
    Ok(())
}

/// One progress line every tick: rate now, rate average, projections in
/// docs/min and docs/hour, ETA against the walk-reported total.
fn spawn_reporter(stats: Arc<Stats>, interval: Duration) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut last_docs: u64 = 0;
        let mut last_tick = Instant::now();
        while !stats.finished.load(Ordering::SeqCst) {
            thread::sleep(interval);
            let docs = stats.docs.load(Ordering::SeqCst);
            let now = Instant::now();
            let dt = now.duration_since(last_tick).as_secs_f64().max(1e-6);
            let elapsed = now.duration_since(stats.started).as_secs_f64();
            let rate_now = (docs - last_docs) as f64 / dt;
            let avg = docs as f64 / elapsed.max(1e-6);
            let total = stats.total.load(Ordering::SeqCst);
            let pace = format!(
                " · {:.1}M/min · {:.0}M/h proj",
                avg * 60.0 / 1e6,
                avg * 3_600.0 / 1e6,
            );
            let tail = if total > docs && avg > 0.0 {
                format!(" · ETA {}", hhmmss((total - docs) as f64 / avg))
            } else {
                String::new()
            };
            eprintln!(
                "[{elapsed}] {docs}{of} docs · {rate_now:.0}/s now · {avg:.0}/s avg{pace}{tail}",
                elapsed = hhmmss(elapsed),
                docs = commas(docs),
                of = if total > 0 { format!("/{}", commas(total)) } else { String::new() },
                rate_now = rate_now,
                avg = avg,
                pace = pace,
                tail = tail,
            );
            last_docs = docs;
            last_tick = now;
        }
    })
}

/// End-of-run report: totals, average pace, where the time went, and the
/// per-minute breakdown.
fn print_summary(cfg: &Config, stats: &Stats) {
    let docs = stats.docs.load(Ordering::SeqCst);
    let bytes = stats.bytes.load(Ordering::SeqCst);
    let elapsed = stats.started.elapsed().as_secs_f64();
    let avg = docs as f64 / elapsed.max(1e-6);
    println!();
    println!(
        "transferred {docs} documents ({gb:.2} GB of rows) into `{table}` in {elapsed}",
        docs = commas(docs),
        gb = bytes as f64 / (1024.0 * 1024.0 * 1024.0),
        table = cfg.ch_table,
        elapsed = hhmmss(elapsed),
    );
    println!(
        "average {avg:.0} docs/s · {per_min:.2}M docs/min · {per_hour:.1}M docs/hour",
        avg = avg,
        per_min = avg * 60.0 / 1e6,
        per_hour = avg * 3_600.0 / 1e6,
    );
    let fetch = stats.fetch_ns.load(Ordering::SeqCst);
    let insert = stats.insert_ns.load(Ordering::SeqCst);
    let project = stats.project_ns.load(Ordering::SeqCst);
    let busy = fetch + insert + project;
    if busy > 0 {
        println!(
            "time split: fetch {f:.0}% · insert {i:.0}% · project {p:.0}% (of worker busy time)",
            f = fetch as f64 / busy as f64 * 100.0,
            i = insert as f64 / busy as f64 * 100.0,
            p = project as f64 / busy as f64 * 100.0,
        );
    }
    let minutes = stats.poison_tolerant_minutes();
    if minutes.len() > 1 || (minutes.len() == 1 && minutes[0] > 0) {
        println!("docs per minute:");
        for (i, n) in minutes.iter().enumerate() {
            println!("  min {:02}: {}", i + 1, commas(*n));
        }
    }
}

/// Turn a run's `Result` into an exit code, with the resume hint attached
/// to failures.
fn finish(result: Result<(), String>) -> ExitCode {
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            eprintln!(
                "the checkpoint holds every range's position — rerun with --resume to continue"
            );
            ExitCode::FAILURE
        }
    }
}

/// The walk positions, one JSON object: per-range bounds, last
/// transferred timestamp, inserted rows. Written atomically (temp file +
/// rename) after every inserted batch — a crash mid-write leaves the
/// previous positions, never a torn file.
fn save_checkpoint(path: &Path, cp: &Checkpoint) -> Result<(), String> {
    let tmp = path.with_extension("tmp");
    let mut body = format!("{{\"estimate\":{},\"ranges\":[", cp.estimate);
    for (i, r) in cp.ranges.iter().enumerate() {
        if i > 0 {
            body.push(',');
        }
        body.push_str(&format!(
            r#"{{"gte":{gte},"lt":{lt},"ts":{ts},"docs":{docs},"complete":{complete}}}"#,
            gte = r.gte,
            lt = r.lt,
            ts = r.ts,
            docs = r.docs,
            complete = r.complete,
        ));
    }
    body.push_str("]}\n");
    std::fs::write(&tmp, body)
        .map_err(|e| format!("cannot write checkpoint {}: {e}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .map_err(|e| format!("cannot update checkpoint {}: {e}", path.display()))
}

/// Load the walk positions; `None` = absent or unreadable (a stale
/// pre-parallel checkpoint file parses as JSON without `ranges` and just
/// starts the walk from the top).
fn load_checkpoint(path: &Path) -> Option<Checkpoint> {
    let text = std::fs::read_to_string(path).ok()?;
    let parsed = json::parse(&text).ok()?;
    let estimate = parsed
        .get("estimate")
        .and_then(|v| v.as_num_text())
        .and_then(|t| t.parse().ok())
        .unwrap_or(0);
    let ranges = parsed.get("ranges")?;
    let json::Json::Arr(items) = ranges else {
        return None;
    };
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let num = |key: &str| -> Option<u64> {
            item.get(key).and_then(|v| v.as_num_text()).and_then(|t| t.parse().ok())
        };
        out.push(RangePos {
            gte: num("gte")?,
            lt: num("lt")?,
            ts: num("ts").unwrap_or(0),
            docs: num("docs").unwrap_or(0),
            complete: matches!(item.get("complete"), Some(json::Json::Bool(true))),
        });
    }
    Some(Checkpoint { estimate, ranges: out })
}

/// Epoch millis as `YYYY-MM-DD HH:MM:SS.mmm` — Howard Hinnant's
/// `civil_from_days`, same trick elastic-filler's ingest uses, so the
/// tool needs no date library.
fn iso_millis(ms: u64) -> String {
    let days = (ms / 86_400_000) as i64;
    let sod = (ms % 86_400_000) / 1_000;
    let mmm = ms % 1_000;
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}.{mmm:03}",
        sod / 3_600,
        sod % 3_600 / 60,
        sod % 60
    )
}

/// Days since the Unix epoch to (year, month, day).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = (doe - (365 * yoe + yoe / 4 - yoe / 100)) as u32;
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Seconds as `HH:MM:SS` (hours pad past 99, as they should).
fn hhmmss(secs: f64) -> String {
    let total = secs as u64;
    format!("{:02}:{:02}:{:02}", total / 3_600, total % 3_600 / 60, total % 60)
}

/// Thousands separators — progress lines are read by humans.
fn commas(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, ch) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn checkpoint_round_trips() {
        let dir = std::env::temp_dir().join("click-filler-test-checkpoint");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cp.json");
        let cp = Checkpoint {
            estimate: 8_561_236_100,
            ranges: vec![
                RangePos { gte: 1_692_662_400_000, lt: 1_692_666_000_000, ts: 1_692_665_999_999, docs: 4_000_000, complete: true },
                RangePos { gte: 1_692_666_000_000, lt: 1_692_669_600_000, ts: 1_692_667_100_000, docs: 123_456, complete: false },
            ],
        };
        save_checkpoint(&path, &cp).unwrap();
        let loaded = load_checkpoint(&path).unwrap();
        assert_eq!(loaded.ranges.len(), cp.ranges.len());
        assert_eq!(loaded.estimate, 8_561_236_100);
        assert!(loaded.ranges[0].complete);
        assert_eq!(loaded.ranges[1].ts, 1_692_667_100_000);
        assert_eq!(loaded.ranges[1].docs, 123_456);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn absent_or_torn_checkpoint_is_none() {
        assert!(load_checkpoint(Path::new("/nonexistent/cp.json")).is_none());
        let dir = std::env::temp_dir().join("click-filler-test-checkpoint");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("torn.json");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(b"{\"ranges\":[{\"gte\":1").unwrap();
        assert!(load_checkpoint(&path).is_none());
        // A pre-parallel v1 checkpoint has no `ranges` key: fresh start.
        std::fs::write(&path, "{\"ts\":1692711000123,\"docs\":42}\n").unwrap();
        assert!(load_checkpoint(&path).is_none());
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn ranges_split_evenly_and_cover_everything() {
        // 12 buckets of 1000 docs -> 3 ranges of ~4 buckets.
        let buckets: Vec<(u64, u64)> = (0..12).map(|i| (1_692_662_400_000 + i * 60_000, 1_000)).collect();
        let jobs = split_ranges(&buckets, 3, 60_000);
        assert_eq!(jobs.len(), 3);
        assert_eq!(jobs[0].gte, buckets[0].0);
        assert_eq!(jobs[0].lt, jobs[1].gte); // contiguous, no gaps
        assert_eq!(jobs[1].lt, jobs[2].gte);
        // The last bound covers the final bucket's whole span.
        assert_eq!(jobs[2].lt, buckets[11].0 + 60_000);
        // Near-equal: 4 buckets each.
        let per = |j: &[RangeJob], b: &[(u64, u64)]| -> Vec<u64> {
            j.iter()
                .map(|r| b.iter().filter(|(k, _)| *k >= r.gte && *k < r.lt).map(|(_, c)| c).sum())
                .collect()
        };
        assert_eq!(per(&jobs, &buckets), vec![4_000, 4_000, 4_000]);
    }

    #[test]
    fn range_split_edge_cases() {
        // Empty index -> no ranges.
        assert!(split_ranges(&[], 4, 60_000).is_empty());
        // One bucket -> one range covering its span.
        let jobs = split_ranges(&[(1_000_000, 5), ], 8, 60_000);
        assert_eq!(jobs.len(), 1);
        assert_eq!((jobs[0].gte, jobs[0].lt), (1_000_000, 1_060_000));
        // More ranges than buckets: clamped to one range per bucket.
        let buckets: Vec<(u64, u64)> = (0..2).map(|i| (i * 60_000, 10)).collect();
        assert_eq!(split_ranges(&buckets, 7, 60_000).len(), 2);
    }

    #[test]
    fn interval_picks_finest_that_stays_bounded() {
        assert_eq!(pick_interval(600_000), ("60s", 60_000)); // 10 min: 10 buckets
        assert_eq!(pick_interval(3_600_000 * 5), ("60s", 60_000)); // 5 h: 300
        assert_eq!(pick_interval(86_400_000), ("60s", 60_000)); // 1 day: 1440
        assert_eq!(pick_interval(86_400_000 * 5), ("10m", 600_000)); // 5 days: 720
        assert_eq!(pick_interval(86_400_000 * 30), ("10m", 600_000)); // 30 days: 4320
        assert_eq!(pick_interval(86_400_000 * 365), ("6h", 21_600_000)); // 1 year: 8760h -> 1460 buckets at 6h
    }

    #[test]
    fn millis_format_as_iso() {
        // 2023-08-22 13:30:00.123 UTC (the instant the ingest tests use).
        assert_eq!(iso_millis(1_692_711_000_123), "2023-08-22 13:30:00.123");
        assert_eq!(iso_millis(0), "1970-01-01 00:00:00.000");
        // Leap day: the civil-from-days edge.
        assert_eq!(iso_millis(951_782_400_000), "2000-02-29 00:00:00.000");
    }

    #[test]
    fn human_formats() {
        assert_eq!(commas(0), "0");
        assert_eq!(commas(999), "999");
        assert_eq!(commas(1_000), "1,000");
        assert_eq!(commas(61_668_134), "61,668,134");
        assert_eq!(hhmmss(0.0), "00:00:00");
        assert_eq!(hhmmss(59.9), "00:00:59");
        assert_eq!(hhmmss(4_372.0), "01:12:52");
    }
}
