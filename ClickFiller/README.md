# click-filler

Move the XDP market-data index from **Elasticsearch into ClickHouse**, as
fast as the two servers allow — in one parallel, timed, resumable pass.

```
pcaps ──> PcapParser ──> elastic-filler ──> Elasticsearch ──> click-filler ──> ClickHouse
  .pcap.zst    (decode)      (ingest)       xdp-messages       (this tool)   xdp_messages
```

Like its siblings, it depends on **nothing**: a hand-rolled HTTP/1.1
client speaks to both servers (ES basic auth on :9200, ClickHouse's
`X-ClickHouse-*` headers on :8123), and a small JSON module round-trips
documents without `serde`. Numbers keep their original digit text, so
`u64` order ids survive exactly; timestamps keep their nanoseconds.

## Quickstart

```sh
make table        # ensure the ClickHouse table (idempotent)
make transfer     # full migration, live timing on stderr
make verify       # counts + time ranges on both sides
```

Defaults: Elasticsearch `127.0.0.1:9200` (`xdp-messages`,
`elastic-admin`/`elastic-password`), ClickHouse `127.0.0.1:8124` (user
`default`, password `clickhouse-password`) — this repo's container
(`make up`, published on 8124/9001 so it can sit next to a host server).
Override with `--ch-port`/`--ch-pass` if your ClickHouse lives elsewhere.

## How it runs

1. **Cut ranges** — one `date_histogram` (minute buckets, exact counts)
   over the index; buckets are split into contiguous ranges of
   near-equal document count (~4 M docs/range, `--ranges` to override,
   capped at 128). Disjoint ranges cannot skip documents.
2. **Walk in parallel** — W worker threads (`--workers`, default 8)
   drain the range queue. Each worker owns an Elasticsearch connection,
   its own PIT snapshot, and a ClickHouse connection; it walks its range
   with `search_after` sorted by `[@timestamp, _shard_doc]` (sorting on
   `_id` is refused by Elasticsearch — fielddata is disallowed), and
   while one worker inserts, the others fetch.
3. **Insert in big batches** — rows are buffered per worker and flushed
   at ~100k rows / 32 MB. Many small parallel inserts would pile up
   thousands of MergeTree parts per partition and throttle ClickHouse;
   big batches keep merges cheap.

Every value moves byte-exact; the only renames are `@timestamp` → `ts`
and the document id joining as `es_id`.

## Timing (how fast did it go?)

A reporter thread prints one line per second (`--report-secs`):

```
[00:03:12] 34,120,448/85,612,361 docs · 135,402/s now · 118,201/s avg · 7.1M/min · 425.5M/h proj · ETA 00:04:52
```

The final summary gives the run's totals:

```
transferred 85,612,361 documents (29.87 GB of rows) into `xdp_messages` in 00:07:12
average 198,145 docs/s · 11.89M docs/min · 713.3M docs/hour
time split: fetch 62% · insert 31% · project 7% (of worker busy time)
docs per minute:
  min 01: 11,204,551
  min 02: 12,088,312
  ...
```

The time split says which side to tune next: `fetch`-heavy → more
workers or smaller pages; `insert`-heavy → fewer workers or bigger
batches.

## Resume & a growing index

After every inserted batch the checkpoint file
(`clickfiller-checkpoint.json`) is rewritten atomically with each
range's walk position. An interrupted run — crash, Ctrl-C, `--limit`,
an error — continues with `--resume`:

* each unfinished range seeks to one millisecond before its stored
  boundary (`_shard_doc` positions are PIT-internal and meaningless
  across snapshots; the timestamp is stable), and
* fresh ranges are appended for documents that arrived after the
  snapshot was cut, so an index that is still being ingested is never
  left behind.

Refetched boundary documents are re-inserted and collapse harmlessly:
the table is a `ReplacingMergeTree` over `(ts, es_id)`, which also
makes whole reruns idempotent. Point queries after reruns: `SELECT
... FINAL` or `SELECT ..., _version`.

## The table

`schema.rs` is the single source of truth: the DDL below and the
projected rows both derive from one column list, so they cannot drift.

```sql
CREATE TABLE IF NOT EXISTS xdp_messages
(
    ts DateTime64(9, 'UTC'), es_id String,
    file LowCardinality(String), msg_type LowCardinality(String), msg_type_id UInt16,
    symbol LowCardinality(String) DEFAULT '', symbol_index Nullable(UInt32),
    ... (source_time, order_id, price, volume, side, firm_id, trade_conditions, ...)
) ENGINE = ReplacingMergeTree
PARTITION BY toDate(ts)
ORDER BY (ts, es_id);
```

Integer widths mirror the feed (`u64` order ids → `UInt64`, `u32`
prices → `UInt32`). Fields absent for a message type take defaults
(`0`/`''`); only fields Elasticsearch stores as meaningful `null`
(`symbol_index`, the refresh header's sequence numbers) are `Nullable`.
`price` stays the raw unscaled numerator — scaling is the analysis
layer's job, same as elastic-filler.

## Flags

```
connection:   --es-host --es-port --es-index --es-user --es-pass
              --ch-host --ch-port --ch-table --ch-user --ch-pass
throughput:   --workers W (8)        parallel range walkers
              --ranges R (auto)      time ranges to cut into
              --fetch-page N (10000) documents per ES search
              --insert-batch N (100000)  rows per CH insert
              --report-secs N (1)    seconds between progress lines
state:        --checkpoint FILE      walk-position file
              --limit N              stop after N docs (checkpoint kept)
              --resume               continue from the checkpoint
              --verify               counts + ranges on both sides
              --create-table-only    ensure the table, transfer nothing
```

## Status & what's next

- The transfer engine, timing, resume, and verify are implemented and
  unit-tested (`make test`); the full 85 M-document migration has not
  been executed yet — `make transfer` runs it.
- Planned next: a continuous `--watch` mode (tail new documents), and
  a web interface for browsing the transferred data (deferred by
  design for now).
