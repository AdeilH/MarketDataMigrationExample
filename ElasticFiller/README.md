# elastic-filler

Replay NYSE American XDP market-data captures into Elasticsearch: the
symbol table goes to Redis (pre-market warm-up), every other message
becomes one linear, searchable document. Standalone project — the
`pcap-parser` library is pulled from GitHub, captures live in `./data`.

```
.pcap.zst captures ──> elastic-filler ──┬─> Redis          (symbol index table)
                                        └─> Elasticsearch  (xdp-messages)
```

The NYSE XDP feed references symbols by index in every subsequent message
(AddOrder, Execution, ...), so a consumer must first collect the
`SymbolIndexMapping` broadcasts. Pass 1 does that: every capture is read,
decompressed (zstd, via the `pcap-parser` library) and dissected on all
cores; per-file tables are merged in sorted file order (deterministic);
a re-mapped index is warned about, not silently overwritten. Pass 2
replays everything else into Elasticsearch, enriched with the symbol
names from Redis.

## Capture data

The captures are Databento's free NYSE American XDP samples — pillar A+B
channels, full trading day 2023-08-22: one **3.9 GB zip** holding the 242
`.pcap.zst` files flat (10-minute rotations, 00:00–23:50 UTC). They
extract into `./data` (gitignored):

```sh
make data    # downloads + extracts into ./data, skips if already present
```

or by hand:

```sh
mkdir -p data
curl -fL -o /tmp/ny4-xase-pillar-ab-20230822.zip \
  https://sample-pcaps-dl.databento.com/xase/20230822/ny4-xase-pillar-ab-20230822.zip
unzip -o /tmp/ny4-xase-pillar-ab-20230822.zip -d data
rm /tmp/ny4-xase-pillar-ab-20230822.zip
```

Point any run at another directory with the positional argument (or
`make messages DATA=<dir>`). `market data © Databento, redistribution of
the sample data is governed by Databento's sample-data terms`.

## Usage

```sh
cargo run --release                          # scan ./data, print symbol table
cargo run --release -- --threads 4           # cap the worker count
cargo run --release -- <data-dir>            # scan another directory
```

Progress goes to stderr (`done/total, in flight, elapsed`); the symbol
table (sorted by index) goes to stdout, plus a per-worker file count so
the parallel spread is visible. Files are scheduled longest-first — big
market-hours captures start immediately, small ones fill the tail. The
exit code is non-zero if any file failed to parse.

Runs without `--populate-symbols` start with an advisory pre-flight
check: if Redis is unreachable or `xdp:symbols` is missing, a warning is
printed to stderr (`Populate it first: --populate-symbols`) before the
scan begins. It is never fatal — the scan itself does not need Redis.

## Pre-market symbol population (Redis)

```sh
make up                                             # redis + elasticsearch + kibana
make symbols                                        # scan + upload
docker compose exec redis redis-cli HLEN xdp:symbols
```

Flags: `--redis-host` (default `127.0.0.1`), `--redis-port` (default
`6379`).

What the populate run writes:

- `xdp:symbols` — hash, `symbol_index -> symbol`. Built in a staging hash
  (`xdp:symbols:incoming`) and swapped in with an atomic `RENAME`, so
  consumers read either the old or the new complete table, never a
  partial upload. Previous tables are replaced wholesale.
- `xdp:symbols:meta` — `symbols` (count) and `populated_at` (Unix
  seconds) for freshness checks before market.

Safety valves: if any capture fails to parse, the upload is refused (a
truncated table is not published); Redis I/O has a 10 s timeout so a
missing container fails fast. Redis persistence is append-only, so the
table survives restarts until the next populate run.

## The stack (`docker-compose.yml`)

`make up` starts redis, elasticsearch 9.2.2 and kibana, plus a one-shot
`setup` service that creates `elastic-admin` (superuser, the ingester's
login) and sets the built-in `kibana_system` account's password. Kibana
serves http://localhost:5601 — log in as `elastic-admin` /
`elastic-password`, create a Data View for `xdp-messages` with
`@timestamp`, and Discover works. One Elasticsearch quirk worth knowing:
the `.kibana*` index namespace is *restricted* — only `kibana_system`
may create those indices, so Kibana connecting as a plain superuser
fails its migrations with endless HTTP 403s.

## Message replay into Elasticsearch (`--store-messages`)

The second pass, run after symbols are populated:

```sh
cargo run --release -- --store-messages                    # whole directory
cargo run --release -- --store-messages --max-files 5      # scope the size
cargo run --release -- --store-messages -- <single-file-or-dir>
```

Flow: one `HGETALL` pulls the whole symbol table into memory → a pool of
workers pulls files from a shared queue (chronological start order), each
with its own Elasticsearch connection and bulk buffer → every message
except symbol mappings becomes one flat JSON document, enriched with the
symbol name. Heartbeats and `SymbolIndexMapping` are excluded — the table
lives in Redis.

Worker count follows your cores by default; cap it with `--threads N` or
`make messages THREADS=8`. Document ids are deterministic per file
(`file:packet:n`), so parallel arrival order is irrelevant and re-running
any set of files overwrites exactly its own documents — interrupted runs
are repaired by repeating the same command. Note on scaling, measured on
a single-node cluster: workers parallelize parsing and connections, but
Elasticsearch itself indexes ~20–25k docs/s regardless of client
concurrency — for bulk loads the ceiling is the server (heap size,
disk), not the number of clients.

Defaults: `http://127.0.0.1:9200`, index `xdp-messages`, user
`elastic-admin` / password `elastic-password`; override with
`--es-host/--es-port/--es-index/--es-user/--es-pass`. On the first run an
index template (`xdp-messages`) sets 1 shard / 0 replicas and keyword
mappings for the filter fields (`symbol`, `msg_type`, `side`, `file`,
`firm_id`, `trade_conditions`) plus a date `@timestamp`; everything else
is dynamically mapped.

Watch the disk: documents are ~310 bytes as JSON but compress heavily in
Lucene. Measured full day: **~150 M messages ≈ 21 GiB** in Elasticsearch
for all 242 captures. Scope with `--max-files` or a scoped directory if
you want less.

Documents carry deterministic ids (`file:packet:n`), so **re-running the
same captures overwrites instead of duplicating** — a run interrupted by
a crash (the ingester also transparently reconnects and resends a bulk
request whose connection died before a response) can simply be repeated.

## Layout

- `src/main.rs` — scan/merge/upload pipeline, message store mode, CLI
- `src/ingest.rs` — pcap timestamps → ISO-8601, message → flat JSON doc
- `src/es.rs` — minimal dependency-free HTTP/1.1 client (bulk, chunked
  responses, basic auth)
- `src/redis.rs` — minimal dependency-free RESP client (just enough
  protocol for PING/DEL/HSET/RENAME/HLEN/HGET)

Depends on [`pcap-parser`](https://github.com/AdeilH/CustomPCAPParser)
as a git dependency; no other external crates. Cargo.lock pins the
upstream commit — `cargo update -p pcap-parser` picks up new parser
work. Captures live in `./data` (gitignored, produced by `make data`).

Next hop: [`../ClickFiller`](../ClickFiller) moves the finished index
into ClickHouse for analysis.
