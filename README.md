# XDP market-data replay pipeline

Two self-contained Rust projects that replay NYSE American XDP captures
into queryable stores — first Elasticsearch, then ClickHouse — with no
external crates anywhere (every HTTP/RESP/JSON client is hand-rolled).

```
                ElasticFiller                        ClickFiller
.pcap.zst ──zstd/pcap──> Redis    (symbol table)         │
  242 files               └──────> Elasticsearch ──search_after──> ClickHouse
  one trading day                  xdp-messages           xdp_messages
  (~150 M messages)                (search / Kibana)      (SQL analytics)
```

## Projects

| Folder | What it does | Read next |
|---|---|---|
| [`ElasticFiller/`](ElasticFiller/) | decode captures → symbol table into Redis + every message into Elasticsearch | [its README](ElasticFiller/README.md) |
| [`ClickFiller/`](ClickFiller/) | move the finished index from Elasticsearch into ClickHouse (parallel, resumable, byte-exact) | [its README](ClickFiller/README.md) |

[`PcapParser/`](PcapParser/) is **not part of this pipeline anymore** — it
is a separate standalone project (and the pipeline's parser source of
truth lives on GitHub:
[AdeilH/CustomPCAPParser](https://github.com/AdeilH/CustomPCAPParser),
consumed by ElasticFiller as a git dependency). Its folder here is kept
only as the original workspace; the capture `data/` now lives inside
`ElasticFiller/data`.

## End-to-end run

One-time per machine:

```sh
# 1. captures (242 files, ~3.9 GB zip → ./data)
cd ElasticFiller && make data

# 2. the datastore stack: redis :6379 + elasticsearch :9200 + kibana :5601
make up            # (still inside ElasticFiller)

# 3. pass 1 + pass 2: symbols to Redis, all messages to Elasticsearch
make symbols       # ~1 min
make messages-all  # ~2 h, lands ~21 GiB / ~150 M docs
```

Then move the index into ClickHouse:

```sh
cd ../ClickFiller
make up            # clickhouse container :8124 (HTTP) / :9001 (native)
make table         # ensure xdp_messages (ReplacingMergeTree, idempotent)
make transfer      # parallel, resumable, live timing on stderr
make verify        # counts + time ranges on both sides
```

Every step is re-runnable: ingester and transfer both use deterministic
ids / a ReplacingMergeTree, so reruns overwrite instead of duplicating,
and `click-filler --resume` continues an interrupted transfer.

## Ports & credentials

| Service | Endpoint | Login |
|---|---|---|
| Redis | `localhost:6379` | none (dev) |
| Elasticsearch | `localhost:9200` | `elastic-admin` / `elastic-password` |
| Kibana | http://localhost:5601 | same |
| ClickHouse | `localhost:8124` HTTP, `9001` native | `default` / `clickhouse-password` |

All overridable per project (`--es-*`, `--ch-*`, `--redis-*` flags and
`make` variables — see each README).

## Querying the result

- **Kibana Discover (ES|QL)** — `FROM xdp-messages | STATS count = COUNT(*) BY msg_type | SORT count DESC`
- **ClickHouse SQL** — `SELECT msg_type, count() FROM xdp_messages FINAL GROUP BY msg_type ORDER BY count() DESC;`
- `symbol_index -> symbol` resolution lives in Redis (`HGET xdp:symbols <index>`) and
  is already baked into every stored document's `symbol` field.
