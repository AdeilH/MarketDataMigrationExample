# click-filler — launch the Elasticsearch -> ClickHouse migration and
# report its metrics.
#
#   make               launch the parallel transfer: live metrics on
#                      screen (docs/s, docs/min, docs/h, ETA), a final
#                      summary (per-minute breakdown + time split), and
#                      the whole thing saved to runs/<timestamp>.log
#   make resume        continue an interrupted transfer (same metrics)
#   make metrics       server-side metrics: rows, time range, disk,
#                      parts (ClickHouse) + docs/size (Elasticsearch)
#   make verify        compare counts / time ranges on both sides
#
# Variables (make VAR=value ...):
#   WORKERS      parallel range walkers          (default 8)
#   RANGES       time ranges (0 = auto ~4M docs) (default 0)
#   INSERT_BATCH rows per ClickHouse insert      (default 100000)
#   FETCH_PAGE   documents per ES search         (default 10000)
#   ES_INDEX     elasticsearch index             (default xdp-messages)
#   TABLE        clickhouse table                (default xdp_messages)

SHELL := /bin/bash

BIN          := target/release/click-filler
ES_INDEX     ?= xdp-messages
TABLE        ?= xdp_messages
WORKERS      ?= 8
RANGES       ?= 0
INSERT_BATCH ?= 100000
FETCH_PAGE   ?= 10000
ES_AUTH      := -u elastic-admin:elastic-password
CH_PORT      ?= 8124
CH_PASS      ?= clickhouse-password
CH           := http://localhost:$(CH_PORT)/
CH_AUTH      := -u default:$(CH_PASS)

FLAGS := --es-index $(ES_INDEX) --ch-table $(TABLE) \
         --workers $(WORKERS) --ranges $(RANGES) \
         --insert-batch $(INSERT_BATCH) --fetch-page $(FETCH_PAGE) \
         --ch-port $(CH_PORT) --ch-pass $(CH_PASS)

.DEFAULT_GOAL := transfer

.PHONY: help build release test transfer resume verify metrics table \
        up down logs ch-query clean

help: ## list targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-12s %s\n", $$1, $$2}'

build: ## debug build
	cargo build

release: $(BIN) ## optimized build (implicit for the run targets)

$(BIN): src/*.rs Cargo.toml
	cargo build --release

test: ## unit tests (JSON, HTTP framing, schema, range splitting)
	cargo test

transfer: release ## launch the migration — live metrics + saved run log (this is `make`)
	@mkdir -p runs
	@set -o pipefail; ./$(BIN) $(FLAGS) 2>&1 | tee "runs/transfer-$$(date +%Y%m%d-%H%M%S).log"

resume: release ## continue an interrupted transfer (same metrics, new log)
	@mkdir -p runs
	@set -o pipefail; ./$(BIN) --resume $(FLAGS) 2>&1 | tee "runs/resume-$$(date +%Y%m%d-%H%M%S).log"

verify: release ## document counts + time ranges on both sides
	./$(BIN) --verify --es-index $(ES_INDEX) --ch-table $(TABLE)

metrics: ## server-side metrics: docs, size, rows, range, disk, parts
	@echo "== elasticsearch  $(ES_INDEX)"
	@printf '   docs     '; \
		curl -sfm 5 $(ES_AUTH) "http://localhost:9200/$(ES_INDEX)/_count" \
			| grep -o '"count":[0-9]*' | cut -d: -f2 || echo unreachable
	@printf '   store    '; \
		curl -sfm 5 $(ES_AUTH) "http://localhost:9200/_cat/indices/$(ES_INDEX)?h=store.size" || echo unreachable
	@echo "== clickhouse     $(TABLE)"
	@printf '   rows     '; \
		curl -sfm 5 $(CH_AUTH) "$(CH)" --data-binary "SELECT count() FROM $(TABLE)" \
			|| echo unreachable; echo
	@printf '   range    '; \
		curl -sfm 5 $(CH_AUTH) "$(CH)" --data-binary \
			"SELECT ifNull(concat(toString(min(ts)), '  ..  ', toString(max(ts))), 'empty') FROM $(TABLE)"; echo
	@printf '   disk     '; \
		curl -sfm 5 $(CH_AUTH) "$(CH)" --data-binary \
			"SELECT ifNull(formatReadableSize(sum(bytes_on_disk)), '0 B') FROM system.parts WHERE database = currentDatabase() AND table = '$(TABLE)' AND active"; echo
	@printf '   parts    '; \
		curl -sfm 5 $(CH_AUTH) "$(CH)" --data-binary \
			"SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = '$(TABLE)' AND active"; echo
	@echo "== run logs"
	@ls -t runs/*.log 2>/dev/null | head -3 || echo "   (none yet — run make transfer)"

table: release ## create the ClickHouse table only (no transfer)
	./$(BIN) --create-table-only --ch-table $(TABLE)

up: ## start the optional clickhouse container (port 8124)
	docker compose up -d
	@until curl -sfm 2 http://localhost:$(CH_PORT)/ping >/dev/null 2>&1; do sleep 2; done
	@echo "clickhouse ready on http://localhost:$(CH_PORT) (user default, password $(CH_PASS))"

down: ## stop the container (data volume persists)
	docker compose down

logs: ## clickhouse container logs
	docker compose logs -f clickhouse

ch-query: ## ad hoc SQL: make ch-query Q="SELECT msg_type, count() FROM xdp_messages GROUP BY msg_type"
	curl -sfm 30 $(CH_AUTH) "$(CH)" --data-binary "$(Q)"

clean: ## remove build artifacts (clickhouse data and run logs are untouched)
	cargo clean
