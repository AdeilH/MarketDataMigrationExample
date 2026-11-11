# elastic-filler — the two-pass pipeline in one place.
#
#   make symbols        pass 1: every capture -> symbol table in Redis
#   make messages       pass 2: non-symbol messages -> Elasticsearch
#
# Variables (make VAR=value ...):
#   MAX_FILES  how many captures `messages` ingests (default 10)
#   THREADS    worker count for `messages` (default: one per core)
#   DATA       capture directory (default ./data)
#   ES_INDEX   elasticsearch index name (default xdp-messages)

BIN       := target/release/elastic-filler
DATA      ?= data
MAX_FILES ?= 10
THREADS   ?=
ES_INDEX  ?= xdp-messages
# Databento sample captures: NYSE American XDP, pillar a+b channels,
# 2023-08-22 — a 3.9 GB zip holding the 242 .pcap.zst files flat.
CAPTURES_ZIP := https://sample-pcaps-dl.databento.com/xase/20230822/ny4-xase-pillar-ab-20230822.zip

.PHONY: help build release test symbols messages messages-all \
        up down data status es-count es-size clean

help: ## list targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-14s %s\n", $$1, $$2}'

build: ## debug build
	cargo build

release: $(BIN) ## optimized build (implicit for the pass targets)

$(BIN): src/*.rs Cargo.toml
	cargo build --release

test: ## unit tests (RESP, HTTP, docs, merge, queue)
	cargo test

symbols: release up ## pass 1: ALL captures -> symbol table in Redis (~1 min)
	./$(BIN) --populate-symbols $(DATA)

messages: release up ## pass 2: first MAX_FILES captures -> Elasticsearch (THREADS=N caps workers)
	./$(BIN) --store-messages --max-files $(MAX_FILES) $(if $(THREADS),--threads $(THREADS),) $(DATA)

messages-all: release up ## pass 2 on ALL captures (~15 GB in Elasticsearch)
	./$(BIN) --store-messages $(if $(THREADS),--threads $(THREADS),) $(DATA)

up: ## start the stack: redis + elasticsearch + kibana
	docker compose up -d
	docker compose up setup 2>&1 | grep -o 'kibana_system ready' || true

down: ## stop the stack (data volumes persist)
	docker compose down

data: ## download + extract the 242 sample captures into ./data (~3.9 GB)
	mkdir -p $(DATA)
	@if test -e "$(DATA)/ny4-xase-pillar-a-20230822T000000.pcap.zst"; then \
		echo "captures already present in $(DATA) — skipping download"; \
	else \
		echo "downloading $(CAPTURES_ZIP)"; \
		curl -fL -o /tmp/ny4-xase-pillar-ab-20230822.zip $(CAPTURES_ZIP) && \
		unzip -o /tmp/ny4-xase-pillar-ab-20230822.zip -d $(DATA) && \
		rm -f /tmp/ny4-xase-pillar-ab-20230822.zip && \
		echo "extracted $$(ls $(DATA)/ny4-xase-pillar-*.pcap.zst | wc -l) captures into $(DATA)"; \
	fi

status: ## symbol table + index at a glance
	@echo "== redis: $(shell docker compose exec -T redis redis-cli HLEN xdp:symbols 2>/dev/null || echo down) symbols"
	@docker compose exec -T redis redis-cli HGETALL xdp:symbols:meta 2>/dev/null || true
	@echo "== elasticsearch: $(shell curl -s -m 3 -u elastic-admin:elastic-password 'http://localhost:9200/$(ES_INDEX)/_count' 2>/dev/null | grep -o '"count":[0-9]*' || echo unreachable) documents in $(ES_INDEX)"

es-count: ## document count in the index
	curl -s -u elastic-admin:elastic-password 'http://localhost:9200/$(ES_INDEX)/_count'

es-size: ## index size + biggest indices + node disk
	@echo "== total (primaries):"
	@curl -s -u elastic-admin:elastic-password 'http://localhost:9200/_stats/store' | \
		python3 -c "import json,sys; s=json.load(sys.stdin)['_all']['primaries']['store']; print(f'{s[\"size_in_bytes\"]/1e9:.2f} GB')"
	@echo "== biggest indices:"
	@curl -s -u elastic-admin:elastic-password \
		'http://localhost:9200/_cat/indices?expand_wildcards=all&bytes=gb&s=store.size:desc&h=index,store.size,docs.count' | head -8
	@echo "== node disk:"
	@curl -s -u elastic-admin:elastic-password 'http://localhost:9200/_cat/allocation?v'

# ClickHouse `SELECT active, count(), sum(bytes_on_disk) ... FROM system.parts
# GROUP BY active` equivalent: shards = active rows, segments = parts.
es-parts: ## system.parts-equivalent storage report for $(ES_INDEX)
	@echo "active  parts  on_disk   docs"
	@curl -s -u elastic-admin:elastic-password \
		'http://localhost:9200/_cat/shards/$(ES_INDEX)?h=prirep,state' | \
		awk '{printf "%-7s ", ($$1=="p" && $$2=="STARTED")?"true":"false"}'
	@curl -s -u elastic-admin:elastic-password \
		'http://localhost:9200/_cat/segments/$(ES_INDEX)?bytes=b&h=segment' | wc -l | awk '{printf "%-6d ", $$1}'
	@curl -s -u elastic-admin:elastic-password \
		'http://localhost:9200/_cat/segments/$(ES_INDEX)?bytes=b&h=size' | \
		awk '{s+=$$1} END {printf "%-9.1fGiB ", s/2**30}'
	@curl -s -u elastic-admin:elastic-password \
		'http://localhost:9200/$(ES_INDEX)/_stats/docs' | \
		python3 -c "import json,sys; d=json.load(sys.stdin)['_all']['primaries']['docs']['count']; print(f'{d:,}')"
	@echo "-- on-disk total (incl. not-in-segment metadata):"
	@curl -s -u elastic-admin:elastic-password \
		'http://localhost:9200/$(ES_INDEX)/_stats/store' | \
		python3 -c "import json,sys; s=json.load(sys.stdin)['_all']['primaries']['store']['size_in_bytes']; print(f'   {s:,} bytes = {s/2**30:.1f} GiB')"
	@echo "-- (no uncompressed equivalent: Lucene always stores compressed;"
	@echo "--    per-field disk usage: GET $(ES_INDEX)/_disk_usage?run_expensive_tasks=true)"

clean: ## remove build artifacts (redis volume is untouched)
	cargo clean
