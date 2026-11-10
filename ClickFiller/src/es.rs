//! The Elasticsearch side of the transfer: health, counts, and the
//! point-in-time + `search_after` pagination that walks every document
//! of the index exactly once.
//!
//! Pagination design (measured against the live 9.2.2 cluster):
//!
//! * Sorting on `_id` is refused — `_id` fielddata is disallowed by
//!   default, and re-enabling it for a 60 M-document index would blow
//!   the heap. The sort is `[@timestamp, _shard_doc]` instead: the PIT
//!   field `_shard_doc` is the recommended tiebreaker and needs no
//!   fielddata.
//! * Within one PIT, pages continue from the previous page's `sort`
//!   values — an exact, duplicate-free walk.
//! * `_shard_doc` values are segment-relative and change between PITs,
//!   so a checkpoint cannot carry them. It carries the boundary
//!   timestamp instead, and resuming seeks from `last_ts - 1 ms`: every
//!   document before that millisecond was already transferred, and the
//!   few refetched boundary documents collapse harmlessly in
//!   ClickHouse's ReplacingMergeTree (see [`crate::schema`]).
//!
//! The wire surface stays minimal: `GET /` (health), `GET /_count`,
//! `POST /{index}/_pit`, `POST /_search`, `DELETE /_pit`.

use crate::http::{base64, percent_encode, Http};
use crate::json::{self, Json};

/// A connected Elasticsearch session with basic-auth credentials baked in.
pub struct Es {
    http: Http,
    index: String,
}

/// One search hit: the document id, its `_source` object, the raw sort
/// values (verbatim text for the next `search_after`), and the boundary
/// timestamp text (first sort value, for the checkpoint).
pub struct Hit {
    pub id: String,
    pub source: Json,
    /// Compact JSON array text, e.g. `[1692711000123,42]`.
    pub sort: String,
    /// First sort value as digit text (epoch millis of `@timestamp`).
    pub ts_text: String,
}

/// One page of the PIT walk.
pub struct Page {
    /// Total documents in the PIT (exact, `track_total_hits`).
    pub total: u64,
    pub hits: Vec<Hit>,
}

impl Es {
    /// Connect to `host:port` as `user:password` (anonymous when both are
    /// empty) and remember which index the transfer reads.
    pub fn connect(
        host: &str,
        port: u16,
        index: &str,
        user: &str,
        password: &str,
    ) -> std::io::Result<Self> {
        let mut headers = Vec::new();
        if !user.is_empty() || !password.is_empty() {
            let encoded = base64(format!("{user}:{password}").as_bytes());
            headers.push(("Authorization".to_string(), format!("Basic {encoded}")));
        }
        Ok(Es {
            http: Http::connect(host, port, headers)?,
            index: index.to_string(),
        })
    }

    fn path(&self, suffix: &str) -> String {
        format!("/{}{suffix}", percent_encode(&self.index))
    }

    /// Health ping: `GET /` must answer 200 with cluster info.
    pub fn ping(&mut self) -> Result<(), String> {
        let (code, body) = self
            .http
            .request("GET", "/", "application/json", b"")
            .map_err(|e| format!("elasticsearch request failed: {e}"))?;
        if code == 200 && body.contains("cluster_name") {
            Ok(())
        } else {
            Err(format!(
                "elasticsearch health check returned HTTP {code}: {}",
                head(&body)
            ))
        }
    }

    /// Document count of the index (`--verify`).
    pub fn count(&mut self) -> Result<u64, String> {
        let (code, body) = self
            .http
            .request("GET", &self.path("/_count"), "application/json", b"")
            .map_err(|e| format!("elasticsearch count failed: {e}"))?;
        if code != 200 {
            return Err(format!("count returned HTTP {code}: {}", head(&body)));
        }
        let parsed = json::parse(&body)?;
        parsed
            .get("count")
            .and_then(|c| c.as_num_text())
            .and_then(|t| t.parse().ok())
            .ok_or_else(|| format!("count response missing `count`: {}", head(&body)))
    }

    /// Oldest (`"asc"`) or newest (`"desc"`) `@timestamp` in the index, as
    /// epoch millis (`--verify`).
    pub fn extreme_ts(&mut self, order: &str) -> Result<u64, String> {
        let body = format!(
            r#"{{"size":1,"_source":false,"track_total_hits":false,"sort":[{{"@timestamp":{{"order":"{order}"}}}}]}}"#
        );
        let (code, resp) = self
            .http
            .request("POST", &self.path("/_search"), "application/json", body.as_bytes())
            .map_err(|e| format!("elasticsearch search failed: {e}"))?;
        if code != 200 {
            return Err(format!("search returned HTTP {code}: {}", head(&resp)));
        }
        let parsed = json::parse(&resp)?;
        let Json::Arr(items) = parsed
            .get("hits")
            .and_then(|h| h.get("hits"))
            .ok_or_else(|| format!("search response missing hits: {}", head(&resp)))?
        else {
            return Err("hits.hits is not an array".to_string());
        };
        // An empty index sorts nothing.
        let Some(first) = items.first() else {
            return Ok(0);
        };
        let sort = first
            .get("sort")
            .ok_or_else(|| "search hit missing `sort`".to_string())?;
        match sort {
            Json::Arr(values) => values
                .first()
                .and_then(|v| v.as_num_text())
                .and_then(|t| t.parse().ok())
                .ok_or_else(|| "sort value is not a number".to_string()),
            _ => Err("sort is not an array".to_string()),
        }
    }

    /// Document counts per time bucket (`date_histogram`), oldest first —
    /// the exact distribution the range splitter balances on. Returns
    /// `(bucket start, epoch ms; document count)` pairs for populated
    /// buckets.
    pub fn ts_histogram(&mut self, interval: &str) -> Result<Vec<(u64, u64)>, String> {
        let body = format!(
            r#"{{"size":0,"track_total_hits":true,"aggs":{{"t":{{"date_histogram":{{"field":"@timestamp","fixed_interval":"{interval}","min_doc_count":0}}}}}}}}"#
        );
        let (code, resp) = self
            .http
            .request("POST", &self.path("/_search"), "application/json", body.as_bytes())
            .map_err(|e| format!("histogram request failed: {e}"))?;
        if code != 200 {
            return Err(format!("histogram returned HTTP {code}: {}", head(&resp)));
        }
        let parsed = json::parse(&resp)?;
        let Json::Arr(buckets) = parsed
            .get("aggregations")
            .and_then(|a| a.get("t"))
            .and_then(|t| t.get("buckets"))
            .ok_or_else(|| format!("histogram response missing buckets: {}", head(&resp)))?
        else {
            return Err("histogram buckets is not an array".to_string());
        };
        let mut out = Vec::with_capacity(buckets.len());
        for bucket in buckets {
            let key = bucket
                .get("key")
                .and_then(|k| k.as_num_text())
                .and_then(|k| k.parse().ok())
                .ok_or_else(|| "histogram bucket missing numeric `key`".to_string())?;
            let count = bucket
                .get("doc_count")
                .and_then(|c| c.as_num_text())
                .and_then(|c| c.parse().ok())
                .ok_or_else(|| "histogram bucket missing `doc_count`".to_string())?;
            out.push((key, count));
        }
        Ok(out)
    }

    /// Open a point-in-time over the index; the walk reads through this
    /// consistent snapshot. Each search refreshes `keep_alive`, so a slow
    /// insert loop cannot let it lapse mid-run.
    pub fn open_pit(&mut self, keep_alive: &str) -> Result<String, String> {
        let path = format!(
            "/{}/_pit?keep_alive={}",
            percent_encode(&self.index),
            percent_encode(keep_alive)
        );
        let (code, body) = self
            .http
            .request("POST", &path, "application/json", b"")
            .map_err(|e| format!("point-in-time request failed: {e}"))?;
        if code != 200 {
            return Err(format!("PIT request returned HTTP {code}: {}", head(&body)));
        }
        let parsed = json::parse(&body)?;
        parsed
            .get("id")
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .ok_or_else(|| format!("PIT response missing `id`: {}", head(&body)))
    }

    /// Fetch the next page: `size` hits continuing at `after` (the raw
    /// `search_after` array text; `None` starts the walk). `filter` is a
    /// raw JSON query object (`{"range":{"@timestamp":...}}`) that scopes
    /// this walk to one worker's time range.
    pub fn search_page(
        &mut self,
        pit: &str,
        keep_alive: &str,
        size: usize,
        after: Option<&str>,
        filter: Option<&str>,
    ) -> Result<Page, String> {
        let mut body = format!(
            r#"{{"size":{size},"track_total_hits":true,"pit":{{"id":{},"keep_alive":{}}},"sort":[{{"@timestamp":"asc"}},{{"_shard_doc":"asc"}}]"#,
            json::Json::Str(pit.to_string()).to_compact(),
            json::Json::Str(keep_alive.to_string()).to_compact(),
        );
        if let Some(filter) = filter {
            body.push_str(r#","query":"#);
            body.push_str(filter);
        }
        if let Some(after) = after {
            body.push_str(r#","search_after":"#);
            body.push_str(after);
        }
        body.push('}');

        let (code, resp) = self
            .http
            .request("POST", "/_search", "application/json", body.as_bytes())
            .map_err(|e| format!("search request failed: {e}"))?;
        if code != 200 {
            return Err(format!("search returned HTTP {code}: {}", head(&resp)));
        }
        parse_page(&resp)
    }

    /// Release the point-in-time. Best effort: a failed close only costs
    /// the cluster some bookkeeping until `keep_alive` expires.
    pub fn close_pit(&mut self, pit: &str) {
        let body = format!(r#"{{"id":{}}}"#, json::Json::Str(pit.to_string()).to_compact());
        let _ = self
            .http
            .request("DELETE", "/_pit", "application/json", body.as_bytes());
    }
}

/// Extract `hits.total.value` and the per-hit (`_id`, `_source`, `sort`)
/// triples from one search response.
fn parse_page(resp: &str) -> Result<Page, String> {
    let parsed = json::parse(resp)?;
    let hits_node = parsed
        .get("hits")
        .ok_or_else(|| format!("search response missing `hits`: {}", head(resp)))?;

    let total = hits_node
        .get("total")
        .and_then(|t| t.get("value"))
        .and_then(|v| v.as_num_text())
        .and_then(|t| t.parse().ok())
        .ok_or_else(|| format!("search response missing `hits.total.value`: {}", head(resp)))?;

    let Json::Arr(items) = hits_node
        .get("hits")
        .ok_or_else(|| format!("search response missing `hits.hits`: {}", head(resp)))?
    else {
        return Err("hits.hits is not an array".to_string());
    };

    let mut hits = Vec::with_capacity(items.len());
    for item in items {
        let id = item
            .get("_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "search hit missing `_id`".to_string())?
            .to_string();
        let source = item
            .get("_source")
            .cloned()
            .ok_or_else(|| format!("search hit `{id}` missing `_source`"))?;
        let sort_values = item
            .get("sort")
            .ok_or_else(|| format!("search hit `{id}` missing `sort`"))?;
        let ts_text = match sort_values {
            Json::Arr(values) => values
                .first()
                .and_then(|v| v.as_num_text())
                .ok_or_else(|| format!("search hit `{id}` has a non-numeric first sort value"))?
                .to_string(),
            _ => return Err(format!("search hit `{id}`: sort is not an array")),
        };
        hits.push(Hit { id, source, sort: sort_values.to_compact(), ts_text });
    }
    Ok(Page { total, hits })
}

/// First 300 chars of a response body, for error messages.
fn head(body: &str) -> String {
    body.chars().take(300).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_parses_hits_ids_sources_and_sorts() {
        let resp = r#"{"hits":{"total":{"value":61668134,"relation":"eq"},"hits":[
            {"_index":"xdp-messages","_id":"a:9:0","_score":null,
             "_source":{"@timestamp":"2023-08-22T13:30:00.123456789Z","msg_type_id":100},
             "sort":[1692711000123,42]},
            {"_index":"xdp-messages","_id":"a:9:1","_score":null,
             "_source":{"@timestamp":"2023-08-22T13:30:00.123999999Z","msg_type_id":102},
             "sort":[1692711000123,43]}]}}"#;
        let page = parse_page(resp).unwrap();
        assert_eq!(page.total, 61_668_134);
        assert_eq!(page.hits.len(), 2);
        assert_eq!(page.hits[0].id, "a:9:0");
        assert_eq!(page.hits[0].ts_text, "1692711000123");
        assert_eq!(page.hits[1].sort, "[1692711000123,43]");
        assert_eq!(
            page.hits[0]
                .source
                .get("@timestamp")
                .unwrap()
                .as_str()
                .unwrap(),
            "2023-08-22T13:30:00.123456789Z"
        );
    }

    #[test]
    fn malformed_pages_are_errors() {
        assert!(parse_page(r#"{"error":"boom"}"#).is_err());
        assert!(parse_page(r#"{"hits":{"hits":[]}}"#).is_err()); // no total
        assert!(parse_page(
            r#"{"hits":{"total":{"value":1,"relation":"eq"},"hits":[{"_source":{}}]}}"#
        )
        .is_err()); // no _id
    }
}
