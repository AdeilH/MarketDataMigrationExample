//! The ClickHouse side of the transfer: health, DDL, row inserts and the
//! `--verify` statistics — all over the HTTP interface (port 8123).
//!
//! Same dependency-free spirit as [`crate::es`]: ClickHouse's HTTP
//! protocol is "the request line is the SQL", which needs nothing the
//! shared [`crate::http`] client doesn't already speak. Auth rides in
//! the `X-ClickHouse-User` / `X-ClickHouse-Key` headers rather than the
//! URL, so credentials don't land in access logs.

use crate::http::{percent_encode, Http};
use crate::schema;

/// A connected ClickHouse session with credentials baked in.
pub struct ClickHouse {
    http: Http,
}

/// Input settings for the JSONEachRow inserts:
///
/// * `date_time_input_format=best_effort` — parse the documents'
///   nanosecond ISO-8601 timestamps (`2023-08-22T13:30:00.123456789Z`);
///   the default parser only takes seconds precision.
/// * `input_format_null_as_default=1` — explicit `null` / absent keys on
///   non-Nullable columns fall back to the column DEFAULT (schema.rs
///   drops those keys already; this is the safety net).
/// * `input_format_skip_unknown_fields=1` — if the table predates a
///   column added to [`schema::COLUMNS`], the extra row key is ignored
///   instead of failing every insert after an upgrade.
const INSERT_SETTINGS: &str = "date_time_input_format=best_effort\
     &input_format_null_as_default=1\
     &input_format_skip_unknown_fields=1";

impl ClickHouse {
    pub fn connect(host: &str, port: u16, user: &str, password: &str) -> std::io::Result<Self> {
        let mut headers = vec![("X-ClickHouse-User".to_string(), user.to_string())];
        if !password.is_empty() {
            headers.push(("X-ClickHouse-Key".to_string(), password.to_string()));
        }
        Ok(ClickHouse {
            http: Http::connect(host, port, headers)?,
        })
    }

    /// Health ping: `GET /ping` answers 200 with `Ok.\n`.
    pub fn ping(&mut self) -> Result<(), String> {
        let (code, body) = self
            .http
            .request("GET", "/ping", "text/plain", b"")
            .map_err(|e| format!("clickhouse request failed: {e}"))?;
        if code == 200 && body.starts_with("Ok.") {
            Ok(())
        } else {
            Err(format!(
                "clickhouse health check returned HTTP {code}: {}",
                head(&body)
            ))
        }
    }

    /// One SQL statement, response body back as text.
    pub fn exec(&mut self, query: &str) -> Result<String, String> {
        let (code, body) = self
            .http
            .request("POST", "/", "text/plain; charset=UTF-8", query.as_bytes())
            .map_err(|e| format!("clickhouse query failed: {e}"))?;
        if code == 200 {
            Ok(body)
        } else {
            Err(format!(
                "clickhouse query returned HTTP {code}: {}",
                head(&body)
            ))
        }
    }

    /// Create the transfer table if it does not exist (idempotent).
    pub fn ensure_table(&mut self, table: &str) -> Result<(), String> {
        self.exec(&schema::create_table_ddl(table)).map(|_| ())
    }

    /// Insert one batch of projected rows (`FORMAT JSONEachRow`): the
    /// INSERT statement goes percent-encoded into the query string, the
    /// newline-delimited rows into the body.
    pub fn insert_rows(&mut self, table: &str, rows: &[String]) -> Result<(), String> {
        let statement = format!("INSERT INTO {table} FORMAT JSONEachRow");
        let path = format!(
            "/?query={}&{}",
            percent_encode(&statement),
            INSERT_SETTINGS
        );
        let mut body = String::with_capacity(rows.iter().map(|r| r.len() + 1).sum::<usize>() + 1);
        for row in rows {
            body.push_str(row);
            body.push('\n');
        }
        let (code, resp) = self
            .http
            .request("POST", &path, "text/plain; charset=UTF-8", body.as_bytes())
            .map_err(|e| format!("clickhouse insert failed: {e}"))?;
        if code == 200 {
            Ok(())
        } else {
            Err(format!(
                "clickhouse insert returned HTTP {code}: {}",
                head(&resp)
            ))
        }
    }

    /// `(row count, oldest ts, newest ts)` for `--verify`. Empty table ->
    /// `(0, None, None)`.
    pub fn stats(&mut self, table: &str) -> Result<(u64, Option<String>, Option<String>), String> {
        let query = format!(
            "SELECT count(), min(ts), max(ts) FROM {table} FORMAT TSV"
        );
        let body = self.exec(&query)?;
        let mut fields = body.trim_end_matches('\n').split('\t');
        let count = fields
            .next()
            .and_then(|c| c.parse().ok())
            .ok_or_else(|| format!("unexpected stats response `{}`", head(&body)))?;
        let bound = |text: Option<&str>| {
            text.filter(|t| !t.is_empty() && *t != "\\N").map(str::to_string)
        };
        Ok((count, bound(fields.next()), bound(fields.next())))
    }
}

/// First 300 chars of a response body, for error messages.
fn head(body: &str) -> String {
    body.chars().take(300).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_settings_are_stable() {
        // The settings ride in the URL; they must stay a valid query
        // string (no stray whitespace from the const layout).
        assert!(!INSERT_SETTINGS.contains(' '));
        assert!(INSERT_SETTINGS.starts_with("date_time_input_format="));
    }
}
