//! A minimal HTTP/1.1 client — just enough for Elasticsearch and
//! ClickHouse, side by side in one transfer tool.
//!
//! Adapted from elastic-filler's `es.rs`: hand-built requests over a
//! plain [`TcpStream`], parse the reply. The client speaks exactly what
//! the transfer pipeline needs — GET for health checks, POST for
//! searches, PITs and inserts — and nothing else (no TLS, no
//! compression, no retries; those live at the operator layer).
//!
//! Auth headers are baked in at connect time: Elasticsearch wants
//! `Authorization: Basic ...`, ClickHouse wants `X-ClickHouse-User` /
//! `X-ClickHouse-Key`.
//!
//! Responses are handled in all three framing shapes the servers use:
//! `Content-Length` bodies, `Transfer-Encoding: chunked`, and
//! connection-close. The connection is kept alive across requests.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// A connected HTTP session with auth headers baked in.
pub struct Http {
    writer: TcpStream,
    reader: BufReader<TcpStream>,
    host_header: String,
    /// Sent verbatim with every request (`Name: value` lines).
    headers: Vec<(String, String)>,
    /// Saved so a dead connection can be transparently re-established.
    addr: (String, u16),
}

/// Why an attempt failed. `Dead` = the connection broke before a response
/// started (the request may or may not have been processed; safe to
/// reconnect and resend once — both sides of this tool are idempotent:
/// Elasticsearch document ids are deterministic upstream, and ClickHouse
/// collapses duplicate `ts`+`es_id` rows in [`crate::schema`]'s
/// ReplacingMergeTree). `Protocol` = the server answered badly; resending
/// would just repeat the problem.
enum Fail {
    Dead(String),
    Protocol(String),
}

impl Http {
    /// Connect to `host:port` with `headers` sent on every request.
    /// 30 s I/O timeouts: a page fetch or a multi-megabyte insert can
    /// legitimately take a moment, but a hung server must not stall a
    /// transfer run forever.
    pub fn connect(host: &str, port: u16, headers: Vec<(String, String)>) -> std::io::Result<Self> {
        let writer = Self::dial(host, port)?;
        let reader = BufReader::new(writer.try_clone()?);
        Ok(Http {
            writer,
            reader,
            host_header: format!("{host}:{port}"),
            headers,
            addr: (host.to_string(), port),
        })
    }

    /// Fresh TCP connection with the standard timeouts.
    fn dial(host: &str, port: u16) -> std::io::Result<TcpStream> {
        let stream = TcpStream::connect((host, port))?;
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(Duration::from_secs(30)))?;
        stream.set_write_timeout(Some(Duration::from_secs(30)))?;
        Ok(stream)
    }

    fn reconnect(&mut self) -> Result<(), String> {
        let writer = Self::dial(&self.addr.0, self.addr.1)
            .map_err(|e| format!("reconnect to {}:{} failed: {e}", self.addr.0, self.addr.1))?;
        let reader = BufReader::new(
            writer
                .try_clone()
                .map_err(|e| format!("reconnect failed: {e}"))?,
        );
        self.writer = writer;
        self.reader = reader;
        Ok(())
    }

    /// One request/response round trip. A connection that dies before a
    /// response starts is re-established and the request resent once.
    pub fn request(
        &mut self,
        method: &str,
        path: &str,
        content_type: &str,
        body: &[u8],
    ) -> Result<(u16, String), String> {
        match self.attempt(method, path, content_type, body) {
            Ok(reply) => Ok(reply),
            Err(Fail::Dead(why)) => {
                self.reconnect().map_err(|e| format!("{why}; {e}"))?;
                self.attempt(method, path, content_type, body)
                    .map_err(|fail| match fail {
                        Fail::Dead(why) | Fail::Protocol(why) => why,
                    })
            }
            Err(Fail::Protocol(why)) => Err(why),
        }
    }

    fn attempt(
        &mut self,
        method: &str,
        path: &str,
        content_type: &str,
        body: &[u8],
    ) -> Result<(u16, String), Fail> {
        let mut req = format!(
            "{method} {path} HTTP/1.1\r\nHost: {}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n",
            self.host_header,
            body.len()
        )
        .into_bytes();
        for (name, value) in &self.headers {
            req.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
        }
        req.extend_from_slice(b"\r\n");
        req.extend_from_slice(body);
        self.writer
            .write_all(&req)
            .map_err(|e| Fail::Dead(format!("write failed: {e}")))?;
        self.read_response()
    }

    /// Read one full HTTP response: status line, headers, then the body in
    /// whatever framing the server picked.
    fn read_response(&mut self) -> Result<(u16, String), Fail> {
        let status = read_line(&mut self.reader)
            .map_err(|e| Fail::Dead(format!("connection died before status line: {e}")))?;
        let code: u16 = status
            .split_whitespace()
            .nth(1)
            .and_then(|c| c.parse().ok())
            .ok_or_else(|| {
                // Empty line = clean EOF: the peer closed the (idle or
                // reset) connection without answering.
                Fail::Dead(format!("bad status line `{status}`"))
            })?;

        let mut content_length: Option<usize> = None;
        let mut chunked = false;
        loop {
            let line = read_line(&mut self.reader)
                .map_err(|e| Fail::Dead(format!("connection died in headers: {e}")))?;
            if line.is_empty() {
                break; // end of headers
            }
            let Some((name, value)) = line.split_once(':') else { continue };
            let name = name.trim().to_ascii_lowercase();
            let value = value.trim();
            if name == "content-length" {
                content_length = value.parse().ok();
            } else if name == "transfer-encoding" && value.eq_ignore_ascii_case("chunked") {
                chunked = true;
            }
        }

        let body = if let Some(n) = content_length {
            let mut buf = vec![0u8; n];
            self.reader
                .read_exact(&mut buf)
                .map_err(|e| Fail::Protocol(format!("short body read: {e}")))?;
            buf
        } else if chunked {
            read_chunked_body(&mut self.reader).map_err(Fail::Protocol)?
        } else {
            // No framing information: server will close the connection.
            let mut buf = Vec::new();
            self.reader
                .read_to_end(&mut buf)
                .map_err(|e| Fail::Protocol(format!("body read failed: {e}")))?;
            buf
        };
        Ok((code, String::from_utf8_lossy(&body).into_owned()))
    }
}

/// De-chunk a `Transfer-Encoding: chunked` body: repeated
/// `<hex-size>[;ext]\r\n <bytes>\r\n`, terminated by a zero-size chunk
/// (trailers after it are discarded). A free function over any reader so
/// tests need no socket.
fn read_chunked_body(r: &mut impl BufRead) -> Result<Vec<u8>, String> {
    let mut body = Vec::new();
    loop {
        let size_line = read_line(r)?;
        let size_txt = size_line.split(';').next().unwrap_or("").trim();
        let n = usize::from_str_radix(size_txt, 16)
            .map_err(|_| format!("bad chunk size `{size_line}`"))?;
        if n == 0 {
            // Trailer section: read up to and including the final CRLF.
            loop {
                if read_line(r)?.is_empty() {
                    break;
                }
            }
            return Ok(body);
        }
        let mut buf = vec![0u8; n + 2]; // data + trailing CRLF
        r.read_exact(&mut buf)
            .map_err(|e| format!("short chunk read: {e}"))?;
        buf.truncate(n);
        body.extend_from_slice(&buf);
    }
}

/// One `\r\n`-terminated line (terminators stripped).
fn read_line(r: &mut impl BufRead) -> Result<String, String> {
    let mut line = Vec::new();
    r.read_until(b'\n', &mut line)
        .map_err(|e| format!("read failed: {e}"))?;
    if line.ends_with(b"\n") {
        line.pop();
    }
    if line.ends_with(b"\r") {
        line.pop();
    }
    Ok(String::from_utf8_lossy(&line).into_owned())
}

/// Standard base64 (RFC 4648, with padding) — enough for an
/// `Authorization: Basic` header.
pub fn base64(input: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b1 = chunk[0];
        let b2 = chunk.get(1).copied().unwrap_or(0);
        let b3 = chunk.get(2).copied().unwrap_or(0);
        let n = (u32::from(b1) << 16) | (u32::from(b2) << 8) | u32::from(b3);
        out.push(T[(n >> 18 & 63) as usize] as char);
        out.push(T[(n >> 12 & 63) as usize] as char);
        out.push(if chunk.len() > 1 { T[(n >> 6 & 63) as usize] as char } else { '=' });
        out.push(if chunk.len() > 2 { T[(n & 63) as usize] as char } else { '=' });
    }
    out
}

/// Percent-encode a query string for a URL path/query component. Anything
/// outside the unreserved set is escaped, so table names, SQL and
/// settings survive the URL intact.
pub fn percent_encode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for &b in text.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_known_values() {
        assert_eq!(base64(b""), "");
        assert_eq!(base64(b"f"), "Zg==");
        assert_eq!(base64(b"fo"), "Zm8=");
        assert_eq!(base64(b"foo"), "Zm9v");
        // The exact header this tool sends against the local cluster.
        assert_eq!(
            base64(b"elastic-admin:elastic-password"),
            "ZWxhc3RpYy1hZG1pbjplbGFzdGljLXBhc3N3b3Jk"
        );
    }

    #[test]
    fn chunked_body_decode() {
        let wire: &[u8] = b"5\r\n{\"a\":\r\n4\r\n123}\r\n0\r\nX-Trailer: y\r\n\r\n";
        let mut r = BufReader::new(wire);
        assert_eq!(
            read_chunked_body(&mut r).unwrap(),
            b"{\"a\":123}".to_vec()
        );
    }

    #[test]
    fn percent_encoding_covers_sql_and_params() {
        assert_eq!(percent_encode("INSERT INTO x"), "INSERT%20INTO%20x");
        assert_eq!(percent_encode("a=b&c"), "a%3Db%26c");
        // Unreserved characters stay readable.
        assert_eq!(percent_encode("xdp_messages-1.2~"), "xdp_messages-1.2~");
    }
}
