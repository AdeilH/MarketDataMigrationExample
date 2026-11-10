//! A minimal RESP (REdis Serialization Protocol) client — just enough
//! Redis for the symbol table, and nothing else.
//!
//! Like the pipeline it feeds, this speaks the protocol from scratch over
//! a plain [`TcpStream`]: encode a command as a multibulk array, read one
//! reply. Only the reply shapes Redis returns for the commands this tool
//! issues (PING, DEL, HSET, RENAME, HLEN, HGET) are decoded, which keeps
//! the whole client to a page of code — the same no-dependency spirit as
//! the zstd and pcap layers in `pcap-parser`.
//!
//! Not a general Redis library: no pub/sub, no transactions, no cluster
//! redirection, no reconnection. Commands are issued one per round trip;
//! the ~25 uploads a full table needs are not worth pipelining.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::time::Duration;

/// A connected Redis session. Reads and writes go over two handles to the
/// same socket (command/response ping-pong; no concurrent use).
pub struct Redis {
    writer: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Redis {
    /// Connect to `host:port`, with 10 s I/O timeouts so a container that
    /// is down fails fast with a clear error instead of hanging a
    /// pre-market run.
    pub fn connect(host: &str, port: u16) -> std::io::Result<Self> {
        let stream = TcpStream::connect((host, port))?;
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(Duration::from_secs(10)))?;
        stream.set_write_timeout(Some(Duration::from_secs(10)))?;
        let reader = BufReader::new(stream.try_clone()?);
        Ok(Redis { writer: stream, reader })
    }

    /// Send one command, read exactly one reply. `Err` covers both I/O
    /// failures and Redis `-ERR…` replies (returned with their message).
    pub fn command(&mut self, args: &[&str]) -> Result<Reply, String> {
        let wire = encode(args);
        self.writer
            .write_all(&wire)
            .map_err(|e| format!("write failed: {e}"))?;
        Reply::read(&mut self.reader)
    }
}

/// RESP multibulk encoding: `*N\r\n`, then `$len\r\n<bytes>\r\n` per arg.
fn encode(args: &[&str]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for arg in args {
        out.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        out.extend_from_slice(arg.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// One decoded server reply. The variants mirror the five RESP types.
#[derive(Debug, PartialEq)]
pub enum Reply {
    /// `+…` status (PONG, OK, RENAME's reply, …).
    Simple(String),
    /// `:N` integer (HSET's new-field count, DEL's count, …).
    Integer(i64),
    /// `$n\r\n…` bulk string.
    Bulk(Vec<u8>),
    /// `$-1` / `*-1` — a missing bulk or array.
    Nil,
    /// `*n` array of nested replies.
    Array(Vec<Reply>),
}

impl Reply {
    /// Read one reply off `r`, recursing for arrays.
    fn read(r: &mut impl BufRead) -> Result<Reply, String> {
        let line = read_line(r)?;
        let head = line.chars().next().ok_or("empty reply line")?;
        let rest = &line[1..];
        match head {
            '+' => Ok(Reply::Simple(rest.to_string())),
            '-' => Err(rest.to_string()),
            ':' => rest
                .parse::<i64>()
                .map(Reply::Integer)
                .map_err(|_| format!("bad integer reply `{rest}`")),
            '$' => {
                let n: i64 = rest
                    .parse()
                    .map_err(|_| format!("bad bulk length `{rest}`"))?;
                if n < 0 {
                    return Ok(Reply::Nil);
                }
                // Bulk payload plus its trailing CRLF.
                let mut buf = vec![0u8; n as usize + 2];
                r.read_exact(&mut buf)
                    .map_err(|e| format!("short bulk read: {e}"))?;
                buf.truncate(n as usize);
                Ok(Reply::Bulk(buf))
            }
            '*' => {
                let n: i64 = rest
                    .parse()
                    .map_err(|_| format!("bad array length `{rest}`"))?;
                if n < 0 {
                    return Ok(Reply::Nil);
                }
                let mut items = Vec::with_capacity(n as usize);
                for _ in 0..n {
                    items.push(Reply::read(r)?);
                }
                Ok(Reply::Array(items))
            }
            other => Err(format!("unexpected reply type `{other}`")),
        }
    }
}

/// One `\r\n`-terminated line off `r` (terminators stripped).
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
    String::from_utf8(line).map_err(|_| "non-UTF-8 reply".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn encode_multibulk() {
        assert_eq!(
            encode(&["SET", "k", "v"]),
            b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n".to_vec()
        );
        assert_eq!(encode(&[]), b"*0\r\n".to_vec());
    }

    #[test]
    fn parse_each_reply_type() {
        let mut r = Cursor::new(b"+PONG\r\n".to_vec());
        assert_eq!(Reply::read(&mut r).unwrap(), Reply::Simple("PONG".into()));

        let mut r = Cursor::new(b":3\r\n".to_vec());
        assert_eq!(Reply::read(&mut r).unwrap(), Reply::Integer(3));

        let mut r = Cursor::new(b"$5\r\nhello\r\n".to_vec());
        assert_eq!(Reply::read(&mut r).unwrap(), Reply::Bulk(b"hello".to_vec()));

        let mut r = Cursor::new(b"$-1\r\n".to_vec());
        assert_eq!(Reply::read(&mut r).unwrap(), Reply::Nil);

        let mut r = Cursor::new(b"*2\r\n:1\r\n$2\r\nok\r\n".to_vec());
        assert_eq!(
            Reply::read(&mut r).unwrap(),
            Reply::Array(vec![Reply::Integer(1), Reply::Bulk(b"ok".to_vec())])
        );
    }

    #[test]
    fn error_reply_and_bad_wire_are_errors() {
        let mut r = Cursor::new(b"-ERR unknown command\r\n".to_vec());
        assert_eq!(
            Reply::read(&mut r).unwrap_err(),
            "ERR unknown command".to_string()
        );
        let mut r = Cursor::new(b"?junk\r\n".to_vec());
        assert!(Reply::read(&mut r).is_err());
    }

    #[test]
    fn reads_stop_at_reply_boundary() {
        // Two replies in the buffer: the first read consumes exactly one.
        let mut r = Cursor::new(b"+OK\r\n:7\r\n".to_vec());
        assert_eq!(Reply::read(&mut r).unwrap(), Reply::Simple("OK".into()));
        assert_eq!(Reply::read(&mut r).unwrap(), Reply::Integer(7));
    }
}
