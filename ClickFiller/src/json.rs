//! A minimal JSON value, parser and serializer — just what moving
//! documents from Elasticsearch to ClickHouse needs.
//!
//! Same dependency-free spirit as the HTTP client ([`crate::http`]):
//! Elasticsearch answers every call of this tool with one JSON document
//! (a search page, a PIT id, a count), and every inserted row is one
//! flat JSON object. That is the whole surface — no deserialization into
//! typed structs, no streaming, no serde.
//!
//! One deliberate choice: numbers keep their original digit text
//! ([`Json::Num`]) instead of parsing to `f64`. Order ids are `u64` on
//! the feed, and `f64` silently corrupts values past 2^53; keeping the
//! raw text round-trips every number byte-identically through
//! Elasticsearch and into ClickHouse.

/// One JSON value. Objects keep insertion order — document fields are
/// re-emitted in [`crate::schema`]'s column order, so the inserted rows
/// are stable and diffable across runs.
#[derive(Debug, Clone, PartialEq)]
pub enum Json {
    Null,
    Bool(bool),
    /// Number as its original digit text (`"1739999999999"`, `"1.5e3"`).
    Num(String),
    Str(String),
    Arr(Vec<Json>),
    Obj(Vec<(String, Json)>),
}

impl Json {
    /// Object member by key (first match; keys are unique in practice).
    pub fn get(&self, key: &str) -> Option<&Json> {
        match self {
            Json::Obj(members) => members.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    /// `&str` content of a [`Json::Str`].
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Json::Str(s) => Some(s),
            _ => None,
        }
    }

    /// Raw digit text of a [`Json::Num`].
    pub fn as_num_text(&self) -> Option<&str> {
        match self {
            Json::Num(t) => Some(t),
            _ => None,
        }
    }

    /// Compact serialization: no spaces, object order preserved.
    pub fn to_compact(&self) -> String {
        let mut out = String::new();
        self.write_compact(&mut out);
        out
    }

    fn write_compact(&self, out: &mut String) {
        match self {
            Json::Null => out.push_str("null"),
            Json::Bool(true) => out.push_str("true"),
            Json::Bool(false) => out.push_str("false"),
            Json::Num(t) => out.push_str(t),
            Json::Str(s) => write_escaped(s, out),
            Json::Arr(items) => {
                out.push('[');
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    item.write_compact(out);
                }
                out.push(']');
            }
            Json::Obj(members) => {
                out.push('{');
                for (i, (k, v)) in members.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    write_escaped(k, out);
                    out.push(':');
                    v.write_compact(out);
                }
                out.push('}');
            }
        }
    }
}

/// Parse one complete JSON document; trailing non-whitespace is an error.
pub fn parse(text: &str) -> Result<Json, String> {
    let mut p = Parser { bytes: text.as_bytes(), pos: 0 };
    p.skip_ws();
    let value = p.value()?;
    p.skip_ws();
    if p.pos != p.bytes.len() {
        return Err(format!("trailing characters at byte {}", p.pos));
    }
    Ok(value)
}

struct Parser<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.pos += 1;
        }
    }

    fn expect(&mut self, b: u8) -> Result<(), String> {
        if self.peek() == Some(b) {
            self.pos += 1;
            Ok(())
        } else {
            Err(format!("expected `{}` at byte {}", b as char, self.pos))
        }
    }

    fn literal(&mut self, word: &str, value: Json) -> Result<Json, String> {
        if self.bytes[self.pos..].starts_with(word.as_bytes()) {
            self.pos += word.len();
            Ok(value)
        } else {
            Err(format!("invalid literal at byte {}", self.pos))
        }
    }

    fn value(&mut self) -> Result<Json, String> {
        match self.peek() {
            Some(b'{') => self.object(),
            Some(b'[') => self.array(),
            Some(b'"') => Ok(Json::Str(self.string()?)),
            Some(b't') => self.literal("true", Json::Bool(true)),
            Some(b'f') => self.literal("false", Json::Bool(false)),
            Some(b'n') => self.literal("null", Json::Null),
            Some(b'-' | b'0'..=b'9') => self.number(),
            other => Err(format!(
                "unexpected {} at byte {}",
                other.map(|b| b as char).unwrap_or('?'),
                self.pos
            )),
        }
    }

    fn object(&mut self) -> Result<Json, String> {
        self.expect(b'{')?;
        let mut members = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b'}') {
            self.pos += 1;
            return Ok(Json::Obj(members));
        }
        loop {
            self.skip_ws();
            let key = self.string()?;
            self.skip_ws();
            self.expect(b':')?;
            self.skip_ws();
            let value = self.value()?;
            members.push((key, value));
            self.skip_ws();
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b'}') => {
                    self.pos += 1;
                    return Ok(Json::Obj(members));
                }
                _ => return Err(format!("expected `,` or `}}` at byte {}", self.pos)),
            }
        }
    }

    fn array(&mut self) -> Result<Json, String> {
        self.expect(b'[')?;
        let mut items = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b']') {
            self.pos += 1;
            return Ok(Json::Arr(items));
        }
        loop {
            self.skip_ws();
            items.push(self.value()?);
            self.skip_ws();
            match self.peek() {
                Some(b',') => self.pos += 1,
                Some(b']') => {
                    self.pos += 1;
                    return Ok(Json::Arr(items));
                }
                _ => return Err(format!("expected `,` or `]` at byte {}", self.pos)),
            }
        }
    }

    /// One JSON string (the opening quote is expected at the cursor).
    fn string(&mut self) -> Result<String, String> {
        self.expect(b'"')?;
        let mut out = String::new();
        loop {
            match self.peek() {
                None => return Err("unterminated string".to_string()),
                Some(b'"') => {
                    self.pos += 1;
                    return Ok(out);
                }
                Some(b'\\') => {
                    self.pos += 1;
                    match self.peek() {
                        Some(b'"') => out.push('"'),
                        Some(b'\\') => out.push('\\'),
                        Some(b'/') => out.push('/'),
                        Some(b'b') => out.push('\u{0008}'),
                        Some(b'f') => out.push('\u{000C}'),
                        Some(b'n') => out.push('\n'),
                        Some(b'r') => out.push('\r'),
                        Some(b't') => out.push('\t'),
                        Some(b'u') => {
                            self.pos += 1;
                            let hi = self.hex4()?;
                            // Surrogate pair: a high surrogate followed by
                            // \uXXXX low surrogate encodes one astral code point.
                            let ch = if (0xD800..0xDC00).contains(&hi) {
                                if self.bytes[self.pos..].starts_with(b"\\u") {
                                    self.pos += 2;
                                    let lo = self.hex4()?;
                                    if !(0xDC00..0xE000).contains(&lo) {
                                        return Err(format!("bad low surrogate U+{lo:04X}"));
                                    }
                                    let c = 0x10000 + ((hi - 0xD800) << 10) + (lo - 0xDC00);
                                    char::from_u32(c).ok_or("bad surrogate pair")?
                                } else {
                                    return Err(format!("lone high surrogate U+{hi:04X}"));
                                }
                            } else {
                                char::from_u32(hi).ok_or_else(|| format!("bad escape U+{hi:04X}"))?
                            };
                            out.push(ch);
                            continue;
                        }
                        other => {
                            return Err(format!(
                                "bad escape `{}` at byte {}",
                                other.map(|b| b as char).unwrap_or('?'),
                                self.pos
                            ))
                        }
                    }
                    self.pos += 1;
                }
                Some(_) => {
                    // Consume one UTF-8 encoded code point straight from
                    // the buffer (the input is a Rust `String`, but a
                    // length check per byte beats a lossy allocation per
                    // char on megabyte pages).
                    let b = self.bytes[self.pos];
                    let len = match b {
                        0x00..=0x7F => 1,
                        0xC2..=0xDF => 2,
                        0xE0..=0xEF => 3,
                        0xF0..=0xF4 => 4,
                        _ => {
                            return Err(format!(
                                "invalid UTF-8 lead byte {b:#04x} in string at byte {}",
                                self.pos
                            ))
                        }
                    };
                    let end = self.pos + len;
                    if end > self.bytes.len() {
                        return Err("truncated UTF-8 sequence in string".to_string());
                    }
                    let chunk = std::str::from_utf8(&self.bytes[self.pos..end])
                        .map_err(|_| format!("invalid UTF-8 in string at byte {}", self.pos))?;
                    out.push_str(chunk);
                    self.pos = end;
                }
            }
        }
    }

    fn hex4(&mut self) -> Result<u32, String> {
        if self.pos + 4 > self.bytes.len() {
            return Err("truncated \\u escape".to_string());
        }
        let text = std::str::from_utf8(&self.bytes[self.pos..self.pos + 4])
            .map_err(|_| "bad \\u escape".to_string())?;
        let n = u32::from_str_radix(text, 16).map_err(|_| format!("bad \\u escape `{text}`"))?;
        self.pos += 4;
        Ok(n)
    }

    /// Number as its original digit text (JSON grammar: no leading zeros,
    /// optional fraction and exponent).
    fn number(&mut self) -> Result<Json, String> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.pos += 1;
        }
        match self.peek() {
            Some(b'0') => self.pos += 1,
            Some(b'1'..=b'9') => {
                while matches!(self.peek(), Some(b'0'..=b'9')) {
                    self.pos += 1;
                }
            }
            _ => return Err(format!("bad number at byte {}", start)),
        }
        if self.peek() == Some(b'.') {
            self.pos += 1;
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return Err(format!("bad fraction at byte {}", self.pos));
            }
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.pos += 1;
            }
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.pos += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.pos += 1;
            }
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return Err(format!("bad exponent at byte {}", self.pos));
            }
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.pos += 1;
            }
        }
        Ok(Json::Num(
            String::from_utf8_lossy(&self.bytes[start..self.pos]).into_owned(),
        ))
    }
}

/// Serialize a JSON string with the two mandatory escapes plus control
/// characters. Document fields come from the feed's ASCII parser, so this
/// is a correctness guard, not a performance path.
fn write_escaped(s: &str, out: &mut String) {
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{0008}' => out.push_str("\\b"),
            '\u{000C}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_scalars_and_containers() {
        let v = parse(r#"{"a":1,"b":[true,false,null],"c":"x"}"#).unwrap();
        assert_eq!(v.get("a").unwrap().as_num_text(), Some("1"));
        assert_eq!(v.get("c").unwrap().as_str(), Some("x"));
        assert_eq!(
            v.to_compact(),
            r#"{"a":1,"b":[true,false,null],"c":"x"}"#.to_string()
        );
    }

    #[test]
    fn numbers_survive_u64_and_floats_verbatim() {
        // 2^53 < 4242424242424242 — an f64 round trip would corrupt this.
        let v = parse(r#"{"order_id":4242424242424242,"ratio":1.5e-3}"#).unwrap();
        assert_eq!(v.get("order_id").unwrap().as_num_text(), Some("4242424242424242"));
        assert_eq!(v.get("ratio").unwrap().as_num_text(), Some("1.5e-3"));
    }

    #[test]
    fn string_escapes_round_trip() {
        let v = parse(r#""quote\" back\\ slash / tab\t emoji😀""#).unwrap();
        assert_eq!(
            v.as_str().unwrap(),
            "quote\" back\\ slash / tab\t emoji😀"
        );
        assert_eq!(
            v.to_compact(),
            r#""quote\" back\\ slash / tab\t emoji😀""#.to_string()
        );
    }

    #[test]
    fn lone_surrogate_is_rejected() {
        assert!(parse(r#""\ud83d""#).is_err());
        assert!(parse(r#""\ud83dxx""#).is_err());
    }

    #[test]
    fn control_characters_escape_on_output() {
        let v = Json::Str("a\u{0001}b".to_string());
        assert_eq!(v.to_compact(), "\"a\\u0001b\"");
    }

    #[test]
    fn invalid_documents_are_errors() {
        assert!(parse("").is_err());
        assert!(parse("{\"a\":}").is_err());
        assert!(parse("{\"a\":1,}").is_err());
        assert!(parse("[1,2").is_err());
        assert!(parse("{} trailing").is_err());
        assert!(parse("01").is_err());
        assert!(parse("tru").is_err());
        assert!(parse("\"abc").is_err());
    }

    #[test]
    fn search_page_shape_parses() {
        // The exact shapes es.rs consumes.
        let pit = parse(r#"{"id":"abc123=="}"#).unwrap();
        assert_eq!(pit.get("id").unwrap().as_str(), Some("abc123=="));

        let page = parse(
            r#"{"hits":{"total":{"value":61668134,"relation":"eq"},"hits":[
                {"_id":"f:9:0","_source":{"@timestamp":"2023-08-22T13:30:00.123456789Z"},"sort":[1692711000123,42]},
                {"_id":"f:9:1","_source":{"@timestamp":"2023-08-22T13:30:00.123456790Z"},"sort":[1692711000123,43]}]}}"#,
        )
        .unwrap();
        let hits = page.get("hits").unwrap().get("hits").unwrap();
        let Json::Arr(hits) = hits else { panic!() };
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].get("_id").unwrap().as_str(), Some("f:9:0"));
        assert_eq!(
            hits[0].get("sort").unwrap().to_compact(),
            "[1692711000123,42]"
        );
        let total = page
            .get("hits")
            .unwrap()
            .get("total")
            .unwrap()
            .get("value")
            .unwrap()
            .as_num_text()
            .unwrap()
            .parse::<u64>()
            .unwrap();
        assert_eq!(total, 61_668_134);
    }
}
