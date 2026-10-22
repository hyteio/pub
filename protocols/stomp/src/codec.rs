//! STOMP frame encoding and decoding.
//!
//! ```text
//! COMMAND\n
//! name:value\n ...
//! \n
//! body\0
//! ```
//! Bare EOLs between frames are heart-beats. Header values are escaped
//! (`\\ \n \r \c`) from STOMP 1.1 on, except in CONNECT/CONNECTED frames.

use pub_core::{bail_kind, Codec, Decoded, Result};

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Frame {
    pub command: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    /// Send without `content-length` (the body then ends at the first NUL).
    /// ActiveMQ and Artemis treat such a SEND as a JMS TextMessage.
    pub omit_content_length: bool,
}

impl Frame {
    /// A heart-beat: a bare EOL on the wire.
    pub fn heartbeat() -> Self {
        Frame::default()
    }

    pub fn new(command: &str) -> Self {
        Frame {
            command: command.to_string(),
            ..Default::default()
        }
    }

    pub fn header(mut self, name: &str, value: impl Into<String>) -> Self {
        self.headers.push((name.to_string(), value.into()));
        self
    }

    pub fn body(mut self, body: impl Into<Vec<u8>>) -> Self {
        self.body = body.into();
        self
    }

    /// First value for a header; STOMP says the first occurrence wins.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    /// `message` header and body of an ERROR frame, for display.
    pub fn error_text(&self) -> String {
        let body = String::from_utf8_lossy(&self.body);
        let body = body.trim();
        match (self.get("message"), body.is_empty()) {
            (Some(m), true) => m.to_string(),
            (Some(m), false) if body.starts_with(m) => body.to_string(),
            (Some(m), false) => format!("{m}: {body}"),
            (None, false) => body.to_string(),
            (None, true) => "server sent ERROR".to_string(),
        }
    }
}

/// Most header lines accepted in one frame (plus the command line).
pub const MAX_HEADER_LINES: usize = 1024;
/// Longest header line accepted.
pub const MAX_HEADER_LINE: usize = 64 * 1024;

#[derive(Debug)]
pub struct StompCodec {
    /// Header escaping; switched on once a 1.1+ session is negotiated.
    pub escape: bool,
    /// Largest `content-length` accepted, normally the connection's max-frame-size.
    /// Checked as soon as the header arrives, before the body is buffered.
    pub max_frame: usize,
    /// Heart-beats (bare EOLs) seen so far: the session's sign of life.
    pub heartbeats_seen: u64,
    /// Where the search for the NUL ending a body without `content-length`
    /// resumes, so a large body isn't rescanned on every read.
    nul_search_from: usize,
}

impl Default for StompCodec {
    fn default() -> Self {
        StompCodec {
            escape: false,
            max_frame: pub_core::limits::DEFAULT_MAX_FRAME_SIZE,
            heartbeats_seen: 0,
            nul_search_from: 0,
        }
    }
}

fn escapes(command: &str, enabled: bool) -> bool {
    enabled && !matches!(command, "CONNECT" | "STOMP" | "CONNECTED")
}

impl Codec for StompCodec {
    type Frame = Frame;

    fn encode(&mut self, f: &Frame, out: &mut Vec<u8>) -> Result<()> {
        if f.command.is_empty() {
            out.push(b'\n'); // a heart-beat
            return Ok(());
        }
        // NUL ends a STOMP frame: one inside a header would end it early and
        // let the rest be read as another frame.
        if f.command.contains(['\0', '\n', '\r'])
            || f.headers
                .iter()
                .any(|(k, v)| k.contains('\0') || v.contains('\0'))
        {
            bail_kind!(
                Usage,
                "a STOMP header can't contain a NUL byte (nor the command a line break)"
            );
        }
        let esc = escapes(&f.command, self.escape);
        out.extend_from_slice(f.command.as_bytes());
        out.push(b'\n');
        for (k, v) in &f.headers {
            if k.eq_ignore_ascii_case("content-length") {
                continue; // always computed below
            }
            if esc {
                escape_into(k, out);
                out.push(b':');
                escape_into(v, out);
            } else {
                if k.contains([':', '\n', '\r']) || v.contains(['\n', '\r']) {
                    bail_kind!(
                        Usage,
                        "STOMP 1.0 header {k:?} cannot contain newlines or ':' in its name"
                    );
                }
                out.extend_from_slice(format!("{k}:{v}").as_bytes());
            }
            out.push(b'\n');
        }
        if f.omit_content_length {
            if f.body.contains(&0) {
                bail_kind!(
                    Usage,
                    "a STOMP frame without content-length cannot contain NUL bytes"
                );
            }
        } else if !f.body.is_empty() || f.command == "SEND" {
            out.extend_from_slice(format!("content-length:{}\n", f.body.len()).as_bytes());
        }
        out.push(b'\n');
        out.extend_from_slice(&f.body);
        out.push(0);
        Ok(())
    }

    fn decode(&mut self, buf: &[u8]) -> Result<Decoded<Frame>> {
        let eols = buf
            .iter()
            .take_while(|&&b| b == b'\n' || b == b'\r')
            .count();
        if eols > 0 {
            self.heartbeats_seen += 1;
            return Ok(Decoded::Skip(eols));
        }
        if buf.is_empty() {
            return Ok(Decoded::NeedMore);
        }

        // Header block ends at the first empty line.
        let mut lines: Vec<&[u8]> = Vec::new();
        let mut pos = 0;
        loop {
            let Some(nl) = buf[pos..].iter().position(|&b| b == b'\n') else {
                if buf.len() - pos > MAX_HEADER_LINE {
                    bail_kind!(
                        Protocol,
                        "STOMP header line longer than {MAX_HEADER_LINE} bytes"
                    );
                }
                return Ok(Decoded::NeedMore);
            };
            if nl > MAX_HEADER_LINE {
                bail_kind!(
                    Protocol,
                    "STOMP header line longer than {MAX_HEADER_LINE} bytes"
                );
            }
            let line = &buf[pos..pos + nl];
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            pos += nl + 1;
            if line.is_empty() {
                break;
            }
            if lines.len() > MAX_HEADER_LINES {
                bail_kind!(
                    Protocol,
                    "STOMP frame has more than {MAX_HEADER_LINES} header lines"
                );
            }
            lines.push(line);
        }

        let command = utf8(lines[0])?.to_string();
        let esc = escapes(&command, self.escape);
        let mut headers = Vec::with_capacity(lines.len() - 1);
        for line in &lines[1..] {
            let line = utf8(line)?;
            let Some((k, v)) = line.split_once(':') else {
                bail_kind!(Protocol, "malformed STOMP header line {line:?}");
            };
            if esc {
                headers.push((unescape(k)?, unescape(v)?));
            } else {
                headers.push((k.to_string(), v.to_string()));
            }
        }

        let content_length = headers
            .iter()
            .find(|(k, _)| k == "content-length")
            .map(|(_, v)| v.trim().parse::<usize>())
            .transpose()
            .map_err(|_| pub_core::err!(Protocol, "invalid content-length header"))?;

        let body_end = match content_length {
            Some(len) if len >= self.max_frame => bail_kind!(
                Protocol,
                "server announced a {len}-byte STOMP body, over max-frame-size ({}); raise it with -O max-frame-size=...",
                pub_core::limits::human(self.max_frame)
            ),
            Some(len) => {
                if buf.len() < pos + len + 1 {
                    return Ok(Decoded::NeedMore);
                }
                if buf[pos + len] != 0 {
                    bail_kind!(
                        Protocol,
                        "STOMP frame body not NUL-terminated after content-length {len}"
                    );
                }
                pos + len
            }
            None => {
                let from = self.nul_search_from.max(pos);
                match buf[from..].iter().position(|&b| b == 0) {
                    Some(n) => from + n,
                    None => {
                        self.nul_search_from = buf.len();
                        return Ok(Decoded::NeedMore);
                    }
                }
            }
        };
        self.nul_search_from = 0;

        let frame = Frame {
            command,
            headers,
            body: buf[pos..body_end].to_vec(),
            omit_content_length: false,
        };
        Ok(Decoded::Frame(frame, body_end + 1))
    }
}

fn utf8(b: &[u8]) -> Result<&str> {
    std::str::from_utf8(b).map_err(|_| pub_core::err!(Protocol, "STOMP frame header is not UTF-8"))
}

fn escape_into(s: &str, out: &mut Vec<u8>) {
    for c in s.chars() {
        match c {
            '\\' => out.extend_from_slice(b"\\\\"),
            '\n' => out.extend_from_slice(b"\\n"),
            '\r' => out.extend_from_slice(b"\\r"),
            ':' => out.extend_from_slice(b"\\c"),
            c => {
                let mut tmp = [0u8; 4];
                out.extend_from_slice(c.encode_utf8(&mut tmp).as_bytes());
            }
        }
    }
}

fn unescape(s: &str) -> Result<String> {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('\\') => out.push('\\'),
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('c') => out.push(':'),
            other => bail_kind!(
                Protocol,
                "invalid STOMP header escape '\\{}'",
                other.map(String::from).unwrap_or_default()
            ),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enc(codec: &mut StompCodec, f: &Frame) -> Vec<u8> {
        let mut out = Vec::new();
        codec.encode(f, &mut out).unwrap();
        out
    }

    #[test]
    fn a_body_without_content_length_arriving_in_pieces() {
        let mut wire = b"MESSAGE\ndestination:/queue/a\n\n".to_vec();
        wire.extend(std::iter::repeat_n(b'x', 100_000));
        wire.push(0);
        let mut c = StompCodec::default();
        let mut got = None;
        for end in (1..=wire.len()).step_by(16 * 1024).chain([wire.len()]) {
            if let Decoded::Frame(f, n) = c.decode(&wire[..end]).unwrap() {
                got = Some((f, n));
                break;
            }
        }
        let (f, n) = got.expect("frame");
        assert_eq!((f.body.len(), n), (100_000, wire.len()));
        // And the next frame starts from scratch.
        assert!(
            matches!(c.decode(b"MESSAGE\n\nab\0").unwrap(), Decoded::Frame(f, 12) if f.body == b"ab")
        );
    }

    #[test]
    fn nul_bytes_cannot_smuggle_a_frame() {
        for escape in [false, true] {
            let mut c = StompCodec {
                escape,
                ..Default::default()
            };
            let f = Frame::new("SEND")
                .header("destination", "/queue/a")
                .header("x", "v\0SEND\ndestination:/queue/evil\n\nboom");
            let mut out = Vec::new();
            assert!(c.encode(&f, &mut out).is_err());
        }
    }

    #[test]
    fn encodes_send_with_content_length() {
        let f = Frame::new("SEND")
            .header("destination", "/queue/a")
            .header("content-length", "999")
            .body("hi");
        assert_eq!(
            enc(&mut StompCodec::default(), &f),
            b"SEND\ndestination:/queue/a\ncontent-length:2\n\nhi\0"
        );
    }

    #[test]
    fn round_trip_with_escaping() {
        let mut c = StompCodec {
            escape: true,
            ..Default::default()
        };
        let f = Frame::new("MESSAGE")
            .header("weird:key", "a\nb\\c")
            .body(b"\0binary\0".to_vec());
        let bytes = enc(&mut c, &f);
        assert!(bytes.starts_with(b"MESSAGE\nweird\\ckey:a\\nb\\\\c\n"));
        let expected = f.header("content-length", "8");
        assert_eq!(
            c.decode(&bytes).unwrap(),
            Decoded::Frame(expected, bytes.len())
        );
    }

    #[test]
    fn connect_frames_are_not_escaped() {
        let mut c = StompCodec {
            escape: true,
            ..Default::default()
        };
        let bytes = b"CONNECTED\nserver:a\\b\n\n\0";
        match c.decode(bytes).unwrap() {
            Decoded::Frame(f, _) => assert_eq!(f.get("server"), Some("a\\b")),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn partial_frames_and_heartbeats() {
        let mut c = StompCodec::default();
        let full = b"\r\n\nMESSAGE\r\ndestination:/q\r\n\r\nhello\0\n";
        assert_eq!(c.decode(full).unwrap(), Decoded::Skip(3));
        let rest = &full[3..];
        for cut in 0..rest.len() - 2 {
            assert_eq!(
                c.decode(&rest[..cut]).unwrap(),
                Decoded::NeedMore,
                "cut {cut}"
            );
        }
        match c.decode(rest).unwrap() {
            Decoded::Frame(f, n) => {
                assert_eq!(f.get("destination"), Some("/q"));
                assert_eq!(f.body, b"hello");
                assert_eq!(&rest[n..], b"\n");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn header_and_body_limits() {
        let mut c = StompCodec {
            max_frame: 100,
            ..Default::default()
        };
        // Oversized content-length is refused before the body arrives.
        let e = c
            .decode(b"MESSAGE\ncontent-length:1000\n\n")
            .unwrap_err()
            .to_string();
        assert!(e.contains("over max-frame-size (100 bytes)"), "{e}");
        assert_eq!(
            c.decode(b"MESSAGE\ncontent-length:10\n\n").unwrap(),
            Decoded::NeedMore
        );

        let long = vec![b'a'; MAX_HEADER_LINE + 1];
        let mut frame = b"MESSAGE\n".to_vec();
        frame.extend_from_slice(&long);
        assert!(c
            .decode(&frame)
            .unwrap_err()
            .to_string()
            .contains("header line longer"));

        let mut many = b"MESSAGE\n".to_vec();
        for i in 0..=MAX_HEADER_LINES {
            many.extend_from_slice(format!("h{i}:v\n").as_bytes());
        }
        many.extend_from_slice(b"\n\0");
        assert!(c
            .decode(&many)
            .unwrap_err()
            .to_string()
            .contains("more than 1024 header lines"));
    }

    #[test]
    fn rejects_bad_content_length() {
        let mut c = StompCodec::default();
        assert!(c.decode(b"MESSAGE\ncontent-length:2\n\nabc\0").is_err());
    }

    #[test]
    fn error_text() {
        let f = Frame::new("ERROR")
            .header("message", "Bad login")
            .body("details here\n");
        assert_eq!(f.error_text(), "Bad login: details here");
    }

    #[test]
    fn omitting_content_length() {
        let mut c = StompCodec::default();
        let mut out = Vec::new();
        let f = Frame {
            omit_content_length: true,
            ..Frame::new("SEND").header("destination", "/q").body("hi")
        };
        c.encode(&f, &mut out).unwrap();
        assert_eq!(out, b"SEND\ndestination:/q\n\nhi\0");
        let bad = Frame {
            omit_content_length: true,
            ..Frame::new("SEND").body(b"a\0b".to_vec())
        };
        assert!(c.encode(&bad, &mut Vec::new()).is_err());
    }
}
