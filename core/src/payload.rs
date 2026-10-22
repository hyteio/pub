//! Payload and header helpers shared by all modules.

use std::fs;
use std::io::{self, IsTerminal, Read};
use std::path::Path;

use crate::error::Result;
use crate::limits::human;
use crate::{bail_kind, err};

/// Parse `-H "Name: value"`.
pub fn parse_header(s: &str) -> Result<(String, String)> {
    let (k, v) = s
        .split_once(':')
        .ok_or_else(|| err!(Usage, "header '{s}' must look like 'Name: value'"))?;
    let k = k.trim();
    if k.is_empty() {
        bail_kind!(Usage, "header '{s}' has an empty name");
    }
    Ok((k.to_string(), v.trim_start().to_string()))
}

/// Parse `key=value`, keeping the key's spelling (destination options are case-sensitive).
pub fn parse_kv(s: &str) -> Result<(String, String)> {
    let (k, v) = s
        .split_once('=')
        .ok_or_else(|| err!(Usage, "'{s}' must look like key=value"))?;
    let k = k.trim();
    if k.is_empty() {
        bail_kind!(Usage, "'{s}' has an empty key");
    }
    Ok((k.to_string(), v.to_string()))
}

/// Load the payload curl-style:
/// `-d text`, `-d @file`, `-d @-` / `-f -` (stdin), `-f file`, or piped stdin when neither is given.
/// At most `max` bytes are read; a larger payload is an error.
pub fn load_payload(data: Option<&str>, file: Option<&Path>, max: usize) -> Result<Vec<u8>> {
    let payload = match (data, file) {
        (Some(_), Some(_)) => bail_kind!(Usage, "use either -d/--data or -f/--file, not both"),
        (Some(d), None) => match d.strip_prefix('@') {
            Some("-") => read_limited(io::stdin(), "stdin", max)?,
            Some(path) => read_file(Path::new(path), max)?,
            None => d.as_bytes().to_vec(),
        },
        (None, Some(f)) if f == Path::new("-") => read_limited(io::stdin(), "stdin", max)?,
        (None, Some(f)) => read_file(f, max)?,
        (None, None) if !io::stdin().is_terminal() => read_limited(io::stdin(), "stdin", max)?,
        (None, None) => bail_kind!(
            Usage,
            "no payload: use -d <data>, -d @<file>, -f <file>, or pipe to stdin"
        ),
    };
    if payload.len() > max {
        bail_kind!(
            Usage,
            "payload is larger than max-payload ({}); raise it with -O max-payload=...",
            human(max)
        );
    }
    Ok(payload)
}

/// The send input as a stream, chosen like [`load_payload`]: `-d DATA`,
/// `-d @FILE`, `-d @-`, `-f FILE`, `-f -`, or a piped stdin. Formats read it
/// and bound what they take.
pub fn open_input(data: Option<&str>, file: Option<&Path>) -> Result<Box<dyn Read>> {
    let open = |path: &Path| -> Result<Box<dyn Read>> {
        if path == Path::new("-") {
            return Ok(Box::new(io::stdin()));
        }
        let f =
            fs::File::open(path).map_err(|e| err!(Io, "cannot read {}: {e}", path.display()))?;
        Ok(Box::new(f))
    };
    match (data, file) {
        (Some(_), Some(_)) => bail_kind!(Usage, "use either -d/--data or -f/--file, not both"),
        (Some(d), None) => match d.strip_prefix('@') {
            Some(path) => open(Path::new(path)),
            None => Ok(Box::new(io::Cursor::new(d.as_bytes().to_vec()))),
        },
        (None, Some(f)) => open(f),
        (None, None) if !io::stdin().is_terminal() => Ok(Box::new(io::stdin())),
        (None, None) => bail_kind!(
            Usage,
            "no payload: use -d <data>, -d @<file>, -f <file>, or pipe to stdin"
        ),
    }
}

/// Read a file (or stdin for `-`), refusing more than `max` bytes.
pub fn read_input(path: &Path, max: usize) -> Result<Vec<u8>> {
    let data = if path == Path::new("-") {
        read_limited(io::stdin(), "stdin", max)?
    } else {
        read_file(path, max)?
    };
    if data.len() > max {
        bail_kind!(Usage, "{} is larger than {}", path.display(), human(max));
    }
    Ok(data)
}

fn read_file(path: &Path, max: usize) -> Result<Vec<u8>> {
    let f = fs::File::open(path).map_err(|e| err!(Io, "cannot read {}: {e}", path.display()))?;
    read_limited(f, &path.display().to_string(), max)
}

/// Read at most `max + 1` bytes, so an oversized input is detected without
/// reading (or buffering) all of it.
fn read_limited(r: impl Read, what: &str, max: usize) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    r.take((max as u64).saturating_add(1))
        .read_to_end(&mut buf)
        .map_err(|e| err!(Io, "cannot read {what}: {e}"))?;
    Ok(buf)
}

/// True when bytes are UTF-8 text without unusual control characters.
pub fn is_printable(bytes: &[u8]) -> bool {
    match std::str::from_utf8(bytes) {
        Ok(s) => s
            .chars()
            .all(|c| !c.is_control() || matches!(c, '\n' | '\r' | '\t' | '\0')),
        Err(e) => e.error_len().is_none() && is_printable(&bytes[..e.valid_up_to()]), // cut mid-char
    }
}

/// Hex dump, 16 bytes per line.
pub fn hexdump(bytes: &[u8]) -> String {
    let mut s = String::new();
    for (i, chunk) in bytes.chunks(16).enumerate() {
        s.push_str(&format!("{:08x}  ", i * 16));
        for j in 0..16 {
            match chunk.get(j) {
                Some(b) => s.push_str(&format!("{b:02x} ")),
                None => s.push_str("   "),
            }
            if j == 7 {
                s.push(' ');
            }
        }
        s.push_str(" |");
        s.extend(chunk.iter().map(|&b| {
            if b.is_ascii_graphic() || b == b' ' {
                b as char
            } else {
                '.'
            }
        }));
        s.push_str("|\n");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn headers() {
        assert_eq!(
            parse_header("X-Id:  42").unwrap(),
            ("X-Id".into(), "42".into())
        );
        assert_eq!(parse_header("url: http://x").unwrap().1, "http://x");
        assert!(parse_header("nocolon").is_err());
        assert_eq!(
            parse_kv("Vhost=a=b").unwrap(),
            ("Vhost".into(), "a=b".into())
        );
    }

    #[test]
    fn payload_sources() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("p.bin");
        fs::write(&f, b"\x00\x01").unwrap();
        assert_eq!(load_payload(Some("hi"), None, 64).unwrap(), b"hi");
        assert_eq!(
            load_payload(Some(&format!("@{}", f.display())), None, 64).unwrap(),
            b"\x00\x01"
        );
        assert_eq!(load_payload(None, Some(&f), 64).unwrap(), b"\x00\x01");
        assert!(load_payload(Some("a"), Some(&f), 64).is_err());
        let e = load_payload(None, Some(&f), 1).unwrap_err().to_string();
        assert!(e.contains("larger than max-payload (1 bytes)"), "{e}");
        assert!(load_payload(Some("abc"), None, 2).is_err());
    }

    #[test]
    fn printable_and_hex() {
        assert!(is_printable(b"SEND\ndestination:/q\n\nhi\0"));
        assert!(!is_printable(b"\x01\x02"));
        assert!(hexdump(b"ABC").starts_with("00000000  41 42 43"));
    }
}
