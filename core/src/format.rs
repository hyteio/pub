//! Message formats: how `receive` writes messages and how `send` reads them.
//!
//! A format serializes pub's protocol-neutral [`Message`], so every format
//! works with every protocol (receive from MQTT as JSON, send a JSON capture
//! over OpenWire). Formats are registered like protocol modules
//! ([`crate::Registry::register_format`]); `raw` and `text` are built in,
//! others live in `formats/<name>/` crates.
//!
//! - writers get `begin` / `write` per message / `finish`, so formats that need
//!   a document wrapper (an XML root element) can have one; they flush after
//!   each message so `recv -n 0` streams;
//! - readers yield [`Record`]s one at a time and must bound what they buffer
//!   ([`FormatContext::document_limit`]).

use std::io::{Read, Write};

use crate::endpoint::Endpoint;
use crate::limits::Limits;
use crate::message::Message;
use crate::settings::Setting;
use crate::{bail_kind, err, Result};

/// The default format: bodies only.
pub const DEFAULT_FORMAT: &str = "raw";

/// What a format reader or writer may consult.
pub struct FormatContext<'a> {
    /// Resolved settings, for the format's own options (`format.json.pretty`).
    pub ep: &'a Endpoint,
    pub limits: Limits,
    /// Make sure each body ends with a newline (`-l`, or stdout is a terminal).
    pub lines: bool,
}

impl FormatContext<'_> {
    /// The most a reader should buffer for one document: a maximum-size body
    /// in base64 plus room for headers.
    pub fn document_limit(&self) -> u64 {
        (self.limits.max_payload as u64)
            .saturating_mul(2)
            .saturating_add(16 << 20)
    }
}

/// A message read from a document, with the destination the document names
/// (used only with `send --dest-from-input`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Record {
    pub message: Message,
    pub destination: Option<String>,
}

pub trait MessageWriter {
    fn begin(&mut self) -> Result<()> {
        Ok(())
    }

    fn write(&mut self, msg: &Message) -> Result<()>;

    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

pub type RecordReader = Box<dyn Iterator<Item = Result<Record>>>;

/// A message format plugin.
pub trait Format: Send + Sync {
    fn name(&self) -> &'static str;

    fn description(&self) -> &'static str;

    /// Options this format reads, conventionally `format.<name>.*`.
    fn settings(&self) -> &'static [Setting] {
        &[]
    }

    /// Whether [`reader`](Self::reader) is implemented (`send -F`).
    fn can_read(&self) -> bool {
        false
    }

    fn writer(&self, out: Box<dyn Write>, ctx: &FormatContext) -> Result<Box<dyn MessageWriter>>;

    fn reader(&self, _input: Box<dyn Read>, _ctx: &FormatContext) -> Result<RecordReader> {
        bail_kind!(
            Unsupported,
            "the {} format can only be written",
            self.name()
        )
    }
}

/// Map an I/O error on the output to pub's error.
pub fn write_error(e: std::io::Error) -> anyhow::Error {
    err!(Io, "write failed: {e}")
}

// ---- built-in formats

/// Bodies only. Reading takes the whole input as one message body.
pub struct RawFormat;

/// pub's human-readable layout (`-i`): `# standard`, `# properties` and
/// `# protocol` sections, a blank line, then the body. Write only.
pub struct TextFormat;

struct BodyWriter {
    out: Box<dyn Write>,
    headers: bool,
    lines: bool,
}

impl MessageWriter for BodyWriter {
    fn write(&mut self, msg: &Message) -> Result<()> {
        msg.write_to(&mut self.out, self.headers, self.lines)
            .map_err(write_error)
    }
}

impl Format for RawFormat {
    fn name(&self) -> &'static str {
        "raw"
    }

    fn description(&self) -> &'static str {
        "message bodies only (the default); reading: the whole input is one body"
    }

    fn can_read(&self) -> bool {
        true
    }

    fn writer(&self, out: Box<dyn Write>, ctx: &FormatContext) -> Result<Box<dyn MessageWriter>> {
        Ok(Box::new(BodyWriter {
            out,
            headers: false,
            lines: ctx.lines,
        }))
    }

    fn reader(&self, input: Box<dyn Read>, ctx: &FormatContext) -> Result<RecordReader> {
        let max = ctx.limits.max_payload;
        let mut body = Vec::new();
        input
            .take((max as u64).saturating_add(1))
            .read_to_end(&mut body)
            .map_err(|e| err!(Io, "cannot read the payload: {e}"))?;
        if body.len() > max {
            bail_kind!(
                Usage,
                "payload is larger than max-payload ({}); raise it with -O max-payload=...",
                crate::limits::human(max)
            );
        }
        let record = Record {
            message: Message::new(body),
            destination: None,
        };
        Ok(Box::new(std::iter::once(Ok(record))))
    }
}

impl Format for TextFormat {
    fn name(&self) -> &'static str {
        "text"
    }

    fn description(&self) -> &'static str {
        "headers in sections, a blank line, then the body (same as -i); write only"
    }

    fn writer(&self, out: Box<dyn Write>, ctx: &FormatContext) -> Result<Box<dyn MessageWriter>> {
        Ok(Box::new(BodyWriter {
            out,
            headers: true,
            lines: ctx.lines,
        }))
    }
}

pub fn builtin() -> Vec<Box<dyn Format>> {
    vec![Box::new(RawFormat), Box::new(TextFormat)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::sync::{Arc, Mutex};

    /// A `Write` whose bytes can be inspected after the writer is boxed away.
    #[derive(Clone, Default)]
    struct Shared(Arc<Mutex<Vec<u8>>>);

    impl Write for Shared {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn ctx(ep: &Endpoint) -> FormatContext<'_> {
        FormatContext {
            ep,
            limits: Limits {
                max_payload: 8,
                ..Default::default()
            },
            lines: true,
        }
    }

    #[test]
    fn raw_and_text() {
        let ep = Endpoint::parse("stomp://h").unwrap();
        let out = Shared::default();
        let mut w = RawFormat.writer(Box::new(out.clone()), &ctx(&ep)).unwrap();
        w.write(&Message::new("a")).unwrap();
        w.write(&Message::new("b\n")).unwrap();
        assert_eq!(&*out.0.lock().unwrap(), b"a\nb\n");

        let out = Shared::default();
        let mut m = Message::new("x");
        m.set_field(crate::Field::Priority, "4").unwrap();
        TextFormat
            .writer(Box::new(out.clone()), &ctx(&ep))
            .unwrap()
            .write(&m)
            .unwrap();
        assert_eq!(
            String::from_utf8(out.0.lock().unwrap().clone()).unwrap(),
            "# standard\npriority: 4\n\nx\n"
        );

        let records: Vec<_> = RawFormat
            .reader(Box::new(Cursor::new(b"payload".to_vec())), &ctx(&ep))
            .unwrap()
            .collect::<Result<_>>()
            .unwrap();
        assert_eq!(records[0].message.body, b"payload");
        let big = RawFormat.reader(Box::new(Cursor::new(vec![0u8; 9])), &ctx(&ep));
        assert!(big.err().unwrap().to_string().contains("max-payload"));
        assert!(TextFormat
            .reader(Box::new(Cursor::new(Vec::new())), &ctx(&ep))
            .is_err());
    }
}
