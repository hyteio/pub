//! curl-style `-v` output on stderr: `*` for info, `>` for bytes sent, `<` for bytes received.

use std::fmt::Display;

use crate::payload::{hexdump, is_printable};

/// Bytes of each frame shown at `-v`; `-vv` shows everything.
const TRUNCATE_AT: usize = 2048;

#[derive(Debug, Clone, Default)]
pub struct Trace {
    pub level: u8,
    redact: Vec<String>,
}

impl Trace {
    pub fn new(level: u8) -> Self {
        Trace {
            level,
            redact: Vec::new(),
        }
    }

    /// Mask this secret (e.g. a password) wherever it appears in traced output.
    pub fn redacting(mut self, secret: &str) -> Self {
        if !secret.is_empty() {
            self.redact.push(secret.to_string());
        }
        self
    }

    pub fn enabled(&self) -> bool {
        self.level > 0
    }

    pub fn info(&self, msg: impl Display) {
        if self.enabled() {
            eprintln!("* {}", self.redact_text(msg.to_string()));
        }
    }

    /// Something the user should know even without `-v`.
    pub fn warn(&self, msg: impl Display) {
        eprintln!("pub: warning: {}", self.redact_text(msg.to_string()));
    }

    /// Extra detail shown only at `-vv`; the message is built lazily.
    pub fn debug<D: Display>(&self, msg: impl FnOnce() -> D) {
        if self.level > 1 {
            eprintln!("* {}", self.redact_text(msg().to_string()));
        }
    }

    fn redact_text(&self, mut text: String) -> String {
        for secret in &self.redact {
            text = text.replace(secret.as_str(), "****");
        }
        text
    }

    /// `bytes` with every occurrence of a secret overwritten by `*`, byte for
    /// byte, so neither the hex dump nor a truncation point can reveal it.
    fn redact_bytes(&self, bytes: &[u8]) -> Vec<u8> {
        let mut out = bytes.to_vec();
        for secret in &self.redact {
            let s = secret.as_bytes();
            let mut i = 0;
            while i + s.len() <= out.len() {
                if &out[i..i + s.len()] == s {
                    out[i..i + s.len()].fill(b'*');
                    i += s.len();
                } else {
                    i += 1;
                }
            }
        }
        out
    }

    pub fn wire_out(&self, bytes: &[u8]) {
        self.wire('>', bytes);
    }

    pub fn wire_in(&self, bytes: &[u8]) {
        self.wire('<', bytes);
    }

    /// Bytes that carry no frame (e.g. heart-beats); only shown at `-vv`.
    pub fn wire_noise(&self, dir: char, bytes: &[u8]) {
        if self.level > 1 {
            eprintln!("{dir} [{} byte keep-alive]", bytes.len());
        }
    }

    fn wire(&self, dir: char, bytes: &[u8]) {
        if !self.enabled() {
            return;
        }
        let masked = self.redact_bytes(bytes);
        let limit = if self.level > 1 {
            bytes.len()
        } else {
            bytes.len().min(TRUNCATE_AT)
        };
        let shown = &masked[..limit];
        let text = if is_printable(shown) {
            String::from_utf8_lossy(shown).replace('\0', "^@")
        } else {
            hexdump(shown)
        };
        for line in text.trim_end_matches('\n').split('\n') {
            eprintln!("{dir} {}", line.trim_end_matches('\r'));
        }
        if limit < bytes.len() {
            eprintln!(
                "{dir} [... {} more bytes, use -vv to show]",
                bytes.len() - limit
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secrets_are_masked_in_the_bytes_before_formatting() {
        let t = Trace::new(1).redacting("hunter2");
        // A binary frame (hex dump) with the secret across a 16-byte row.
        let mut frame = vec![0x10u8, 0x00, 0x04];
        frame.extend_from_slice(b"MQTT\x05user-name-hunter2-tail");
        let masked = t.redact_bytes(&frame);
        assert!(!masked.windows(7).any(|w| w == b"hunter2"));
        assert!(!hexdump(&masked).contains("68 75 6e 74 65 72 32"));
        assert_eq!(masked.len(), frame.len());
        // Overlapping and repeated occurrences.
        assert_eq!(
            t.redact_bytes(b"hunter2hunter2x"),
            b"**************x".to_vec()
        );
        assert_eq!(t.redact_text("pw=hunter2".into()), "pw=****");
    }
}
