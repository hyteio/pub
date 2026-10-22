//! Size limits on everything pub reads from the network or the user.
//!
//! The configurable ones are ordinary settings (`max-frame-size`, `max-payload`,
//! `max-headers`, `max-header-value`), so they layer like any other: a single
//! alias can raise `max-frame-size` for a broker that sends large messages.

use crate::endpoint::Endpoint;
use crate::error::Result;
use crate::message::Message;
use crate::{bail_kind, err};

pub const DEFAULT_MAX_FRAME_SIZE: usize = 64 * 1024 * 1024;
pub const DEFAULT_MAX_PAYLOAD: usize = 64 * 1024 * 1024;
pub const DEFAULT_MAX_HEADERS: usize = 256;
pub const DEFAULT_MAX_HEADER_VALUE: usize = 64 * 1024;

/// Frames a session may set aside while it waits for a particular reply (each
/// is also bounded by `max-frame-size`).
pub const MAX_HELD_FRAMES: usize = 10_000;

/// Refuse to set aside another frame when `held` are already waiting.
pub fn check_held(held: usize) -> Result<()> {
    if held >= MAX_HELD_FRAMES {
        bail_kind!(
            Protocol,
            "the broker sent more than {MAX_HELD_FRAMES} frames while pub waited for a reply"
        );
    }
    Ok(())
}

// Fixed limits.
pub const MAX_HEADER_NAME: usize = 256;
pub const MAX_DNS_ADDRESSES: usize = 16;
pub const MAX_SELECTOR: usize = 4 * 1024;
pub const MAX_DESTINATION: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Largest frame accepted from the server (bytes buffered for one frame).
    pub max_frame_size: usize,
    /// Largest message body read from -d / -f / stdin.
    pub max_payload: usize,
    /// Headers + properties per message (and raw subscription headers).
    pub max_headers: usize,
    /// Bytes per header or property value.
    pub max_header_value: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            max_payload: DEFAULT_MAX_PAYLOAD,
            max_headers: DEFAULT_MAX_HEADERS,
            max_header_value: DEFAULT_MAX_HEADER_VALUE,
        }
    }
}

impl Limits {
    pub fn from_endpoint(ep: &Endpoint) -> Result<Self> {
        let d = Limits::default();
        let size = |key: &str, default: usize| -> Result<usize> {
            match ep.prop_size(key)? {
                None => Ok(default),
                Some(0) => bail_kind!(Config, "{key} must be greater than 0"),
                Some(n) => Ok(n),
            }
        };
        let count = |key: &str, default: usize| -> Result<usize> {
            match ep.prop(key) {
                None => Ok(default),
                Some(v) => v
                    .trim()
                    .parse::<usize>()
                    .ok()
                    .filter(|n| *n > 0)
                    .ok_or_else(|| err!(Config, "{key} must be a positive number, got '{v}'")),
            }
        };
        Ok(Limits {
            max_frame_size: size("max-frame-size", d.max_frame_size)?,
            max_payload: size("max-payload", d.max_payload)?,
            max_headers: count("max-headers", d.max_headers)?,
            max_header_value: size("max-header-value", d.max_header_value)?,
        })
    }
}

/// Human-readable byte count for messages (`64 MiB`, `4 KiB`, `1000 bytes`).
pub fn human(n: usize) -> String {
    match n {
        n if n >= 1 << 20 && n % (1 << 20) == 0 => format!("{} MiB", n >> 20),
        n if n >= 1 << 10 && n % (1 << 10) == 0 => format!("{} KiB", n >> 10),
        n => format!("{n} bytes"),
    }
}

/// Fail if `value` is longer than `max` bytes.
pub fn check_len(what: &str, value: &str, max: usize) -> Result<()> {
    if value.len() > max {
        bail_kind!(
            Usage,
            "{what} is {} bytes; the limit is {}",
            value.len(),
            human(max)
        );
    }
    Ok(())
}

/// Count and size checks on an outgoing message's headers, properties and body.
pub fn check_message(msg: &Message, limits: &Limits) -> Result<()> {
    let count = msg.fields.len() + msg.properties.len() + msg.protocol.len();
    if count > limits.max_headers {
        bail_kind!(
            Usage,
            "message has {count} headers and properties; the limit is max-headers ({})",
            limits.max_headers
        );
    }
    let names_values = msg
        .fields
        .iter()
        .map(|(f, v)| (f.name().to_string(), v.clone()))
        .chain(
            msg.properties
                .iter()
                .map(|p| (p.name.clone(), p.value.to_string())),
        )
        .chain(msg.protocol.iter().cloned());
    for (name, value) in names_values {
        check_len("header name", &name, MAX_HEADER_NAME)?;
        if value.len() > limits.max_header_value {
            bail_kind!(
                Usage,
                "value of '{name}' is {} bytes; the limit is max-header-value ({})",
                value.len(),
                human(limits.max_header_value)
            );
        }
    }
    if msg.body.len() > limits.max_payload {
        bail_kind!(
            Usage,
            "payload is larger than max-payload ({})",
            human(limits.max_payload)
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Value;

    #[test]
    fn from_endpoint() {
        let ep = Endpoint::parse("stomp://h?max-frame-size=1m&max-headers=8").unwrap();
        let l = Limits::from_endpoint(&ep).unwrap();
        assert_eq!(
            (l.max_frame_size, l.max_headers, l.max_payload),
            (1 << 20, 8, DEFAULT_MAX_PAYLOAD)
        );
        assert!(
            Limits::from_endpoint(&Endpoint::parse("stomp://h?max-headers=0").unwrap()).is_err()
        );
        assert!(
            Limits::from_endpoint(&Endpoint::parse("stomp://h?max-frame-size=0").unwrap()).is_err()
        );
    }

    #[test]
    fn message_checks() {
        let limits = Limits {
            max_headers: 2,
            max_header_value: 4,
            max_payload: 3,
            ..Limits::default()
        };
        let mut m = Message::new("abc");
        m.set_property("a", Value::String("1234".into()));
        assert!(check_message(&m, &limits).is_ok());
        m.set_property("b", Value::String("12345".into()));
        assert!(check_message(&m, &limits)
            .unwrap_err()
            .to_string()
            .contains("value of 'b' is 5 bytes"));
        m.set_property("b", Value::String("1".into()));
        m.set_property("c", Value::String("1".into()));
        assert!(check_message(&m, &limits)
            .unwrap_err()
            .to_string()
            .contains("3 headers"));
        let mut m = Message::new("abcd");
        assert!(check_message(&m, &limits)
            .unwrap_err()
            .to_string()
            .contains("max-payload"));
        m.body.clear();
        m.set_property(&"n".repeat(300), Value::Int(1));
        assert!(check_message(&m, &limits)
            .unwrap_err()
            .to_string()
            .contains("header name"));
        assert_eq!(human(64 << 20), "64 MiB");
        assert_eq!(human(1000), "1000 bytes");
    }
}
