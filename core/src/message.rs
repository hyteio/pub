//! The protocol-neutral message model.
//!
//! A [`Message`] separates what JMS separates:
//! - **standard fields** — a fixed, protocol-neutral set ([`Field`]) such as
//!   `correlation-id` or `priority`. Core owns the names (and JMS aliases); each
//!   module maps them to its wire format and declares which it can carry.
//! - **properties** — typed application properties (JMS properties, AMQP
//!   application-properties, MQTT user properties, ...).
//! - **protocol headers** — raw wire headers a module claimed; passed through untouched.
//!
//! On the command line `-H name: value` resolves in that order: standard field,
//! then a protocol header the module claims, otherwise a String property.
//! `-P name[:type]=value` is always a property.

use std::collections::BTreeMap;
use std::fmt;
use std::io::{self, Write};
use std::time::{SystemTime, UNIX_EPOCH};

/// Milliseconds since the Unix epoch (0 if the clock is before it), the unit
/// messaging timestamps and expirations use.
pub fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis().min(i64::MAX as u128) as i64)
}

use crate::config::parse_bool;
use crate::endpoint::parse_duration;
use crate::endpoint::Endpoint;
use crate::error::Result;
use crate::module::Module;
use crate::settings::{matches_pattern, Issue, Severity};
use crate::{bail_kind, err};

/// Protocol-neutral standard message fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Field {
    MessageId,
    Destination,
    Timestamp,
    CorrelationId,
    ReplyTo,
    Type,
    Priority,
    Persistent,
    Ttl,
    Expiration,
    DeliveryDelay,
    GroupId,
    GroupSeq,
    ContentType,
    Redelivered,
    DeliveryCount,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldKind {
    Text,
    /// 0–9
    Priority,
    Bool,
    /// Given as a duration (`30s`, `5m`), stored as milliseconds.
    Duration,
    Int,
    /// Milliseconds since the epoch (receive only).
    EpochMillis,
}

pub struct FieldInfo {
    pub field: Field,
    pub name: &'static str,
    /// Also accepted on input, case-insensitively.
    pub aliases: &'static [&'static str],
    pub kind: FieldKind,
    /// False for fields only the broker sets.
    pub settable: bool,
    pub help: &'static str,
}

const fn fi(
    field: Field,
    name: &'static str,
    aliases: &'static [&'static str],
    kind: FieldKind,
    settable: bool,
    help: &'static str,
) -> FieldInfo {
    FieldInfo {
        field,
        name,
        aliases,
        kind,
        settable,
        help,
    }
}

use FieldKind as K;

pub const FIELDS: &[FieldInfo] = &[
    fi(
        Field::MessageId,
        "message-id",
        &["JMSMessageID"],
        K::Text,
        false,
        "assigned by the broker",
    ),
    fi(
        Field::Destination,
        "destination",
        &["JMSDestination"],
        K::Text,
        false,
        "where the message was sent (use --dest to choose)",
    ),
    fi(
        Field::Timestamp,
        "timestamp",
        &["JMSTimestamp"],
        K::EpochMillis,
        false,
        "send time, ms since epoch",
    ),
    fi(
        Field::CorrelationId,
        "correlation-id",
        &["JMSCorrelationID"],
        K::Text,
        true,
        "links a reply to its request",
    ),
    fi(
        Field::ReplyTo,
        "reply-to",
        &["JMSReplyTo"],
        K::Text,
        true,
        "destination for replies",
    ),
    fi(
        Field::Type,
        "type",
        &["JMSType"],
        K::Text,
        true,
        "application message type",
    ),
    fi(
        Field::Priority,
        "priority",
        &["JMSPriority"],
        K::Priority,
        true,
        "0 (lowest) to 9 (highest)",
    ),
    fi(
        Field::Persistent,
        "persistent",
        &["JMSDeliveryMode"],
        K::Bool,
        true,
        "survive a broker restart",
    ),
    fi(
        Field::Ttl,
        "ttl",
        &["time-to-live"],
        K::Duration,
        true,
        "expire after this long, e.g. 30s, 5m",
    ),
    fi(
        Field::Expiration,
        "expiration",
        &["JMSExpiration", "expires"],
        K::EpochMillis,
        false,
        "expiry time, ms since epoch (set ttl instead)",
    ),
    fi(
        Field::DeliveryDelay,
        "delivery-delay",
        &["JMSDeliveryDelay"],
        K::Duration,
        true,
        "deliver no sooner than this, e.g. 10s",
    ),
    fi(
        Field::GroupId,
        "group-id",
        &["JMSXGroupID"],
        K::Text,
        true,
        "message group for ordered processing",
    ),
    fi(
        Field::GroupSeq,
        "group-seq",
        &["JMSXGroupSeq"],
        K::Int,
        true,
        "sequence within the group",
    ),
    fi(
        Field::ContentType,
        "content-type",
        &[],
        K::Text,
        true,
        "MIME type of the body (also -t)",
    ),
    fi(
        Field::Redelivered,
        "redelivered",
        &["JMSRedelivered"],
        K::Bool,
        false,
        "delivered before",
    ),
    fi(
        Field::DeliveryCount,
        "delivery-count",
        &["JMSXDeliveryCount"],
        K::Int,
        false,
        "delivery attempts",
    ),
];

impl Field {
    pub fn info(self) -> &'static FieldInfo {
        FIELDS
            .iter()
            .find(|f| f.field == self)
            .expect("every Field has a FIELDS entry")
    }

    pub fn name(self) -> &'static str {
        self.info().name
    }

    /// Look up by name or alias, case-insensitively.
    pub fn lookup(name: &str) -> Option<Field> {
        FIELDS
            .iter()
            .find(|f| {
                f.name.eq_ignore_ascii_case(name)
                    || f.aliases.iter().any(|a| a.eq_ignore_ascii_case(name))
            })
            .map(|f| f.field)
    }

    /// Validate and normalize an input value for this field.
    pub fn normalize(self, value: &str) -> std::result::Result<String, String> {
        let v = value.trim();
        match self.info().kind {
            K::Text => Ok(value.to_string()),
            K::Priority => match v.parse::<u8>() {
                Ok(p) if p <= 9 => Ok(p.to_string()),
                _ => Err(format!("expected 0-9, got '{value}'")),
            },
            K::Bool => {
                // JMSDeliveryMode style values are accepted too.
                let b = match v.to_ascii_uppercase().as_str() {
                    "PERSISTENT" => Some(true),
                    "NON_PERSISTENT" => Some(false),
                    _ => parse_bool(v),
                };
                b.map(|b| b.to_string())
                    .ok_or_else(|| format!("expected true/false, got '{value}'"))
            }
            K::Duration => parse_duration(v)
                .map(|d| d.as_millis().to_string())
                .ok_or_else(|| format!("expected a duration (e.g. 30s, 5m), got '{value}'")),
            K::Int | K::EpochMillis => v
                .parse::<i64>()
                .map(|n| n.to_string())
                .map_err(|_| format!("expected a whole number, got '{value}'")),
        }
    }
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A typed property value (the JMS property types).
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    String(String),
    Bool(bool),
    Byte(i8),
    Short(i16),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
}

impl Value {
    pub const TYPES: &'static [&'static str] = &[
        "string", "bool", "byte", "short", "int", "long", "float", "double",
    ];

    pub fn type_name(&self) -> &'static str {
        match self {
            Value::String(_) => "string",
            Value::Bool(_) => "bool",
            Value::Byte(_) => "byte",
            Value::Short(_) => "short",
            Value::Int(_) => "int",
            Value::Long(_) => "long",
            Value::Float(_) => "float",
            Value::Double(_) => "double",
        }
    }

    pub fn parse(ty: &str, raw: &str) -> std::result::Result<Value, String> {
        let bad = || format!("'{raw}' is not a valid {ty}");
        let t = raw.trim();
        Ok(match ty.to_ascii_lowercase().as_str() {
            "string" | "str" => Value::String(raw.to_string()),
            "bool" | "boolean" => Value::Bool(parse_bool(t).ok_or_else(bad)?),
            "byte" => Value::Byte(t.parse().map_err(|_| bad())?),
            "short" => Value::Short(t.parse().map_err(|_| bad())?),
            "int" | "integer" => Value::Int(t.parse().map_err(|_| bad())?),
            "long" => Value::Long(t.parse().map_err(|_| bad())?),
            "float" => Value::Float(t.parse().map_err(|_| bad())?),
            "double" => Value::Double(t.parse().map_err(|_| bad())?),
            other => {
                return Err(format!(
                    "unknown property type '{other}' (use {})",
                    Value::TYPES.join(", ")
                ))
            }
        })
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::String(s) => f.write_str(s),
            Value::Bool(b) => write!(f, "{b}"),
            Value::Byte(n) => write!(f, "{n}"),
            Value::Short(n) => write!(f, "{n}"),
            Value::Int(n) => write!(f, "{n}"),
            Value::Long(n) => write!(f, "{n}"),
            Value::Float(n) => write!(f, "{n}"),
            Value::Double(n) => write!(f, "{n}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Property {
    pub name: String,
    pub value: Value,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Message {
    /// Normalized values (see [`Field::normalize`]).
    pub fields: BTreeMap<Field, String>,
    pub properties: Vec<Property>,
    /// Raw protocol headers, in order.
    pub protocol: Vec<(String, String)>,
    pub body: Vec<u8>,
    /// Text or bytes: set from `message-type` when sending; when receiving, what
    /// the protocol said, or `None` if it doesn't say.
    pub body_type: Option<BodyType>,
}

/// `message-type`: whether the body is text (UTF-8) or opaque bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyType {
    Text,
    Bytes,
}

impl BodyType {
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "text" => Some(BodyType::Text),
            "bytes" => Some(BodyType::Bytes),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            BodyType::Text => "text",
            BodyType::Bytes => "bytes",
        }
    }
}

impl Message {
    /// Set the body type from the endpoint's `message-type` (default bytes);
    /// a text body must be valid UTF-8.
    pub fn apply_message_type(&mut self, ep: &Endpoint) -> Result<()> {
        let t = match ep.prop("message-type") {
            None => BodyType::Bytes,
            Some(v) => BodyType::parse(v)
                .ok_or_else(|| err!(Config, "message-type must be text or bytes, got '{v}'"))?,
        };
        if t == BodyType::Text && std::str::from_utf8(&self.body).is_err() {
            bail_kind!(Usage, "message-type = text needs a UTF-8 body; this one isn't (use -O message-type=bytes)");
        }
        self.body_type = Some(t);
        Ok(())
    }
}

impl Message {
    pub fn new(body: impl Into<Vec<u8>>) -> Self {
        Message {
            body: body.into(),
            ..Default::default()
        }
    }

    pub fn field(&self, f: Field) -> Option<&str> {
        self.fields.get(&f).map(String::as_str)
    }

    /// Set a field from user input, validating and normalizing it.
    pub fn set_field(&mut self, f: Field, value: &str) -> Result<()> {
        let v = f
            .normalize(value)
            .map_err(|why| err!(Usage, "{f}: {why}"))?;
        self.fields.insert(f, v);
        Ok(())
    }

    pub fn property(&self, name: &str) -> Option<&Value> {
        self.properties
            .iter()
            .find(|p| p.name == name)
            .map(|p| &p.value)
    }

    /// For replaying a received message: work the original time-to-live out
    /// of the absolute `expiration` and the send `timestamp` (both set from one
    /// clock reading when it was sent). Does nothing if `ttl` is already set or
    /// either time is missing; returns the ttl in ms when it set one.
    pub fn restore_ttl(&mut self) -> Option<i64> {
        if self.fields.contains_key(&Field::Ttl) {
            return None;
        }
        let ms = |f| {
            self.field(f)
                .and_then(|v| v.parse::<i64>().ok())
                .filter(|n| *n > 0)
        };
        let ttl = ms(Field::Expiration)? - ms(Field::Timestamp)?;
        if ttl <= 0 {
            return None;
        }
        self.fields.insert(Field::Ttl, ttl.to_string());
        Some(ttl)
    }

    /// Set a property, replacing one of the same (case-sensitive) name.
    pub fn set_property(&mut self, name: &str, value: Value) {
        self.properties.retain(|p| p.name != name);
        self.properties.push(Property {
            name: name.to_string(),
            value,
        });
    }

    /// `-H "Name: value"`: standard field, else a protocol header the module
    /// claims (`protocol_headers` patterns), else a String property.
    pub fn apply_header(&mut self, raw: &str, protocol_headers: &[&str]) -> Result<()> {
        let (name, value) = raw
            .split_once(':')
            .map(|(k, v)| (k.trim(), v.trim_start()))
            .filter(|(k, _)| !k.is_empty())
            .ok_or_else(|| err!(Usage, "header '{raw}' must look like 'Name: value'"))?;
        if let Some(f) = Field::lookup(name) {
            if !f.info().settable {
                let hint = if f == Field::Expiration {
                    "; set ttl instead"
                } else {
                    ""
                };
                bail_kind!(
                    Usage,
                    "'{name}' ({f}) is set by the broker and cannot be sent{hint}"
                );
            }
            return self.set_field(f, value);
        }
        if protocol_headers.iter().any(|p| matches_pattern(p, name)) {
            self.protocol.push((name.to_string(), value.to_string()));
            return Ok(());
        }
        self.set_property(name, Value::String(value.to_string()));
        Ok(())
    }

    /// `-P "name[:type]=value"`: always a property; type defaults to string.
    pub fn apply_property(&mut self, raw: &str) -> Result<()> {
        let (lhs, value) = raw.split_once('=').ok_or_else(|| {
            err!(
                Usage,
                "property '{raw}' must look like name=value or name:type=value"
            )
        })?;
        let (name, ty) = match lhs.split_once(':') {
            Some((n, t)) => (n.trim(), t.trim()),
            None => (lhs.trim(), "string"),
        };
        if name.is_empty() {
            bail_kind!(Usage, "property '{raw}' has an empty name");
        }
        let value =
            Value::parse(ty, value).map_err(|why| err!(Usage, "property '{name}': {why}"))?;
        self.set_property(name, value);
        Ok(())
    }

    /// Print `-i` output: labelled groups of fields, properties and protocol
    /// headers, a blank line, then the body.
    pub fn write_to(
        &self,
        out: &mut dyn Write,
        include_headers: bool,
        ensure_newline: bool,
    ) -> io::Result<()> {
        if include_headers {
            if !self.fields.is_empty() || self.body_type.is_some() {
                writeln!(out, "# standard")?;
                for (f, v) in &self.fields {
                    writeln!(out, "{f}: {v}")?;
                }
                if let Some(t) = self.body_type {
                    writeln!(out, "message-type: {}", t.name())?;
                }
            }
            if !self.properties.is_empty() {
                writeln!(out, "# properties")?;
                for p in &self.properties {
                    match &p.value {
                        Value::String(s) => writeln!(out, "{}: {s}", p.name)?,
                        v => writeln!(out, "{} ({}): {v}", p.name, v.type_name())?,
                    }
                }
            }
            if !self.protocol.is_empty() {
                writeln!(out, "# protocol")?;
                for (k, v) in &self.protocol {
                    writeln!(out, "{k}: {v}")?;
                }
            }
            writeln!(out)?;
        }
        out.write_all(&self.body)?;
        if ensure_newline && !self.body.ends_with(b"\n") {
            out.write_all(b"\n")?;
        }
        out.flush()
    }
}

/// Check an outgoing message against what the module can carry.
///
/// Fields the module cannot represent and typed properties it would flatten to
/// strings are warnings (errors in strict mode); property names the protocol
/// reserves are always errors, since they would silently change meaning.
pub fn check_message(module: &dyn Module, ep: &Endpoint, msg: &Message) -> Vec<Issue> {
    let caps = module.message_capabilities(ep);
    let who = match caps.label {
        Some(label) => format!("the {} module ({label})", module.name()),
        None => format!("the {} module", module.name()),
    };
    let mut issues = Vec::new();
    let mut issue = |severity, message: String| {
        issues.push(Issue {
            severity,
            location: "message".into(),
            message,
        });
    };
    for f in msg.fields.keys() {
        if !caps.fields.contains(f) {
            issue(
                Severity::Warning,
                format!("{who} cannot carry '{f}'; it will not be sent"),
            );
        }
    }
    if msg.body_type == Some(BodyType::Text) && !caps.text_bodies {
        issue(
            Severity::Warning,
            format!("{who} cannot mark a body as text; it will be sent as bytes"),
        );
    }
    for p in &msg.properties {
        if !caps.properties {
            issue(
                Severity::Warning,
                format!(
                    "{who} cannot carry properties; '{}' will not be sent",
                    p.name
                ),
            );
        } else if caps
            .reserved_property_names
            .iter()
            .any(|r| r.eq_ignore_ascii_case(&p.name))
        {
            issue(
                Severity::Error,
                format!(
                    "property '{}' cannot be sent by {who}: the name is reserved by the protocol",
                    p.name
                ),
            );
        } else if !caps.typed_properties && !matches!(p.value, Value::String(_)) {
            issue(
                Severity::Warning,
                format!(
                    "{who} sends properties as strings; '{}' ({}) will arrive as a String",
                    p.name,
                    p.value.type_name()
                ),
            );
        }
    }
    issues
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_field_has_info_and_unique_names() {
        let mut names = Vec::new();
        for f in FIELDS {
            assert_eq!(f.field.info().name, f.name);
            names.push(f.name.to_ascii_lowercase());
            names.extend(f.aliases.iter().map(|a| a.to_ascii_lowercase()));
        }
        let n = names.len();
        names.sort();
        names.dedup();
        assert_eq!(names.len(), n, "duplicate field name or alias");
    }

    #[test]
    fn header_resolution() {
        let mut m = Message::default();
        m.apply_header("JMSCorrelationID: abc", &[]).unwrap();
        m.apply_header("Priority: 9", &[]).unwrap();
        m.apply_header("ttl: 5m", &[]).unwrap();
        m.apply_header("JMSDeliveryMode: NON_PERSISTENT", &[])
            .unwrap();
        m.apply_header("x-opt-thing: 1", &["x-opt-*"]).unwrap();
        m.apply_header("orderId: 42", &["x-opt-*"]).unwrap();
        assert_eq!(m.field(Field::CorrelationId), Some("abc"));
        assert_eq!(m.field(Field::Priority), Some("9"));
        assert_eq!(m.field(Field::Ttl), Some("300000"));
        assert_eq!(m.field(Field::Persistent), Some("false"));
        assert_eq!(
            m.protocol,
            vec![("x-opt-thing".to_string(), "1".to_string())]
        );
        assert_eq!(m.property("orderId"), Some(&Value::String("42".into())));

        assert!(m.apply_header("priority: 11", &[]).is_err());
        assert!(m
            .apply_header("message-id: x", &[])
            .unwrap_err()
            .to_string()
            .contains("set by the broker"));
        assert!(m.apply_header("nocolon", &[]).is_err());
    }

    #[test]
    fn ttl_from_expiration_and_timestamp() {
        let mut m = Message::new("x");
        m.set_field(Field::Timestamp, "1000").unwrap();
        m.set_field(Field::Expiration, "61000").unwrap();
        assert_eq!(m.restore_ttl(), Some(60_000));
        assert_eq!(m.field(Field::Ttl), Some("60000"));
        // An explicit ttl wins; missing or zero times give nothing.
        m.set_field(Field::Ttl, "5s").unwrap();
        assert_eq!(m.restore_ttl(), None);
        assert_eq!(m.field(Field::Ttl), Some("5000"));
        let mut no_ts = Message::new("x");
        no_ts.set_field(Field::Expiration, "61000").unwrap();
        assert_eq!(no_ts.restore_ttl(), None);
        let mut zero = Message::new("x");
        zero.set_field(Field::Timestamp, "0").unwrap();
        zero.set_field(Field::Expiration, "61000").unwrap();
        assert_eq!(zero.restore_ttl(), None);
        let mut backwards = Message::new("x");
        backwards.set_field(Field::Timestamp, "9000").unwrap();
        backwards.set_field(Field::Expiration, "1000").unwrap();
        assert_eq!(backwards.restore_ttl(), None);
    }

    #[test]
    fn typed_properties() {
        let mut m = Message::default();
        m.apply_property("orderId:long=42").unwrap();
        m.apply_property("vip:bool=yes").unwrap();
        m.apply_property("type=gold").unwrap(); // a property, not JMSType
        m.apply_property("note=a=b").unwrap();
        assert_eq!(m.property("orderId"), Some(&Value::Long(42)));
        assert_eq!(m.property("vip"), Some(&Value::Bool(true)));
        assert_eq!(m.property("type"), Some(&Value::String("gold".into())));
        assert_eq!(m.property("note"), Some(&Value::String("a=b".into())));
        assert!(m.fields.is_empty());
        assert!(m.apply_property("n:int=lots").is_err());
        assert!(m.apply_property("n:decimal=1").is_err());
    }

    #[test]
    fn grouped_output() {
        let mut m = Message::new("hi");
        m.set_field(Field::Priority, "4").unwrap();
        m.set_property("n", Value::Int(5));
        m.set_property("s", Value::String("x".into()));
        m.protocol.push(("subscription".into(), "1".into()));
        let mut out = Vec::new();
        m.write_to(&mut out, true, false).unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "# standard\npriority: 4\n# properties\nn (int): 5\ns: x\n# protocol\nsubscription: 1\n\nhi"
        );
    }

    #[test]
    fn message_type_from_endpoint() {
        let ep = |q: &str| Endpoint::parse(&format!("stomp://h{q}")).unwrap();
        let mut m = Message::new("héllo");
        m.apply_message_type(&ep("")).unwrap();
        assert_eq!(m.body_type, Some(BodyType::Bytes)); // the global default
        m.apply_message_type(&ep("?message-type=text")).unwrap();
        assert_eq!(m.body_type, Some(BodyType::Text));

        let mut bin = Message::new(vec![0xFF, 0xFE]);
        let e = bin
            .apply_message_type(&ep("?message-type=text"))
            .unwrap_err();
        assert!(e.to_string().contains("needs a UTF-8 body"), "{e}");
        assert!(bin.apply_message_type(&ep("?message-type=bytes")).is_ok());

        let mut out = Vec::new();
        m.write_to(&mut out, true, false).unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "# standard\nmessage-type: text\n\nhéllo"
        );
    }
}
