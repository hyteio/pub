//! AMQP 1.0 messages (part 3.2) and their mapping to pub's message model.
//!
//! The mapping follows ActiveMQ's JMS transformer (`activemq-amqp`,
//! `JMSMappingInbound/OutboundTransformer`) and Qpid JMS, so JMS consumers on
//! ActiveMQ and Artemis see what a Java client would have sent:
//!
//! - text: an `amqp-value` string (a TextMessage whatever the content type), or
//!   with `text-body = data`, a `data` section with `text/plain; charset=utf-8`;
//! - bytes: a `data` section without a content type (a BytesMessage);
//! - fields: `header` (durable, priority, ttl), `properties` (message-id,
//!   to, subject = JMSType, reply-to, correlation-id, content-type, creation
//!   and expiry times, groups); `delivery-delay` → `x-opt-delivery-delay`;
//! - typed properties → `application-properties`;
//! - `x-opt-jms-msg-type` / `x-opt-jms-dest` / `x-opt-jms-reply-to`
//!   annotations, as Qpid JMS sends them.
//!
//! On receive, an `amqp-value` string, or a `data` section with a textual
//! content type that is valid UTF-8, is text; everything else is bytes.

use pub_core::message::Property;
use pub_core::{bail_kind, err, BodyType, Field as PubField, Message, Result, Value as PubValue};

use crate::described::{section, Field, Header, Map, Properties, Symbol, Timestamp};
use crate::types::Value;

/// `x-opt-jms-msg-type` values (Qpid JMS / ActiveMQ).
pub const JMS_MESSAGE: i8 = 0;
pub const JMS_BYTES_MESSAGE: i8 = 3;
pub const JMS_TEXT_MESSAGE: i8 = 5;
/// `x-opt-jms-dest` / `x-opt-jms-reply-to` values.
pub const JMS_QUEUE: i8 = 0;
pub const JMS_TOPIC: i8 = 1;

#[derive(Debug, Clone, PartialEq, Default)]
pub enum Body {
    #[default]
    None,
    Data(Vec<Vec<u8>>),
    Value(Value),
    Sequence(Vec<Vec<Value>>),
}

/// An AMQP message: its sections, in order.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AmqpMessage {
    pub header: Option<Header>,
    pub delivery_annotations: Option<Map>,
    pub message_annotations: Option<Map>,
    pub properties: Option<Properties>,
    pub application_properties: Option<Map>,
    pub body: Body,
    pub footer: Option<Map>,
}

impl AmqpMessage {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        let mut put = |v: Value| v.encode(&mut out);
        if let Some(h) = &self.header {
            put(h.to_value())?;
        }
        if let Some(m) = &self.delivery_annotations {
            put(Value::described(
                section::DELIVERY_ANNOTATIONS,
                m.to_value(),
            ))?;
        }
        if let Some(m) = &self.message_annotations {
            put(Value::described(section::MESSAGE_ANNOTATIONS, m.to_value()))?;
        }
        if let Some(p) = &self.properties {
            put(p.to_value())?;
        }
        if let Some(m) = &self.application_properties {
            put(Value::described(
                section::APPLICATION_PROPERTIES,
                m.to_value(),
            ))?;
        }
        match &self.body {
            Body::None => {}
            Body::Data(chunks) => {
                for c in chunks {
                    put(Value::described(section::DATA, Value::Binary(c.clone())))?;
                }
            }
            Body::Value(v) => put(Value::described(section::AMQP_VALUE, v.clone()))?,
            Body::Sequence(seqs) => {
                for s in seqs {
                    put(Value::described(
                        section::AMQP_SEQUENCE,
                        Value::List(s.clone()),
                    ))?;
                }
            }
        }
        if let Some(m) = &self.footer {
            put(Value::described(section::FOOTER, m.to_value()))?;
        }
        Ok(out)
    }

    pub fn decode(mut bytes: &[u8]) -> Result<Self> {
        let mut m = AmqpMessage::default();
        while !bytes.is_empty() {
            let (v, used) = Value::decode(bytes)?;
            bytes = &bytes[used..];
            let code = v
                .descriptor_code()
                .ok_or_else(|| err!(Protocol, "AMQP message section without a descriptor"))?;
            let inner = || match &v {
                Value::Described(_, b) => (**b).clone(),
                _ => unreachable!(),
            };
            match code {
                section::HEADER => m.header = Some(Header::from_value(v)?),
                section::DELIVERY_ANNOTATIONS => {
                    m.delivery_annotations = Some(Map::from_value(inner(), "delivery-annotations")?)
                }
                section::MESSAGE_ANNOTATIONS => {
                    m.message_annotations = Some(Map::from_value(inner(), "message-annotations")?)
                }
                section::PROPERTIES => m.properties = Some(Properties::from_value(v)?),
                section::APPLICATION_PROPERTIES => {
                    m.application_properties =
                        Some(Map::from_value(inner(), "application-properties")?)
                }
                section::DATA => match inner() {
                    Value::Binary(b) => match &mut m.body {
                        Body::Data(chunks) => chunks.push(b),
                        Body::None => m.body = Body::Data(vec![b]),
                        _ => bail_kind!(Protocol, "AMQP message mixes body section kinds"),
                    },
                    other => bail_kind!(Protocol, "AMQP data section holds {}", other.type_name()),
                },
                section::AMQP_SEQUENCE => match inner() {
                    Value::List(items) => match &mut m.body {
                        Body::Sequence(seqs) => seqs.push(items),
                        Body::None => m.body = Body::Sequence(vec![items]),
                        _ => bail_kind!(Protocol, "AMQP message mixes body section kinds"),
                    },
                    other => bail_kind!(
                        Protocol,
                        "AMQP sequence section holds {}",
                        other.type_name()
                    ),
                },
                section::AMQP_VALUE => {
                    if m.body != Body::None {
                        bail_kind!(Protocol, "AMQP message has more than one body");
                    }
                    m.body = Body::Value(inner());
                }
                section::FOOTER => m.footer = Some(Map::from_value(inner(), "footer")?),
                other => bail_kind!(Protocol, "unknown AMQP message section {other:#x}"),
            }
        }
        Ok(m)
    }
}

/// An AMQP address for a pub destination name, with the capability and the
/// JMS destination type ActiveMQ and Artemis understand:
/// `/queue/X`, `queue://X` → `X` + `queue`; `/topic/X`, `topic://X` → `X` +
/// `topic`; RabbitMQ's v2 addresses (`/queues/X`, `/exchanges/X/key`) as given;
/// anything else without its leading `/`.
pub fn address(name: &str) -> Result<(String, Option<&'static str>)> {
    let lower = name.to_ascii_lowercase();
    let (addr, cap) = if let Some(i) = ["/queue/", "queue://"]
        .iter()
        .find(|p| lower.starts_with(**p))
    {
        (&name[i.len()..], Some("queue"))
    } else if let Some(i) = ["/topic/", "topic://"]
        .iter()
        .find(|p| lower.starts_with(**p))
    {
        (&name[i.len()..], Some("topic"))
    } else if lower.starts_with("/queues/") || lower.starts_with("/exchanges/") {
        (name, None)
    } else {
        (name.strip_prefix('/').unwrap_or(name), None)
    };
    if addr.is_empty() {
        bail_kind!(Usage, "empty AMQP address in '{name}'");
    }
    Ok((addr.to_string(), cap))
}

/// Textual content types, as ActiveMQ's `AmqpContentTypeSupport` decides:
/// text/*, application/json|xml|javascript|ecmascript|xml-dtd, *+json, *+xml;
/// with a UTF-8 (or no) charset.
pub fn is_textual_utf8(content_type: &str) -> bool {
    let lower = content_type.to_ascii_lowercase();
    let (mime, params) = lower.split_once(';').unwrap_or((&lower, ""));
    let Some((ty, sub)) = mime.trim().split_once('/') else {
        return false;
    };
    let textual = ty == "text"
        || (ty == "application"
            && (matches!(
                sub,
                "json" | "xml" | "javascript" | "ecmascript" | "xml-dtd"
            ) || sub.ends_with("+json")
                || sub.ends_with("+xml")));
    let charset = params
        .split(';')
        .filter_map(|p| p.trim().strip_prefix("charset="))
        .next()
        .map(|c| c.trim_matches('"'));
    textual && charset.is_none_or(|c| c == "utf-8" || c == "utf8")
}

/// Address prefixes a broker advertises in its open properties
/// (`queue-prefix`, `topic-prefix`): ActiveMQ names destinations by
/// address alone (`topic://news`) and ignores capabilities, so Qpid JMS — and
/// pub — prefix queue and topic addresses when told to. Artemis uses the
/// capabilities instead and advertises none.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Prefixes {
    pub queue: Option<String>,
    pub topic: Option<String>,
}

impl Prefixes {
    /// The address to use on the wire.
    pub fn apply(&self, address: &str, capability: Option<&str>) -> String {
        let prefix = match capability {
            Some("queue") => self.queue.as_deref(),
            Some("topic") => self.topic.as_deref(),
            _ => None,
        };
        match prefix {
            Some(p) if !address.starts_with(p) => format!("{p}{address}"),
            _ => address.to_string(),
        }
    }
}

/// How text bodies are sent (`text-body`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextBody {
    /// An `amqp-value` string (JMS clients and brokers: a TextMessage).
    Value,
    /// A `data` section with `text/plain; charset=utf-8` (non-JMS peers).
    Data,
}

fn app_value(v: &PubValue) -> Value {
    match v {
        PubValue::String(s) => Value::String(s.clone()),
        PubValue::Bool(b) => Value::Bool(*b),
        PubValue::Byte(n) => Value::Byte(*n),
        PubValue::Short(n) => Value::Short(*n),
        PubValue::Int(n) => Value::Int(*n),
        PubValue::Long(n) => Value::Long(*n),
        PubValue::Float(n) => Value::Float(*n),
        PubValue::Double(n) => Value::Double(*n),
    }
}

/// pub's message as an AMQP message for `destination` (a name as in [`address`]).
pub fn to_amqp(
    msg: &Message,
    destination: &str,
    message_id: Option<String>,
    text_body: TextBody,
    prefixes: &Prefixes,
) -> Result<AmqpMessage> {
    let field = |f: PubField| msg.field(f);
    let now = pub_core::now_millis();
    let (to, dest_kind) = address(destination)?;
    let to = prefixes.apply(&to, dest_kind);

    let persistent = field(PubField::Persistent).is_none_or(|v| v == "true");
    let priority = match field(PubField::Priority) {
        Some(p) => Some(
            p.parse::<u8>()
                .map_err(|_| err!(Usage, "invalid priority '{p}'"))?,
        ),
        None => None,
    };
    let ttl = match field(PubField::Ttl) {
        Some(t) => Some(
            t.parse::<u64>()
                .ok()
                .and_then(|n| u32::try_from(n).ok())
                .ok_or_else(|| {
                    err!(
                        Usage,
                        "ttl '{t}' is too large for AMQP (32-bit milliseconds)"
                    )
                })?,
        ),
        None => None,
    };
    let reply_to = match field(PubField::ReplyTo) {
        Some(r) => Some(address(r)?),
        None => None,
    };
    let group_sequence = match field(PubField::GroupSeq) {
        Some(s) => Some(
            s.parse::<u32>()
                .map_err(|_| err!(Usage, "group-seq must be 0 or more, got '{s}'"))?,
        ),
        None => None,
    };

    let text = msg.body_type == Some(BodyType::Text);
    let mut content_type = field(PubField::ContentType).map(str::to_string);
    let body = if text {
        let s = std::str::from_utf8(&msg.body)
            .map_err(|_| err!(Usage, "message-type = text needs a UTF-8 body"))?;
        match text_body {
            TextBody::Value => Body::Value(Value::String(s.to_string())),
            TextBody::Data => {
                content_type.get_or_insert_with(|| "text/plain; charset=utf-8".to_string());
                Body::Data(vec![msg.body.clone()])
            }
        }
    } else {
        Body::Data(vec![msg.body.clone()])
    };

    let mut annotations = vec![(
        Value::symbol("x-opt-jms-msg-type"),
        Value::Byte(if text {
            JMS_TEXT_MESSAGE
        } else {
            JMS_BYTES_MESSAGE
        }),
    )];
    match dest_kind {
        Some("queue") => {
            annotations.push((Value::symbol("x-opt-jms-dest"), Value::Byte(JMS_QUEUE)))
        }
        Some("topic") => {
            annotations.push((Value::symbol("x-opt-jms-dest"), Value::Byte(JMS_TOPIC)))
        }
        _ => {}
    }
    match reply_to.as_ref().and_then(|(_, k)| *k) {
        Some("queue") => {
            annotations.push((Value::symbol("x-opt-jms-reply-to"), Value::Byte(JMS_QUEUE)))
        }
        Some("topic") => {
            annotations.push((Value::symbol("x-opt-jms-reply-to"), Value::Byte(JMS_TOPIC)))
        }
        _ => {}
    }
    if let Some(d) = field(PubField::DeliveryDelay) {
        let ms: i64 = d
            .parse()
            .map_err(|_| err!(Usage, "invalid delivery-delay '{d}'"))?;
        annotations.push((Value::symbol("x-opt-delivery-delay"), Value::Long(ms)));
    }

    let props = Properties {
        message_id: message_id.map(Value::String),
        to: Some(to),
        subject: field(PubField::Type).map(str::to_string),
        reply_to: reply_to.as_ref().map(|(a, k)| prefixes.apply(a, *k)),
        correlation_id: field(PubField::CorrelationId).map(|c| Value::String(c.to_string())),
        content_type: content_type.map(Symbol),
        absolute_expiry_time: ttl.map(|t| Timestamp(now + t as i64)),
        creation_time: Some(Timestamp(now)),
        group_id: field(PubField::GroupId).map(str::to_string),
        group_sequence,
        ..Default::default()
    };
    let application_properties = (!msg.properties.is_empty()).then(|| {
        Map(msg
            .properties
            .iter()
            .map(|p| (Value::String(p.name.clone()), app_value(&p.value)))
            .collect())
    });
    Ok(AmqpMessage {
        header: Some(Header {
            durable: Some(persistent),
            priority,
            ttl,
            ..Default::default()
        }),
        message_annotations: Some(Map(annotations)),
        properties: Some(props),
        application_properties,
        body,
        ..Default::default()
    })
}

/// A message id or correlation id as text.
pub fn id_text(v: &Value) -> String {
    match v {
        Value::String(s) | Value::Symbol(s) => s.clone(),
        Value::Ulong(n) => n.to_string(),
        Value::Uuid(u) => format!(
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            u[0], u[1], u[2], u[3], u[4], u[5], u[6], u[7], u[8], u[9], u[10], u[11], u[12], u[13], u[14], u[15]
        ),
        Value::Binary(b) => b.iter().map(|x| format!("{x:02x}")).collect(),
        other => format!("{other:?}"),
    }
}

/// An application property as one of pub's typed values.
fn pub_value(v: &Value) -> Option<PubValue> {
    Some(match v {
        Value::Null => return None,
        Value::String(s) | Value::Symbol(s) => PubValue::String(s.clone()),
        Value::Bool(b) => PubValue::Bool(*b),
        Value::Byte(n) => PubValue::Byte(*n),
        Value::Short(n) => PubValue::Short(*n),
        Value::Int(n) => PubValue::Int(*n),
        Value::Long(n) => PubValue::Long(*n),
        Value::Float(n) => PubValue::Float(*n),
        Value::Double(n) => PubValue::Double(*n),
        Value::Ubyte(n) => PubValue::Short(*n as i16),
        Value::Ushort(n) => PubValue::Int(*n as i32),
        Value::Uint(n) => PubValue::Long(*n as i64),
        Value::Ulong(n) => match i64::try_from(*n) {
            Ok(n) => PubValue::Long(n),
            Err(_) => PubValue::String(n.to_string()),
        },
        Value::Timestamp(t) => PubValue::Long(*t),
        Value::Char(c) => PubValue::String(c.to_string()),
        other => PubValue::String(id_text(other)),
    })
}

fn annotation_text(v: &Value) -> String {
    match v {
        Value::String(s) | Value::Symbol(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Byte(n) => n.to_string(),
        Value::Short(n) => n.to_string(),
        Value::Int(n) => n.to_string(),
        Value::Long(n) | Value::Timestamp(n) => n.to_string(),
        Value::Ubyte(n) => n.to_string(),
        Value::Ushort(n) => n.to_string(),
        Value::Uint(n) => n.to_string(),
        Value::Ulong(n) => n.to_string(),
        other => format!("{other:?}"),
    }
}

/// An AMQP message as pub's message.
pub fn from_amqp(m: &AmqpMessage) -> Result<Message> {
    let content_type = m
        .properties
        .as_ref()
        .and_then(|p| p.content_type.as_ref())
        .map(|c| c.0.clone());
    let (body, body_type, section_kind) = match &m.body {
        Body::None => (Vec::new(), BodyType::Bytes, None),
        Body::Value(Value::String(s)) => (s.clone().into_bytes(), BodyType::Text, None),
        Body::Value(Value::Null) => (Vec::new(), BodyType::Text, None),
        Body::Value(Value::Binary(b)) => (b.clone(), BodyType::Bytes, None),
        Body::Value(other) => (
            other.to_bytes()?,
            BodyType::Bytes,
            Some(format!("amqp-value {}", other.type_name())),
        ),
        Body::Data(chunks) => {
            let bytes = chunks.concat();
            let text = content_type.as_deref().is_some_and(is_textual_utf8)
                && std::str::from_utf8(&bytes).is_ok();
            (
                bytes,
                if text {
                    BodyType::Text
                } else {
                    BodyType::Bytes
                },
                None,
            )
        }
        Body::Sequence(seqs) => {
            let v = Value::List(seqs.iter().map(|s| Value::List(s.clone())).collect());
            (v.to_bytes()?, BodyType::Bytes, Some("amqp-sequence".into()))
        }
    };
    let mut msg = Message::new(body);
    msg.body_type = Some(body_type);
    let mut set = |f: PubField, v: Option<String>| {
        if let Some(v) = v {
            msg.fields.insert(f, v);
        }
    };
    if let Some(h) = &m.header {
        set(
            PubField::Persistent,
            Some(h.durable.unwrap_or(false).to_string()),
        );
        set(
            PubField::Priority,
            Some(h.priority.unwrap_or(4).min(9).to_string()),
        );
        let count = h.delivery_count.unwrap_or(0);
        set(PubField::Redelivered, Some((count > 0).to_string()));
        set(
            PubField::DeliveryCount,
            Some((count as u64 + 1).to_string()),
        );
    }
    if let Some(p) = &m.properties {
        set(PubField::MessageId, p.message_id.as_ref().map(id_text));
        set(PubField::Destination, p.to.clone());
        set(PubField::Type, p.subject.clone());
        set(PubField::ReplyTo, p.reply_to.clone());
        set(
            PubField::CorrelationId,
            p.correlation_id.as_ref().map(id_text),
        );
        set(
            PubField::ContentType,
            p.content_type.as_ref().map(|c| c.0.clone()),
        );
        set(
            PubField::Timestamp,
            p.creation_time.map(|t| t.0.to_string()),
        );
        set(
            PubField::Expiration,
            p.absolute_expiry_time.map(|t| t.0.to_string()),
        );
        set(PubField::GroupId, p.group_id.clone());
        set(PubField::GroupSeq, p.group_sequence.map(|s| s.to_string()));
    }
    if let Some(Map(props)) = &m.application_properties {
        for (k, v) in props {
            if let (Some(name), Some(value)) = (k.as_str(), pub_value(v)) {
                msg.properties.push(Property {
                    name: name.to_string(),
                    value,
                });
            }
        }
    }
    if let Some(Map(ann)) = &m.message_annotations {
        for (k, v) in ann {
            if let Some(k) = k.as_str() {
                msg.protocol.push((k.to_string(), annotation_text(v)));
            }
        }
    }
    if let Some(kind) = section_kind {
        msg.protocol.push(("body-section".into(), kind));
    }
    if let Some(u) = m.properties.as_ref().and_then(|p| p.user_id.as_ref()) {
        msg.protocol
            .push(("user-id".into(), String::from_utf8_lossy(&u.0).into_owned()));
    }
    Ok(msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addresses() {
        assert_eq!(
            address("/queue/orders").unwrap(),
            ("orders".into(), Some("queue"))
        );
        assert_eq!(
            address("topic://prices").unwrap(),
            ("prices".into(), Some("topic"))
        );
        assert_eq!(
            address("/queues/orders").unwrap(),
            ("/queues/orders".into(), None)
        );
        assert_eq!(
            address("/exchanges/amq.topic/a.b").unwrap(),
            ("/exchanges/amq.topic/a.b".into(), None)
        );
        assert_eq!(address("/orders").unwrap(), ("orders".into(), None));
        assert!(address("/queue/").is_err());
    }

    #[test]
    fn advertised_prefixes() {
        let activemq = Prefixes {
            queue: Some("queue://".into()),
            topic: Some("topic://".into()),
        };
        assert_eq!(activemq.apply("news", Some("topic")), "topic://news");
        assert_eq!(activemq.apply("orders", Some("queue")), "queue://orders");
        assert_eq!(activemq.apply("orders", None), "orders");
        assert_eq!(Prefixes::default().apply("news", Some("topic")), "news");
        let a = to_amqp(&sample(), "/topic/news", None, TextBody::Value, &activemq).unwrap();
        let p = a.properties.unwrap();
        assert_eq!(p.to.as_deref(), Some("topic://news"));
        assert_eq!(p.reply_to.as_deref(), Some("topic://replies"));
    }

    #[test]
    fn textual_content_types() {
        for t in [
            "text/plain",
            "text/html; charset=UTF-8",
            "application/json",
            "application/vnd.x+json",
            "application/atom+xml",
        ] {
            assert!(is_textual_utf8(t), "{t}");
        }
        for t in [
            "application/octet-stream",
            "image/png",
            "text/plain; charset=iso-8859-1",
            "nonsense",
        ] {
            assert!(!is_textual_utf8(t), "{t}");
        }
    }

    fn sample() -> Message {
        let mut m = Message::new("héllo".as_bytes().to_vec());
        m.body_type = Some(BodyType::Text);
        for (f, v) in [
            (PubField::CorrelationId, "c-1"),
            (PubField::Type, "order"),
            (PubField::Priority, "7"),
            (PubField::Ttl, "60000"),
            (PubField::ReplyTo, "/topic/replies"),
            (PubField::GroupId, "g"),
            (PubField::GroupSeq, "3"),
            (PubField::DeliveryDelay, "5000"),
        ] {
            m.fields.insert(f, v.to_string());
        }
        m.properties.push(Property {
            name: "n".into(),
            value: PubValue::Int(5),
        });
        m.properties.push(Property {
            name: "s".into(),
            value: PubValue::String("x".into()),
        });
        m
    }

    #[test]
    fn outbound_mapping() {
        let a = to_amqp(
            &sample(),
            "/queue/orders",
            Some("ID:pub-1".into()),
            TextBody::Value,
            &Prefixes::default(),
        )
        .unwrap();
        assert_eq!(a.body, Body::Value(Value::String("héllo".into())));
        let h = a.header.as_ref().unwrap();
        assert_eq!(
            (h.durable, h.priority, h.ttl),
            (Some(true), Some(7), Some(60000))
        );
        let p = a.properties.as_ref().unwrap();
        assert_eq!(p.to.as_deref(), Some("orders"));
        assert_eq!(p.subject.as_deref(), Some("order"));
        assert_eq!(p.reply_to.as_deref(), Some("replies"));
        assert_eq!(
            p.absolute_expiry_time.unwrap().0 - p.creation_time.unwrap().0,
            60000
        );
        let ann = a.message_annotations.as_ref().unwrap();
        assert_eq!(
            ann.get("x-opt-jms-msg-type"),
            Some(&Value::Byte(JMS_TEXT_MESSAGE))
        );
        assert_eq!(ann.get("x-opt-jms-dest"), Some(&Value::Byte(JMS_QUEUE)));
        assert_eq!(ann.get("x-opt-jms-reply-to"), Some(&Value::Byte(JMS_TOPIC)));
        assert_eq!(ann.get("x-opt-delivery-delay"), Some(&Value::Long(5000)));
        assert_eq!(
            a.application_properties.as_ref().unwrap().get("n"),
            Some(&Value::Int(5))
        );

        let d = to_amqp(&sample(), "q", None, TextBody::Data, &Prefixes::default()).unwrap();
        assert_eq!(d.body, Body::Data(vec!["héllo".as_bytes().to_vec()]));
        assert_eq!(
            d.properties.unwrap().content_type.unwrap().0,
            "text/plain; charset=utf-8"
        );
        let bytes = to_amqp(
            &Message::new(vec![0u8, 255]),
            "q",
            None,
            TextBody::Value,
            &Prefixes::default(),
        )
        .unwrap();
        assert_eq!(bytes.body, Body::Data(vec![vec![0, 255]]));
        assert_eq!(
            bytes.message_annotations.unwrap().get("x-opt-jms-msg-type"),
            Some(&Value::Byte(JMS_BYTES_MESSAGE))
        );
    }

    #[test]
    fn round_trip_through_the_wire_format() {
        let a = to_amqp(
            &sample(),
            "/queue/orders",
            Some("ID:pub-1".into()),
            TextBody::Value,
            &Prefixes::default(),
        )
        .unwrap();
        let back = AmqpMessage::decode(&a.encode().unwrap()).unwrap();
        assert_eq!(back, a);
        let m = from_amqp(&back).unwrap();
        assert_eq!(m.body, "héllo".as_bytes());
        assert_eq!(m.body_type, Some(BodyType::Text));
        let f = |x: PubField| m.field(x).map(str::to_string);
        assert_eq!(f(PubField::MessageId).as_deref(), Some("ID:pub-1"));
        assert_eq!(f(PubField::Destination).as_deref(), Some("orders"));
        assert_eq!(f(PubField::CorrelationId).as_deref(), Some("c-1"));
        assert_eq!(f(PubField::Type).as_deref(), Some("order"));
        assert_eq!(
            (
                f(PubField::Priority).as_deref(),
                f(PubField::Persistent).as_deref()
            ),
            (Some("7"), Some("true"))
        );
        assert_eq!(m.property("n"), Some(&PubValue::Int(5)));
        assert!(m
            .protocol
            .iter()
            .any(|(k, v)| k == "x-opt-jms-msg-type" && v == "5"));
    }

    #[test]
    fn inbound_body_typing_follows_activemq() {
        let with = |body: Body, ct: Option<&str>| AmqpMessage {
            properties: Some(Properties {
                content_type: ct.map(Symbol::new),
                ..Default::default()
            }),
            body,
            ..Default::default()
        };
        let t = |m: AmqpMessage| from_amqp(&m).unwrap().body_type.unwrap();
        assert_eq!(
            t(with(
                Body::Value(Value::String("x".into())),
                Some("application/octet-stream")
            )),
            BodyType::Text
        );
        assert_eq!(
            t(with(
                Body::Data(vec![b"{}".to_vec()]),
                Some("application/json")
            )),
            BodyType::Text
        );
        assert_eq!(
            t(with(Body::Data(vec![b"x".to_vec()]), None)),
            BodyType::Bytes
        );
        assert_eq!(
            t(with(Body::Data(vec![vec![0xff]]), Some("text/plain"))),
            BodyType::Bytes
        ); // not UTF-8
        assert_eq!(
            t(with(Body::Value(Value::Binary(vec![1])), None)),
            BodyType::Bytes
        );
        let map = from_amqp(&with(Body::Value(Value::Map(vec![])), None)).unwrap();
        assert!(map
            .protocol
            .iter()
            .any(|(k, v)| k == "body-section" && v == "amqp-value map"));
        // Multiple data sections are one body.
        assert_eq!(
            from_amqp(&with(
                Body::Data(vec![b"ab".to_vec(), b"cd".to_vec()]),
                None
            ))
            .unwrap()
            .body,
            b"abcd"
        );
    }
}
