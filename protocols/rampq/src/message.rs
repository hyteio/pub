//! Destinations, and pub's message model as AMQP 0-9-1 content.
//!
//! - `/queue/NAME` (or `queue://NAME`, or a bare name): the queue — publish
//!   through the default exchange with the queue name as routing key;
//! - `/exchange/NAME/ROUTING-KEY`: publish to an exchange; receiving binds a
//!   private queue (or, with `--durable`, a named durable one) with the key;
//! - `/topic/KEY`: `amq.topic` with that key, as RabbitMQ's STOMP plugin does.
//!
//! Properties: content-type (text gets `text/plain; charset=utf-8`), delivery
//! mode (persistent unless `persistent: false`), priority, correlation-id,
//! reply-to, expiration (= ttl in ms), type, message-id, timestamp (seconds);
//! application properties → the typed `headers` table. On receive, a textual
//! UTF-8 content type is `message-type: text`; exchange, routing key and
//! redelivered appear under `# protocol`.

use std::time::{SystemTime, UNIX_EPOCH};

use pub_core::message::Property;
use pub_core::{bail_kind, err, BodyType, Field, Message, Result, Value};

use crate::codec::BasicProperties;
use crate::wire::{FieldValue, Table};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    Queue(String),
    Exchange { name: String, routing_key: String },
}

impl Target {
    pub fn parse(name: &str) -> Result<Target> {
        let lower = name.to_ascii_lowercase();
        let t = if let Some(q) = ["/queue/", "queue://"]
            .iter()
            .find(|p| lower.starts_with(**p))
        {
            Target::Queue(name[q.len()..].to_string())
        } else if let Some(rest) = name.strip_prefix("/exchange/") {
            let (ex, key) = rest.split_once('/').unwrap_or((rest, ""));
            if ex.is_empty() {
                bail_kind!(Usage, "no exchange name in '{name}'");
            }
            Target::Exchange {
                name: ex.to_string(),
                routing_key: key.to_string(),
            }
        } else if let Some(t) = ["/topic/", "topic://"]
            .iter()
            .find(|p| lower.starts_with(**p))
        {
            Target::Exchange {
                name: "amq.topic".into(),
                routing_key: name[t.len()..].to_string(),
            }
        } else {
            Target::Queue(name.strip_prefix('/').unwrap_or(name).to_string())
        };
        if let Target::Queue(q) = &t {
            if q.is_empty() {
                bail_kind!(Usage, "empty queue name in '{name}'");
            }
        }
        Ok(t)
    }

    /// Where a publish goes: (exchange, routing key).
    pub fn publish_to(&self) -> (&str, &str) {
        match self {
            Target::Queue(q) => ("", q),
            Target::Exchange { name, routing_key } => (name, routing_key),
        }
    }

    pub fn display(&self) -> String {
        match self {
            Target::Queue(q) => format!("queue {q}"),
            Target::Exchange { name, routing_key } if routing_key.is_empty() => {
                format!("exchange {name}")
            }
            Target::Exchange { name, routing_key } => {
                format!("exchange {name} (key {routing_key})")
            }
        }
    }
}

/// A destination name as pub shows it: `/queue/X` for the default exchange,
/// `/exchange/E/KEY` otherwise.
pub fn destination_name(exchange: &str, routing_key: &str) -> String {
    if exchange.is_empty() {
        format!("/queue/{routing_key}")
    } else {
        format!("/exchange/{exchange}/{routing_key}")
    }
}

/// Textual content types (text/*, JSON, XML, JavaScript) with a UTF-8 or no charset.
pub fn is_textual_utf8(content_type: &str) -> bool {
    let lower = content_type.to_ascii_lowercase();
    let (mime, params) = lower.split_once(';').unwrap_or((&lower, ""));
    let Some((ty, sub)) = mime.trim().split_once('/') else {
        return false;
    };
    let textual = ty == "text"
        || (ty == "application"
            && (matches!(sub, "json" | "xml" | "javascript" | "ecmascript")
                || sub.ends_with("+json")
                || sub.ends_with("+xml")));
    let charset = params
        .split(';')
        .filter_map(|p| p.trim().strip_prefix("charset="))
        .next()
        .map(|c| c.trim_matches('"'));
    textual && charset.is_none_or(|c| c == "utf-8" || c == "utf8")
}

fn header_value(v: &Value) -> FieldValue {
    match v {
        Value::String(s) => FieldValue::LongStr(s.as_bytes().to_vec()),
        Value::Bool(b) => FieldValue::Bool(*b),
        Value::Byte(n) => FieldValue::I8(*n),
        Value::Short(n) => FieldValue::I16(*n),
        Value::Int(n) => FieldValue::I32(*n),
        Value::Long(n) => FieldValue::I64(*n),
        Value::Float(n) => FieldValue::F32(*n),
        Value::Double(n) => FieldValue::F64(*n),
    }
}

/// The reply-to a RabbitMQ consumer expects: a queue name (or the name as given).
fn reply_to(name: &str) -> Result<String> {
    Ok(match Target::parse(name)? {
        Target::Queue(q) => q,
        Target::Exchange { .. } => name.to_string(),
    })
}

/// pub's message as basic content properties.
pub fn to_properties(msg: &Message, message_id: Option<String>) -> Result<BasicProperties> {
    let field = |f: Field| msg.field(f);
    let text = msg.body_type == Some(BodyType::Text);
    if text && std::str::from_utf8(&msg.body).is_err() {
        bail_kind!(Usage, "message-type = text needs a UTF-8 body");
    }
    let content_type = field(Field::ContentType)
        .map(str::to_string)
        .or_else(|| text.then(|| "text/plain; charset=utf-8".to_string()));
    let persistent = field(Field::Persistent).is_none_or(|v| v == "true");
    let priority = match field(Field::Priority) {
        Some(p) => Some(
            p.parse::<u8>()
                .map_err(|_| err!(Usage, "invalid priority '{p}'"))?,
        ),
        None => None,
    };
    let headers: Table = msg
        .properties
        .iter()
        .map(|p| (p.name.clone(), header_value(&p.value)))
        .collect();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    Ok(BasicProperties {
        content_type,
        headers: (!headers.is_empty()).then_some(headers),
        delivery_mode: Some(if persistent { 2 } else { 1 }),
        priority,
        correlation_id: field(Field::CorrelationId).map(str::to_string),
        reply_to: field(Field::ReplyTo).map(reply_to).transpose()?,
        expiration: field(Field::Ttl).map(str::to_string),
        message_id,
        timestamp: Some(now.as_secs()),
        kind: field(Field::Type).map(str::to_string),
        ..Default::default()
    })
}

fn pub_value(v: &FieldValue) -> Option<Value> {
    Some(match v {
        FieldValue::Void => return None,
        FieldValue::Bool(b) => Value::Bool(*b),
        FieldValue::I8(n) => Value::Byte(*n),
        FieldValue::I16(n) => Value::Short(*n),
        FieldValue::I32(n) => Value::Int(*n),
        FieldValue::I64(n) => Value::Long(*n),
        FieldValue::U8(n) => Value::Short(*n as i16),
        FieldValue::U16(n) => Value::Int(*n as i32),
        FieldValue::U32(n) => Value::Long(*n as i64),
        FieldValue::F32(n) => Value::Float(*n),
        FieldValue::F64(n) => Value::Double(*n),
        FieldValue::Timestamp(t) => Value::Long(*t as i64),
        FieldValue::LongStr(s) => Value::String(String::from_utf8_lossy(s).into_owned()),
        FieldValue::Bytes(b) => Value::String(b.iter().map(|x| format!("{x:02x}")).collect()),
        other => Value::String(format!("{other:?}")),
    })
}

/// A delivery as pub's message.
pub fn from_delivery(
    exchange: &str,
    routing_key: &str,
    redelivered: bool,
    props: &BasicProperties,
    body: Vec<u8>,
) -> Message {
    let text = props.content_type.as_deref().is_some_and(is_textual_utf8)
        && std::str::from_utf8(&body).is_ok();
    let mut msg = Message::new(body);
    msg.body_type = Some(if text {
        BodyType::Text
    } else {
        BodyType::Bytes
    });
    let mut set = |f: Field, v: Option<String>| {
        if let Some(v) = v {
            msg.fields.insert(f, v);
        }
    };
    set(Field::MessageId, props.message_id.clone());
    set(
        Field::Destination,
        Some(destination_name(exchange, routing_key)),
    );
    set(
        Field::Timestamp,
        props.timestamp.map(|t| t.saturating_mul(1000).to_string()),
    );
    set(Field::CorrelationId, props.correlation_id.clone());
    set(Field::ReplyTo, props.reply_to.clone());
    set(Field::Type, props.kind.clone());
    set(
        Field::Priority,
        props.priority.map(|p| p.min(9).to_string()),
    );
    set(
        Field::Persistent,
        props.delivery_mode.map(|m| (m == 2).to_string()),
    );
    set(Field::ContentType, props.content_type.clone());
    set(Field::Redelivered, Some(redelivered.to_string()));
    if let Some(h) = &props.headers {
        for (k, v) in h {
            if let Some(value) = pub_value(v) {
                msg.properties.push(Property {
                    name: k.clone(),
                    value,
                });
            }
        }
    }
    msg.protocol.push(("exchange".into(), exchange.to_string()));
    msg.protocol
        .push(("routing-key".into(), routing_key.to_string()));
    for (k, v) in [
        ("expiration", &props.expiration),
        ("content-encoding", &props.content_encoding),
        ("user-id", &props.user_id),
        ("app-id", &props.app_id),
    ] {
        if let Some(v) = v {
            msg.protocol.push((k.into(), v.clone()));
        }
    }
    msg
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn targets() {
        assert_eq!(
            Target::parse("/queue/orders").unwrap(),
            Target::Queue("orders".into())
        );
        assert_eq!(
            Target::parse("orders").unwrap(),
            Target::Queue("orders".into())
        );
        assert_eq!(
            Target::parse("/exchange/logs/app.error").unwrap(),
            Target::Exchange {
                name: "logs".into(),
                routing_key: "app.error".into()
            }
        );
        assert_eq!(
            Target::parse("/exchange/fan").unwrap(),
            Target::Exchange {
                name: "fan".into(),
                routing_key: "".into()
            }
        );
        assert_eq!(
            Target::parse("/exchange/x/a/b").unwrap(),
            Target::Exchange {
                name: "x".into(),
                routing_key: "a/b".into()
            }
        );
        assert_eq!(
            Target::parse("/topic/sensors.#").unwrap(),
            Target::Exchange {
                name: "amq.topic".into(),
                routing_key: "sensors.#".into()
            }
        );
        assert!(Target::parse("/queue/").is_err());
        assert!(Target::parse("/exchange/").is_err());
        assert_eq!(Target::parse("/queue/q").unwrap().publish_to(), ("", "q"));
    }

    #[test]
    fn mapping_round_trip() {
        let mut m = Message::new("héllo".as_bytes().to_vec());
        m.body_type = Some(BodyType::Text);
        for (f, v) in [
            (Field::CorrelationId, "c-1"),
            (Field::Priority, "7"),
            (Field::Ttl, "60000"),
            (Field::Type, "order"),
            (Field::ReplyTo, "/queue/replies"),
        ] {
            m.fields.insert(f, v.to_string());
        }
        m.properties.push(Property {
            name: "n".into(),
            value: Value::Int(5),
        });
        m.properties.push(Property {
            name: "s".into(),
            value: Value::String("x".into()),
        });
        let p = to_properties(&m, Some("ID:1".into())).unwrap();
        assert_eq!(p.content_type.as_deref(), Some("text/plain; charset=utf-8"));
        assert_eq!((p.delivery_mode, p.priority), (Some(2), Some(7)));
        assert_eq!(
            (p.expiration.as_deref(), p.reply_to.as_deref()),
            (Some("60000"), Some("replies"))
        );

        let back = from_delivery("", "orders", true, &p, m.body.clone());
        assert_eq!(back.body_type, Some(BodyType::Text));
        assert_eq!(back.field(Field::Destination), Some("/queue/orders"));
        assert_eq!(back.field(Field::CorrelationId), Some("c-1"));
        assert_eq!(back.field(Field::Redelivered), Some("true"));
        assert_eq!(back.property("n"), Some(&Value::Int(5)));
        assert!(back
            .protocol
            .contains(&("routing-key".into(), "orders".into())));

        let bytes = to_properties(&Message::new(vec![0u8, 255]), None).unwrap();
        assert_eq!(bytes.content_type, None);
        assert_eq!(
            from_delivery("x", "k", false, &bytes, vec![0, 255]).body_type,
            Some(BodyType::Bytes)
        );
        assert_eq!(
            from_delivery("x", "k", false, &bytes, vec![]).field(Field::Destination),
            Some("/exchange/x/k")
        );
    }
}
