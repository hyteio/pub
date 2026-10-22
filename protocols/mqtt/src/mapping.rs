//! pub's message model <-> MQTT 5 PUBLISH.
//!
//! | pub                    | MQTT 5                                  |
//! |------------------------|-----------------------------------------|
//! | `content-type`         | Content Type property                   |
//! | `reply-to`             | Response Topic                          |
//! | `correlation-id`       | Correlation Data (binary; UTF-8 here)   |
//! | `ttl`                  | Message Expiry Interval (whole seconds, rounded up) |
//! | `message-type = text`  | Payload Format Indicator = 1 (UTF-8)    |
//! | properties             | User Properties (strings, may repeat)   |
//! | `destination` (recv)   | topic name                              |
//!
//! MQTT has no priority, type, persistence flag, groups or delivery delay; the
//! CLI reports those as unsupported. User properties are their own namespace,
//! so no property names are reserved. Receive-side details with no standard
//! field (QoS, retain, dup, remaining expiry, subscription ids)
//! are reported as protocol headers.

use pub_core::message::Property;
use pub_core::{BodyType, Field, Message, Value};

use crate::codec::{prop, PropValue, Props, Publish};

/// Standard fields this module can send.
pub const SEND_FIELDS: &[Field] = &[
    Field::ContentType,
    Field::ReplyTo,
    Field::CorrelationId,
    Field::Ttl,
];

/// PUBLISH properties for an outgoing message.
pub fn publish_props(msg: &Message) -> Props {
    let mut p = Props::default();
    if msg.body_type == Some(BodyType::Text) {
        p.push(prop::PAYLOAD_FORMAT_INDICATOR, PropValue::Byte(1));
    }
    if let Some(ct) = msg.field(Field::ContentType) {
        p.push(prop::CONTENT_TYPE, PropValue::Str(ct.to_string()));
    }
    if let Some(rt) = msg.field(Field::ReplyTo) {
        p.push(prop::RESPONSE_TOPIC, PropValue::Str(rt.to_string()));
    }
    if let Some(c) = msg.field(Field::CorrelationId) {
        p.push(
            prop::CORRELATION_DATA,
            PropValue::Bin(c.as_bytes().to_vec()),
        );
    }
    // A ttl of 0 means no expiry, as in JMS.
    if let Some(ms) = msg
        .field(Field::Ttl)
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&ms| ms > 0)
    {
        let secs = ms.div_ceil(1000).clamp(1, u32::MAX as u64) as u32;
        p.push(prop::MESSAGE_EXPIRY_INTERVAL, PropValue::U32(secs));
    }
    for prop_ in &msg.properties {
        p.push(
            prop::USER_PROPERTY,
            PropValue::Pair(prop_.name.clone(), prop_.value.to_string()),
        );
    }
    p
}

/// Incoming PUBLISH -> Message.
pub fn message_from_publish(p: Publish) -> Message {
    let mut msg = Message::new(p.payload);
    msg.fields.insert(Field::Destination, p.topic);
    msg.protocol.push(("qos".into(), p.qos.to_string()));
    if p.retain {
        msg.protocol.push(("retain".into(), "true".into()));
    }
    if p.dup {
        msg.protocol.push(("dup".into(), "true".into()));
    }
    for (id, v) in p.props.0 {
        match (id, v) {
            (prop::CONTENT_TYPE, PropValue::Str(s)) => {
                msg.fields.insert(Field::ContentType, s);
            }
            (prop::RESPONSE_TOPIC, PropValue::Str(s)) => {
                msg.fields.insert(Field::ReplyTo, s);
            }
            (prop::CORRELATION_DATA, PropValue::Bin(b)) => {
                let text = match String::from_utf8(b) {
                    Ok(s) => s,
                    Err(e) => e.into_bytes().iter().map(|b| format!("{b:02x}")).collect(),
                };
                msg.fields.insert(Field::CorrelationId, text);
            }
            (prop::USER_PROPERTY, PropValue::Pair(k, v)) => {
                // MQTT allows repeated names; keep them all, in order.
                msg.properties.push(Property {
                    name: k,
                    value: Value::String(v),
                });
            }
            (prop::MESSAGE_EXPIRY_INTERVAL, PropValue::U32(s)) => {
                msg.protocol
                    .push(("message-expiry-interval".into(), s.to_string()));
            }
            (prop::PAYLOAD_FORMAT_INDICATOR, PropValue::Byte(b)) => {
                msg.body_type = Some(if b == 1 {
                    BodyType::Text
                } else {
                    BodyType::Bytes
                });
            }
            (prop::SUBSCRIPTION_IDENTIFIER, PropValue::VarInt(n)) => {
                msg.protocol
                    .push(("subscription-identifier".into(), n.to_string()));
            }
            (prop::TOPIC_ALIAS, PropValue::U16(n)) => {
                msg.protocol.push(("topic-alias".into(), n.to_string()));
            }
            _ => {}
        }
    }
    msg
}

/// MQTT topic filter matching, including `$share/group/` and the rule that
/// wildcards at the first level do not match `$`-topics.
pub fn topic_matches(filter: &str, topic: &str) -> bool {
    let filter = filter
        .strip_prefix("$share/")
        .and_then(|rest| rest.split_once('/'))
        .map_or(filter, |(_, f)| f);
    if topic.starts_with('$') && (filter.starts_with('+') || filter.starts_with('#')) {
        return false;
    }
    let (mut f, mut t) = (filter.split('/'), topic.split('/'));
    loop {
        match (f.next(), t.next()) {
            (Some("#"), _) => return true,
            (Some("+"), Some(_)) => {}
            (Some(a), Some(b)) if a == b => {}
            (None, None) => return true,
            _ => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_through_publish() {
        let mut m = Message::new("hi");
        m.set_field(Field::ContentType, "text/plain").unwrap();
        m.set_field(Field::ReplyTo, "replies/1").unwrap();
        m.set_field(Field::CorrelationId, "c-1").unwrap();
        m.set_field(Field::Ttl, "1500ms").unwrap();
        m.set_property("region", Value::String("EU".into()));
        m.set_property("n", Value::Int(5));

        let props = publish_props(&m);
        assert_eq!(props.num(prop::MESSAGE_EXPIRY_INTERVAL), Some(2)); // rounded up
        let p = Publish {
            topic: "t/1".into(),
            qos: 1,
            packet_id: Some(1),
            props,
            payload: b"hi".to_vec(),
            ..Default::default()
        };
        let back = message_from_publish(p);
        assert_eq!(back.field(Field::Destination), Some("t/1"));
        assert_eq!(back.field(Field::ContentType), Some("text/plain"));
        assert_eq!(back.field(Field::ReplyTo), Some("replies/1"));
        assert_eq!(back.field(Field::CorrelationId), Some("c-1"));
        assert_eq!(back.property("n"), Some(&Value::String("5".into())));
        assert!(back.protocol.contains(&("qos".into(), "1".into())));
        assert!(back
            .protocol
            .contains(&("message-expiry-interval".into(), "2".into())));
    }

    #[test]
    fn repeated_user_properties_and_binary_correlation() {
        let mut props = Props::default();
        props
            .push(prop::USER_PROPERTY, PropValue::Pair("k".into(), "1".into()))
            .push(prop::USER_PROPERTY, PropValue::Pair("k".into(), "2".into()))
            .push(prop::CORRELATION_DATA, PropValue::Bin(vec![0xFF, 0x00]));
        let m = message_from_publish(Publish {
            topic: "t".into(),
            props,
            ..Default::default()
        });
        assert_eq!(m.properties.len(), 2);
        assert_eq!(m.field(Field::CorrelationId), Some("ff00"));
    }

    #[test]
    fn text_body_type_uses_payload_format_indicator() {
        let mut m = Message::new("hi");
        m.body_type = Some(BodyType::Text);
        let props = publish_props(&m);
        assert_eq!(props.num(prop::PAYLOAD_FORMAT_INDICATOR), Some(1));
        let back = message_from_publish(Publish {
            topic: "t".into(),
            props,
            ..Default::default()
        });
        assert_eq!(back.body_type, Some(BodyType::Text));

        m.body_type = Some(BodyType::Bytes);
        assert_eq!(publish_props(&m).num(prop::PAYLOAD_FORMAT_INDICATOR), None);
        assert_eq!(message_from_publish(Publish::default()).body_type, None);
    }
}
