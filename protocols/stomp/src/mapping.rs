//! How pub's message model maps onto STOMP frames.
//!
//! STOMP has one flat, string-only header namespace. Brokers then interpret it:
//! ActiveMQ and Artemis turn the headers below into JMS headers and everything
//! else into String properties. So on the wire:
//! - standard fields -> the header names in [`FIELD_HEADERS`] (`ttl` becomes an
//!   absolute `expires`);
//! - properties -> plain headers (values stringified);
//! - protocol headers -> verbatim.
//!
//! Property names that collide with headers STOMP or the brokers interpret are
//! rejected ([`RESERVED`]), since `-P priority=1` would otherwise quietly set JMSPriority.

use pub_core::module::{DestinationOption, OptionScope};
use pub_core::{Field, Message, Value};

use crate::codec::Frame;

/// Standard field <-> STOMP header name (as used by ActiveMQ/Artemis).
pub const FIELD_HEADERS: &[(Field, &str)] = &[
    (Field::MessageId, "message-id"),
    (Field::Destination, "destination"),
    (Field::Timestamp, "timestamp"),
    (Field::CorrelationId, "correlation-id"),
    (Field::ReplyTo, "reply-to"),
    (Field::Type, "type"),
    (Field::Priority, "priority"),
    (Field::Persistent, "persistent"),
    (Field::Expiration, "expires"),
    (Field::GroupId, "JMSXGroupID"),
    (Field::GroupSeq, "JMSXGroupSeq"),
    (Field::ContentType, "content-type"),
    (Field::Redelivered, "redelivered"),
    (Field::DeliveryCount, "JMSXDeliveryCount"),
];

/// Headers ActiveMQ and Artemis add on delivery that become properties here:
/// the JMS spec's provider-set JMSX names and Artemis's internal ones.
pub const PROVIDER_PROPERTIES: &[&str] = &[
    "JMSXUserID",
    "JMSXAppID",
    "JMSXProducerTXID",
    "JMSXConsumerTXID",
    "JMSXRcvTimestamp",
    "JMSXState",
    "__AMQ_*",
    "_AMQ_*",
    "__HDR_*",
    // ActiveMQ's AMQP transformer records what the AMQP message looked like.
    "JMS_AMQP_*",
];

/// Fields this module can send. `delivery-delay` is broker-specific over STOMP
/// (e.g. ActiveMQ's `AMQ_SCHEDULED_DELAY` property), so it is not mapped.
pub const SEND_FIELDS: &[Field] = &[
    Field::CorrelationId,
    Field::ReplyTo,
    Field::Type,
    Field::Priority,
    Field::Persistent,
    Field::Ttl,
    Field::GroupId,
    Field::GroupSeq,
    Field::ContentType,
];

/// Frame headers that belong to the STOMP conversation, not the message.
const PROTOCOL: &[&str] = &[
    "subscription",
    "ack",
    "content-length",
    "receipt",
    "receipt-id",
    "transaction",
];

/// Names a property may not use over STOMP.
pub const RESERVED: &[&str] = &[
    "destination",
    "receipt",
    "content-length",
    "content-type",
    "transaction",
    "id",
    "ack",
    "subscription",
    "message-id",
    "receipt-id",
    "selector",
    "host",
    "login",
    "passcode",
    "heart-beat",
    "accept-version",
    "version",
    "session",
    "server",
    "message",
    "correlation-id",
    "reply-to",
    "type",
    "priority",
    "persistent",
    "expires",
    "timestamp",
    "redelivered",
    "JMSXGroupID",
    "JMSXGroupSeq",
    "JMSXDeliveryCount",
];

pub const DESTINATION_OPTIONS: &[DestinationOption] = &[
    DestinationOption {
        pattern: "activemq.*",
        scope: OptionScope::Consume,
        help: "ActiveMQ consumer options, e.g. activemq.prefetchSize, activemq.exclusive",
    },
    DestinationOption {
        pattern: "prefetch-count",
        scope: OptionScope::Consume,
        help: "RabbitMQ consumer prefetch",
    },
    DestinationOption {
        pattern: "x-*",
        scope: OptionScope::Both,
        help: "RabbitMQ queue arguments for auto-declared queues, e.g. x-queue-type, x-max-length",
    },
];

fn header_for(f: Field) -> Option<&'static str> {
    FIELD_HEADERS.iter().find(|(x, _)| *x == f).map(|(_, h)| *h)
}

/// Message -> SEND headers. Fields the module cannot carry are skipped (the CLI
/// has already warned or refused, per strict mode).
pub fn message_headers(msg: &Message) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for (f, v) in &msg.fields {
        match f {
            Field::Ttl => {
                // A ttl of 0 means no expiry, as in JMS.
                let ttl: i64 = v.parse().unwrap_or(0);
                if ttl > 0 {
                    let expires = pub_core::now_millis().saturating_add(ttl);
                    out.push(("expires".to_string(), expires.to_string()));
                }
            }
            f if SEND_FIELDS.contains(f) => {
                if let Some(h) = header_for(*f) {
                    out.push((h.to_string(), v.clone()));
                }
            }
            _ => {}
        }
    }
    out.extend(
        msg.properties
            .iter()
            .map(|p| (p.name.clone(), p.value.to_string())),
    );
    out.extend(msg.protocol.iter().cloned());
    out
}

/// MESSAGE frame -> Message: known headers become fields, conversation headers
/// become protocol headers, the rest are (String) properties.
pub fn message_from_frame(frame: Frame) -> Message {
    let mut msg = Message::new(frame.body);
    for (k, v) in frame.headers {
        if let Some((f, _)) = FIELD_HEADERS.iter().find(|(_, h)| *h == k) {
            if *f == Field::Expiration && v == "0" {
                continue; // 0 = never expires
            }
            msg.fields.entry(*f).or_insert(v); // first occurrence wins in STOMP
        } else if PROTOCOL.contains(&k.as_str()) {
            msg.protocol.push((k, v));
        } else if msg.property(&k).is_none() {
            msg.properties.push(pub_core::message::Property {
                name: k,
                value: Value::String(v),
            });
        }
    }
    msg
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_mapping() {
        let mut m = Message::new("x");
        m.set_field(Field::CorrelationId, "c1").unwrap();
        m.set_field(Field::GroupId, "g").unwrap();
        m.set_field(Field::Ttl, "1s").unwrap();
        m.set_property("orderId", Value::Long(42));
        m.protocol.push(("x-raw".into(), "1".into()));
        let h = message_headers(&m);
        assert!(h.contains(&("correlation-id".into(), "c1".into())));
        assert!(h.contains(&("JMSXGroupID".into(), "g".into())));
        assert!(h.contains(&("orderId".into(), "42".into())));
        assert!(h.contains(&("x-raw".into(), "1".into())));
        let expires: i64 = h
            .iter()
            .find(|(k, _)| k == "expires")
            .unwrap()
            .1
            .parse()
            .unwrap();
        assert!(expires > pub_core::now_millis() && expires <= pub_core::now_millis() + 1000);
    }

    #[test]
    fn receive_mapping() {
        let f = Frame::new("MESSAGE")
            .header("message-id", "m1")
            .header("subscription", "s1")
            .header("priority", "4")
            .header("expires", "0")
            .header("orderId", "42")
            .header("JMSXGroupID", "g")
            .body("hi");
        let m = message_from_frame(f);
        assert_eq!(m.field(Field::MessageId), Some("m1"));
        assert_eq!(m.field(Field::Priority), Some("4"));
        assert_eq!(m.field(Field::GroupId), Some("g"));
        assert_eq!(m.field(Field::Expiration), None);
        assert_eq!(
            m.protocol,
            vec![("subscription".to_string(), "s1".to_string())]
        );
        assert_eq!(m.property("orderId"), Some(&Value::String("42".into())));
        assert_eq!(m.body, b"hi");
    }

    #[test]
    fn mapped_headers_are_reserved() {
        for (_, h) in FIELD_HEADERS {
            assert!(RESERVED.contains(h), "{h} should be reserved");
        }
    }
}
