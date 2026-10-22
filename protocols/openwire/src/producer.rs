//! Sending: destination names, producer options, and pub's message model
//! mapped onto an ActiveMQ message (`ActiveMQSession.send`).
//!
//! - `message-type = text` → ActiveMQTextMessage (`writeUTF8` content), else
//!   ActiveMQBytesMessage (raw content);
//! - typed properties → the marshalled primitive map;
//! - standard fields → the native message fields (JMSCorrelationID, JMSReplyTo,
//!   JMSType, JMSPriority, JMSDeliveryMode, JMSExpiration = now + ttl, JMSXGroupID/Seq);
//!   `delivery-delay` → ActiveMQ's `AMQ_SCHEDULED_DELAY` property;
//! - `jms.useCompression` deflates the content exactly as the Java client does;
//! - with the `administrative` header strategy, a document's `message-id`,
//!   `timestamp` and `expiration` are sent as they are (exact replay).

use pub_core::{bail_kind, err, Endpoint, Field, Message, Result, Value};

use crate::primitive::{encode_map, encode_text, Primitive};
use crate::types::{self, kind, DataStructure, MessageFields};

/// Java's `ActiveMQMessage.DEFAULT_PRIORITY`.
pub const DEFAULT_PRIORITY: u8 = 4;

/// ActiveMQ's scheduler property (`ScheduledMessage.AMQ_SCHEDULED_DELAY`).
pub const SCHEDULED_DELAY: &str = "AMQ_SCHEDULED_DELAY";

/// Java's `Message.DEFAULT_MINIMUM_MESSAGE_SIZE`, part of the size the producer
/// window counts.
const MINIMUM_MESSAGE_SIZE: u64 = 1024;

/// A destination name as pub accepts it: `queue://X`, `topic://X`,
/// `/queue/X`, `/topic/X`, or a bare name (a queue, like `createQueue`). Java
/// options after `?` (`queue://X?producer.windowSize=1024`) are split off and
/// returned, as `ActiveMQDestination.setPhysicalName` does.
pub fn parse_destination(name: &str) -> Result<(DataStructure, Vec<(String, String)>)> {
    let (name, query) = match name.split_once('?') {
        Some((n, q)) => (n, Some(q)),
        None => (name, None),
    };
    let lower = name.to_ascii_lowercase();
    let (topic, physical) = if let Some(rest) = strip(&lower, name, &["queue://", "/queue/"]) {
        (false, rest)
    } else if let Some(rest) = strip(&lower, name, &["topic://", "/topic/"]) {
        (true, rest)
    } else if strip(
        &lower,
        name,
        &[
            "temp-queue://",
            "temp-topic://",
            "/temp-queue/",
            "/temp-topic/",
        ],
    )
    .is_some()
    {
        bail_kind!(Usage, "temporary destinations are not supported: '{name}'");
    } else {
        (false, name.strip_prefix('/').unwrap_or(name))
    };
    if physical.is_empty() {
        bail_kind!(Usage, "empty destination name in '{name}'");
    }
    let dest = Box::new(types::Destination {
        physical_name: Some(physical.to_string()),
    });
    let ds = if topic {
        DataStructure::Topic(dest)
    } else {
        DataStructure::Queue(dest)
    };
    let mut options = Vec::new();
    for pair in query.unwrap_or("").split('&').filter(|p| !p.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        options.push((k.to_string(), v.to_string()));
    }
    Ok((ds, options))
}

fn strip<'a>(lower: &str, name: &'a str, prefixes: &[&str]) -> Option<&'a str> {
    prefixes
        .iter()
        .find(|p| lower.starts_with(**p))
        .map(|p| &name[p.len()..])
}

/// `queue://X` / `topic://X`, as ActiveMQ prints destinations.
pub fn destination_text(ds: &DataStructure) -> String {
    let (prefix, d) = match ds {
        DataStructure::Queue(d) => ("queue://", d),
        DataStructure::Topic(d) => ("topic://", d),
        DataStructure::TempQueue(d) => ("temp-queue://", d),
        DataStructure::TempTopic(d) => ("temp-topic://", d),
        other => return other.name().to_string(),
    };
    format!("{prefix}{}", d.physical_name.as_deref().unwrap_or(""))
}

/// ProducerInfo fields settable from `producer.*` destination options.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerOptions {
    pub window_size: i32,
    pub dispatch_async: bool,
}

/// Apply `producer.*` options the way `ActiveMQMessageProducer` does: known
/// ones set ProducerInfo fields, unknown ones are an error. Other options
/// (e.g. `consumer.*`) are not the producer's business.
pub fn producer_options(options: &[(String, String)], window_size: i32) -> Result<ProducerOptions> {
    let mut p = ProducerOptions {
        window_size,
        dispatch_async: false,
    };
    for (key, value) in options {
        let Some(name) = key.strip_prefix("producer.") else {
            continue;
        };
        match name.to_ascii_lowercase().as_str() {
            "windowsize" => {
                p.window_size = value
                    .trim()
                    .parse::<i32>()
                    .ok()
                    .filter(|n| *n >= 0)
                    .ok_or_else(|| err!(Usage, "{key} must be a byte count, got '{value}'"))?
            }
            "dispatchasync" => {
                p.dispatch_async = match value.trim().to_ascii_lowercase().as_str() {
                    "true" => true,
                    "false" => false,
                    _ => bail_kind!(Usage, "{key} must be true or false, got '{value}'"),
                }
            }
            _ => bail_kind!(
                Usage,
                "unknown producer option '{key}' (ActiveMQ supports producer.windowSize and producer.dispatchAsync)"
            ),
        }
    }
    Ok(p)
}

/// The connection factory's send settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SendOptions {
    pub use_async_send: bool,
    pub always_sync_send: bool,
    /// ms; 0 = no send timeout (pub still bounds the wait by `receive-timeout`).
    pub send_timeout_ms: u64,
    pub producer_window_size: i32,
    pub use_compression: bool,
    pub disable_timestamps: bool,
}

impl SendOptions {
    pub fn from_endpoint(ep: &Endpoint) -> Result<Self> {
        let count = |key: &str| -> Result<u64> {
            match ep.prop(key) {
                None => Ok(0),
                Some(v) => v
                    .trim()
                    .parse::<u64>()
                    .map_err(|_| err!(Config, "{key} must be a whole number, got '{v}'")),
            }
        };
        let window = count("jms.producerWindowSize")?;
        Ok(SendOptions {
            use_async_send: ep.prop_bool("jms.useAsyncSend", false)?,
            always_sync_send: ep.prop_bool("jms.alwaysSyncSend", false)?,
            send_timeout_ms: count("jms.sendTimeout")?,
            producer_window_size: i32::try_from(window)
                .map_err(|_| err!(Config, "jms.producerWindowSize is too large: {window}"))?,
            use_compression: ep.prop_bool("jms.useCompression", false)?,
            disable_timestamps: ep.prop_bool("jms.disableTimeStampsByDefault", false)?,
        })
    }

    /// Java's rule: asynchronous iff there is no send timeout, sends aren't
    /// forced synchronous, and the message is non-persistent or async sends are on.
    pub fn is_async(&self, persistent: bool) -> bool {
        self.send_timeout_ms == 0 && !self.always_sync_send && (!persistent || self.use_async_send)
    }
}

fn primitive(v: &Value) -> Primitive {
    match v {
        Value::String(s) => Primitive::String(s.clone()),
        Value::Bool(b) => Primitive::Bool(*b),
        Value::Byte(n) => Primitive::Byte(*n),
        Value::Short(n) => Primitive::Short(*n),
        Value::Int(n) => Primitive::Int(*n),
        Value::Long(n) => Primitive::Long(*n),
        Value::Float(n) => Primitive::Float(*n),
        Value::Double(n) => Primitive::Double(*n),
    }
}

fn zlib(data: &[u8]) -> Vec<u8> {
    // java.util.zip.Deflater's default level.
    miniz_oxide::deflate::compress_to_vec_zlib(data, 6)
}

/// Build the OpenWire message for one send. `message_id` and `producer_id`
/// are the producer's; the command id and response flag are set by the session.
pub fn build_message(
    msg: &Message,
    destination: &DataStructure,
    producer_id: &DataStructure,
    message_id: DataStructure,
    opts: &SendOptions,
) -> Result<types::Message> {
    let field = |f: Field| msg.fields.get(&f).map(String::as_str);
    let now = pub_core::now_millis();

    let persistent = field(Field::Persistent).is_none_or(|v| v == "true");
    let priority = match field(Field::Priority) {
        Some(p) => p
            .parse::<u8>()
            .ok()
            .filter(|p| *p <= 9)
            .ok_or_else(|| err!(Usage, "priority must be 0-9, got '{p}'"))?,
        None => DEFAULT_PRIORITY,
    };
    let message_id = match field(Field::MessageId) {
        Some(id) => preserved_message_id(id, &message_id),
        None => message_id,
    };
    let timestamp = match field(Field::Timestamp) {
        Some(t) => t
            .parse::<i64>()
            .map_err(|_| err!(Usage, "invalid timestamp '{t}'"))?,
        None if opts.disable_timestamps => 0,
        None => now,
    };
    let expiration = match field(Field::Expiration) {
        Some(e) => Some(
            e.parse::<i64>()
                .map_err(|_| err!(Usage, "invalid expiration '{e}'"))?,
        ),
        None => None,
    };
    let ttl: i64 = match field(Field::Ttl) {
        Some(t) => t.parse().map_err(|_| err!(Usage, "invalid ttl '{t}'"))?,
        None => 0,
    };
    let group_sequence = match field(Field::GroupSeq) {
        Some(s) => s
            .parse::<i32>()
            .map_err(|_| err!(Usage, "group-seq must fit in 32 bits, got '{s}'"))?,
        None => 0,
    };
    let reply_to = match field(Field::ReplyTo) {
        Some(r) => Some(Box::new(parse_destination(r)?.0)),
        None => None,
    };

    let mut props: Vec<(String, Primitive)> = msg
        .properties
        .iter()
        .map(|p| (p.name.clone(), primitive(&p.value)))
        .collect();
    if let Some(d) = field(Field::DeliveryDelay) {
        let ms: i64 = d
            .parse()
            .map_err(|_| err!(Usage, "invalid delivery-delay '{d}'"))?;
        if ms > 0 {
            props.retain(|(k, _)| k != SCHEDULED_DELAY);
            props.push((SCHEDULED_DELAY.to_string(), Primitive::Long(ms)));
        }
    }
    let marshalled_properties = if props.is_empty() {
        None
    } else {
        Some(encode_map(&props)?)
    };

    let text = msg.body_type == Some(pub_core::BodyType::Text);
    let content = if text {
        let s = std::str::from_utf8(&msg.body)
            .map_err(|_| err!(Usage, "message-type = text needs a UTF-8 body"))?;
        let utf8 = encode_text(s);
        if opts.use_compression {
            zlib(&utf8)
        } else {
            utf8
        }
    } else if opts.use_compression {
        // ActiveMQBytesMessage: original length, then the deflated bytes.
        let mut out = (msg.body.len() as i32).to_be_bytes().to_vec();
        out.extend(zlib(&msg.body));
        out
    } else {
        msg.body.clone()
    };

    Ok(types::Message {
        kind: if text { kind::TEXT } else { kind::BYTES },
        fields: MessageFields {
            producer_id: Some(Box::new(producer_id.clone())),
            destination: Some(Box::new(destination.clone())),
            message_id: Some(Box::new(message_id)),
            group_id: field(Field::GroupId).map(str::to_string),
            group_sequence,
            correlation_id: field(Field::CorrelationId).map(str::to_string),
            persistent,
            expiration: expiration.unwrap_or(if ttl > 0 { now.saturating_add(ttl) } else { 0 }),
            priority,
            reply_to,
            timestamp,
            jms_type: field(Field::Type).map(str::to_string),
            content: Some(content),
            marshalled_properties,
            compressed: opts.use_compression,
            ..Default::default()
        },
        blob: None,
    })
}

/// A message id kept from a captured message. ActiveMQ ids
/// (`connection:session:producer:sequence`) are parsed the way Java's
/// `MessageId(String)` does, so brokers see the original producer and sequence;
/// anything else (an Artemis UUID) is sent as the id's text view.
pub fn preserved_message_id(id: &str, fallback: &DataStructure) -> DataStructure {
    let parse = || -> Option<types::MessageId> {
        let (rest, seq) = id.rsplit_once(':')?;
        let (rest, producer) = rest.rsplit_once(':')?;
        let (connection, session) = rest.rsplit_once(':')?;
        Some(types::MessageId {
            text_view: None,
            producer_id: Some(Box::new(DataStructure::ProducerId(Box::new(
                types::ProducerId {
                    connection_id: Some(connection.to_string()),
                    value: producer.parse().ok()?,
                    session_id: session.parse().ok()?,
                },
            )))),
            producer_sequence_id: seq.parse().ok()?,
            broker_sequence_id: 0,
        })
    };
    let mid = parse().unwrap_or_else(|| {
        let mut m = match fallback {
            DataStructure::MessageId(m) => (**m).clone(),
            _ => types::MessageId::default(),
        };
        m.text_view = Some(id.to_string());
        m
    });
    DataStructure::MessageId(Box::new(mid))
}

/// `Message.getSize()`: what the producer window and ProducerAck count.
pub fn message_size(m: &types::Message) -> u64 {
    let len = |b: &Option<Vec<u8>>| b.as_ref().map_or(0, |b| b.len() as u64);
    MINIMUM_MESSAGE_SIZE + len(&m.fields.marshalled_properties) + len(&m.fields.content)
}

/// `ProducerId.toString()` + `:` + sequence: the JMSMessageID Java reports.
pub fn message_id_text(producer: &types::ProducerId, sequence: i64) -> String {
    format!(
        "{}:{}:{}:{sequence}",
        producer.connection_id.as_deref().unwrap_or(""),
        producer.session_id,
        producer.value
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitive::{decode_map, decode_text};
    use pub_core::message::Property;
    use pub_core::BodyType;

    fn dest(ds: &DataStructure) -> String {
        destination_text(ds)
    }

    #[test]
    fn destination_names() {
        let p = |n: &str| dest(&parse_destination(n).unwrap().0);
        assert_eq!(p("/queue/orders"), "queue://orders");
        assert_eq!(p("/topic/a.b"), "topic://a.b");
        assert_eq!(p("queue://orders"), "queue://orders");
        assert_eq!(p("TOPIC://Prices"), "topic://Prices");
        assert_eq!(p("orders"), "queue://orders");
        assert_eq!(p("/orders"), "queue://orders");
        assert_eq!(p("/queue/a/b"), "queue://a/b");
        // Composite destinations pass through as the physical name.
        assert_eq!(p("queue://a,b"), "queue://a,b");
        let (d, opts) =
            parse_destination("queue://x?producer.windowSize=10&consumer.prefetchSize=1").unwrap();
        assert_eq!(dest(&d), "queue://x");
        assert_eq!(opts.len(), 2);
        assert!(parse_destination("/queue/").is_err());
        assert!(parse_destination("temp-queue://x").is_err());
    }

    #[test]
    fn producer_destination_options() {
        let o = |k: &str, v: &str| vec![(k.to_string(), v.to_string())];
        let p = producer_options(&o("producer.windowSize", "2048"), 0).unwrap();
        assert_eq!(p.window_size, 2048);
        assert!(
            producer_options(&o("producer.dispatchAsync", "true"), 0)
                .unwrap()
                .dispatch_async
        );
        assert_eq!(
            producer_options(&o("consumer.prefetchSize", "1"), 7)
                .unwrap()
                .window_size,
            7
        );
        let e = producer_options(&o("producer.windowSiz", "1"), 0).unwrap_err();
        assert!(e.to_string().contains("unknown producer option"), "{e}");
        assert!(producer_options(&o("producer.windowSize", "x"), 0).is_err());
    }

    #[test]
    fn java_send_rules() {
        let d = SendOptions::default();
        assert!(!d.is_async(true)); // persistent: sync
        assert!(d.is_async(false)); // non-persistent: async
        let a = SendOptions {
            use_async_send: true,
            ..d
        };
        assert!(a.is_async(true));
        let s = SendOptions {
            always_sync_send: true,
            ..a
        };
        assert!(!s.is_async(false));
        let t = SendOptions {
            send_timeout_ms: 1000,
            ..a
        };
        assert!(!t.is_async(false));
    }

    fn ids() -> (DataStructure, DataStructure, types::ProducerId) {
        let pid = types::ProducerId {
            connection_id: Some("ID:c".into()),
            value: 1,
            session_id: 1,
        };
        let pds = DataStructure::ProducerId(Box::new(pid.clone()));
        let mid = DataStructure::MessageId(Box::new(types::MessageId {
            producer_id: Some(Box::new(pds.clone())),
            producer_sequence_id: 1,
            ..Default::default()
        }));
        (pds, mid, pid)
    }

    #[test]
    fn message_mapping() {
        let (pds, mid, pid) = ids();
        let mut msg = Message::new(b"hello \0 \xF0\x9F\x98\x80".to_vec());
        msg.body_type = Some(BodyType::Text);
        for (f, v) in [
            (Field::CorrelationId, "c-1"),
            (Field::ReplyTo, "/topic/replies"),
            (Field::Type, "order"),
            (Field::Priority, "7"),
            (Field::Persistent, "false"),
            (Field::Ttl, "60000"),
            (Field::DeliveryDelay, "5000"),
            (Field::GroupId, "g"),
            (Field::GroupSeq, "3"),
        ] {
            msg.fields.insert(f, v.to_string());
        }
        msg.properties.push(Property {
            name: "n".into(),
            value: Value::Int(42),
        });
        let (dq, _) = parse_destination("/queue/q").unwrap();
        let m = build_message(&msg, &dq, &pds, mid, &SendOptions::default()).unwrap();
        let f = &m.fields;
        assert_eq!(m.kind, kind::TEXT);
        assert_eq!(
            decode_text(f.content.as_deref().unwrap())
                .unwrap()
                .as_deref(),
            Some("hello \0 \u{1F600}")
        );
        assert_eq!(f.correlation_id.as_deref(), Some("c-1"));
        assert_eq!(dest(f.reply_to.as_deref().unwrap()), "topic://replies");
        assert_eq!(f.jms_type.as_deref(), Some("order"));
        assert_eq!((f.priority, f.persistent), (7, false));
        assert_eq!(f.expiration - f.timestamp, 60_000);
        assert_eq!((f.group_id.as_deref(), f.group_sequence), (Some("g"), 3));
        let props = decode_map(f.marshalled_properties.as_deref().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(
            props,
            vec![
                ("n".to_string(), Primitive::Int(42)),
                (SCHEDULED_DELAY.to_string(), Primitive::Long(5000)),
            ]
        );
        assert_eq!(message_id_text(&pid, 1), "ID:c:1:1:1");

        // Defaults: bytes, persistent, priority 4, no expiry, no properties.
        let (pds, mid, _) = ids();
        let plain = build_message(
            &Message::new(b"x".to_vec()),
            &dq,
            &pds,
            mid,
            &SendOptions {
                disable_timestamps: true,
                ..Default::default()
            },
        )
        .unwrap();
        let f = &plain.fields;
        assert_eq!(plain.kind, kind::BYTES);
        assert_eq!(f.content.as_deref(), Some(&b"x"[..]));
        assert_eq!(
            (f.persistent, f.priority, f.expiration, f.timestamp),
            (true, 4, 0, 0)
        );
        assert!(f.marshalled_properties.is_none());
        assert_eq!(message_size(&plain), 1025);
    }

    #[test]
    fn administrative_fields_are_sent_as_given() {
        let (pds, mid, _) = ids();
        let (dq, _) = parse_destination("q").unwrap();
        let mut msg = Message::new(b"x".to_vec());
        msg.fields
            .insert(Field::MessageId, "ID:host-1-2-3:4:5:6".into());
        msg.fields.insert(Field::Timestamp, "1000".into());
        msg.fields.insert(Field::Expiration, "61000".into());
        let m = build_message(&msg, &dq, &pds, mid.clone(), &SendOptions::default()).unwrap();
        assert_eq!((m.fields.timestamp, m.fields.expiration), (1000, 61000));
        match m.fields.message_id.as_deref() {
            Some(DataStructure::MessageId(id)) => {
                assert_eq!(id.producer_sequence_id, 6);
                assert_eq!(
                    crate::consumer::message_id_string(m.fields.message_id.as_deref().unwrap())
                        .as_deref(),
                    Some("ID:host-1-2-3:4:5:6")
                );
                assert!(id.text_view.is_none());
            }
            other => panic!("{other:?}"),
        }
        // A non-ActiveMQ id travels as the text view.
        msg.fields
            .insert(Field::MessageId, "ID:395e7136-bed8-11f1".into());
        let m = build_message(&msg, &dq, &pds, mid, &SendOptions::default()).unwrap();
        assert_eq!(
            crate::consumer::message_id_string(m.fields.message_id.as_deref().unwrap()).as_deref(),
            Some("ID:395e7136-bed8-11f1")
        );
    }

    #[test]
    fn compression_matches_java() {
        let (pds, mid, _) = ids();
        let (dq, _) = parse_destination("q").unwrap();
        let opts = SendOptions {
            use_compression: true,
            ..Default::default()
        };
        let body = b"abcabcabcabcabcabcabcabc".repeat(10);

        let m = build_message(&Message::new(body.clone()), &dq, &pds, mid.clone(), &opts).unwrap();
        let c = m.fields.content.unwrap();
        assert!(m.fields.compressed);
        assert_eq!(&c[..4], &(body.len() as i32).to_be_bytes());
        assert_eq!(
            miniz_oxide::inflate::decompress_to_vec_zlib(&c[4..]).unwrap(),
            body
        );

        let mut text = Message::new(body.clone());
        text.body_type = Some(BodyType::Text);
        let m = build_message(&text, &dq, &pds, mid, &opts).unwrap();
        let raw = miniz_oxide::inflate::decompress_to_vec_zlib(&m.fields.content.unwrap()).unwrap();
        assert_eq!(decode_text(&raw).unwrap().unwrap().as_bytes(), &body[..]);
    }
}
