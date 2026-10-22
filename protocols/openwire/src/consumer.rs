//! Receiving: consumer settings, `consumer.*` destination options, prefetch,
//! and an ActiveMQ message mapped back onto pub's message model
//! (`ActiveMQMessageConsumer`, `ActiveMQMessage`).
//!
//! - TextMessage → text body; BytesMessage → bytes; compressed content is
//!   inflated exactly as the Java client does;
//! - Map/Stream/Object/plain messages keep their raw content and say what they
//!   are in the `message-class` protocol header;
//! - native fields → standard fields (message-id, destination, timestamp,
//!   correlation-id, reply-to, type, priority, persistent, expiration, group-id/seq,
//!   redelivered, delivery-count); the property map → typed properties.

use std::time::{SystemTime, UNIX_EPOCH};

use pub_core::message::Property;
use pub_core::{bail_kind, err, BodyType, Endpoint, Field, Result, Value};

use crate::primitive::{decode_map, decode_text, Primitive};
use crate::producer::destination_text;
use crate::types::{self, kind, DataStructure};

/// Java's `ActiveMQPrefetchPolicy` defaults.
pub const QUEUE_PREFETCH: i32 = 1000;
pub const TOPIC_PREFETCH: i32 = 32767;
pub const DURABLE_TOPIC_PREFETCH: i32 = 100;
pub const QUEUE_BROWSER_PREFETCH: i32 = 500;

/// Which prefetch policy value applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerKind {
    Queue,
    Topic,
    DurableTopic,
    Browser,
}

/// `MessageAck.ackType`.
pub mod ack {
    /// The message can't be processed: the broker sends it to the dead-letter queue.
    pub const POISON: u8 = 1;
    pub const STANDARD: u8 = 2;
    pub const EXPIRED: u8 = 6;
}

/// Connection-factory settings that shape consumers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerSettings {
    /// `jms.prefetchPolicy.queuePrefetch` / `.all`, when set explicitly.
    pub queue_prefetch: Option<i32>,
    /// `jms.prefetchPolicy.topicPrefetch` / `.all`, when set explicitly.
    pub topic_prefetch: Option<i32>,
    /// `jms.prefetchPolicy.durableTopicPrefetch` / `.all`.
    pub durable_topic_prefetch: Option<i32>,
    /// `jms.prefetchPolicy.queueBrowserPrefetch` / `.all`.
    pub browser_prefetch: Option<i32>,
    pub maximum_pending: i32,
    /// `prefetch-follows-count`: without an explicit prefetch, use `min(-n, Java default)`.
    pub follows_count: bool,
    pub dispatch_async: bool,
    pub exclusive: bool,
    pub retroactive: bool,
    pub send_acks_async: bool,
    pub expiry_check: bool,
}

impl Default for ConsumerSettings {
    fn default() -> Self {
        ConsumerSettings {
            queue_prefetch: None,
            topic_prefetch: None,
            durable_topic_prefetch: None,
            browser_prefetch: None,
            maximum_pending: 0,
            follows_count: true,
            dispatch_async: true,
            exclusive: false,
            retroactive: false,
            send_acks_async: true,
            expiry_check: true,
        }
    }
}

fn count_setting(ep: &Endpoint, key: &str) -> Result<Option<i32>> {
    ep.prop(key)
        .map(|v| {
            v.trim()
                .parse::<i32>()
                .ok()
                .filter(|n| *n >= 0)
                .ok_or_else(|| err!(Config, "{key} must be a whole number, got '{v}'"))
        })
        .transpose()
}

impl ConsumerSettings {
    pub fn from_endpoint(ep: &Endpoint) -> Result<Self> {
        let all = count_setting(ep, "jms.prefetchPolicy.all")?;
        Ok(ConsumerSettings {
            queue_prefetch: count_setting(ep, "jms.prefetchPolicy.queuePrefetch")?.or(all),
            topic_prefetch: count_setting(ep, "jms.prefetchPolicy.topicPrefetch")?.or(all),
            durable_topic_prefetch: count_setting(ep, "jms.prefetchPolicy.durableTopicPrefetch")?
                .or(all),
            browser_prefetch: count_setting(ep, "jms.prefetchPolicy.queueBrowserPrefetch")?.or(all),
            maximum_pending: count_setting(ep, "jms.prefetchPolicy.maximumPendingMessageLimit")?
                .unwrap_or(0),
            follows_count: ep.prop_bool("prefetch-follows-count", true)?,
            dispatch_async: ep.prop_bool("jms.dispatchAsync", true)?,
            exclusive: ep.prop_bool("jms.exclusiveConsumer", false)?,
            retroactive: ep.prop_bool("jms.useRetroactiveConsumer", false)?,
            send_acks_async: ep.prop_bool("jms.sendAcksAsync", true)?,
            expiry_check: ep.prop_bool("jms.consumerExpiryCheckEnabled", true)?,
        })
    }

    /// The prefetch for a new consumer: an explicit policy value, else Java's
    /// default, lowered to the number of messages wanted when following `-n`
    /// (so a one-message receive doesn't take 1000 messages off other consumers).
    pub fn prefetch(&self, topic: bool, count: Option<usize>) -> i32 {
        let kind = if topic {
            ConsumerKind::Topic
        } else {
            ConsumerKind::Queue
        };
        self.prefetch_for(kind, count)
    }

    /// [`prefetch`](Self::prefetch) for any kind of consumer.
    pub fn prefetch_for(&self, kind: ConsumerKind, count: Option<usize>) -> i32 {
        let (explicit, java) = match kind {
            ConsumerKind::Queue => (self.queue_prefetch, QUEUE_PREFETCH),
            ConsumerKind::Topic => (self.topic_prefetch, TOPIC_PREFETCH),
            ConsumerKind::DurableTopic => (self.durable_topic_prefetch, DURABLE_TOPIC_PREFETCH),
            ConsumerKind::Browser => (self.browser_prefetch, QUEUE_BROWSER_PREFETCH),
        };
        if let Some(p) = explicit {
            return p;
        }
        match (self.follows_count, count) {
            (true, Some(n)) => n.min(java as usize).max(1) as i32,
            _ => java,
        }
    }
}

fn parse_bool(key: &str, v: &str) -> Result<bool> {
    match v.trim().to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => bail_kind!(Usage, "{key} must be true or false, got '{v}'"),
    }
}

fn parse_num<T: std::str::FromStr>(key: &str, v: &str) -> Result<T> {
    v.trim()
        .parse::<T>()
        .map_err(|_| err!(Usage, "{key}: invalid number '{v}'"))
}

/// Apply `consumer.*` options to a ConsumerInfo the way `ActiveMQMessageConsumer`
/// does (bean properties of ConsumerInfo); unknown ones are an error. Other
/// options (e.g. `producer.*`) are not the consumer's business.
pub fn apply_consumer_options(
    info: &mut types::ConsumerInfo,
    options: &[(String, String)],
) -> Result<()> {
    for (key, value) in options {
        let Some(name) = key.strip_prefix("consumer.") else {
            continue;
        };
        match name.to_ascii_lowercase().as_str() {
            "prefetchsize" => {
                info.prefetch_size = parse_num(key, value)?;
                if info.prefetch_size < 0 {
                    bail_kind!(Usage, "{key} cannot be negative");
                }
            }
            "maximumpendingmessagelimit" => info.maximum_pending_message_limit = parse_num(key, value)?,
            "dispatchasync" => info.dispatch_async = parse_bool(key, value)?,
            "selector" => info.selector = Some(value.clone()),
            "nolocal" => info.no_local = parse_bool(key, value)?,
            "exclusive" => info.exclusive = parse_bool(key, value)?,
            "retroactive" => info.retroactive = parse_bool(key, value)?,
            "priority" => info.priority = parse_num(key, value)?,
            "norangeacks" => info.no_range_acks = parse_bool(key, value)?,
            // A durable subscription, as with Java (needs a fixed client ID).
            "subscriptionname" => info.subscription_name = Some(value.clone()),
            _ => bail_kind!(
                Usage,
                "unknown consumer option '{key}' (ActiveMQ supports prefetchSize, maximumPendingMessageLimit, \
                 dispatchAsync, selector, noLocal, exclusive, retroactive, priority, noRangeAcks, \
                 subscriptionName)"
            ),
        }
    }
    Ok(())
}

/// `Message.isExpired()`.
pub fn is_expired(m: &types::Message) -> bool {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64);
    m.fields.expiration > 0 && now > m.fields.expiration
}

/// `MessageId.toString()`: the text view if the broker gave one, else
/// `producerId:sequence`.
pub fn message_id_string(ds: &DataStructure) -> Option<String> {
    let DataStructure::MessageId(id) = ds else {
        return None;
    };
    if let Some(t) = &id.text_view {
        return Some(t.clone());
    }
    match id.producer_id.as_deref() {
        Some(DataStructure::ProducerId(p)) => {
            Some(crate::producer::message_id_text(p, id.producer_sequence_id))
        }
        _ => None,
    }
}

/// Inflate a zlib stream the way Java's lazy `InflaterInputStream` /
/// `Inflater.inflate(buffer)` reads it: output beyond `limit` is an error
/// unless `stop_at_limit` (then the first `limit` bytes are the answer), and a
/// stream that ends early yields what it had. Both happen in practice: the
/// Java sender deflates a BytesMessage's whole backing buffer, and Artemis
/// re-deflates bodies without finishing the stream.
fn inflate(data: &[u8], limit: usize, stop_at_limit: bool) -> Result<Vec<u8>> {
    use miniz_oxide::inflate::core::{decompress, inflate_flags, DecompressorOxide};
    use miniz_oxide::inflate::TINFLStatus;
    let flags = inflate_flags::TINFL_FLAG_PARSE_ZLIB_HEADER
        | inflate_flags::TINFL_FLAG_USING_NON_WRAPPING_OUTPUT_BUF;
    let mut state = DecompressorOxide::new();
    let mut out = vec![0u8; limit.min(data.len().saturating_mul(4).max(1024))];
    let (mut in_pos, mut out_pos) = (0, 0);
    loop {
        let (status, read, written) =
            decompress(&mut state, &data[in_pos..], &mut out, out_pos, flags);
        in_pos += read;
        out_pos += written;
        match status {
            TINFLStatus::Done
            | TINFLStatus::NeedsMoreInput
            | TINFLStatus::FailedCannotMakeProgress => {
                out.truncate(out_pos);
                return Ok(out);
            }
            TINFLStatus::HasMoreOutput if out.len() < limit => {
                let grown = out.len().saturating_mul(2).min(limit);
                out.resize(grown, 0);
            }
            TINFLStatus::HasMoreOutput if stop_at_limit => return Ok(out),
            // Over a local limit, not necessarily a bad message: Usage, so the
            // receive stops instead of dead-lettering it.
            TINFLStatus::HasMoreOutput => bail_kind!(
                Usage,
                "compressed message body inflates beyond max-payload ({limit} bytes); raise it with -O max-payload=..."
            ),
            other => bail_kind!(
                Protocol,
                "cannot inflate compressed message body: {other:?}"
            ),
        }
    }
}

/// The body as the Java client would see it: inflated, and for text decoded.
fn body(m: &types::Message, max_payload: usize) -> Result<(Vec<u8>, BodyType)> {
    let content = m.fields.content.as_deref().unwrap_or(&[]);
    let compressed = m.fields.compressed && !content.is_empty();
    match m.kind {
        kind::TEXT => {
            let raw = if compressed {
                inflate(content, max_payload.saturating_add(4), false)?
            } else {
                content.to_vec()
            };
            let text = if raw.is_empty() {
                None
            } else {
                decode_text(&raw)?
            };
            Ok((text.unwrap_or_default().into_bytes(), BodyType::Text))
        }
        kind::BYTES if compressed => {
            // ActiveMQBytesMessage: original length, then the deflated bytes.
            if content.len() < 4 {
                bail_kind!(Protocol, "compressed BytesMessage is truncated");
            }
            let len = i32::from_be_bytes(content[..4].try_into().unwrap());
            if len < 0 {
                bail_kind!(Protocol, "compressed BytesMessage claims {len} bytes");
            }
            if len as usize > max_payload {
                bail_kind!(
                    Usage,
                    "compressed BytesMessage claims {len} bytes (max-payload is {max_payload})"
                );
            }
            let out = inflate(&content[4..], len as usize, true)?;
            if out.len() != len as usize {
                bail_kind!(
                    Protocol,
                    "compressed BytesMessage inflates to {} bytes, header says {len}",
                    out.len()
                );
            }
            Ok((out, BodyType::Bytes))
        }
        _ if compressed => Ok((inflate(content, max_payload, false)?, BodyType::Bytes)),
        _ => Ok((content.to_vec(), BodyType::Bytes)),
    }
}

fn value(p: Primitive) -> Option<Value> {
    Some(match p {
        Primitive::Null => return None,
        Primitive::Bool(b) => Value::Bool(b),
        Primitive::Byte(n) => Value::Byte(n),
        Primitive::Short(n) => Value::Short(n),
        Primitive::Int(n) => Value::Int(n),
        Primitive::Long(n) => Value::Long(n),
        Primitive::Float(n) => Value::Float(n),
        Primitive::Double(n) => Value::Double(n),
        Primitive::String(s) => Value::String(s),
        Primitive::Char(c) => Value::String(String::from_utf16_lossy(&[c])),
        Primitive::Bytes(b) => Value::String(b.iter().map(|x| format!("{x:02x}")).collect()),
        other @ (Primitive::Map(_) | Primitive::List(_)) => Value::String(format!("{other:?}")),
    })
}

fn class_name(k: u8) -> &'static str {
    match k {
        kind::MESSAGE => "Message",
        kind::BYTES => "BytesMessage",
        kind::MAP => "MapMessage",
        kind::OBJECT => "ObjectMessage",
        kind::STREAM => "StreamMessage",
        kind::TEXT => "TextMessage",
        kind::BLOB => "BlobMessage",
        _ => "unknown",
    }
}

/// An ActiveMQ message as pub's message. `redelivery_counter` comes from the
/// MessageDispatch (the broker's count), not the message itself.
pub fn to_pub_message(
    m: &types::Message,
    redelivery_counter: i32,
    max_payload: usize,
) -> Result<pub_core::Message> {
    let (body, body_type) = body(m, max_payload)?;
    let mut msg = pub_core::Message::new(body);
    msg.body_type = Some(body_type);
    let f = &m.fields;
    let mut set = |field: Field, v: Option<String>| {
        if let Some(v) = v {
            msg.fields.insert(field, v);
        }
    };
    set(
        Field::MessageId,
        f.message_id.as_deref().and_then(message_id_string),
    );
    set(
        Field::Destination,
        f.destination.as_deref().map(destination_text),
    );
    set(
        Field::Timestamp,
        (f.timestamp > 0).then(|| f.timestamp.to_string()),
    );
    set(Field::CorrelationId, f.correlation_id.clone());
    set(Field::ReplyTo, f.reply_to.as_deref().map(destination_text));
    set(Field::Type, f.jms_type.clone());
    set(Field::Priority, Some(f.priority.to_string()));
    set(Field::Persistent, Some(f.persistent.to_string()));
    set(
        Field::Expiration,
        (f.expiration > 0).then(|| f.expiration.to_string()),
    );
    set(Field::GroupId, f.group_id.clone());
    set(
        Field::GroupSeq,
        f.group_id.as_ref().map(|_| f.group_sequence.to_string()),
    );
    let redeliveries = redelivery_counter.max(f.redelivery_counter).max(0);
    set(Field::Redelivered, Some((redeliveries > 0).to_string()));
    set(
        Field::DeliveryCount,
        Some((redeliveries as i64 + 1).to_string()),
    );

    if let Some(props) = f.marshalled_properties.as_deref() {
        for (name, p) in decode_map(props)?.unwrap_or_default() {
            if let Some(value) = value(p) {
                msg.properties.push(Property { name, value });
            }
        }
    }
    if !matches!(m.kind, kind::TEXT | kind::BYTES) {
        msg.protocol
            .push(("message-class".into(), class_name(m.kind).into()));
    }
    if let Some(d) = f.original_destination.as_deref() {
        msg.protocol
            .push(("original-destination".into(), destination_text(d)));
    }
    if let Some(u) = &f.user_id {
        msg.protocol.push(("user-id".into(), u.clone()));
    }
    for (name, t) in [
        ("broker-in-time", f.broker_in_time),
        ("broker-out-time", f.broker_out_time),
    ] {
        if t > 0 {
            msg.protocol.push((name.into(), t.to_string()));
        }
    }
    Ok(msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitive::{encode_map, encode_text};
    use crate::producer::{build_message, parse_destination, SendOptions};

    const MAX: usize = 1 << 20;

    #[test]
    fn prefetch_follows_count_unless_explicit() {
        let s = ConsumerSettings::default();
        assert_eq!(s.prefetch(false, Some(1)), 1);
        assert_eq!(s.prefetch(false, Some(5000)), 1000);
        assert_eq!(s.prefetch(true, Some(5000)), 5000);
        assert_eq!(s.prefetch(false, None), 1000);
        assert_eq!(s.prefetch(true, None), 32767);
        let java = ConsumerSettings {
            follows_count: false,
            ..s
        };
        assert_eq!(java.prefetch(false, Some(1)), 1000);
        assert_eq!(s.prefetch_for(ConsumerKind::DurableTopic, Some(5000)), 100);
        assert_eq!(s.prefetch_for(ConsumerKind::Browser, None), 500);
        assert_eq!(s.prefetch_for(ConsumerKind::Browser, Some(3)), 3);
        let explicit = ConsumerSettings {
            queue_prefetch: Some(0),
            ..s
        };
        assert_eq!(explicit.prefetch(false, Some(10)), 0);
        assert_eq!(explicit.prefetch(true, Some(10)), 10);
    }

    #[test]
    fn consumer_destination_options() {
        let mut info = types::ConsumerInfo {
            prefetch_size: 5,
            ..Default::default()
        };
        let o = |k: &str, v: &str| (k.to_string(), v.to_string());
        apply_consumer_options(
            &mut info,
            &[
                o("consumer.prefetchSize", "10"),
                o("consumer.exclusive", "true"),
                o("consumer.selector", "a = 1"),
                o("consumer.priority", "3"),
                o("producer.windowSize", "1"),
            ],
        )
        .unwrap();
        assert_eq!(
            (info.prefetch_size, info.exclusive, info.priority),
            (10, true, 3)
        );
        assert_eq!(info.selector.as_deref(), Some("a = 1"));
        let bad = |k: &str, v: &str| {
            apply_consumer_options(&mut info.clone(), &[o(k, v)])
                .unwrap_err()
                .to_string()
        };
        assert!(bad("consumer.prefetchSiz", "1").contains("unknown consumer option"));
        assert!(bad("consumer.prefetchSize", "-1").contains("negative"));
        assert!(bad("consumer.exclusive", "yes").contains("true or false"));
        let mut durable = types::ConsumerInfo::default();
        apply_consumer_options(&mut durable, &[o("consumer.subscriptionName", "audit")]).unwrap();
        assert_eq!(durable.subscription_name.as_deref(), Some("audit"));
    }

    fn sent(msg: &pub_core::Message, opts: &SendOptions) -> types::Message {
        let pid = DataStructure::ProducerId(Box::new(types::ProducerId {
            connection_id: Some("ID:c".into()),
            value: 1,
            session_id: 1,
        }));
        let mid = DataStructure::MessageId(Box::new(types::MessageId {
            producer_id: Some(Box::new(pid.clone())),
            producer_sequence_id: 9,
            ..Default::default()
        }));
        build_message(
            msg,
            &parse_destination("/queue/q").unwrap().0,
            &pid,
            mid,
            opts,
        )
        .unwrap()
    }

    #[test]
    fn round_trips_pubs_own_messages() {
        let mut msg = pub_core::Message::new("héllo 😀".as_bytes().to_vec());
        msg.body_type = Some(BodyType::Text);
        msg.fields.insert(Field::CorrelationId, "c".into());
        msg.fields.insert(Field::ReplyTo, "topic://r".into());
        msg.fields.insert(Field::GroupId, "g".into());
        msg.fields.insert(Field::GroupSeq, "2".into());
        msg.fields.insert(Field::Ttl, "60000".into());
        msg.properties.push(Property {
            name: "n".into(),
            value: Value::Short(-3),
        });
        for opts in [
            SendOptions::default(),
            SendOptions {
                use_compression: true,
                ..Default::default()
            },
        ] {
            let back = to_pub_message(&sent(&msg, &opts), 0, MAX).unwrap();
            assert_eq!(back.body, msg.body);
            assert_eq!(back.body_type, Some(BodyType::Text));
            let f = |x: Field| back.fields.get(&x).map(String::as_str);
            assert_eq!(f(Field::MessageId), Some("ID:c:1:1:9"));
            assert_eq!(f(Field::Destination), Some("queue://q"));
            assert_eq!(f(Field::ReplyTo), Some("topic://r"));
            assert_eq!(f(Field::CorrelationId), Some("c"));
            assert_eq!(
                (f(Field::GroupId), f(Field::GroupSeq)),
                (Some("g"), Some("2"))
            );
            assert_eq!(
                (f(Field::Priority), f(Field::Persistent)),
                (Some("4"), Some("true"))
            );
            assert_eq!(
                (f(Field::Redelivered), f(Field::DeliveryCount)),
                (Some("false"), Some("1"))
            );
            assert!(f(Field::Expiration).is_some() && f(Field::Timestamp).is_some());
            assert_eq!(back.properties, msg.properties);
            assert!(back.protocol.is_empty());
        }
        let bytes = pub_core::Message::new(vec![0u8, 1, 2, 255]);
        for opts in [
            SendOptions::default(),
            SendOptions {
                use_compression: true,
                ..Default::default()
            },
        ] {
            let back = to_pub_message(&sent(&bytes, &opts), 2, MAX).unwrap();
            assert_eq!(
                (back.body.as_slice(), back.body_type),
                (&[0u8, 1, 2, 255][..], Some(BodyType::Bytes))
            );
            assert_eq!(
                back.fields.get(&Field::DeliveryCount).map(String::as_str),
                Some("3")
            );
            assert_eq!(
                back.fields.get(&Field::Redelivered).map(String::as_str),
                Some("true")
            );
        }
    }

    #[test]
    fn other_message_kinds_and_property_types() {
        let props = encode_map(&[
            ("c".into(), Primitive::Char('x' as u16)),
            ("b".into(), Primitive::Bytes(vec![1, 0xab])),
            ("z".into(), Primitive::Null),
        ])
        .unwrap();
        let m = types::Message {
            kind: kind::MAP,
            fields: types::MessageFields {
                content: Some(encode_map(&[("k".into(), Primitive::Int(1))]).unwrap()),
                marshalled_properties: Some(props),
                ..Default::default()
            },
            blob: None,
        };
        let back = to_pub_message(&m, 0, MAX).unwrap();
        assert_eq!(
            back.protocol,
            vec![("message-class".to_string(), "MapMessage".to_string())]
        );
        assert_eq!(back.properties.len(), 2);
        assert_eq!(back.properties[0].value, Value::String("x".into()));
        assert_eq!(back.properties[1].value, Value::String("01ab".into()));
        // A null text body is an empty one.
        let empty = types::Message {
            kind: kind::TEXT,
            ..Default::default()
        };
        assert!(to_pub_message(&empty, 0, MAX).unwrap().body.is_empty());
    }

    #[test]
    fn java_compressed_bytes_with_trailing_buffer() {
        // What ActiveMQBytesMessage.doCompress sends: length 4, but the whole
        // (larger) backing buffer deflated.
        let mut content = 4i32.to_be_bytes().to_vec();
        let mut buffer = vec![0u8, 1, 2, 255];
        buffer.resize(1028, 0);
        content.extend(miniz_oxide::deflate::compress_to_vec_zlib(&buffer, 6));
        let m = types::Message {
            kind: kind::BYTES,
            fields: types::MessageFields {
                content: Some(content.clone()),
                compressed: true,
                ..Default::default()
            },
            blob: None,
        };
        assert_eq!(to_pub_message(&m, 0, MAX).unwrap().body, [0u8, 1, 2, 255]);
        // A claimed length beyond max-payload is refused before allocating.
        content[..4].copy_from_slice(&i32::MAX.to_be_bytes());
        let mut big = m.clone();
        big.fields.content = Some(content);
        let e = to_pub_message(&big, 0, MAX).unwrap_err();
        assert!(e.to_string().contains("max-payload"), "{e}");
    }

    #[test]
    fn unterminated_streams_yield_what_they_have() {
        // Artemis re-deflates without finishing the stream; Java reads it lazily.
        let full = miniz_oxide::deflate::compress_to_vec_zlib(&encode_text("abc"), 6);
        let m = types::Message {
            kind: kind::TEXT,
            fields: types::MessageFields {
                content: Some(full[..full.len() - 4].to_vec()), // no Adler-32
                compressed: true,
                ..Default::default()
            },
            blob: None,
        };
        assert_eq!(to_pub_message(&m, 0, MAX).unwrap().body, b"abc");
    }

    #[test]
    fn inflation_is_bounded() {
        let big = pub_core::Message::new(vec![b'a'; 100_000]);
        let m = sent(
            &big,
            &SendOptions {
                use_compression: true,
                ..Default::default()
            },
        );
        let e = to_pub_message(&m, 0, 1000).unwrap_err();
        assert!(e.to_string().contains("max-payload"), "{e}");
    }

    #[test]
    fn expiry() {
        let mut m = types::Message::default();
        assert!(!is_expired(&m));
        m.fields.expiration = 1;
        assert!(is_expired(&m));
        m.fields.expiration = i64::MAX;
        assert!(!is_expired(&m));
    }
}
