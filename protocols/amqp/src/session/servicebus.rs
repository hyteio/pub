//! Azure Service Bus specifics.
//!
//! Service Bus speaks AMQP 1.0 but ignores parts of it that pub relies on
//! elsewhere: it accepts a selector filter and a `copy` distribution mode
//! without honouring either (a "browse" would delete the messages), its
//! transactions can't span two entities, and subscriptions are entities of
//! their own rather than durable links. So pub recognises it (a
//! `*.servicebus.*` host, the gateway's container id `<32 hex>_G<n>`, or
//! `broker-type = servicebus`), browses with the management node's
//! `com.microsoft:peek-message` operation, and refuses the rest.

use std::time::{Instant, SystemTime, UNIX_EPOCH};

use pub_core::module::{Destination, ReceiveOptions};
use pub_core::{bail_kind, err, Endpoint, Message, Result};

use super::{AmqpSession, Sender};
use crate::described::*;
use crate::message::{self, AmqpMessage, Body};
use crate::types::Value;

/// Messages asked for per peek request.
const PEEK_BATCH: usize = 100;

/// Service Bus ignores `x-opt-delivery-delay`; it schedules by the absolute
/// `x-opt-scheduled-enqueue-time`.
pub fn schedule(m: &mut AmqpMessage) {
    let Some(ann) = m.message_annotations.as_mut() else {
        return;
    };
    let Some(pos) = ann
        .0
        .iter()
        .position(|(k, _)| k.as_str() == Some("x-opt-delivery-delay"))
    else {
        return;
    };
    let delay = int(Some(&ann.0[pos].1)).unwrap_or(0);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64);
    ann.0[pos] = (
        Value::symbol("x-opt-scheduled-enqueue-time"),
        Value::Timestamp(now.saturating_add(delay)),
    );
}

/// Whether the broker is Azure Service Bus (or its emulator).
pub fn detect(ep: &Endpoint, remote_container: &str) -> Result<bool> {
    match ep.prop("broker-type").unwrap_or("auto") {
        "servicebus" => return Ok(true),
        "other" => return Ok(false),
        "auto" => {}
        other => bail_kind!(
            Config,
            "broker-type must be auto, servicebus or other, got '{other}'"
        ),
    }
    let host = ep.host.to_ascii_lowercase();
    Ok(host.contains(".servicebus.") || is_gateway_id(remote_container))
}

/// Service Bus gateways identify as `<32 hex digits>_G<n>`.
fn is_gateway_id(id: &str) -> bool {
    match id.split_once("_G") {
        Some((hex, n)) => {
            hex.len() == 32
                && hex.bytes().all(|b| b.is_ascii_hexdigit())
                && !n.is_empty()
                && n.bytes().all(|b| b.is_ascii_digit())
        }
        None => false,
    }
}

/// The entity a destination names: Service Bus addresses are plain paths
/// (`orders`, `events/Subscriptions/audit`).
fn entity(dest: &Destination) -> Result<String> {
    let (address, _) = message::address(&dest.name)?;
    if address.is_empty() {
        bail_kind!(Usage, "no queue or subscription to browse");
    }
    Ok(address)
}

fn int(v: Option<&Value>) -> Option<i64> {
    match v? {
        Value::Int(n) => Some(*n as i64),
        Value::Long(n) => Some(*n),
        Value::Uint(n) => Some(*n as i64),
        Value::Ulong(n) => Some(*n as i64),
        Value::Short(n) => Some(*n as i64),
        Value::Ushort(n) => Some(*n as i64),
        _ => None,
    }
}

fn map_get<'a>(pairs: &'a [(Value, Value)], key: &str) -> Option<&'a Value> {
    pairs
        .iter()
        .find(|(k, _)| k.as_str() == Some(key))
        .map(|(_, v)| v)
}

impl AmqpSession {
    pub(super) fn servicebus_refuse_selector(&self) -> Result<()> {
        bail_kind!(
            Unsupported,
            "Azure Service Bus doesn't filter receivers by selector (it would hand over every message); \
             filter with a subscription rule and receive from TOPIC/Subscriptions/NAME"
        )
    }

    pub(super) fn servicebus_refuse_durable(&self) -> Result<()> {
        bail_kind!(
            Unsupported,
            "Azure Service Bus subscriptions are entities, not durable links: create one (portal, CLI, \
             config) and receive from TOPIC/Subscriptions/NAME"
        )
    }

    pub(super) fn servicebus_refuse_move(&self) -> Result<()> {
        bail_kind!(
            Unsupported,
            "Azure Service Bus transactions can't span two entities, so pub can't move messages safely there"
        )
    }

    /// Browse by peeking: `com.microsoft:peek-message` requests to the
    /// entity's `$management` node, by sequence number, until the broker
    /// answers 204 (no more) or `opts.count` is reached. Nothing is locked or
    /// consumed.
    pub(super) fn servicebus_peek(
        &mut self,
        dest: &Destination,
        opts: &ReceiveOptions,
        sink: &mut dyn FnMut(Message) -> Result<()>,
    ) -> Result<usize> {
        let entity = entity(dest)?;
        let node = format!("{entity}/$management");
        let (sender, receiver, reply_to) = self.management_links(&node)?;
        let result = self.peek_loop(&reply_to, sender, receiver, opts, sink);
        let _ = self.detach(receiver, true);
        let _ = self.detach(sender, true);
        self.senders.remove(&node);
        result
    }

    /// A sender to the management node and a receiver for its answers.
    fn management_links(&mut self, node: &str) -> Result<(u32, u32, String)> {
        let sender = self.next_handle;
        let receiver = sender + 1;
        self.next_handle += 2;
        let reply_to = format!("{}-mgmt-{receiver}", self.container_id);
        self.attach(
            Attach {
                name: format!("{}-mgmt-sender-{sender}", self.container_id),
                handle: sender,
                role: SENDER,
                snd_settle_mode: Some(settle::SND_SETTLED),
                rcv_settle_mode: Some(settle::RCV_FIRST),
                source: Some(Source {
                    address: Some(reply_to.clone()),
                    ..Default::default()
                }),
                target: Some(
                    Target {
                        address: Some(node.to_string()),
                        ..Default::default()
                    }
                    .into(),
                ),
                initial_delivery_count: Some(0),
                ..Default::default()
            },
            &format!("a management link to {node}"),
        )?;
        self.senders.insert(
            node.to_string(),
            Sender {
                handle: sender,
                credit: 0,
                delivery_count: 0,
            },
        );
        self.attach(
            Attach {
                name: format!("{}-mgmt-receiver-{receiver}", self.container_id),
                handle: receiver,
                role: RECEIVER,
                snd_settle_mode: Some(settle::SND_SETTLED),
                rcv_settle_mode: Some(settle::RCV_FIRST),
                source: Some(Source {
                    address: Some(node.to_string()),
                    ..Default::default()
                }),
                target: Some(
                    Target {
                        address: Some(reply_to.clone()),
                        ..Default::default()
                    }
                    .into(),
                ),
                ..Default::default()
            },
            &format!("a management reply link from {node}"),
        )?;
        Ok((sender, receiver, reply_to))
    }

    fn peek_loop(
        &mut self,
        reply_to: &str,
        sender: u32,
        receiver: u32,
        opts: &ReceiveOptions,
        sink: &mut dyn FnMut(Message) -> Result<()>,
    ) -> Result<usize> {
        let deadline = opts.timeout.map(|t| Instant::now() + t);
        let node_key = self
            .senders
            .iter()
            .find(|(_, s)| s.handle == sender)
            .map(|(k, _)| k.clone())
            .expect("management sender");
        let mut from: i64 = 0;
        let mut received = 0usize;
        let mut reply_count = 0u32;
        while opts.count.is_none_or(|c| received < c) {
            if deadline.is_some_and(|d| Instant::now() >= d) {
                break;
            }
            let want = opts
                .count
                .map_or(PEEK_BATCH, |c| (c - received).min(PEEK_BATCH));
            self.next_message += 1;
            let request = AmqpMessage {
                properties: Some(Properties {
                    message_id: Some(Value::String(format!(
                        "{}-peek-{}",
                        self.container_id, self.next_message
                    ))),
                    reply_to: Some(reply_to.to_string()),
                    ..Default::default()
                }),
                application_properties: Some(Map(vec![(
                    Value::String("operation".into()),
                    Value::String("com.microsoft:peek-message".into()),
                )])),
                body: Body::Value(Value::Map(vec![
                    (
                        Value::String("from-sequence-number".into()),
                        Value::Long(from),
                    ),
                    (
                        Value::String("message-count".into()),
                        Value::Int(want as i32),
                    ),
                ])),
                ..Default::default()
            }
            .encode()?;
            // One answer per request: one credit.
            self.send_session_flow(Some((receiver, reply_count, 1, false)))?;
            self.wait_for_credit(&node_key)?;
            let delivery_id = self.next_delivery_id;
            self.next_delivery_id = self.next_delivery_id.wrapping_add(1);
            self.next_tag += 1;
            let tag = self.next_tag.to_be_bytes().to_vec();
            self.transfer(sender, delivery_id, tag, &request, true, None)?;
            if let Some(s) = self.senders.get_mut(&node_key) {
                s.credit -= 1;
                s.delivery_count = s.delivery_count.wrapping_add(1);
            }
            let reply = self.management_reply(receiver)?;
            reply_count = reply_count.wrapping_add(1);

            let props = reply.application_properties.unwrap_or_default();
            let status = int(props.get("statusCode")).unwrap_or(0);
            match status {
                204 => break, // nothing (more) to peek
                200 => {}
                _ => {
                    let why = props
                        .get("statusDescription")
                        .and_then(Value::as_str)
                        .unwrap_or("no description");
                    let condition = props
                        .get("errorCondition")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let what = format!("peeking failed: {status} {why} {condition}");
                    return Err(if status == 404 {
                        err!(Usage, "{what}")
                    } else if status == 401 || status == 403 {
                        err!(Auth, "{what}")
                    } else {
                        err!(Protocol, "{what}")
                    });
                }
            }
            let messages = match &reply.body {
                Body::Value(Value::Map(m)) => match map_get(m, "messages") {
                    Some(Value::List(l)) | Some(Value::Array(l)) => l.clone(),
                    _ => Vec::new(),
                },
                _ => Vec::new(),
            };
            if messages.is_empty() {
                break;
            }
            let batch_from = from;
            for entry in messages {
                let Value::Map(pairs) = entry else { continue };
                let Some(Value::Binary(bytes)) = map_get(&pairs, "message") else {
                    continue;
                };
                let amqp = AmqpMessage::decode(bytes)?;
                let seq = amqp
                    .message_annotations
                    .as_ref()
                    .and_then(|a| int(a.get("x-opt-sequence-number")));
                sink(message::from_amqp(&amqp)?)?;
                received += 1;
                if let Some(s) = seq {
                    from = from.max(s.saturating_add(1));
                }
                if opts.count.is_some_and(|c| received >= c) {
                    break;
                }
            }
            // Peeking resumes after the last sequence number; without one it
            // would show the same messages again and again.
            if from == batch_from && opts.count.is_none_or(|c| received < c) {
                bail_kind!(
                    Protocol,
                    "the broker's peeked messages carry no x-opt-sequence-number to continue from"
                );
            }
        }
        Ok(received)
    }

    /// The next (possibly multi-frame) message on the management receiver.
    fn management_reply(&mut self, handle: u32) -> Result<AmqpMessage> {
        let deadline = self.conn.reply_deadline();
        let max = self.conn.limits().max_payload.saturating_add(1 << 20);
        let mut bytes = Vec::new();
        loop {
            let Some((p, payload)) = self.recv_until(deadline)? else {
                bail_kind!(Timeout, "no answer from the management node");
            };
            match p {
                crate::codec::Performative::Transfer(t) if t.handle == handle => {
                    if t.aborted == Some(true) {
                        bytes.clear();
                        continue;
                    }
                    if bytes.len() + payload.len() > max {
                        bail_kind!(Protocol, "management reply larger than max-payload");
                    }
                    bytes.extend_from_slice(&payload);
                    if t.more != Some(true) {
                        return AmqpMessage::decode(&bytes);
                    }
                }
                crate::codec::Performative::Detach(d) if d.handle == handle => {
                    return Err(super::amqp_error(
                        "the broker closed the management link",
                        d.error.as_ref(),
                    ));
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delivery_delay_becomes_a_scheduled_enqueue_time() {
        let mut m = AmqpMessage {
            message_annotations: Some(Map(vec![(
                Value::symbol("x-opt-delivery-delay"),
                Value::Long(5000),
            )])),
            ..Default::default()
        };
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        schedule(&mut m);
        let ann = m.message_annotations.unwrap();
        assert!(ann.get("x-opt-delivery-delay").is_none());
        match ann.get("x-opt-scheduled-enqueue-time") {
            Some(Value::Timestamp(t)) => assert!((before + 5000..before + 6000).contains(t)),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn gateway_ids() {
        assert!(is_gateway_id("f36ac1e703624b30a0140bc5ef7487cf_G0"));
        assert!(is_gateway_id("0123456789abcdef0123456789ABCDEF_G12"));
        assert!(!is_gateway_id("f36ac1e703624b30a0140bc5ef7487cf"));
        assert!(!is_gateway_id("mock-broker"));
        assert!(!is_gateway_id("f36ac1e703624b30a0140bc5ef7487c_G0"));
        assert!(!is_gateway_id("f36ac1e703624b30a0140bc5ef7487cf_Gx"));
    }
}
