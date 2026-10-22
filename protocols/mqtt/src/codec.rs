//! MQTT 5 packet encoding and decoding.
//!
//! Every packet is a fixed header (type and flags, then the "remaining length"
//! as a variable byte integer) followed by a body. MQTT 5 adds properties
//! (a length-prefixed list of typed id/value pairs) to most packets, and reason
//! codes to acknowledgements.
//!
//! Both directions are implemented so the in-process test broker can reuse the
//! codec. [`MqttCodec::version`] selects MQTT 5 (level 5) or 3.1.1 (level 4);
//! with 3.1.1, anything only MQTT 5 can express (properties, reason codes) is
//! refused on encode and never expected on decode.

use pub_core::{bail_kind, err, Codec, Decoded, Result};

pub const PROTOCOL_LEVEL_5: u8 = 5;
/// MQTT 3.1.1: no properties, no reason codes on acknowledgements, no AUTH.
pub const PROTOCOL_LEVEL_311: u8 = 4;
/// Largest value a variable byte integer can hold (and so the largest packet body).
pub const MAX_VARINT: u32 = 268_435_455;

// ---- properties

/// Property identifiers (MQTT 5 §2.2.2.2).
pub mod prop {
    pub const PAYLOAD_FORMAT_INDICATOR: u8 = 0x01;
    pub const MESSAGE_EXPIRY_INTERVAL: u8 = 0x02;
    pub const CONTENT_TYPE: u8 = 0x03;
    pub const RESPONSE_TOPIC: u8 = 0x08;
    pub const CORRELATION_DATA: u8 = 0x09;
    pub const SUBSCRIPTION_IDENTIFIER: u8 = 0x0B;
    pub const SESSION_EXPIRY_INTERVAL: u8 = 0x11;
    pub const ASSIGNED_CLIENT_IDENTIFIER: u8 = 0x12;
    pub const SERVER_KEEP_ALIVE: u8 = 0x13;
    pub const AUTHENTICATION_METHOD: u8 = 0x15;
    pub const AUTHENTICATION_DATA: u8 = 0x16;
    pub const REQUEST_PROBLEM_INFORMATION: u8 = 0x17;
    pub const WILL_DELAY_INTERVAL: u8 = 0x18;
    pub const REQUEST_RESPONSE_INFORMATION: u8 = 0x19;
    pub const RESPONSE_INFORMATION: u8 = 0x1A;
    pub const SERVER_REFERENCE: u8 = 0x1C;
    pub const REASON_STRING: u8 = 0x1F;
    pub const RECEIVE_MAXIMUM: u8 = 0x21;
    pub const TOPIC_ALIAS_MAXIMUM: u8 = 0x22;
    pub const TOPIC_ALIAS: u8 = 0x23;
    pub const MAXIMUM_QOS: u8 = 0x24;
    pub const RETAIN_AVAILABLE: u8 = 0x25;
    pub const USER_PROPERTY: u8 = 0x26;
    pub const MAXIMUM_PACKET_SIZE: u8 = 0x27;
    pub const WILDCARD_SUBSCRIPTION_AVAILABLE: u8 = 0x28;
    pub const SUBSCRIPTION_IDENTIFIER_AVAILABLE: u8 = 0x29;
    pub const SHARED_SUBSCRIPTION_AVAILABLE: u8 = 0x2A;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PropType {
    Byte,
    U16,
    U32,
    VarInt,
    Str,
    Bin,
    Pair,
}

fn prop_type(id: u8) -> Option<PropType> {
    use prop::*;
    Some(match id {
        PAYLOAD_FORMAT_INDICATOR
        | REQUEST_PROBLEM_INFORMATION
        | REQUEST_RESPONSE_INFORMATION
        | MAXIMUM_QOS
        | RETAIN_AVAILABLE
        | WILDCARD_SUBSCRIPTION_AVAILABLE
        | SUBSCRIPTION_IDENTIFIER_AVAILABLE
        | SHARED_SUBSCRIPTION_AVAILABLE => PropType::Byte,
        SERVER_KEEP_ALIVE | RECEIVE_MAXIMUM | TOPIC_ALIAS_MAXIMUM | TOPIC_ALIAS => PropType::U16,
        MESSAGE_EXPIRY_INTERVAL
        | SESSION_EXPIRY_INTERVAL
        | WILL_DELAY_INTERVAL
        | MAXIMUM_PACKET_SIZE => PropType::U32,
        SUBSCRIPTION_IDENTIFIER => PropType::VarInt,
        CONTENT_TYPE
        | RESPONSE_TOPIC
        | ASSIGNED_CLIENT_IDENTIFIER
        | AUTHENTICATION_METHOD
        | RESPONSE_INFORMATION
        | SERVER_REFERENCE
        | REASON_STRING => PropType::Str,
        CORRELATION_DATA | AUTHENTICATION_DATA => PropType::Bin,
        USER_PROPERTY => PropType::Pair,
        _ => return None,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PropValue {
    Byte(u8),
    U16(u16),
    U32(u32),
    VarInt(u32),
    Str(String),
    Bin(Vec<u8>),
    Pair(String, String),
}

/// Properties in wire order (user properties may repeat and order matters).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Props(pub Vec<(u8, PropValue)>);

impl Props {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn push(&mut self, id: u8, v: PropValue) -> &mut Self {
        self.0.push((id, v));
        self
    }

    pub fn get(&self, id: u8) -> Option<&PropValue> {
        self.0.iter().find(|(i, _)| *i == id).map(|(_, v)| v)
    }

    pub fn str(&self, id: u8) -> Option<&str> {
        match self.get(id) {
            Some(PropValue::Str(s)) => Some(s),
            _ => None,
        }
    }

    pub fn bin(&self, id: u8) -> Option<&[u8]> {
        match self.get(id) {
            Some(PropValue::Bin(b)) => Some(b),
            _ => None,
        }
    }

    /// Any integer property as u32.
    pub fn num(&self, id: u8) -> Option<u32> {
        match self.get(id) {
            Some(PropValue::Byte(n)) => Some(*n as u32),
            Some(PropValue::U16(n)) => Some(*n as u32),
            Some(PropValue::U32(n)) | Some(PropValue::VarInt(n)) => Some(*n),
            _ => None,
        }
    }

    pub fn user(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0.iter().filter_map(|(_, v)| match v {
            PropValue::Pair(k, v) => Some((k.as_str(), v.as_str())),
            _ => None,
        })
    }

    pub fn reason_string(&self) -> Option<&str> {
        self.str(prop::REASON_STRING)
    }
}

// ---- packets

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Connect {
    /// Protocol level as decoded (5 or 4); encoding uses the codec's version.
    pub protocol_level: u8,
    pub clean_start: bool,
    pub keep_alive: u16,
    pub props: Props,
    pub client_id: String,
    pub username: Option<String>,
    pub password: Option<Vec<u8>>,
    /// Published by the broker if the connection ends without a normal DISCONNECT.
    pub will: Option<Will>,
}

/// The will message in CONNECT.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Will {
    pub qos: u8,
    pub retain: bool,
    /// MQTT 5 only (will delay, payload format, content type, ...).
    pub props: Props,
    pub topic: String,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConnAck {
    pub session_present: bool,
    pub reason: u8,
    pub props: Props,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Publish {
    pub dup: bool,
    pub qos: u8,
    pub retain: bool,
    pub topic: String,
    /// Present for QoS 1 and 2.
    pub packet_id: Option<u16>,
    pub props: Props,
    pub payload: Vec<u8>,
}

/// PUBACK, PUBREC, PUBREL, PUBCOMP.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Ack {
    pub packet_id: u16,
    pub reason: u8,
    pub props: Props,
}

impl Ack {
    pub fn new(packet_id: u16) -> Self {
        Ack {
            packet_id,
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Subscribe {
    pub packet_id: u16,
    pub props: Props,
    /// Topic filter and subscription options byte (QoS, no-local, retain-as-published, retain handling).
    pub filters: Vec<(String, u8)>,
}

/// SUBACK and UNSUBACK.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SubAck {
    pub packet_id: u16,
    pub props: Props,
    pub reasons: Vec<u8>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Unsubscribe {
    pub packet_id: u16,
    pub props: Props,
    pub filters: Vec<String>,
}

/// DISCONNECT and AUTH.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReasonProps {
    pub reason: u8,
    pub props: Props,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Packet {
    Connect(Connect),
    ConnAck(ConnAck),
    Publish(Publish),
    PubAck(Ack),
    PubRec(Ack),
    PubRel(Ack),
    PubComp(Ack),
    Subscribe(Subscribe),
    SubAck(SubAck),
    Unsubscribe(Unsubscribe),
    UnsubAck(SubAck),
    PingReq,
    PingResp,
    Disconnect(ReasonProps),
    Auth(ReasonProps),
}

impl Packet {
    pub fn name(&self) -> &'static str {
        match self {
            Packet::Connect(_) => "CONNECT",
            Packet::ConnAck(_) => "CONNACK",
            Packet::Publish(_) => "PUBLISH",
            Packet::PubAck(_) => "PUBACK",
            Packet::PubRec(_) => "PUBREC",
            Packet::PubRel(_) => "PUBREL",
            Packet::PubComp(_) => "PUBCOMP",
            Packet::Subscribe(_) => "SUBSCRIBE",
            Packet::SubAck(_) => "SUBACK",
            Packet::Unsubscribe(_) => "UNSUBSCRIBE",
            Packet::UnsubAck(_) => "UNSUBACK",
            Packet::PingReq => "PINGREQ",
            Packet::PingResp => "PINGRESP",
            Packet::Disconnect(_) => "DISCONNECT",
            Packet::Auth(_) => "AUTH",
        }
    }
}

/// Human name for a reason code (MQTT 5 §2.4), for error messages.
pub fn reason_name(code: u8) -> &'static str {
    match code {
        0x00 => "Success",
        0x01 => "Granted QoS 1",
        0x02 => "Granted QoS 2",
        0x04 => "Disconnect with Will Message",
        0x10 => "No matching subscribers",
        0x11 => "No subscription existed",
        0x18 => "Continue authentication",
        0x19 => "Re-authenticate",
        0x80 => "Unspecified error",
        0x81 => "Malformed Packet",
        0x82 => "Protocol Error",
        0x83 => "Implementation specific error",
        0x84 => "Unsupported Protocol Version",
        0x85 => "Client Identifier not valid",
        0x86 => "Bad User Name or Password",
        0x87 => "Not authorized",
        0x88 => "Server unavailable",
        0x89 => "Server busy",
        0x8A => "Banned",
        0x8B => "Server shutting down",
        0x8C => "Bad authentication method",
        0x8D => "Keep Alive timeout",
        0x8E => "Session taken over",
        0x8F => "Topic Filter invalid",
        0x90 => "Topic Name invalid",
        0x91 => "Packet Identifier in use",
        0x92 => "Packet Identifier not found",
        0x93 => "Receive Maximum exceeded",
        0x94 => "Topic Alias invalid",
        0x95 => "Packet too large",
        0x96 => "Message rate too high",
        0x97 => "Quota exceeded",
        0x98 => "Administrative action",
        0x99 => "Payload format invalid",
        0x9A => "Retain not supported",
        0x9B => "QoS not supported",
        0x9C => "Use another server",
        0x9D => "Server moved",
        0x9E => "Shared Subscriptions not supported",
        0x9F => "Connection rate exceeded",
        0xA0 => "Maximum connect time",
        0xA1 => "Subscription Identifiers not supported",
        0xA2 => "Wildcard Subscriptions not supported",
        _ => "Unknown reason",
    }
}

/// "Not authorized (0x87): <reason string>".
pub fn describe_reason(code: u8, props: &Props) -> String {
    match props.reason_string() {
        Some(s) => format!("{} (0x{code:02X}): {s}", reason_name(code)),
        None => format!("{} (0x{code:02X})", reason_name(code)),
    }
}

// ---- primitives

pub fn put_varint(out: &mut Vec<u8>, mut n: u32) -> Result<()> {
    if n > MAX_VARINT {
        bail_kind!(
            Usage,
            "MQTT packet too large ({n} bytes; the protocol limit is {MAX_VARINT})"
        );
    }
    loop {
        let mut byte = (n % 128) as u8;
        n /= 128;
        if n > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if n == 0 {
            return Ok(());
        }
    }
}

fn put_u16(out: &mut Vec<u8>, n: u16) {
    out.extend_from_slice(&n.to_be_bytes());
}

fn put_str(out: &mut Vec<u8>, s: &str) -> Result<()> {
    if s.len() > u16::MAX as usize {
        bail_kind!(Usage, "MQTT string longer than 65535 bytes");
    }
    if s.contains('\0') {
        bail_kind!(Usage, "MQTT strings cannot contain U+0000");
    }
    put_u16(out, s.len() as u16);
    out.extend_from_slice(s.as_bytes());
    Ok(())
}

fn put_bin(out: &mut Vec<u8>, b: &[u8]) -> Result<()> {
    if b.len() > u16::MAX as usize {
        bail_kind!(Usage, "MQTT binary data longer than 65535 bytes");
    }
    put_u16(out, b.len() as u16);
    out.extend_from_slice(b);
    Ok(())
}

fn put_props(out: &mut Vec<u8>, props: &Props) -> Result<()> {
    let mut body = Vec::new();
    for (id, v) in &props.0 {
        body.push(*id);
        match v {
            PropValue::Byte(n) => body.push(*n),
            PropValue::U16(n) => put_u16(&mut body, *n),
            PropValue::U32(n) => body.extend_from_slice(&n.to_be_bytes()),
            PropValue::VarInt(n) => put_varint(&mut body, *n)?,
            PropValue::Str(s) => put_str(&mut body, s)?,
            PropValue::Bin(b) => put_bin(&mut body, b)?,
            PropValue::Pair(k, v) => {
                put_str(&mut body, k)?;
                put_str(&mut body, v)?;
            }
        }
    }
    put_varint(out, u32::try_from(body.len()).unwrap_or(u32::MAX))?;
    out.extend_from_slice(&body);
    Ok(())
}

/// Read a variable byte integer from the front of `buf`: `Ok(None)` if more bytes
/// are needed, else the value and how many bytes it used.
fn get_varint(buf: &[u8]) -> Result<Option<(u32, usize)>> {
    let mut value: u32 = 0;
    for i in 0..4 {
        let Some(&b) = buf.get(i) else {
            return Ok(None);
        };
        value |= ((b & 0x7F) as u32) << (7 * i);
        if b & 0x80 == 0 {
            return Ok(Some((value, i + 1)));
        }
    }
    bail_kind!(
        Protocol,
        "malformed MQTT packet: variable byte integer longer than 4 bytes"
    )
}

/// Cursor over a packet body; every read is bounds-checked.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

fn malformed(what: &str) -> anyhow::Error {
    err!(Protocol, "malformed MQTT packet: {what}")
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.remaining() < n {
            return Err(malformed("truncated"));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    fn u32(&mut self) -> Result<u32> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn varint(&mut self) -> Result<u32> {
        match get_varint(&self.buf[self.pos..])? {
            Some((v, n)) => {
                self.pos += n;
                Ok(v)
            }
            None => Err(malformed("truncated variable byte integer")),
        }
    }

    fn string(&mut self) -> Result<String> {
        let len = self.u16()? as usize;
        let bytes = self.take(len)?;
        let s = std::str::from_utf8(bytes).map_err(|_| malformed("string is not UTF-8"))?;
        if s.contains('\0') {
            return Err(malformed("string contains U+0000"));
        }
        Ok(s.to_string())
    }

    fn binary(&mut self) -> Result<Vec<u8>> {
        let len = self.u16()? as usize;
        Ok(self.take(len)?.to_vec())
    }

    fn props(&mut self) -> Result<Props> {
        let len = self.varint()? as usize;
        let mut r = Reader::new(self.take(len)?);
        let mut props = Props::default();
        while r.remaining() > 0 {
            let id = r.u8()?;
            let ty =
                prop_type(id).ok_or_else(|| malformed(&format!("unknown property 0x{id:02X}")))?;
            let v = match ty {
                PropType::Byte => PropValue::Byte(r.u8()?),
                PropType::U16 => PropValue::U16(r.u16()?),
                PropType::U32 => PropValue::U32(r.u32()?),
                PropType::VarInt => PropValue::VarInt(r.varint()?),
                PropType::Str => PropValue::Str(r.string()?),
                PropType::Bin => PropValue::Bin(r.binary()?),
                PropType::Pair => PropValue::Pair(r.string()?, r.string()?),
            };
            props.0.push((id, v));
        }
        Ok(props)
    }

    fn rest(&mut self) -> &'a [u8] {
        let s = &self.buf[self.pos..];
        self.pos = self.buf.len();
        s
    }

    fn done(&self, packet: &str) -> Result<()> {
        if self.remaining() != 0 {
            return Err(malformed(&format!(
                "{} unexpected trailing bytes in {packet}",
                self.remaining()
            )));
        }
        Ok(())
    }
}

// ---- codec

#[derive(Debug)]
pub struct MqttCodec {
    /// Protocol level: 5 = MQTT 5.0, 4 = MQTT 3.1.1.
    pub version: u8,
    /// Largest packet accepted; checked from the fixed header, before the body is buffered.
    pub max_frame: usize,
}

impl Default for MqttCodec {
    fn default() -> Self {
        MqttCodec {
            version: PROTOCOL_LEVEL_5,
            max_frame: pub_core::limits::DEFAULT_MAX_FRAME_SIZE,
        }
    }
}

fn only_v5(v5: bool, empty: bool, what: &str) -> Result<()> {
    if !v5 && !empty {
        bail_kind!(
            Unsupported,
            "{what} need MQTT 5; this connection uses 3.1.1"
        );
    }
    Ok(())
}

fn ack_body(out: &mut Vec<u8>, a: &Ack, v5: bool) -> Result<()> {
    put_u16(out, a.packet_id);
    if !v5 {
        return only_v5(
            false,
            a.reason == 0 && a.props.is_empty(),
            "acknowledgement reason codes",
        );
    }
    if a.reason != 0 || !a.props.is_empty() {
        out.push(a.reason);
        if !a.props.is_empty() {
            put_props(out, &a.props)?;
        }
    }
    Ok(())
}

fn reason_body(out: &mut Vec<u8>, r: &ReasonProps, v5: bool) -> Result<()> {
    if !v5 {
        return only_v5(
            false,
            r.reason == 0 && r.props.is_empty(),
            "DISCONNECT reason codes",
        );
    }
    if r.reason != 0 || !r.props.is_empty() {
        out.push(r.reason);
        if !r.props.is_empty() {
            put_props(out, &r.props)?;
        }
    }
    Ok(())
}

fn decode_ack(r: &mut Reader, name: &str, v5: bool) -> Result<Ack> {
    let packet_id = r.u16()?;
    if !v5 {
        r.done(name)?;
        return Ok(Ack::new(packet_id));
    }
    let reason = if r.remaining() > 0 { r.u8()? } else { 0 };
    let props = if r.remaining() > 0 {
        r.props()?
    } else {
        Props::default()
    };
    r.done(name)?;
    Ok(Ack {
        packet_id,
        reason,
        props,
    })
}

fn decode_reason(r: &mut Reader, name: &str, v5: bool) -> Result<ReasonProps> {
    if !v5 {
        r.done(name)?;
        return Ok(ReasonProps::default());
    }
    let reason = if r.remaining() > 0 { r.u8()? } else { 0 };
    let props = if r.remaining() > 0 {
        r.props()?
    } else {
        Props::default()
    };
    r.done(name)?;
    Ok(ReasonProps { reason, props })
}

impl Codec for MqttCodec {
    type Frame = Packet;

    fn encode(&mut self, p: &Packet, out: &mut Vec<u8>) -> Result<()> {
        let v5 = self.version == PROTOCOL_LEVEL_5;
        // Properties are written only for MQTT 5; with 3.1.1 they must be empty.
        let props = |body: &mut Vec<u8>, props: &Props, what: &str| -> Result<()> {
            if v5 {
                put_props(body, props)
            } else {
                only_v5(false, props.is_empty(), what)
            }
        };
        let mut body = Vec::new();
        let (kind, flags): (u8, u8) = match p {
            Packet::Connect(c) => {
                put_str(&mut body, "MQTT")?;
                body.push(self.version);
                let mut f = 0u8;
                if c.username.is_some() {
                    f |= 0x80;
                }
                if c.password.is_some() {
                    f |= 0x40;
                }
                if c.clean_start {
                    f |= 0x02;
                }
                if let Some(w) = &c.will {
                    if w.qos > 2 {
                        bail_kind!(Usage, "MQTT will QoS must be 0, 1 or 2");
                    }
                    f |= 0x04 | (w.qos << 3);
                    if w.retain {
                        f |= 0x20;
                    }
                }
                if !v5 && c.password.is_some() && c.username.is_none() {
                    bail_kind!(Usage, "MQTT 3.1.1 needs a username to send a password");
                }
                body.push(f);
                put_u16(&mut body, c.keep_alive);
                props(&mut body, &c.props, "CONNECT properties")?;
                put_str(&mut body, &c.client_id)?;
                if let Some(w) = &c.will {
                    props(&mut body, &w.props, "will properties")?;
                    put_str(&mut body, &w.topic)?;
                    put_bin(&mut body, &w.payload)?;
                }
                if let Some(u) = &c.username {
                    put_str(&mut body, u)?;
                }
                if let Some(pw) = &c.password {
                    put_bin(&mut body, pw)?;
                }
                (1, 0)
            }
            Packet::ConnAck(c) => {
                body.push(u8::from(c.session_present));
                body.push(c.reason);
                props(&mut body, &c.props, "CONNACK properties")?;
                (2, 0)
            }
            Packet::Publish(pb) => {
                if pb.qos > 2 {
                    bail_kind!(Usage, "MQTT QoS must be 0, 1 or 2");
                }
                put_str(&mut body, &pb.topic)?;
                if pb.qos > 0 {
                    let id = pb.packet_id.ok_or_else(|| {
                        err!(Protocol, "QoS {} PUBLISH needs a packet id", pb.qos)
                    })?;
                    put_u16(&mut body, id);
                }
                props(&mut body, &pb.props, "message properties")?;
                body.extend_from_slice(&pb.payload);
                let flags = (u8::from(pb.dup) << 3) | (pb.qos << 1) | u8::from(pb.retain);
                (3, flags)
            }
            Packet::PubAck(a) => {
                ack_body(&mut body, a, v5)?;
                (4, 0)
            }
            Packet::PubRec(a) => {
                ack_body(&mut body, a, v5)?;
                (5, 0)
            }
            Packet::PubRel(a) => {
                ack_body(&mut body, a, v5)?;
                (6, 0b0010)
            }
            Packet::PubComp(a) => {
                ack_body(&mut body, a, v5)?;
                (7, 0)
            }
            Packet::Subscribe(s) => {
                put_u16(&mut body, s.packet_id);
                props(&mut body, &s.props, "subscription properties")?;
                for (f, opts) in &s.filters {
                    if !v5 && opts & !0x03 != 0 {
                        bail_kind!(
                            Unsupported,
                            "subscription options beyond QoS (e.g. retain-handling) need MQTT 5"
                        );
                    }
                    put_str(&mut body, f)?;
                    body.push(*opts);
                }
                (8, 0b0010)
            }
            Packet::SubAck(s) | Packet::UnsubAck(s) => {
                put_u16(&mut body, s.packet_id);
                props(&mut body, &s.props, "SUBACK properties")?;
                // 3.1.1's UNSUBACK carries only the packet id.
                if v5 || matches!(p, Packet::SubAck(_)) {
                    body.extend_from_slice(&s.reasons);
                }
                (
                    if matches!(p, Packet::SubAck(_)) {
                        9
                    } else {
                        11
                    },
                    0,
                )
            }
            Packet::Unsubscribe(u) => {
                put_u16(&mut body, u.packet_id);
                props(&mut body, &u.props, "UNSUBSCRIBE properties")?;
                for f in &u.filters {
                    put_str(&mut body, f)?;
                }
                (10, 0b0010)
            }
            Packet::PingReq => (12, 0),
            Packet::PingResp => (13, 0),
            Packet::Disconnect(r) => {
                reason_body(&mut body, r, v5)?;
                (14, 0)
            }
            Packet::Auth(r) => {
                only_v5(v5, false, "AUTH packets")?;
                reason_body(&mut body, r, v5)?;
                (15, 0)
            }
        };
        out.push((kind << 4) | flags);
        put_varint(out, u32::try_from(body.len()).unwrap_or(u32::MAX))?;
        out.extend_from_slice(&body);
        Ok(())
    }

    fn decode(&mut self, buf: &[u8]) -> Result<Decoded<Packet>> {
        let Some(&first) = buf.first() else {
            return Ok(Decoded::NeedMore);
        };
        let Some((len, len_bytes)) = get_varint(&buf[1..])? else {
            return Ok(Decoded::NeedMore);
        };
        let len = len as usize;
        if len >= self.max_frame {
            bail_kind!(
                Protocol,
                "server announced a {len}-byte MQTT packet, over max-frame-size ({}); raise it with -O max-frame-size=...",
                pub_core::limits::human(self.max_frame)
            );
        }
        let total = 1 + len_bytes + len;
        if buf.len() < total {
            return Ok(Decoded::NeedMore);
        }
        let v5 = self.version == PROTOCOL_LEVEL_5;
        let (kind, flags) = (first >> 4, first & 0x0F);
        let mut r = Reader::new(&buf[1 + len_bytes..total]);
        let require_flags = |want: u8, name: &str| -> Result<()> {
            if flags != want {
                return Err(malformed(&format!("{name} with flags {flags:#06b}")));
            }
            Ok(())
        };

        let packet = match kind {
            1 => {
                require_flags(0, "CONNECT")?;
                if r.string()? != "MQTT" {
                    return Err(malformed("CONNECT protocol name is not MQTT"));
                }
                // CONNECT says which version the rest of the connection uses.
                let level = r.u8()?;
                if level != PROTOCOL_LEVEL_5 && level != PROTOCOL_LEVEL_311 {
                    bail_kind!(Protocol, "unsupported MQTT protocol level {level}");
                }
                let f = r.u8()?;
                if f & 0x04 == 0 && f & 0x38 != 0 {
                    return Err(malformed("CONNECT will QoS/retain without a will"));
                }
                let keep_alive = r.u16()?;
                let props = if level == PROTOCOL_LEVEL_5 {
                    r.props()?
                } else {
                    Props::default()
                };
                let client_id = r.string()?;
                let will = if f & 0x04 != 0 {
                    let props = if level == PROTOCOL_LEVEL_5 {
                        r.props()?
                    } else {
                        Props::default()
                    };
                    let qos = (f >> 3) & 0x03;
                    if qos > 2 {
                        return Err(malformed("CONNECT will QoS 3"));
                    }
                    Some(Will {
                        qos,
                        retain: f & 0x20 != 0,
                        props,
                        topic: r.string()?,
                        payload: r.binary()?,
                    })
                } else {
                    None
                };
                let username = if f & 0x80 != 0 {
                    Some(r.string()?)
                } else {
                    None
                };
                let password = if f & 0x40 != 0 {
                    Some(r.binary()?)
                } else {
                    None
                };
                r.done("CONNECT")?;
                Packet::Connect(Connect {
                    protocol_level: level,
                    clean_start: f & 0x02 != 0,
                    keep_alive,
                    props,
                    client_id,
                    username,
                    password,
                    will,
                })
            }
            2 => {
                require_flags(0, "CONNACK")?;
                let session_present = r.u8()? & 0x01 != 0;
                let reason = r.u8()?;
                let props = if v5 && r.remaining() > 0 {
                    r.props()?
                } else {
                    Props::default()
                };
                r.done("CONNACK")?;
                Packet::ConnAck(ConnAck {
                    session_present,
                    reason,
                    props,
                })
            }
            3 => {
                let qos = (flags >> 1) & 0x03;
                if qos == 3 {
                    return Err(malformed("PUBLISH with QoS 3"));
                }
                let topic = r.string()?;
                let packet_id = match qos {
                    0 => None,
                    _ => match r.u16()? {
                        0 => return Err(malformed("PUBLISH with packet id 0")),
                        id => Some(id),
                    },
                };
                let props = if v5 { r.props()? } else { Props::default() };
                let payload = r.rest().to_vec();
                Packet::Publish(Publish {
                    dup: flags & 0x08 != 0,
                    qos,
                    retain: flags & 0x01 != 0,
                    topic,
                    packet_id,
                    props,
                    payload,
                })
            }
            4 => {
                require_flags(0, "PUBACK")?;
                Packet::PubAck(decode_ack(&mut r, "PUBACK", v5)?)
            }
            5 => {
                require_flags(0, "PUBREC")?;
                Packet::PubRec(decode_ack(&mut r, "PUBREC", v5)?)
            }
            6 => {
                require_flags(0b0010, "PUBREL")?;
                Packet::PubRel(decode_ack(&mut r, "PUBREL", v5)?)
            }
            7 => {
                require_flags(0, "PUBCOMP")?;
                Packet::PubComp(decode_ack(&mut r, "PUBCOMP", v5)?)
            }
            8 => {
                require_flags(0b0010, "SUBSCRIBE")?;
                let packet_id = r.u16()?;
                let props = if v5 { r.props()? } else { Props::default() };
                let mut filters = Vec::new();
                while r.remaining() > 0 {
                    filters.push((r.string()?, r.u8()?));
                }
                if filters.is_empty() {
                    return Err(malformed("SUBSCRIBE without topic filters"));
                }
                Packet::Subscribe(Subscribe {
                    packet_id,
                    props,
                    filters,
                })
            }
            9 | 11 => {
                require_flags(0, "SUBACK/UNSUBACK")?;
                let packet_id = r.u16()?;
                let props = if v5 { r.props()? } else { Props::default() };
                let reasons = r.rest().to_vec();
                if !v5 && kind == 11 && !reasons.is_empty() {
                    return Err(malformed("3.1.1 UNSUBACK with a payload"));
                }
                let ack = SubAck {
                    packet_id,
                    props,
                    reasons,
                };
                if kind == 9 {
                    Packet::SubAck(ack)
                } else {
                    Packet::UnsubAck(ack)
                }
            }
            10 => {
                require_flags(0b0010, "UNSUBSCRIBE")?;
                let packet_id = r.u16()?;
                let props = if v5 { r.props()? } else { Props::default() };
                let mut filters = Vec::new();
                while r.remaining() > 0 {
                    filters.push(r.string()?);
                }
                Packet::Unsubscribe(Unsubscribe {
                    packet_id,
                    props,
                    filters,
                })
            }
            12 | 13 => {
                require_flags(0, "PINGREQ/PINGRESP")?;
                r.done("PINGREQ/PINGRESP")?;
                if kind == 12 {
                    Packet::PingReq
                } else {
                    Packet::PingResp
                }
            }
            14 => {
                require_flags(0, "DISCONNECT")?;
                Packet::Disconnect(decode_reason(&mut r, "DISCONNECT", v5)?)
            }
            15 if v5 => {
                require_flags(0, "AUTH")?;
                Packet::Auth(decode_reason(&mut r, "AUTH", v5)?)
            }
            other => return Err(malformed(&format!("reserved packet type {other}"))),
        };
        Ok(Decoded::Frame(packet, total))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(p: Packet) {
        round_trip_at(PROTOCOL_LEVEL_5, p)
    }

    fn round_trip_at(version: u8, p: Packet) {
        let mut c = MqttCodec {
            version,
            ..Default::default()
        };
        let mut bytes = Vec::new();
        c.encode(&p, &mut bytes).unwrap();
        assert_eq!(
            c.decode(&bytes).unwrap(),
            Decoded::Frame(p.clone(), bytes.len()),
            "{p:?}"
        );
        for cut in 0..bytes.len() {
            assert_eq!(
                c.decode(&bytes[..cut]).unwrap(),
                Decoded::NeedMore,
                "{} cut at {cut}",
                p.name()
            );
        }
    }

    #[test]
    fn varints() {
        for (n, enc) in [
            (0u32, vec![0x00]),
            (127, vec![0x7F]),
            (128, vec![0x80, 0x01]),
            (16_383, vec![0xFF, 0x7F]),
            (16_384, vec![0x80, 0x80, 0x01]),
            (MAX_VARINT, vec![0xFF, 0xFF, 0xFF, 0x7F]),
        ] {
            let mut out = Vec::new();
            put_varint(&mut out, n).unwrap();
            assert_eq!(out, enc, "{n}");
            assert_eq!(get_varint(&enc).unwrap(), Some((n, enc.len())));
        }
        assert!(put_varint(&mut Vec::new(), MAX_VARINT + 1).is_err());
        assert!(get_varint(&[0xFF, 0xFF, 0xFF, 0xFF, 0x01]).is_err());
        assert_eq!(get_varint(&[0x80]).unwrap(), None);
    }

    fn all_props() -> Props {
        let mut p = Props::default();
        p.push(prop::PAYLOAD_FORMAT_INDICATOR, PropValue::Byte(1))
            .push(prop::MESSAGE_EXPIRY_INTERVAL, PropValue::U32(60))
            .push(
                prop::CONTENT_TYPE,
                PropValue::Str("application/json".into()),
            )
            .push(prop::RESPONSE_TOPIC, PropValue::Str("replies/1".into()))
            .push(prop::CORRELATION_DATA, PropValue::Bin(vec![0, 1, 2]))
            .push(prop::SUBSCRIPTION_IDENTIFIER, PropValue::VarInt(200_000))
            .push(prop::TOPIC_ALIAS, PropValue::U16(3))
            .push(
                prop::USER_PROPERTY,
                PropValue::Pair("region".into(), "EU".into()),
            )
            .push(
                prop::USER_PROPERTY,
                PropValue::Pair("region".into(), "US".into()),
            );
        p
    }

    #[test]
    fn packets_round_trip() {
        round_trip(Packet::Connect(Connect {
            protocol_level: PROTOCOL_LEVEL_5,
            clean_start: true,
            keep_alive: 60,
            props: {
                let mut p = Props::default();
                p.push(prop::SESSION_EXPIRY_INTERVAL, PropValue::U32(0));
                p
            },
            client_id: "pub-1".into(),
            username: Some("app".into()),
            password: Some(b"s3cret".to_vec()),
            will: Some(Will {
                qos: 2,
                retain: true,
                props: {
                    let mut p = Props::default();
                    p.push(prop::WILL_DELAY_INTERVAL, PropValue::U32(30));
                    p
                },
                topic: "status/pub-1".into(),
                payload: b"offline".to_vec(),
            }),
        }));
        round_trip(Packet::ConnAck(ConnAck {
            session_present: false,
            reason: 0,
            props: Props::default(),
        }));
        round_trip(Packet::Publish(Publish {
            dup: false,
            qos: 2,
            retain: true,
            topic: "sensors/kitchen/temp".into(),
            packet_id: Some(7),
            props: all_props(),
            payload: b"\x00binary\xFF".to_vec(),
        }));
        round_trip(Packet::Publish(Publish {
            topic: "a".into(),
            payload: b"q0".to_vec(),
            ..Default::default()
        }));
        round_trip(Packet::PubAck(Ack::new(7)));
        round_trip(Packet::PubAck(Ack {
            packet_id: 7,
            reason: 0x10,
            props: Props::default(),
        }));
        round_trip(Packet::PubRec(Ack {
            packet_id: 9,
            reason: 0x87,
            props: {
                let mut p = Props::default();
                p.push(prop::REASON_STRING, PropValue::Str("nope".into()));
                p
            },
        }));
        round_trip(Packet::PubRel(Ack::new(9)));
        round_trip(Packet::PubComp(Ack::new(9)));
        round_trip(Packet::Subscribe(Subscribe {
            packet_id: 2,
            props: Props::default(),
            filters: vec![("sensors/+/temp".into(), 1), ("alerts/#".into(), 0x22)],
        }));
        round_trip(Packet::SubAck(SubAck {
            packet_id: 2,
            props: Props::default(),
            reasons: vec![1, 0x87],
        }));
        round_trip(Packet::Unsubscribe(Unsubscribe {
            packet_id: 3,
            props: Props::default(),
            filters: vec!["a/#".into()],
        }));
        round_trip(Packet::UnsubAck(SubAck {
            packet_id: 3,
            props: Props::default(),
            reasons: vec![0],
        }));
        round_trip(Packet::PingReq);
        round_trip(Packet::PingResp);
        round_trip(Packet::Disconnect(ReasonProps::default()));
        round_trip(Packet::Disconnect(ReasonProps {
            reason: 0x8E,
            props: Props::default(),
        }));
    }

    #[test]
    fn malformed_packets() {
        let mut c = MqttCodec::default();
        let err = |c: &mut MqttCodec, b: &[u8]| c.decode(b).unwrap_err().to_string();
        assert!(matches!(
            c.decode(&[0x62, 0x02, 0, 1]).unwrap(),
            Decoded::Frame(Packet::PubRel(_), 4)
        ));
        assert!(err(&mut c, &[0x60, 0x02, 0, 1]).contains("PUBREL with flags")); // PUBREL needs flags 0010
        assert!(err(&mut c, &[0x36, 0x00]).contains("QoS 3"));
        assert!(err(&mut c, &[0x00, 0x00]).contains("reserved packet type 0"));
        assert!(err(&mut c, &[0xD0, 0x01, 0x00]).contains("trailing bytes"));
        // Unknown property id in a PUBACK.
        assert!(err(&mut c, &[0x40, 0x05, 0, 1, 0, 1, 0x7F]).contains("unknown property 0x7F"));
        // String that runs past the end of the packet.
        assert!(err(&mut c, &[0x30, 0x03, 0, 5, b'a']).contains("truncated"));
        // Invalid UTF-8 topic.
        assert!(err(&mut c, &[0x30, 0x04, 0, 1, 0xFF, 0]).contains("not UTF-8"));

        let mut small = MqttCodec {
            max_frame: 100,
            ..Default::default()
        };
        let e = err(&mut small, &[0x30, 0xE8, 0x07]); // remaining length 1000
        assert!(e.contains("over max-frame-size (100 bytes)"), "{e}");
    }

    #[test]
    fn encode_rejects_bad_input() {
        let mut c = MqttCodec::default();
        let mut out = Vec::new();
        let p = Packet::Publish(Publish {
            qos: 1,
            topic: "a".into(),
            ..Default::default()
        });
        assert!(c.encode(&p, &mut out).is_err()); // QoS 1 without packet id
        let p = Packet::Publish(Publish {
            topic: "a\0b".into(),
            ..Default::default()
        });
        assert!(c.encode(&p, &mut out).is_err());
        assert_eq!(reason_name(0x87), "Not authorized");
    }

    #[test]
    fn mqtt_311_packets() {
        let v4 = PROTOCOL_LEVEL_311;
        round_trip_at(
            v4,
            Packet::Connect(Connect {
                protocol_level: v4,
                clean_start: false,
                keep_alive: 30,
                client_id: "worker".into(),
                username: Some("app".into()),
                password: Some(b"pw".to_vec()),
                ..Default::default()
            }),
        );
        round_trip_at(
            v4,
            Packet::ConnAck(ConnAck {
                session_present: true,
                reason: 0,
                props: Props::default(),
            }),
        );
        round_trip_at(
            v4,
            Packet::Publish(Publish {
                qos: 1,
                packet_id: Some(5),
                topic: "a/b".into(),
                payload: b"x".to_vec(),
                ..Default::default()
            }),
        );
        for ack in [
            Packet::PubAck(Ack::new(5)),
            Packet::PubRec(Ack::new(5)),
            Packet::PubRel(Ack::new(5)),
            Packet::PubComp(Ack::new(5)),
        ] {
            round_trip_at(v4, ack);
        }
        round_trip_at(
            v4,
            Packet::Subscribe(Subscribe {
                packet_id: 6,
                props: Props::default(),
                filters: vec![("a/#".into(), 1)],
            }),
        );
        round_trip_at(
            v4,
            Packet::SubAck(SubAck {
                packet_id: 6,
                props: Props::default(),
                reasons: vec![1, 0x80],
            }),
        );
        round_trip_at(
            v4,
            Packet::UnsubAck(SubAck {
                packet_id: 7,
                ..Default::default()
            }),
        );
        round_trip_at(v4, Packet::Disconnect(ReasonProps::default()));

        // Exact 3.1.1 wire bytes: CONNECT with protocol level 4 and no properties.
        let mut c = MqttCodec {
            version: v4,
            ..Default::default()
        };
        let mut out = Vec::new();
        c.encode(
            &Packet::Connect(Connect {
                clean_start: true,
                keep_alive: 60,
                client_id: "c".into(),
                ..Default::default()
            }),
            &mut out,
        )
        .unwrap();
        assert_eq!(
            out,
            [0x10, 13, 0, 4, b'M', b'Q', b'T', b'T', 4, 0x02, 0, 60, 0, 1, b'c']
        );
    }

    #[test]
    fn mqtt_311_refuses_mqtt5_only_content() {
        let mut c = MqttCodec {
            version: PROTOCOL_LEVEL_311,
            ..Default::default()
        };
        let mut out = Vec::new();
        let mut props = Props::default();
        props.push(prop::CONTENT_TYPE, PropValue::Str("text/plain".into()));
        let e = c
            .encode(
                &Packet::Publish(Publish {
                    topic: "a".into(),
                    props,
                    ..Default::default()
                }),
                &mut out,
            )
            .unwrap_err();
        assert!(
            e.to_string().contains("message properties need MQTT 5"),
            "{e}"
        );
        assert!(c
            .encode(
                &Packet::PubAck(Ack {
                    packet_id: 1,
                    reason: 0x10,
                    props: Props::default()
                }),
                &mut out
            )
            .is_err());
        assert!(c
            .encode(
                &Packet::Subscribe(Subscribe {
                    packet_id: 1,
                    props: Props::default(),
                    filters: vec![("a".into(), 0x21)]
                }),
                &mut out
            )
            .is_err());
        assert!(c
            .encode(&Packet::Auth(ReasonProps::default()), &mut out)
            .is_err());
        let pw_only = Connect {
            password: Some(b"x".to_vec()),
            ..Default::default()
        };
        assert!(c
            .encode(&Packet::Connect(pw_only), &mut out)
            .unwrap_err()
            .to_string()
            .contains("needs a username"));
        // On decode, a 3.1.1 ack longer than its packet id is malformed; AUTH does not exist.
        assert!(c.decode(&[0x40, 0x03, 0, 1, 0]).is_err());
        assert!(c
            .decode(&[0xF0, 0x00])
            .unwrap_err()
            .to_string()
            .contains("reserved packet type 15"));
    }
}
