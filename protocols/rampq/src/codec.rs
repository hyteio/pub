//! AMQP 0-9-1 framing and methods (spec sections 4.2 and 1.x, the classes pub
//! uses: connection, channel, exchange, queue, basic, confirm, tx).
//!
//! ```text
//! u8 type (1 method, 2 content header, 3 body, 8 heartbeat)
//! u16 channel, u32 size, payload, u8 0xCE
//! ```

use pub_core::{bail_kind, Codec, Decoded, Result};

use crate::wire::{FieldValue, Reader, Table, Writer};

pub const FRAME_METHOD: u8 = 1;
pub const FRAME_HEADER: u8 = 2;
pub const FRAME_BODY: u8 = 3;
pub const FRAME_HEARTBEAT: u8 = 8;
const FRAME_END: u8 = 0xCE;

/// The protocol header a client sends first.
pub const PROTOCOL_HEADER: [u8; 8] = *b"AMQP\x00\x00\x09\x01";

/// A method argument's wire encoding.
trait Arg: Sized {
    fn put(&self, w: &mut Writer) -> Result<()>;
    fn get(r: &mut Reader) -> Result<Self>;
}

impl Arg for u8 {
    fn put(&self, w: &mut Writer) -> Result<()> {
        w.octet(*self);
        Ok(())
    }
    fn get(r: &mut Reader) -> Result<Self> {
        r.octet()
    }
}

impl Arg for u16 {
    fn put(&self, w: &mut Writer) -> Result<()> {
        w.short(*self);
        Ok(())
    }
    fn get(r: &mut Reader) -> Result<Self> {
        r.short()
    }
}

impl Arg for u32 {
    fn put(&self, w: &mut Writer) -> Result<()> {
        w.long(*self);
        Ok(())
    }
    fn get(r: &mut Reader) -> Result<Self> {
        r.long()
    }
}

impl Arg for u64 {
    fn put(&self, w: &mut Writer) -> Result<()> {
        w.longlong(*self);
        Ok(())
    }
    fn get(r: &mut Reader) -> Result<Self> {
        r.longlong()
    }
}

impl Arg for bool {
    fn put(&self, w: &mut Writer) -> Result<()> {
        w.bit(*self);
        Ok(())
    }
    fn get(r: &mut Reader) -> Result<Self> {
        r.bit()
    }
}

/// `shortstr` arguments are `String`; `longstr` ones are [`LongStr`].
impl Arg for String {
    fn put(&self, w: &mut Writer) -> Result<()> {
        w.shortstr(self)
    }
    fn get(r: &mut Reader) -> Result<Self> {
        r.shortstr()
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct LongStr(pub Vec<u8>);

impl Arg for LongStr {
    fn put(&self, w: &mut Writer) -> Result<()> {
        w.longstr(&self.0)
    }
    fn get(r: &mut Reader) -> Result<Self> {
        r.longstr().map(LongStr)
    }
}

impl Arg for Table {
    fn put(&self, w: &mut Writer) -> Result<()> {
        w.table(self)
    }
    fn get(r: &mut Reader) -> Result<Self> {
        r.table()
    }
}

macro_rules! methods {
    ($($name:ident ($class:literal, $method:literal) { $($field:ident : $ty:ty),* $(,)? })*) => {
        $(
            #[derive(Debug, Clone, PartialEq, Default)]
            pub struct $name { $(pub $field: $ty),* }
        )*

        /// Every method pub sends or receives.
        #[derive(Debug, Clone, PartialEq)]
        pub enum Method { $($name($name)),* }

        impl Method {
            pub fn ids(&self) -> (u16, u16) {
                match self { $(Method::$name(_) => ($class, $method)),* }
            }

            pub fn name(&self) -> &'static str {
                match self { $(Method::$name(_) => stringify!($name)),* }
            }

            fn encode(&self, w: &mut Writer) -> Result<()> {
                let (c, m) = self.ids();
                w.short(c);
                w.short(m);
                match self {
                    $(Method::$name(_x) => { $(_x.$field.put(w)?;)* })*
                }
                Ok(())
            }

            fn decode(r: &mut Reader) -> Result<Method> {
                let (c, m) = (r.short()?, r.short()?);
                Ok(match (c, m) {
                    $(($class, $method) => Method::$name($name { $($field: <$ty as Arg>::get(r)?),* }),)*
                    _ => bail_kind!(Protocol, "unknown AMQP 0-9-1 method {c}.{m}"),
                })
            }
        }
    };
}

methods! {
    ConnectionStart (10, 10) { version_major: u8, version_minor: u8, server_properties: Table, mechanisms: LongStr, locales: LongStr }
    ConnectionStartOk (10, 11) { client_properties: Table, mechanism: String, response: LongStr, locale: String }
    ConnectionSecure (10, 20) { challenge: LongStr }
    ConnectionSecureOk (10, 21) { response: LongStr }
    ConnectionTune (10, 30) { channel_max: u16, frame_max: u32, heartbeat: u16 }
    ConnectionTuneOk (10, 31) { channel_max: u16, frame_max: u32, heartbeat: u16 }
    ConnectionOpen (10, 40) { virtual_host: String, reserved_1: String, reserved_2: bool }
    ConnectionOpenOk (10, 41) { reserved_1: String }
    ConnectionClose (10, 50) { reply_code: u16, reply_text: String, class_id: u16, method_id: u16 }
    ConnectionCloseOk (10, 51) {}
    ConnectionBlocked (10, 60) { reason: String }
    ConnectionUnblocked (10, 61) {}
    ChannelOpen (20, 10) { reserved_1: String }
    ChannelOpenOk (20, 11) { reserved_1: LongStr }
    ChannelFlow (20, 20) { active: bool }
    ChannelFlowOk (20, 21) { active: bool }
    ChannelClose (20, 40) { reply_code: u16, reply_text: String, class_id: u16, method_id: u16 }
    ChannelCloseOk (20, 41) {}
    ExchangeDeclare (40, 10) { reserved_1: u16, exchange: String, kind: String, passive: bool, durable: bool, auto_delete: bool, internal: bool, no_wait: bool, arguments: Table }
    ExchangeDeclareOk (40, 11) {}
    ExchangeDelete (40, 20) { reserved_1: u16, exchange: String, if_unused: bool, no_wait: bool }
    ExchangeDeleteOk (40, 21) {}
    QueueDeclare (50, 10) { reserved_1: u16, queue: String, passive: bool, durable: bool, exclusive: bool, auto_delete: bool, no_wait: bool, arguments: Table }
    QueueDeclareOk (50, 11) { queue: String, message_count: u32, consumer_count: u32 }
    QueueBind (50, 20) { reserved_1: u16, queue: String, exchange: String, routing_key: String, no_wait: bool, arguments: Table }
    QueueBindOk (50, 21) {}
    QueuePurge (50, 30) { reserved_1: u16, queue: String, no_wait: bool }
    QueuePurgeOk (50, 31) { message_count: u32 }
    QueueDelete (50, 40) { reserved_1: u16, queue: String, if_unused: bool, if_empty: bool, no_wait: bool }
    QueueDeleteOk (50, 41) { message_count: u32 }
    QueueUnbind (50, 50) { reserved_1: u16, queue: String, exchange: String, routing_key: String, arguments: Table }
    QueueUnbindOk (50, 51) {}
    BasicQos (60, 10) { prefetch_size: u32, prefetch_count: u16, global: bool }
    BasicQosOk (60, 11) {}
    BasicConsume (60, 20) { reserved_1: u16, queue: String, consumer_tag: String, no_local: bool, no_ack: bool, exclusive: bool, no_wait: bool, arguments: Table }
    BasicConsumeOk (60, 21) { consumer_tag: String }
    BasicCancel (60, 30) { consumer_tag: String, no_wait: bool }
    BasicCancelOk (60, 31) { consumer_tag: String }
    BasicPublish (60, 40) { reserved_1: u16, exchange: String, routing_key: String, mandatory: bool, immediate: bool }
    BasicReturn (60, 50) { reply_code: u16, reply_text: String, exchange: String, routing_key: String }
    BasicDeliver (60, 60) { consumer_tag: String, delivery_tag: u64, redelivered: bool, exchange: String, routing_key: String }
    BasicGet (60, 70) { reserved_1: u16, queue: String, no_ack: bool }
    BasicGetOk (60, 71) { delivery_tag: u64, redelivered: bool, exchange: String, routing_key: String, message_count: u32 }
    BasicGetEmpty (60, 72) { reserved_1: String }
    BasicAck (60, 80) { delivery_tag: u64, multiple: bool }
    BasicReject (60, 90) { delivery_tag: u64, requeue: bool }
    BasicRecover (60, 110) { requeue: bool }
    BasicRecoverOk (60, 111) {}
    BasicNack (60, 120) { delivery_tag: u64, multiple: bool, requeue: bool }
    ConfirmSelect (85, 10) { nowait: bool }
    ConfirmSelectOk (85, 11) {}
    TxSelect (90, 10) {}
    TxSelectOk (90, 11) {}
    TxCommit (90, 20) {}
    TxCommitOk (90, 21) {}
    TxRollback (90, 30) {}
    TxRollbackOk (90, 31) {}
}

/// `basic` content properties, in flag order (bit 15 first).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct BasicProperties {
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub headers: Option<Table>,
    /// 1 = transient, 2 = persistent.
    pub delivery_mode: Option<u8>,
    pub priority: Option<u8>,
    pub correlation_id: Option<String>,
    pub reply_to: Option<String>,
    /// Milliseconds, as a string.
    pub expiration: Option<String>,
    pub message_id: Option<String>,
    /// Seconds since the epoch.
    pub timestamp: Option<u64>,
    pub kind: Option<String>,
    pub user_id: Option<String>,
    pub app_id: Option<String>,
    pub cluster_id: Option<String>,
}

impl BasicProperties {
    fn encode(&self, w: &mut Writer) -> Result<()> {
        let flags = [
            self.content_type.is_some(),
            self.content_encoding.is_some(),
            self.headers.is_some(),
            self.delivery_mode.is_some(),
            self.priority.is_some(),
            self.correlation_id.is_some(),
            self.reply_to.is_some(),
            self.expiration.is_some(),
            self.message_id.is_some(),
            self.timestamp.is_some(),
            self.kind.is_some(),
            self.user_id.is_some(),
            self.app_id.is_some(),
            self.cluster_id.is_some(),
        ];
        let mut bits = 0u16;
        for (i, f) in flags.iter().enumerate() {
            if *f {
                bits |= 1 << (15 - i);
            }
        }
        w.short(bits);
        let s = |w: &mut Writer, v: &Option<String>| v.as_ref().map_or(Ok(()), |v| w.shortstr(v));
        s(w, &self.content_type)?;
        s(w, &self.content_encoding)?;
        if let Some(h) = &self.headers {
            w.table(h)?;
        }
        if let Some(d) = self.delivery_mode {
            w.octet(d);
        }
        if let Some(p) = self.priority {
            w.octet(p);
        }
        s(w, &self.correlation_id)?;
        s(w, &self.reply_to)?;
        s(w, &self.expiration)?;
        s(w, &self.message_id)?;
        if let Some(t) = self.timestamp {
            w.longlong(t);
        }
        s(w, &self.kind)?;
        s(w, &self.user_id)?;
        s(w, &self.app_id)?;
        s(w, &self.cluster_id)
    }

    fn decode(r: &mut Reader) -> Result<Self> {
        let flags = r.short()?;
        let has = |i: u16| flags & (1 << (15 - i)) != 0;
        let s = |r: &mut Reader, i: u16| -> Result<Option<String>> {
            if has(i) {
                r.shortstr().map(Some)
            } else {
                Ok(None)
            }
        };
        Ok(BasicProperties {
            content_type: s(r, 0)?,
            content_encoding: s(r, 1)?,
            headers: if has(2) { Some(r.table()?) } else { None },
            delivery_mode: if has(3) { Some(r.octet()?) } else { None },
            priority: if has(4) { Some(r.octet()?) } else { None },
            correlation_id: s(r, 5)?,
            reply_to: s(r, 6)?,
            expiration: s(r, 7)?,
            message_id: s(r, 8)?,
            timestamp: if has(9) { Some(r.longlong()?) } else { None },
            kind: s(r, 10)?,
            user_id: s(r, 11)?,
            app_id: s(r, 12)?,
            cluster_id: s(r, 13)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    /// The server rejects our version by answering with its protocol header.
    ProtocolHeader([u8; 8]),
    Method {
        channel: u16,
        method: Method,
    },
    Header {
        channel: u16,
        class_id: u16,
        body_size: u64,
        properties: BasicProperties,
    },
    Body {
        channel: u16,
        data: Vec<u8>,
    },
    Heartbeat,
}

impl Frame {
    pub fn method(channel: u16, method: Method) -> Frame {
        Frame::Method { channel, method }
    }
}

#[derive(Debug)]
pub struct RampqCodec {
    /// Largest frame accepted (the negotiated frame-max, or pub's limit).
    pub max_frame: usize,
    /// Send the protocol header before the next frame (connection start).
    pub send_header: bool,
}

impl Default for RampqCodec {
    fn default() -> Self {
        RampqCodec {
            max_frame: pub_core::limits::DEFAULT_MAX_FRAME_SIZE,
            send_header: true,
        }
    }
}

fn put_frame(out: &mut Vec<u8>, kind: u8, channel: u16, payload: &[u8]) -> Result<()> {
    let size =
        u32::try_from(payload.len()).map_err(|_| pub_core::err!(Usage, "AMQP frame too large"))?;
    out.push(kind);
    out.extend_from_slice(&channel.to_be_bytes());
    out.extend_from_slice(&size.to_be_bytes());
    out.extend_from_slice(payload);
    out.push(FRAME_END);
    Ok(())
}

impl Codec for RampqCodec {
    type Frame = Frame;

    fn encode(&mut self, f: &Frame, out: &mut Vec<u8>) -> Result<()> {
        match f {
            Frame::ProtocolHeader(h) => out.extend_from_slice(h),
            Frame::Method { channel, method } => {
                let mut w = Writer::new();
                method.encode(&mut w)?;
                put_frame(out, FRAME_METHOD, *channel, &w.buf)?;
            }
            Frame::Header {
                channel,
                class_id,
                body_size,
                properties,
            } => {
                let mut w = Writer::new();
                w.short(*class_id);
                w.short(0); // weight
                w.longlong(*body_size);
                properties.encode(&mut w)?;
                put_frame(out, FRAME_HEADER, *channel, &w.buf)?;
            }
            Frame::Body { channel, data } => put_frame(out, FRAME_BODY, *channel, data)?,
            Frame::Heartbeat => put_frame(out, FRAME_HEARTBEAT, 0, &[])?,
        }
        Ok(())
    }

    fn decode(&mut self, buf: &[u8]) -> Result<Decoded<Frame>> {
        if buf.starts_with(b"AMQP") {
            let Some(h) = buf.get(..8) else {
                return Ok(Decoded::NeedMore);
            };
            return Ok(Decoded::Frame(
                Frame::ProtocolHeader(h.try_into().unwrap()),
                8,
            ));
        }
        let Some(head) = buf.get(..7) else {
            return Ok(Decoded::NeedMore);
        };
        let kind = head[0];
        let channel = u16::from_be_bytes([head[1], head[2]]);
        let size = u32::from_be_bytes(head[3..7].try_into().unwrap()) as usize;
        if size + 8 > self.max_frame {
            bail_kind!(
                Protocol,
                "AMQP frame of {} bytes exceeds the frame limit ({})",
                size + 8,
                self.max_frame
            );
        }
        let total = 7 + size + 1;
        if buf.len() < total {
            return Ok(Decoded::NeedMore);
        }
        if buf[total - 1] != FRAME_END {
            bail_kind!(Protocol, "AMQP frame not terminated by 0xCE");
        }
        let payload = &buf[7..7 + size];
        let frame = match kind {
            FRAME_METHOD => {
                let mut r = Reader::new(payload);
                let method = Method::decode(&mut r)?;
                Frame::Method { channel, method }
            }
            FRAME_HEADER => {
                let mut r = Reader::new(payload);
                let class_id = r.short()?;
                let _weight = r.short()?;
                let body_size = r.longlong()?;
                let properties = BasicProperties::decode(&mut r)?;
                Frame::Header {
                    channel,
                    class_id,
                    body_size,
                    properties,
                }
            }
            FRAME_BODY => Frame::Body {
                channel,
                data: payload.to_vec(),
            },
            FRAME_HEARTBEAT => Frame::Heartbeat,
            other => bail_kind!(Protocol, "unknown AMQP 0-9-1 frame type {other}"),
        };
        Ok(Decoded::Frame(frame, total))
    }
}

/// A string table value, as servers send them.
pub fn table_str(t: &Table, key: &str) -> Option<String> {
    match crate::wire::get(t, key)? {
        FieldValue::LongStr(s) | FieldValue::Bytes(s) => {
            Some(String::from_utf8_lossy(s).into_owned())
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round(f: Frame) {
        let mut out = Vec::new();
        RampqCodec::default().encode(&f, &mut out).unwrap();
        match RampqCodec::default().decode(&out).unwrap() {
            Decoded::Frame(back, n) => {
                assert_eq!(n, out.len());
                assert_eq!(back, f);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn exact_bytes() {
        let mut out = Vec::new();
        RampqCodec::default()
            .encode(
                &Frame::method(1, Method::ChannelOpen(ChannelOpen::default())),
                &mut out,
            )
            .unwrap();
        assert_eq!(out, [1, 0, 1, 0, 0, 0, 5, 0, 20, 0, 10, 0, 0xCE]);
        let mut hb = Vec::new();
        RampqCodec::default()
            .encode(&Frame::Heartbeat, &mut hb)
            .unwrap();
        assert_eq!(hb, [8, 0, 0, 0, 0, 0, 0, 0xCE]);
        // Bits: passive, durable, exclusive, auto-delete, no-wait in one octet.
        let mut q = Vec::new();
        RampqCodec::default()
            .encode(
                &Frame::method(
                    1,
                    Method::QueueDeclare(QueueDeclare {
                        queue: "q".into(),
                        durable: true,
                        no_wait: true,
                        ..Default::default()
                    }),
                ),
                &mut q,
            )
            .unwrap();
        assert_eq!(
            &q[7..q.len() - 1],
            [0, 50, 0, 10, 0, 0, 1, b'q', 0b10010, 0, 0, 0, 0]
        );
    }

    #[test]
    fn frames_round_trip() {
        round(Frame::method(
            0,
            Method::ConnectionTune(ConnectionTune {
                channel_max: 2047,
                frame_max: 131072,
                heartbeat: 60,
            }),
        ));
        round(Frame::method(
            1,
            Method::BasicDeliver(BasicDeliver {
                consumer_tag: "ctag".into(),
                delivery_tag: 9,
                redelivered: true,
                exchange: "amq.topic".into(),
                routing_key: "a.b".into(),
            }),
        ));
        round(Frame::Header {
            channel: 1,
            class_id: 60,
            body_size: 5,
            properties: BasicProperties {
                content_type: Some("text/plain".into()),
                headers: Some(vec![("n".into(), FieldValue::I32(5))]),
                delivery_mode: Some(2),
                priority: Some(7),
                correlation_id: Some("c-1".into()),
                expiration: Some("60000".into()),
                timestamp: Some(1_790_000_000),
                kind: Some("order".into()),
                ..Default::default()
            },
        });
        round(Frame::Body {
            channel: 1,
            data: b"hello".to_vec(),
        });
        round(Frame::Heartbeat);
    }

    #[test]
    fn refuses_what_it_should() {
        let mut c = RampqCodec {
            max_frame: 100,
            ..Default::default()
        };
        assert!(c.decode(&[1, 0, 0, 0, 0, 1, 0, 0]).is_err()); // over the limit, before buffering
        assert!(c.decode(&[3, 0, 1, 0, 0, 0, 1, 9, 0x00]).is_err()); // wrong frame end
        assert!(matches!(
            c.decode(&[3, 0, 1, 0, 0, 0, 5, 1]).unwrap(),
            Decoded::NeedMore
        ));
        assert!(matches!(
            c.decode(b"AMQP\x00\x00\x09\x01").unwrap(),
            Decoded::Frame(Frame::ProtocolHeader(_), 8)
        ));
    }
}
