//! AMQP 1.0 framing (part 2.3) on top of the shared [`pub_core::Connection`].
//!
//! ```text
//! "AMQP" id 1 0 0      protocol header: id 0 = AMQP, 3 = SASL (not a frame)
//! u32 size             whole frame, header included
//! u8  doff             data offset in 4-byte words (≥ 2)
//! u8  type             0 = AMQP, 1 = SASL
//! u16 channel          (AMQP; ignored for SASL)
//! ... extended header  (doff·4 − 8 bytes, skipped)
//! ... body             a performative, then (transfers) payload bytes
//! ```
//!
//! A frame with no body is an empty frame: a heartbeat.

use pub_core::{bail_kind, Codec, Decoded, Result};

use crate::described::*;
use crate::types::Value;

/// Protocol ids in the header.
pub const PROTOCOL_AMQP: u8 = 0;
pub const PROTOCOL_SASL: u8 = 3;

/// A performative on an AMQP frame. Frames are read, handled and dropped one
/// at a time, so the size difference between variants costs nothing.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
pub enum Performative {
    Open(Open),
    Begin(Begin),
    Attach(Attach),
    Flow(Flow),
    Transfer(Transfer),
    Disposition(Disposition),
    Detach(Detach),
    End(End),
    Close(Close),
}

impl Performative {
    pub fn name(&self) -> &'static str {
        match self {
            Performative::Open(_) => "open",
            Performative::Begin(_) => "begin",
            Performative::Attach(_) => "attach",
            Performative::Flow(_) => "flow",
            Performative::Transfer(_) => "transfer",
            Performative::Disposition(_) => "disposition",
            Performative::Detach(_) => "detach",
            Performative::End(_) => "end",
            Performative::Close(_) => "close",
        }
    }

    fn to_value(&self) -> Value {
        match self {
            Performative::Open(p) => p.to_value(),
            Performative::Begin(p) => p.to_value(),
            Performative::Attach(p) => p.to_value(),
            Performative::Flow(p) => p.to_value(),
            Performative::Transfer(p) => p.to_value(),
            Performative::Disposition(p) => p.to_value(),
            Performative::Detach(p) => p.to_value(),
            Performative::End(p) => p.to_value(),
            Performative::Close(p) => p.to_value(),
        }
    }

    fn from_value(v: Value) -> Result<Self> {
        Ok(match v.descriptor_code() {
            Some(0x10) => Performative::Open(Open::from_value(v)?),
            Some(0x11) => Performative::Begin(Begin::from_value(v)?),
            Some(0x12) => Performative::Attach(Attach::from_value(v)?),
            Some(0x13) => Performative::Flow(Flow::from_value(v)?),
            Some(0x14) => Performative::Transfer(Transfer::from_value(v)?),
            Some(0x15) => Performative::Disposition(Disposition::from_value(v)?),
            Some(0x16) => Performative::Detach(Detach::from_value(v)?),
            Some(0x17) => Performative::End(End::from_value(v)?),
            Some(0x18) => Performative::Close(Close::from_value(v)?),
            _ => bail_kind!(Protocol, "unknown AMQP performative {v:?}"),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SaslFrame {
    Mechanisms(SaslMechanisms),
    Init(SaslInit),
    Challenge(SaslChallenge),
    Response(SaslResponse),
    Outcome(SaslOutcome),
}

impl SaslFrame {
    fn to_value(&self) -> Value {
        match self {
            SaslFrame::Mechanisms(f) => f.to_value(),
            SaslFrame::Init(f) => f.to_value(),
            SaslFrame::Challenge(f) => f.to_value(),
            SaslFrame::Response(f) => f.to_value(),
            SaslFrame::Outcome(f) => f.to_value(),
        }
    }

    fn from_value(v: Value) -> Result<Self> {
        Ok(match v.descriptor_code() {
            Some(0x40) => SaslFrame::Mechanisms(SaslMechanisms::from_value(v)?),
            Some(0x41) => SaslFrame::Init(SaslInit::from_value(v)?),
            Some(0x42) => SaslFrame::Challenge(SaslChallenge::from_value(v)?),
            Some(0x43) => SaslFrame::Response(SaslResponse::from_value(v)?),
            Some(0x44) => SaslFrame::Outcome(SaslOutcome::from_value(v)?),
            _ => bail_kind!(Protocol, "unknown AMQP SASL frame {v:?}"),
        })
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    /// The protocol header (`AMQP` + id + version 1.0.0).
    Header(u8),
    Amqp {
        channel: u16,
        performative: Performative,
        /// Message bytes after a transfer.
        payload: Vec<u8>,
    },
    Sasl(SaslFrame),
    /// No body: a heartbeat.
    Empty,
}

impl Frame {
    pub fn amqp(channel: u16, performative: Performative) -> Frame {
        Frame::Amqp {
            channel,
            performative,
            payload: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub struct AmqpCodec {
    /// The next thing to read is a protocol header, not a frame.
    pub expect_header: bool,
    /// Largest frame accepted; checked from the size field before buffering.
    pub max_frame: usize,
}

impl Default for AmqpCodec {
    fn default() -> Self {
        AmqpCodec {
            expect_header: true,
            max_frame: pub_core::limits::DEFAULT_MAX_FRAME_SIZE,
        }
    }
}

fn frame(out: &mut Vec<u8>, kind: u8, channel: u16, body: &[u8], payload: &[u8]) -> Result<()> {
    let size = 8 + body.len() + payload.len();
    let size = u32::try_from(size)
        .map_err(|_| pub_core::err!(Usage, "AMQP frame too large ({size} bytes)"))?;
    out.extend_from_slice(&size.to_be_bytes());
    out.push(2); // doff: no extended header
    out.push(kind);
    out.extend_from_slice(&channel.to_be_bytes());
    out.extend_from_slice(body);
    out.extend_from_slice(payload);
    Ok(())
}

impl Codec for AmqpCodec {
    type Frame = Frame;

    fn encode(&mut self, f: &Frame, out: &mut Vec<u8>) -> Result<()> {
        match f {
            Frame::Header(id) => out.extend_from_slice(&[b'A', b'M', b'Q', b'P', *id, 1, 0, 0]),
            Frame::Amqp {
                channel,
                performative,
                payload,
            } => frame(
                out,
                0,
                *channel,
                &performative.to_value().to_bytes()?,
                payload,
            )?,
            Frame::Sasl(s) => frame(out, 1, 0, &s.to_value().to_bytes()?, &[])?,
            Frame::Empty => frame(out, 0, 0, &[], &[])?,
        }
        Ok(())
    }

    fn decode(&mut self, buf: &[u8]) -> Result<Decoded<Frame>> {
        if self.expect_header {
            let Some(h) = buf.get(..8) else {
                return Ok(Decoded::NeedMore);
            };
            if &h[..4] != b"AMQP" {
                bail_kind!(Protocol, "not an AMQP peer (header {:02x?})", h);
            }
            if h[5..] != [1, 0, 0] {
                bail_kind!(
                    Protocol,
                    "the peer speaks AMQP {}.{}.{}; pub speaks 1.0.0",
                    h[5],
                    h[6],
                    h[7]
                );
            }
            self.expect_header = false;
            return Ok(Decoded::Frame(Frame::Header(h[4]), 8));
        }
        let Some(head) = buf.get(..8) else {
            return Ok(Decoded::NeedMore);
        };
        let size = u32::from_be_bytes(head[..4].try_into().unwrap()) as usize;
        let doff = head[4] as usize * 4;
        if size < 8 || doff < 8 || doff > size {
            bail_kind!(
                Protocol,
                "malformed AMQP frame (size {size}, data offset {doff})"
            );
        }
        if size > self.max_frame {
            bail_kind!(
                Protocol,
                "AMQP frame of {size} bytes exceeds max-frame-size ({})",
                self.max_frame
            );
        }
        if buf.len() < size {
            return Ok(Decoded::NeedMore);
        }
        let body = &buf[doff..size];
        let kind = head[5];
        let channel = u16::from_be_bytes([head[6], head[7]]);
        if body.is_empty() {
            return Ok(Decoded::Frame(Frame::Empty, size));
        }
        let (value, used) = Value::decode(body)?;
        let f = match kind {
            0 => Frame::Amqp {
                channel,
                performative: Performative::from_value(value)?,
                payload: body[used..].to_vec(),
            },
            1 => {
                if used != body.len() {
                    bail_kind!(Protocol, "trailing bytes after an AMQP SASL frame");
                }
                Frame::Sasl(SaslFrame::from_value(value)?)
            }
            other => bail_kind!(Protocol, "unknown AMQP frame type {other}"),
        };
        Ok(Decoded::Frame(f, size))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(f: &Frame) -> Vec<u8> {
        let mut out = Vec::new();
        AmqpCodec::default().encode(f, &mut out).unwrap();
        out
    }

    fn decode(c: &mut AmqpCodec, b: &[u8]) -> Frame {
        match c.decode(b).unwrap() {
            Decoded::Frame(f, n) => {
                assert_eq!(n, b.len());
                f
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn headers_frames_and_heartbeats() {
        let mut c = AmqpCodec::default();
        assert_eq!(
            encode(&Frame::Header(PROTOCOL_SASL)),
            b"AMQP\x03\x01\x00\x00"
        );
        assert_eq!(decode(&mut c, b"AMQP\x00\x01\x00\x00"), Frame::Header(0));
        assert!(!c.expect_header);

        assert_eq!(encode(&Frame::Empty), [0, 0, 0, 8, 2, 0, 0, 0]);
        assert_eq!(decode(&mut c, &[0, 0, 0, 8, 2, 0, 0, 0]), Frame::Empty);

        let close = Frame::amqp(3, Performative::Close(Close::default()));
        let bytes = encode(&close);
        assert_eq!(bytes, [0, 0, 0, 12, 2, 0, 0, 3, 0x00, 0x53, 0x18, 0x45]);
        assert_eq!(decode(&mut c, &bytes), close);

        // A transfer carries its payload after the performative.
        let t = Frame::Amqp {
            channel: 1,
            performative: Performative::Transfer(Transfer {
                handle: 0,
                delivery_id: Some(5),
                ..Default::default()
            }),
            payload: vec![0x00, 0x53, 0x77, 0xa1, 2, b'h', b'i'],
        };
        assert_eq!(decode(&mut c, &encode(&t)), t);

        // An extended header is skipped.
        let mut ext = vec![0, 0, 0, 16, 3, 0, 0, 0, 9, 9, 9, 9];
        ext.extend_from_slice(&[0x00, 0x53, 0x18, 0x45]);
        assert_eq!(
            decode(&mut c, &ext),
            Frame::amqp(0, Performative::Close(Close::default()))
        );

        let sasl = Frame::Sasl(SaslFrame::Outcome(SaslOutcome {
            code: 0,
            additional_data: None,
        }));
        assert_eq!(decode(&mut c, &encode(&sasl)), sasl);
    }

    #[test]
    fn refuses_what_it_should() {
        assert!(AmqpCodec::default().decode(b"HTTP/1.1 400").is_err());
        assert!(AmqpCodec::default()
            .decode(b"AMQP\x00\x00\x09\x01")
            .is_err()); // 0-9-1
        let mut c = AmqpCodec {
            expect_header: false,
            max_frame: 100,
        };
        assert!(c.decode(&[0, 0, 1, 0, 2, 0, 0, 0]).is_err()); // over the limit, before buffering
        assert!(c.decode(&[0, 0, 0, 4, 2, 0, 0, 0]).is_err()); // smaller than its header
        assert!(c.decode(&[0, 0, 0, 8, 1, 0, 0, 0]).is_err()); // doff < 2
        assert!(matches!(
            c.decode(&[0, 0, 0, 20, 2, 0]).unwrap(),
            Decoded::NeedMore
        ));
    }
}
