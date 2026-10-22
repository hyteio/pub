//! OpenWire framing on top of the shared [`pub_core::Connection`].
//!
//! ```text
//! i32 size        bytes that follow (big-endian)
//! u8  type        data structure type; 0 = null
//! ... body        tight: boolean stream + data; loose: data
//! ```
//!
//! The codec starts in loose mode — the first WireFormatInfo is always loose —
//! and the session switches [`OpenWireCodec::encoding`] once negotiation settles.

use pub_core::{bail_kind, Codec, Decoded, Result};

use crate::types::DataStructure;
use crate::wire::{Encoding, Reader, Writer};

#[derive(Debug)]
pub struct OpenWireCodec {
    pub encoding: Encoding,
    /// Largest frame accepted; checked from the size prefix before buffering the body.
    pub max_frame: usize,
}

impl Default for OpenWireCodec {
    fn default() -> Self {
        OpenWireCodec {
            encoding: Encoding::default(),
            max_frame: pub_core::limits::DEFAULT_MAX_FRAME_SIZE,
        }
    }
}

impl Codec for OpenWireCodec {
    type Frame = DataStructure;

    fn encode(&mut self, ds: &DataStructure, out: &mut Vec<u8>) -> Result<()> {
        let mut w = Writer::new(self.encoding);
        ds.marshal_body(&mut w)?;
        let body = w.finish();
        let size = 1 + body.len();
        if size > i32::MAX as usize {
            bail_kind!(Usage, "OpenWire frame too large ({size} bytes)");
        }
        out.extend_from_slice(&(size as i32).to_be_bytes());
        out.push(ds.type_id());
        out.extend_from_slice(&body);
        Ok(())
    }

    fn decode(&mut self, buf: &[u8]) -> Result<Decoded<DataStructure>> {
        let Some(prefix) = buf.get(..4) else {
            return Ok(Decoded::NeedMore);
        };
        let size = i32::from_be_bytes(prefix.try_into().unwrap());
        if size < 1 {
            bail_kind!(Protocol, "malformed OpenWire frame: size {size}");
        }
        let size = size as usize;
        if size > self.max_frame {
            bail_kind!(
                Protocol,
                "server announced a {size}-byte OpenWire frame, over max-frame-size ({}); raise it with -O max-frame-size=...",
                pub_core::limits::human(self.max_frame)
            );
        }
        let total = 4 + size;
        if buf.len() < total {
            return Ok(Decoded::NeedMore);
        }
        let t = buf[4];
        // A null command, or a command pub doesn't use (ControlCommand,
        // FlushCommand, MessageDispatchNotification, ...): its length is known.
        if t == 0 || !DataStructure::supports(t) {
            return Ok(Decoded::Skip(total));
        }
        let mut r = Reader::new(self.encoding, &buf[5..total])?;
        let ds = DataStructure::unmarshal_body(t, &mut r)?;
        r.done(ds.name())?;
        Ok(Decoded::Frame(ds, total))
    }
}
