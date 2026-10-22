//! OpenWire field encoding, loose and tight (`v12/BaseDataStreamMarshaller`).
//!
//! **Loose**: every field in order — booleans as one byte, longs as 8 bytes,
//! strings/byte sequences/nested objects behind a presence byte.
//!
//! **Tight**: the same fields in the same order, but every boolean — field
//! values, presence flags, the "ASCII only" flag of strings, the 2-bit size of
//! longs — goes into a separate bit stream (`BooleanStream`) written ahead of the
//! data. Java computes sizes in one pass and writes in a second; both visit the
//! fields identically, so writing bits and data together in one pass produces
//! the same bytes. (Verified against all 61 v12 marshallers: tight data order ==
//! loose order minus the booleans.)
//!
//! A field's Rust type selects its encoding through [`Field`].

use pub_core::{bail_kind, err, Result};

use crate::types::{BrokerError, DataStructure, StackFrame};
use crate::utf;

/// Negotiated encoding options that change the byte layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Encoding {
    pub tight: bool,
    /// Throwables carry stack traces (and causes) when negotiated.
    pub stack_traces: bool,
}

/// Longest string tight encoding accepts (`utflen >= Short.MAX_VALUE` is refused).
const TIGHT_MAX_STRING: usize = i16::MAX as usize - 1;

pub struct Writer {
    pub enc: Encoding,
    bits: Vec<u8>,
    nbits: usize,
    data: Vec<u8>,
}

impl Writer {
    pub fn new(enc: Encoding) -> Self {
        Writer {
            enc,
            bits: Vec::new(),
            nbits: 0,
            data: Vec::new(),
        }
    }

    fn bit(&mut self, v: bool) {
        if self.nbits.is_multiple_of(8) {
            self.bits.push(0);
        }
        if v {
            *self.bits.last_mut().unwrap() |= 1 << (self.nbits % 8);
        }
        self.nbits += 1;
    }

    pub fn bool(&mut self, v: bool) {
        if self.enc.tight {
            self.bit(v)
        } else {
            self.data.push(u8::from(v))
        }
    }

    pub fn u8(&mut self, v: u8) {
        self.data.push(v);
    }

    pub fn i16(&mut self, v: i16) {
        self.data.extend_from_slice(&v.to_be_bytes());
    }

    pub fn i32(&mut self, v: i32) {
        self.data.extend_from_slice(&v.to_be_bytes());
    }

    pub fn raw(&mut self, b: &[u8]) {
        self.data.extend_from_slice(b);
    }

    pub fn i64(&mut self, v: i64) {
        if !self.enc.tight {
            self.data.extend_from_slice(&v.to_be_bytes());
            return;
        }
        let u = v as u64;
        if u == 0 {
            self.bit(false);
            self.bit(false);
        } else if u & 0xFFFF_FFFF_FFFF_0000 == 0 {
            self.bit(false);
            self.bit(true);
            self.data.extend_from_slice(&(u as u16).to_be_bytes());
        } else if u & 0xFFFF_FFFF_0000_0000 == 0 {
            self.bit(true);
            self.bit(false);
            self.data.extend_from_slice(&(u as u32).to_be_bytes());
        } else {
            self.bit(true);
            self.bit(true);
            self.data.extend_from_slice(&u.to_be_bytes());
        }
    }

    pub fn string(&mut self, v: Option<&str>) -> Result<()> {
        self.bool(v.is_some());
        let Some(s) = v else { return Ok(()) };
        if self.enc.tight {
            let ascii = utf::is_ascii_only(s);
            self.bit(ascii);
            let bytes = if ascii {
                s.as_bytes().to_vec()
            } else {
                utf::encode(s)
            };
            if bytes.len() > TIGHT_MAX_STRING {
                bail_kind!(
                    Usage,
                    "string of {} bytes is too long for OpenWire tight encoding",
                    bytes.len()
                );
            }
            self.i16(bytes.len() as i16);
            self.raw(&bytes);
        } else {
            let bytes = utf::encode(s);
            if bytes.len() > u16::MAX as usize {
                bail_kind!(
                    Usage,
                    "string of {} bytes is too long for OpenWire",
                    bytes.len()
                );
            }
            self.data
                .extend_from_slice(&(bytes.len() as u16).to_be_bytes());
            self.raw(&bytes);
        }
        Ok(())
    }

    pub fn bytes(&mut self, v: Option<&[u8]>) {
        self.bool(v.is_some());
        if let Some(b) = v {
            self.i32(b.len() as i32);
            self.raw(b);
        }
    }

    pub fn nested(&mut self, v: Option<&DataStructure>) -> Result<()> {
        self.bool(v.is_some());
        let Some(ds) = v else { return Ok(()) };
        if self.enc.tight && ds.is_marshal_aware() {
            self.bit(false); // no cached marshalled form
        }
        self.u8(ds.type_id());
        ds.marshal_body(self)
    }

    pub fn array(&mut self, v: Option<&[DataStructure]>) -> Result<()> {
        self.bool(v.is_some());
        if let Some(items) = v {
            self.i16(items.len() as i16);
            for item in items {
                self.nested(Some(item))?;
            }
        }
        Ok(())
    }

    pub fn throwable(&mut self, v: Option<&BrokerError>) -> Result<()> {
        self.bool(v.is_some());
        let Some(e) = v else { return Ok(()) };
        self.string(e.class.as_deref())?;
        self.string(e.message.as_deref())?;
        if self.enc.stack_traces {
            self.i16(e.stack.len() as i16);
            for f in &e.stack {
                self.string(f.class.as_deref())?;
                self.string(f.method.as_deref())?;
                self.string(f.file.as_deref())?;
                self.i32(f.line);
            }
            self.throwable(e.cause.as_deref())?;
        }
        Ok(())
    }

    /// The encoded body: bit stream (tight only) followed by the data.
    pub fn finish(self) -> Vec<u8> {
        if !self.enc.tight {
            return self.data;
        }
        let n = self.bits.len();
        let mut out = Vec::with_capacity(3 + n + self.data.len());
        if n < 64 {
            out.push(n as u8);
        } else if n < 256 {
            out.push(0xC0);
            out.push(n as u8);
        } else {
            out.push(0x80);
            out.extend_from_slice(&(n as u16).to_be_bytes());
        }
        out.extend_from_slice(&self.bits);
        out.extend_from_slice(&self.data);
        out
    }
}

pub struct Reader<'a> {
    pub enc: Encoding,
    bits: &'a [u8],
    bitpos: usize,
    data: &'a [u8],
    pos: usize,
    /// Nested objects and exception causes currently open.
    depth: usize,
}

/// Nested objects (and exception causes) a frame may stack up; real commands
/// use a few levels.
const MAX_NESTING: usize = 32;

fn malformed(what: impl std::fmt::Display) -> anyhow::Error {
    err!(Protocol, "malformed OpenWire frame: {what}")
}

impl<'a> Reader<'a> {
    /// `body` is everything after the type byte.
    pub fn new(enc: Encoding, body: &'a [u8]) -> Result<Self> {
        if !enc.tight {
            return Ok(Reader {
                enc,
                bits: &[],
                bitpos: 0,
                data: body,
                pos: 0,
                depth: 0,
            });
        }
        let first = *body
            .first()
            .ok_or_else(|| malformed("missing boolean stream"))?;
        let (count, start) = match first {
            0xC0 => (
                *body
                    .get(1)
                    .ok_or_else(|| malformed("truncated boolean stream"))? as usize,
                2,
            ),
            0x80 => {
                let b = body
                    .get(1..3)
                    .ok_or_else(|| malformed("truncated boolean stream"))?;
                (u16::from_be_bytes([b[0], b[1]]) as usize, 3)
            }
            n => (n as usize, 1),
        };
        let bits = body
            .get(start..start + count)
            .ok_or_else(|| malformed("truncated boolean stream"))?;
        Ok(Reader {
            enc,
            bits,
            bitpos: 0,
            data: &body[start + count..],
            pos: 0,
            depth: 0,
        })
    }

    fn bit(&mut self) -> Result<bool> {
        let byte = *self
            .bits
            .get(self.bitpos / 8)
            .ok_or_else(|| malformed("boolean stream exhausted"))?;
        let v = (byte >> (self.bitpos % 8)) & 1 != 0;
        self.bitpos += 1;
        Ok(v)
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.data.len() - self.pos < n {
            return Err(malformed(format!(
                "needs {n} more bytes, {} left",
                self.data.len() - self.pos
            )));
        }
        let s = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    pub fn bool(&mut self) -> Result<bool> {
        if self.enc.tight {
            self.bit()
        } else {
            Ok(self.take(1)?[0] != 0)
        }
    }

    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    pub fn i16(&mut self) -> Result<i16> {
        let b = self.take(2)?;
        Ok(i16::from_be_bytes([b[0], b[1]]))
    }

    pub fn i32(&mut self) -> Result<i32> {
        let b = self.take(4)?;
        Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    pub fn raw(&mut self, n: usize) -> Result<Vec<u8>> {
        Ok(self.take(n)?.to_vec())
    }

    pub fn i64(&mut self) -> Result<i64> {
        if !self.enc.tight {
            let b = self.take(8)?;
            return Ok(i64::from_be_bytes(b.try_into().unwrap()));
        }
        // Java reads the 2- and 4-byte forms as unsigned.
        Ok(match (self.bit()?, self.bit()?) {
            (false, false) => 0,
            (false, true) => self.i16()? as u16 as i64,
            (true, false) => self.i32()? as u32 as i64,
            (true, true) => {
                let b = self.take(8)?;
                i64::from_be_bytes(b.try_into().unwrap())
            }
        })
    }

    pub fn string(&mut self) -> Result<Option<String>> {
        if !self.bool()? {
            return Ok(None);
        }
        if self.enc.tight && self.bit()? {
            let n = self.i16()? as u16 as usize;
            return Ok(Some(utf::decode_ascii(self.take(n)?)));
        }
        let n = self.i16()? as u16 as usize;
        utf::decode(self.take(n)?).map(Some)
    }

    pub fn bytes(&mut self) -> Result<Option<Vec<u8>>> {
        if !self.bool()? {
            return Ok(None);
        }
        let n = self.i32()?;
        if n < 0 {
            return Err(malformed(format!("negative length {n}")));
        }
        Ok(Some(self.raw(n as usize)?))
    }

    pub fn nested(&mut self) -> Result<Option<DataStructure>> {
        if !self.bool()? {
            return Ok(None);
        }
        let t = self.u8()?;
        if self.enc.tight && DataStructure::type_is_marshal_aware(t) && self.bit()? {
            bail_kind!(Protocol, "OpenWire cached marshalled form is not supported");
        }
        self.enter()?;
        let ds = DataStructure::unmarshal_body(t, self);
        self.depth -= 1;
        ds.map(Some)
    }

    fn enter(&mut self) -> Result<()> {
        if self.depth == MAX_NESTING {
            return Err(malformed(format!(
                "objects nested more than {MAX_NESTING} deep"
            )));
        }
        self.depth += 1;
        Ok(())
    }

    pub fn array(&mut self) -> Result<Option<Vec<DataStructure>>> {
        if !self.bool()? {
            return Ok(None);
        }
        let n = self.i16()?;
        let mut items = Vec::new();
        for _ in 0..n.max(0) {
            if let Some(ds) = self.nested()? {
                items.push(ds);
            }
        }
        Ok(Some(items))
    }

    pub fn throwable(&mut self) -> Result<Option<BrokerError>> {
        if !self.bool()? {
            return Ok(None);
        }
        let class = self.string()?;
        let message = self.string()?;
        let mut stack = Vec::new();
        let mut cause = None;
        if self.enc.stack_traces {
            for _ in 0..self.i16()?.max(0) {
                stack.push(StackFrame {
                    class: self.string()?,
                    method: self.string()?,
                    file: self.string()?,
                    line: self.i32()?,
                });
            }
            self.enter()?;
            let c = self.throwable();
            self.depth -= 1;
            cause = c?.map(Box::new);
        }
        Ok(Some(BrokerError {
            class,
            message,
            stack,
            cause,
        }))
    }

    /// The body must be fully consumed (leftover bits in the bit stream are fine).
    pub fn done(&self, what: &str) -> Result<()> {
        if self.pos != self.data.len() {
            return Err(malformed(format!(
                "{} unexpected trailing bytes after {what}",
                self.data.len() - self.pos
            )));
        }
        Ok(())
    }
}

/// How a field type is encoded; implemented for every type used in [`crate::types`].
pub trait Field: Sized {
    fn put(&self, w: &mut Writer) -> Result<()>;
    fn get(r: &mut Reader) -> Result<Self>;
}

macro_rules! simple_field {
    ($t:ty, $w:ident, $r:ident) => {
        impl Field for $t {
            fn put(&self, w: &mut Writer) -> Result<()> {
                w.$w(*self);
                Ok(())
            }
            fn get(r: &mut Reader) -> Result<Self> {
                r.$r()
            }
        }
    };
}
simple_field!(bool, bool, bool);
simple_field!(u8, u8, u8);
simple_field!(i16, i16, i16);
simple_field!(i32, i32, i32);
simple_field!(i64, i64, i64);

impl Field for Option<String> {
    fn put(&self, w: &mut Writer) -> Result<()> {
        w.string(self.as_deref())
    }
    fn get(r: &mut Reader) -> Result<Self> {
        r.string()
    }
}

/// ByteSequence and byte[] fields.
impl Field for Option<Vec<u8>> {
    fn put(&self, w: &mut Writer) -> Result<()> {
        w.bytes(self.as_deref());
        Ok(())
    }
    fn get(r: &mut Reader) -> Result<Self> {
        r.bytes()
    }
}

/// Nested and (with the cache off) cached objects.
impl Field for Option<Box<DataStructure>> {
    fn put(&self, w: &mut Writer) -> Result<()> {
        w.nested(self.as_deref())
    }
    fn get(r: &mut Reader) -> Result<Self> {
        Ok(r.nested()?.map(Box::new))
    }
}

/// Object arrays.
impl Field for Option<Vec<DataStructure>> {
    fn put(&self, w: &mut Writer) -> Result<()> {
        w.array(self.as_deref())
    }
    fn get(r: &mut Reader) -> Result<Self> {
        r.array()
    }
}

impl Field for Option<BrokerError> {
    fn put(&self, w: &mut Writer) -> Result<()> {
        w.throwable(self.as_ref())
    }
    fn get(r: &mut Reader) -> Result<Self> {
        r.throwable()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LOOSE: Encoding = Encoding {
        tight: false,
        stack_traces: false,
    };
    const TIGHT: Encoding = Encoding {
        tight: true,
        stack_traces: false,
    };

    #[test]
    fn nesting_is_bounded() {
        let enc = Encoding {
            tight: false,
            stack_traces: true,
        };
        let chain = |n: usize| {
            let mut e = crate::types::BrokerError::default();
            for _ in 0..n {
                e = crate::types::BrokerError {
                    cause: Some(Box::new(e)),
                    ..Default::default()
                };
            }
            let mut w = Writer::new(enc);
            w.throwable(Some(&e)).unwrap();
            w.finish()
        };
        let ok = chain(MAX_NESTING - 1);
        assert!(Reader::new(enc, &ok).unwrap().throwable().is_ok());
        let deep = chain(MAX_NESTING + 5);
        let e = Reader::new(enc, &deep).unwrap().throwable().unwrap_err();
        assert!(format!("{e:#}").contains("nested more than"), "{e:#}");
    }

    #[test]
    fn boolean_stream_bit_order_and_prefixes() {
        let mut w = Writer::new(TIGHT);
        for v in [true, false, true, true, false, false, false, false, true] {
            w.bool(v);
        }
        // 9 bits = 2 bytes, least significant bit first: 0b0000_1101, 0b0000_0001.
        assert_eq!(w.finish(), vec![2, 0x0D, 0x01]);

        for (bools, prefix) in [
            (63 * 8, vec![63u8]),
            (64 * 8, vec![0xC0, 64]),
            (256 * 8, vec![0x80, 0x01, 0x00]),
        ] {
            let mut w = Writer::new(TIGHT);
            for _ in 0..bools {
                w.bool(true);
            }
            let out = w.finish();
            assert_eq!(&out[..prefix.len()], &prefix[..], "{bools} bits");
            let mut r = Reader::new(TIGHT, &out).unwrap();
            for _ in 0..bools {
                assert!(r.bool().unwrap());
            }
        }
    }

    #[test]
    fn tight_longs_use_the_smallest_unsigned_form() {
        for (v, data_len) in [
            (0i64, 0usize),
            (1, 2),
            (0xFFFF, 2),
            (0x1_0000, 4),
            (0xFFFF_FFFF, 4),
            (0x1_0000_0000, 8),
            (-1, 8),
        ] {
            let mut w = Writer::new(TIGHT);
            w.i64(v);
            let out = w.finish();
            assert_eq!(out.len(), 2 + data_len, "{v}"); // prefix byte + 1 bit byte + data
            assert_eq!(Reader::new(TIGHT, &out).unwrap().i64().unwrap(), v);
        }
        let mut w = Writer::new(LOOSE);
        w.i64(5);
        assert_eq!(w.finish(), 5i64.to_be_bytes());
    }

    #[test]
    fn strings_in_both_encodings() {
        for enc in [LOOSE, TIGHT] {
            for s in [None, Some("queue.orders"), Some("é\0€😀"), Some("")] {
                let mut w = Writer::new(enc);
                w.string(s).unwrap();
                let out = w.finish();
                assert_eq!(
                    Reader::new(enc, &out).unwrap().string().unwrap().as_deref(),
                    s,
                    "{enc:?} {s:?}"
                );
            }
        }
        // Loose: presence byte, u16 length, modified UTF-8.
        let mut w = Writer::new(LOOSE);
        w.string(Some("a\0")).unwrap();
        assert_eq!(w.finish(), vec![1, 0, 3, b'a', 0xC0, 0x80]);
        // Tight ASCII: bits (present, ascii), then u16 length and raw bytes.
        let mut w = Writer::new(TIGHT);
        w.string(Some("ab")).unwrap();
        assert_eq!(w.finish(), vec![1, 0b11, 0, 2, b'a', b'b']);
        // Tight non-ASCII: ascii bit clear, modified UTF-8.
        let mut w = Writer::new(TIGHT);
        w.string(Some("é")).unwrap();
        assert_eq!(w.finish(), vec![1, 0b01, 0, 2, 0xC3, 0xA9]);
    }

    #[test]
    fn readers_reject_truncated_input() {
        assert!(Reader::new(LOOSE, &[1, 0, 5, b'a'])
            .unwrap()
            .string()
            .is_err());
        assert!(Reader::new(LOOSE, &[1, 0xFF, 0xFF, 0xFF, 0xFF])
            .unwrap()
            .bytes()
            .is_err());
        assert!(Reader::new(TIGHT, &[3, 0]).is_err());
        let mut r = Reader::new(TIGHT, &[0]).unwrap();
        assert!(r.bool().is_err());
    }
}
