//! ActiveMQ's typed "primitive map" (`util/MarshallingSupport`), used for
//! message properties (`marshalledProperties`) and WireFormatInfo options,
//! plus the TextMessage body encoding.
//!
//! Map: `i32 count` (−1 = null), then per entry `writeUTF name`, `u8 type`, value.

use pub_core::{bail_kind, err, Result};

use crate::utf;

#[derive(Debug, Clone, PartialEq)]
pub enum Primitive {
    Null,
    Bool(bool),
    Byte(i8),
    /// A Java `char` (one UTF-16 unit).
    Char(u16),
    Short(i16),
    Int(i32),
    Long(i64),
    Double(f64),
    Float(f32),
    String(String),
    Bytes(Vec<u8>),
    Map(Vec<(String, Primitive)>),
    List(Vec<Primitive>),
}

const NULL: u8 = 0;
const BOOLEAN: u8 = 1;
const BYTE: u8 = 2;
const CHAR: u8 = 3;
const SHORT: u8 = 4;
const INTEGER: u8 = 5;
const LONG: u8 = 6;
const DOUBLE: u8 = 7;
const FLOAT: u8 = 8;
const STRING: u8 = 9;
const BYTE_ARRAY: u8 = 10;
const MAP: u8 = 11;
const LIST: u8 = 12;
const BIG_STRING: u8 = 13;

/// Java switches to BIG_STRING at `length() >= Short.MAX_VALUE / 4` UTF-16 units.
const BIG_STRING_UNITS: usize = (i16::MAX / 4) as usize;

fn write_utf(out: &mut Vec<u8>, s: &str) -> Result<()> {
    let b = utf::encode(s);
    if b.len() > u16::MAX as usize {
        bail_kind!(Usage, "property name of {} bytes is too long", b.len());
    }
    out.extend_from_slice(&(b.len() as u16).to_be_bytes());
    out.extend_from_slice(&b);
    Ok(())
}

/// `MarshallingSupport.writeUTF8`: i32 length + modified UTF-8.
pub fn encode_text(s: &str) -> Vec<u8> {
    let b = utf::encode(s);
    let mut out = Vec::with_capacity(4 + b.len());
    out.extend_from_slice(&(b.len() as i32).to_be_bytes());
    out.extend_from_slice(&b);
    out
}

pub fn encode_map(map: &[(String, Primitive)]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    put_map(&mut out, map)?;
    Ok(out)
}

fn put_map(out: &mut Vec<u8>, map: &[(String, Primitive)]) -> Result<()> {
    out.extend_from_slice(&(map.len() as i32).to_be_bytes());
    for (k, v) in map {
        write_utf(out, k)?;
        put(out, v)?;
    }
    Ok(())
}

fn put(out: &mut Vec<u8>, v: &Primitive) -> Result<()> {
    match v {
        Primitive::Null => out.push(NULL),
        Primitive::Bool(b) => out.extend_from_slice(&[BOOLEAN, u8::from(*b)]),
        Primitive::Byte(b) => out.extend_from_slice(&[BYTE, *b as u8]),
        Primitive::Char(c) => {
            out.push(CHAR);
            out.extend_from_slice(&c.to_be_bytes());
        }
        Primitive::Short(n) => {
            out.push(SHORT);
            out.extend_from_slice(&n.to_be_bytes());
        }
        Primitive::Int(n) => {
            out.push(INTEGER);
            out.extend_from_slice(&n.to_be_bytes());
        }
        Primitive::Long(n) => {
            out.push(LONG);
            out.extend_from_slice(&n.to_be_bytes());
        }
        Primitive::Double(n) => {
            out.push(DOUBLE);
            out.extend_from_slice(&n.to_be_bytes());
        }
        Primitive::Float(n) => {
            out.push(FLOAT);
            out.extend_from_slice(&n.to_be_bytes());
        }
        Primitive::String(s) if s.encode_utf16().count() < BIG_STRING_UNITS => {
            out.push(STRING);
            write_utf(out, s)?;
        }
        Primitive::String(s) => {
            out.push(BIG_STRING);
            out.extend_from_slice(&encode_text(s));
        }
        Primitive::Bytes(b) => {
            out.push(BYTE_ARRAY);
            out.extend_from_slice(&(b.len() as i32).to_be_bytes());
            out.extend_from_slice(b);
        }
        Primitive::Map(m) => {
            out.push(MAP);
            put_map(out, m)?;
        }
        Primitive::List(items) => {
            out.push(LIST);
            out.extend_from_slice(&(items.len() as i32).to_be_bytes());
            for i in items {
                put(out, i)?;
            }
        }
    }
    Ok(())
}

struct Cursor<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.b.len() - self.pos < n {
            bail_kind!(Protocol, "truncated OpenWire property data");
        }
        let s = &self.b[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        Ok(self.take(N)?.try_into().unwrap())
    }
    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.array()?))
    }
    fn utf(&mut self) -> Result<String> {
        let n = u16::from_be_bytes(self.array()?) as usize;
        utf::decode(self.take(n)?)
    }
    fn utf8_long(&mut self) -> Result<Option<String>> {
        let n = self.i32()?;
        if n < 0 {
            return Ok(None);
        }
        utf::decode(self.take(n as usize)?).map(Some)
    }
}

/// Decode a marshalled map; `None` for a null map (count −1).
pub fn decode_map(bytes: &[u8]) -> Result<Option<Vec<(String, Primitive)>>> {
    let mut c = Cursor { b: bytes, pos: 0 };
    get_map(&mut c, 0)
}

const MAX_DEPTH: usize = 32;

fn get_map(c: &mut Cursor, depth: usize) -> Result<Option<Vec<(String, Primitive)>>> {
    let n = c.i32()?;
    if n < 0 {
        return Ok(None);
    }
    let mut out = Vec::new();
    for _ in 0..n {
        let k = c.utf()?;
        out.push((k, get(c, depth)?));
    }
    Ok(Some(out))
}

fn get(c: &mut Cursor, depth: usize) -> Result<Primitive> {
    if depth > MAX_DEPTH {
        bail_kind!(Protocol, "OpenWire property maps nested too deeply");
    }
    let t = c.take(1)?[0];
    Ok(match t {
        NULL => Primitive::Null,
        BOOLEAN => Primitive::Bool(c.take(1)?[0] != 0),
        BYTE => Primitive::Byte(c.take(1)?[0] as i8),
        CHAR => Primitive::Char(u16::from_be_bytes(c.array()?)),
        SHORT => Primitive::Short(i16::from_be_bytes(c.array()?)),
        INTEGER => Primitive::Int(c.i32()?),
        LONG => Primitive::Long(i64::from_be_bytes(c.array()?)),
        DOUBLE => Primitive::Double(f64::from_be_bytes(c.array()?)),
        FLOAT => Primitive::Float(f32::from_be_bytes(c.array()?)),
        STRING => Primitive::String(c.utf()?),
        BIG_STRING => Primitive::String(c.utf8_long()?.unwrap_or_default()),
        BYTE_ARRAY => {
            let n = c.i32()?;
            if n < 0 {
                bail_kind!(
                    Protocol,
                    "negative byte array length in OpenWire properties"
                );
            }
            Primitive::Bytes(c.take(n as usize)?.to_vec())
        }
        MAP => Primitive::Map(get_map(c, depth + 1)?.unwrap_or_default()),
        LIST => {
            let n = c.i32()?;
            let mut items = Vec::new();
            for _ in 0..n.max(0) {
                items.push(get(c, depth + 1)?);
            }
            Primitive::List(items)
        }
        other => return Err(err!(Protocol, "unknown OpenWire property type {other}")),
    })
}

/// Decode a TextMessage body (`writeUTF8`); `None` for a null text.
pub fn decode_text(bytes: &[u8]) -> Result<Option<String>> {
    Cursor { b: bytes, pos: 0 }.utf8_long()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_round_trip_every_type() {
        let map = vec![
            ("null".to_string(), Primitive::Null),
            ("b".into(), Primitive::Bool(true)),
            ("i8".into(), Primitive::Byte(-3)),
            ("c".into(), Primitive::Char(0x20AC)),
            ("s".into(), Primitive::Short(-2)),
            ("i".into(), Primitive::Int(42)),
            ("l".into(), Primitive::Long(1 << 40)),
            ("d".into(), Primitive::Double(1.5)),
            ("f".into(), Primitive::Float(-0.25)),
            ("str".into(), Primitive::String("é\0x".into())),
            (
                "big".into(),
                Primitive::String("y".repeat(BIG_STRING_UNITS)),
            ),
            ("bytes".into(), Primitive::Bytes(vec![0, 255])),
            (
                "map".into(),
                Primitive::Map(vec![("k".into(), Primitive::Int(1))]),
            ),
            (
                "list".into(),
                Primitive::List(vec![Primitive::Long(7), Primitive::String("z".into())]),
            ),
        ];
        let bytes = encode_map(&map).unwrap();
        assert_eq!(decode_map(&bytes).unwrap(), Some(map));
        assert_eq!(decode_map(&(-1i32).to_be_bytes()).unwrap(), None);
    }

    #[test]
    fn exact_bytes() {
        // {"n": int 5}: count 1, name "n", INTEGER, 5
        let bytes = encode_map(&[("n".into(), Primitive::Int(5))]).unwrap();
        assert_eq!(bytes, [0, 0, 0, 1, 0, 1, b'n', 5, 0, 0, 0, 5]);
        // The big-string cutoff is in UTF-16 units: 8190 stays STRING, 8191 becomes BIG_STRING.
        let small = encode_map(&[("k".into(), Primitive::String("a".repeat(8190)))]).unwrap();
        let big = encode_map(&[("k".into(), Primitive::String("a".repeat(8191)))]).unwrap();
        assert_eq!((small[7], big[7]), (STRING, BIG_STRING));
        // TextMessage body: i32 length + modified UTF-8.
        assert_eq!(encode_text("a\0"), [0, 0, 0, 3, b'a', 0xC0, 0x80]);
        assert_eq!(
            decode_text(&encode_text("héllo")).unwrap().as_deref(),
            Some("héllo")
        );
        assert_eq!(decode_text(&(-1i32).to_be_bytes()).unwrap(), None);
    }

    #[test]
    fn rejects_bad_input() {
        assert!(decode_map(&[0, 0, 0, 1, 0, 1, b'n', 99]).is_err());
        assert!(decode_map(&[0, 0, 0, 1, 0, 1, b'n', INTEGER, 0]).is_err());
    }
}
