//! AMQP 0-9-1 field encodings (spec section 4.2.5, with RabbitMQ's errata for
//! field tables) and a reader/writer that packs consecutive bits.

use pub_core::{bail_kind, err, Result};

/// A field-table value. Type tags are RabbitMQ's (its errata differ from the
/// 0-9-1 spec for `s` and others; every client follows RabbitMQ).
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    Bool(bool),
    I8(i8),
    U8(u8),
    I16(i16),
    U16(u16),
    I32(i32),
    U32(u32),
    I64(i64),
    F32(f32),
    F64(f64),
    /// Scale, unscaled value.
    Decimal(u8, u32),
    /// `S`: long string (UTF-8 in practice, bytes on the wire).
    LongStr(Vec<u8>),
    /// `x`: byte array.
    Bytes(Vec<u8>),
    Array(Vec<FieldValue>),
    /// Seconds since the epoch.
    Timestamp(u64),
    Table(Table),
    Void,
}

pub type Table = Vec<(String, FieldValue)>;

/// Find a key in a table.
pub fn get<'a>(t: &'a Table, key: &str) -> Option<&'a FieldValue> {
    t.iter().find(|(k, _)| k == key).map(|(_, v)| v)
}

/// Deepest nesting of tables and arrays accepted.
const MAX_DEPTH: usize = 32;

#[derive(Default)]
pub struct Writer {
    pub buf: Vec<u8>,
    /// The octet bits are being packed into, and how many it holds.
    bits: Option<(usize, u8)>,
}

impl Writer {
    pub fn new() -> Self {
        Self::default()
    }

    fn plain(&mut self) -> &mut Vec<u8> {
        self.bits = None;
        &mut self.buf
    }

    pub fn octet(&mut self, v: u8) {
        self.plain().push(v);
    }

    pub fn short(&mut self, v: u16) {
        self.plain().extend_from_slice(&v.to_be_bytes());
    }

    pub fn long(&mut self, v: u32) {
        self.plain().extend_from_slice(&v.to_be_bytes());
    }

    pub fn longlong(&mut self, v: u64) {
        self.plain().extend_from_slice(&v.to_be_bytes());
    }

    /// Consecutive bits share octets, least significant bit first.
    pub fn bit(&mut self, v: bool) {
        let (pos, n) = match self.bits {
            Some((pos, n)) if n < 8 => (pos, n),
            _ => {
                self.buf.push(0);
                (self.buf.len() - 1, 0)
            }
        };
        if v {
            self.buf[pos] |= 1 << n;
        }
        self.bits = Some((pos, n + 1));
    }

    pub fn shortstr(&mut self, s: &str) -> Result<()> {
        let n = u8::try_from(s.len()).map_err(|_| {
            err!(
                Usage,
                "'{}…' is longer than 255 bytes (an AMQP short string)",
                s.chars().take(20).collect::<String>()
            )
        })?;
        let b = self.plain();
        b.push(n);
        b.extend_from_slice(s.as_bytes());
        Ok(())
    }

    pub fn longstr(&mut self, s: &[u8]) -> Result<()> {
        let n = u32::try_from(s.len()).map_err(|_| err!(Usage, "value too large for AMQP"))?;
        let b = self.plain();
        b.extend_from_slice(&n.to_be_bytes());
        b.extend_from_slice(s);
        Ok(())
    }

    pub fn table(&mut self, t: &Table) -> Result<()> {
        let mut inner = Writer::new();
        for (k, v) in t {
            inner.shortstr(k)?;
            inner.value(v)?;
        }
        self.longstr(&inner.buf)
    }

    fn value(&mut self, v: &FieldValue) -> Result<()> {
        match v {
            FieldValue::Bool(b) => {
                self.octet(b't');
                self.octet(*b as u8);
            }
            FieldValue::I8(n) => {
                self.octet(b'b');
                self.octet(*n as u8);
            }
            FieldValue::U8(n) => {
                self.octet(b'B');
                self.octet(*n);
            }
            FieldValue::I16(n) => {
                self.octet(b's');
                self.short(*n as u16);
            }
            FieldValue::U16(n) => {
                self.octet(b'u');
                self.short(*n);
            }
            FieldValue::I32(n) => {
                self.octet(b'I');
                self.long(*n as u32);
            }
            FieldValue::U32(n) => {
                self.octet(b'i');
                self.long(*n);
            }
            FieldValue::I64(n) => {
                self.octet(b'l');
                self.longlong(*n as u64);
            }
            FieldValue::F32(f) => {
                self.octet(b'f');
                self.long(f.to_bits());
            }
            FieldValue::F64(f) => {
                self.octet(b'd');
                self.longlong(f.to_bits());
            }
            FieldValue::Decimal(scale, v) => {
                self.octet(b'D');
                self.octet(*scale);
                self.long(*v);
            }
            FieldValue::LongStr(s) => {
                self.octet(b'S');
                self.longstr(s)?;
            }
            FieldValue::Bytes(s) => {
                self.octet(b'x');
                self.longstr(s)?;
            }
            FieldValue::Array(items) => {
                self.octet(b'A');
                let mut inner = Writer::new();
                for i in items {
                    inner.value(i)?;
                }
                self.longstr(&inner.buf)?;
            }
            FieldValue::Timestamp(t) => {
                self.octet(b'T');
                self.longlong(*t);
            }
            FieldValue::Table(t) => {
                self.octet(b'F');
                self.table(t)?;
            }
            FieldValue::Void => self.octet(b'V'),
        }
        Ok(())
    }
}

pub struct Reader<'a> {
    b: &'a [u8],
    pos: usize,
    /// The octet bits are being read from, and how many were read.
    bits: Option<(u8, u8)>,
}

impl<'a> Reader<'a> {
    pub fn new(b: &'a [u8]) -> Self {
        Reader {
            b,
            pos: 0,
            bits: None,
        }
    }

    pub fn done(&self) -> bool {
        self.pos == self.b.len()
    }

    pub fn rest(&self) -> &'a [u8] {
        &self.b[self.pos..]
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        self.bits = None;
        let end = self
            .pos
            .checked_add(n)
            .filter(|e| *e <= self.b.len())
            .ok_or_else(|| err!(Protocol, "truncated AMQP 0-9-1 frame"))?;
        let s = &self.b[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    pub fn octet(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    pub fn short(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }

    pub fn long(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    pub fn longlong(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    pub fn bit(&mut self) -> Result<bool> {
        let (octet, n) = match self.bits {
            Some((o, n)) if n < 8 => (o, n),
            _ => (self.octet()?, 0),
        };
        self.bits = Some((octet, n + 1));
        Ok(octet & (1 << n) != 0)
    }

    pub fn shortstr(&mut self) -> Result<String> {
        let n = self.octet()? as usize;
        let b = self.take(n)?;
        String::from_utf8(b.to_vec()).map_err(|_| err!(Protocol, "AMQP short string is not UTF-8"))
    }

    pub fn longstr(&mut self) -> Result<Vec<u8>> {
        let n = self.long()? as usize;
        Ok(self.take(n)?.to_vec())
    }

    pub fn table(&mut self) -> Result<Table> {
        self.table_at(0)
    }

    fn table_at(&mut self, depth: usize) -> Result<Table> {
        if depth > MAX_DEPTH {
            bail_kind!(
                Protocol,
                "AMQP field table nested more than {MAX_DEPTH} levels"
            );
        }
        let n = self.long()? as usize;
        let mut inner = Reader::new(self.take(n)?);
        let mut t = Vec::new();
        while !inner.done() {
            let k = inner.shortstr()?;
            let v = inner.value(depth + 1)?;
            t.push((k, v));
        }
        Ok(t)
    }

    fn value(&mut self, depth: usize) -> Result<FieldValue> {
        if depth > MAX_DEPTH {
            bail_kind!(
                Protocol,
                "AMQP field value nested more than {MAX_DEPTH} levels"
            );
        }
        Ok(match self.octet()? {
            b't' => FieldValue::Bool(self.octet()? != 0),
            b'b' => FieldValue::I8(self.octet()? as i8),
            b'B' => FieldValue::U8(self.octet()?),
            b's' => FieldValue::I16(self.short()? as i16),
            b'u' => FieldValue::U16(self.short()?),
            b'I' => FieldValue::I32(self.long()? as i32),
            b'i' => FieldValue::U32(self.long()?),
            b'l' | b'L' => FieldValue::I64(self.longlong()? as i64),
            b'f' => FieldValue::F32(f32::from_bits(self.long()?)),
            b'd' => FieldValue::F64(f64::from_bits(self.longlong()?)),
            b'D' => FieldValue::Decimal(self.octet()?, self.long()?),
            b'S' => FieldValue::LongStr(self.longstr()?),
            b'x' => FieldValue::Bytes(self.longstr()?),
            b'A' => {
                let n = self.long()? as usize;
                let mut inner = Reader::new(self.take(n)?);
                let mut items = Vec::new();
                while !inner.done() {
                    items.push(inner.value(depth + 1)?);
                }
                FieldValue::Array(items)
            }
            b'T' => FieldValue::Timestamp(self.longlong()?),
            b'F' => FieldValue::Table(self.table_at(depth + 1)?),
            b'V' => FieldValue::Void,
            other => bail_kind!(Protocol, "unknown AMQP field type '{}'", other as char),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bits_pack_lsb_first_and_reset() {
        let mut w = Writer::new();
        w.bit(true);
        w.bit(false);
        w.bit(true);
        w.short(7);
        w.bit(true);
        assert_eq!(w.buf, [0b101, 0, 7, 1]);
        let mut r = Reader::new(&w.buf);
        assert_eq!(
            (r.bit().unwrap(), r.bit().unwrap(), r.bit().unwrap()),
            (true, false, true)
        );
        assert_eq!(r.short().unwrap(), 7);
        assert!(r.bit().unwrap());
        assert!(r.done());
        // Nine bits take two octets.
        let mut w = Writer::new();
        for _ in 0..9 {
            w.bit(true);
        }
        assert_eq!(w.buf, [0xff, 1]);
    }

    #[test]
    fn tables_round_trip() {
        let t: Table = vec![
            ("b".into(), FieldValue::Bool(true)),
            ("i8".into(), FieldValue::I8(-1)),
            ("s".into(), FieldValue::I16(-300)),
            ("i".into(), FieldValue::I32(42)),
            ("l".into(), FieldValue::I64(1 << 40)),
            ("f".into(), FieldValue::F32(0.5)),
            ("d".into(), FieldValue::F64(2.5)),
            ("str".into(), FieldValue::LongStr(b"x".to_vec())),
            ("x".into(), FieldValue::Bytes(vec![0, 255])),
            ("t".into(), FieldValue::Timestamp(1_790_000_000)),
            (
                "a".into(),
                FieldValue::Array(vec![FieldValue::I32(1), FieldValue::LongStr(b"y".to_vec())]),
            ),
            (
                "nested".into(),
                FieldValue::Table(vec![("k".into(), FieldValue::Void)]),
            ),
        ];
        let mut w = Writer::new();
        w.table(&t).unwrap();
        assert_eq!(Reader::new(&w.buf).table().unwrap(), t);
        let mut w = Writer::new();
        w.table(&vec![("k".into(), FieldValue::LongStr(b"v".to_vec()))])
            .unwrap();
        assert_eq!(w.buf, [0, 0, 0, 8, 1, b'k', b'S', 0, 0, 0, 1, b'v']);
    }

    #[test]
    fn rejects_bad_input() {
        assert!(Reader::new(&[0, 0, 0, 9, 1]).table().is_err()); // longer than the input
        assert!(Reader::new(&[0, 0, 0, 3, 1, b'k', b'?']).table().is_err()); // unknown type
        assert!(Reader::new(&[2, 0xff, 0xfe]).shortstr().is_err()); // not UTF-8
        assert!(Writer::new().shortstr(&"x".repeat(256)).is_err());
    }
}
