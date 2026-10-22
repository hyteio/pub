//! The AMQP 1.0 type system (OASIS AMQP 1.0, part 1): values and their encodings.
//!
//! Every encoding is decoded; encoding picks the most compact form (`uint0`,
//! `smalluint`, `str8`, `list8`, ...), as the spec recommends. Decoding is
//! bounded: compound sizes must fit the input and nesting is limited.

use pub_core::{bail_kind, err, Result};

/// Deepest nesting of lists, maps, arrays and described values accepted.
const MAX_DEPTH: usize = 32;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Ubyte(u8),
    Ushort(u16),
    Uint(u32),
    Ulong(u64),
    Byte(i8),
    Short(i16),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    /// Decimals are carried as their raw IEEE 754 decimal bytes.
    Decimal32([u8; 4]),
    Decimal64([u8; 8]),
    Decimal128([u8; 16]),
    Char(char),
    /// Milliseconds since the Unix epoch.
    Timestamp(i64),
    Uuid([u8; 16]),
    Binary(Vec<u8>),
    String(String),
    Symbol(String),
    List(Vec<Value>),
    /// Pairs in order (AMQP maps are ordered and keys may be any type).
    Map(Vec<(Value, Value)>),
    /// Values of one type.
    Array(Vec<Value>),
    Described(Box<Value>, Box<Value>),
}

impl Value {
    pub fn symbol(s: impl Into<String>) -> Value {
        Value::Symbol(s.into())
    }

    /// A described value with a numeric descriptor (`domain:code`).
    pub fn described(code: u64, value: Value) -> Value {
        Value::Described(Box::new(Value::Ulong(code)), Box::new(value))
    }

    /// The numeric descriptor, if this is a value described by one.
    pub fn descriptor_code(&self) -> Option<u64> {
        match self {
            Value::Described(d, _) => match **d {
                Value::Ulong(c) => Some(c),
                _ => None,
            },
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) | Value::Symbol(s) => Some(s),
            _ => None,
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "null",
            Value::Bool(_) => "boolean",
            Value::Ubyte(_) => "ubyte",
            Value::Ushort(_) => "ushort",
            Value::Uint(_) => "uint",
            Value::Ulong(_) => "ulong",
            Value::Byte(_) => "byte",
            Value::Short(_) => "short",
            Value::Int(_) => "int",
            Value::Long(_) => "long",
            Value::Float(_) => "float",
            Value::Double(_) => "double",
            Value::Decimal32(_) => "decimal32",
            Value::Decimal64(_) => "decimal64",
            Value::Decimal128(_) => "decimal128",
            Value::Char(_) => "char",
            Value::Timestamp(_) => "timestamp",
            Value::Uuid(_) => "uuid",
            Value::Binary(_) => "binary",
            Value::String(_) => "string",
            Value::Symbol(_) => "symbol",
            Value::List(_) => "list",
            Value::Map(_) => "map",
            Value::Array(_) => "array",
            Value::Described(..) => "described",
        }
    }

    pub fn encode(&self, out: &mut Vec<u8>) -> Result<()> {
        match self {
            Value::Null => out.push(0x40),
            Value::Bool(true) => out.push(0x41),
            Value::Bool(false) => out.push(0x42),
            Value::Ubyte(n) => out.extend_from_slice(&[0x50, *n]),
            Value::Ushort(n) => {
                out.push(0x60);
                out.extend_from_slice(&n.to_be_bytes());
            }
            Value::Uint(0) => out.push(0x43),
            Value::Uint(n) if *n <= 255 => out.extend_from_slice(&[0x52, *n as u8]),
            Value::Uint(n) => {
                out.push(0x70);
                out.extend_from_slice(&n.to_be_bytes());
            }
            Value::Ulong(0) => out.push(0x44),
            Value::Ulong(n) if *n <= 255 => out.extend_from_slice(&[0x53, *n as u8]),
            Value::Ulong(n) => {
                out.push(0x80);
                out.extend_from_slice(&n.to_be_bytes());
            }
            Value::Byte(n) => out.extend_from_slice(&[0x51, *n as u8]),
            Value::Short(n) => {
                out.push(0x61);
                out.extend_from_slice(&n.to_be_bytes());
            }
            Value::Int(n) if (-128..=127).contains(n) => {
                out.extend_from_slice(&[0x54, *n as i8 as u8])
            }
            Value::Int(n) => {
                out.push(0x71);
                out.extend_from_slice(&n.to_be_bytes());
            }
            Value::Long(n) if (-128..=127).contains(n) => {
                out.extend_from_slice(&[0x55, *n as i8 as u8])
            }
            Value::Long(n) => {
                out.push(0x81);
                out.extend_from_slice(&n.to_be_bytes());
            }
            Value::Float(f) => {
                out.push(0x72);
                out.extend_from_slice(&f.to_bits().to_be_bytes());
            }
            Value::Double(f) => {
                out.push(0x82);
                out.extend_from_slice(&f.to_bits().to_be_bytes());
            }
            Value::Decimal32(b) => {
                out.push(0x74);
                out.extend_from_slice(b);
            }
            Value::Decimal64(b) => {
                out.push(0x84);
                out.extend_from_slice(b);
            }
            Value::Decimal128(b) => {
                out.push(0x94);
                out.extend_from_slice(b);
            }
            Value::Char(c) => {
                out.push(0x73);
                out.extend_from_slice(&(*c as u32).to_be_bytes());
            }
            Value::Timestamp(t) => {
                out.push(0x83);
                out.extend_from_slice(&t.to_be_bytes());
            }
            Value::Uuid(u) => {
                out.push(0x98);
                out.extend_from_slice(u);
            }
            Value::Binary(b) => variable(out, 0xa0, 0xb0, b)?,
            Value::String(s) => variable(out, 0xa1, 0xb1, s.as_bytes())?,
            Value::Symbol(s) => {
                if !s.is_ascii() {
                    bail_kind!(Usage, "AMQP symbols are ASCII; '{s}' isn't");
                }
                variable(out, 0xa3, 0xb3, s.as_bytes())?
            }
            Value::List(items) if items.is_empty() => out.push(0x45),
            Value::List(items) => {
                let mut body = Vec::new();
                for v in items {
                    v.encode(&mut body)?;
                }
                compound(out, 0xc0, 0xd0, items.len(), &body)?;
            }
            Value::Map(pairs) => {
                let mut body = Vec::new();
                for (k, v) in pairs {
                    k.encode(&mut body)?;
                    v.encode(&mut body)?;
                }
                compound(out, 0xc1, 0xd1, pairs.len() * 2, &body)?;
            }
            Value::Array(items) => encode_array(out, items)?,
            Value::Described(d, v) => {
                out.push(0x00);
                d.encode(out)?;
                v.encode(out)?;
            }
        }
        Ok(())
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        self.encode(&mut out)?;
        Ok(out)
    }

    /// Decode one value from the start of `bytes`; returns it and the bytes used.
    pub fn decode(bytes: &[u8]) -> Result<(Value, usize)> {
        let mut r = Reader {
            b: bytes,
            pos: 0,
            budget: bytes.len().saturating_add(EXPANSION_ALLOWANCE),
        };
        let v = r.value(0)?;
        Ok((v, r.pos))
    }
}

fn len32(n: usize, what: &str) -> Result<u32> {
    u32::try_from(n).map_err(|_| err!(Usage, "{what} too large for AMQP ({n} bytes)"))
}

/// vbin / str / sym: an 8-bit length form when it fits.
fn variable(out: &mut Vec<u8>, short: u8, long: u8, b: &[u8]) -> Result<()> {
    if b.len() <= 255 {
        out.extend_from_slice(&[short, b.len() as u8]);
    } else {
        out.push(long);
        out.extend_from_slice(&len32(b.len(), "value")?.to_be_bytes());
    }
    out.extend_from_slice(b);
    Ok(())
}

/// list / map: size counts the count field plus the body.
fn compound(out: &mut Vec<u8>, short: u8, long: u8, count: usize, body: &[u8]) -> Result<()> {
    if body.len() < 255 && count <= 255 {
        out.extend_from_slice(&[short, (body.len() + 1) as u8, count as u8]);
    } else {
        out.push(long);
        out.extend_from_slice(&len32(body.len() + 4, "list")?.to_be_bytes());
        out.extend_from_slice(&len32(count, "list")?.to_be_bytes());
    }
    out.extend_from_slice(body);
    Ok(())
}

/// Arrays share one constructor: the widest form of their element type.
fn encode_array(out: &mut Vec<u8>, items: &[Value]) -> Result<()> {
    let mut body = Vec::new();
    let ctor: Vec<u8> = match items.first() {
        None => vec![0x40], // an empty array of nulls
        Some(Value::Symbol(_)) | Some(Value::String(_)) => {
            let sym = matches!(items[0], Value::Symbol(_));
            for v in items {
                let s = match (v, sym) {
                    (Value::Symbol(s), true) | (Value::String(s), false) => s,
                    _ => bail_kind!(Usage, "AMQP arrays hold one type"),
                };
                body.extend_from_slice(&len32(s.len(), "array element")?.to_be_bytes());
                body.extend_from_slice(s.as_bytes());
            }
            vec![if sym { 0xb3 } else { 0xb1 }]
        }
        Some(Value::Ulong(_)) => {
            for v in items {
                let Value::Ulong(n) = v else {
                    bail_kind!(Usage, "AMQP arrays hold one type")
                };
                body.extend_from_slice(&n.to_be_bytes());
            }
            vec![0x80]
        }
        Some(Value::Uint(_)) => {
            for v in items {
                let Value::Uint(n) = v else {
                    bail_kind!(Usage, "AMQP arrays hold one type")
                };
                body.extend_from_slice(&n.to_be_bytes());
            }
            vec![0x70]
        }
        Some(other) => bail_kind!(Usage, "pub doesn't encode arrays of {}", other.type_name()),
    };
    let count = items.len();
    let inner = ctor.len() + body.len();
    if inner < 255 && count <= 255 {
        out.extend_from_slice(&[0xe0, (inner + 1) as u8, count as u8]);
    } else {
        out.push(0xf0);
        out.extend_from_slice(&len32(inner + 4, "array")?.to_be_bytes());
        out.extend_from_slice(&len32(count, "array")?.to_be_bytes());
    }
    out.extend_from_slice(&ctor);
    out.extend_from_slice(&body);
    Ok(())
}

struct Reader<'a> {
    b: &'a [u8],
    pos: usize,
    /// What decoding may still produce beyond the input itself: array elements
    /// that take no bytes (null, true, ...) and copies of an array's descriptor
    /// are charged here, so a small input can't decode into a huge value.
    budget: usize,
}

/// Decoded size allowed beyond the input's own length.
const EXPANSION_ALLOWANCE: usize = 64 * 1024;

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|e| *e <= self.b.len())
            .ok_or_else(|| err!(Protocol, "truncated AMQP value"))?;
        let s = &self.b[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn arr<const N: usize>(&mut self) -> Result<[u8; N]> {
        Ok(self.take(N)?.try_into().unwrap())
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.arr()?))
    }

    fn value(&mut self, depth: usize) -> Result<Value> {
        if depth > MAX_DEPTH {
            bail_kind!(Protocol, "AMQP value nested more than {MAX_DEPTH} levels");
        }
        let ctor = self.u8()?;
        if ctor == 0x00 {
            let d = self.value(depth + 1)?;
            let v = self.value(depth + 1)?;
            return Ok(Value::Described(Box::new(d), Box::new(v)));
        }
        self.body(ctor, depth)
    }

    /// A value's data after its constructor.
    fn body(&mut self, ctor: u8, depth: usize) -> Result<Value> {
        Ok(match ctor {
            0x40 => Value::Null,
            0x41 => Value::Bool(true),
            0x42 => Value::Bool(false),
            0x56 => Value::Bool(self.u8()? != 0),
            0x50 => Value::Ubyte(self.u8()?),
            0x60 => Value::Ushort(u16::from_be_bytes(self.arr()?)),
            0x70 => Value::Uint(self.u32()?),
            0x52 => Value::Uint(self.u8()? as u32),
            0x43 => Value::Uint(0),
            0x80 => Value::Ulong(u64::from_be_bytes(self.arr()?)),
            0x53 => Value::Ulong(self.u8()? as u64),
            0x44 => Value::Ulong(0),
            0x51 => Value::Byte(self.u8()? as i8),
            0x61 => Value::Short(i16::from_be_bytes(self.arr()?)),
            0x71 => Value::Int(i32::from_be_bytes(self.arr()?)),
            0x54 => Value::Int(self.u8()? as i8 as i32),
            0x81 => Value::Long(i64::from_be_bytes(self.arr()?)),
            0x55 => Value::Long(self.u8()? as i8 as i64),
            0x72 => Value::Float(f32::from_bits(self.u32()?)),
            0x82 => Value::Double(f64::from_bits(u64::from_be_bytes(self.arr()?))),
            0x74 => Value::Decimal32(self.arr()?),
            0x84 => Value::Decimal64(self.arr()?),
            0x94 => Value::Decimal128(self.arr()?),
            0x73 => {
                let c = self.u32()?;
                Value::Char(
                    char::from_u32(c).ok_or_else(|| err!(Protocol, "invalid AMQP char {c:#x}"))?,
                )
            }
            0x83 => Value::Timestamp(i64::from_be_bytes(self.arr()?)),
            0x98 => Value::Uuid(self.arr()?),
            0xa0 | 0xb0 | 0xa1 | 0xb1 | 0xa3 | 0xb3 => {
                let n = if ctor & 0xf0 == 0xa0 {
                    self.u8()? as usize
                } else {
                    self.u32()? as usize
                };
                let bytes = self.take(n)?;
                match ctor {
                    0xa0 | 0xb0 => Value::Binary(bytes.to_vec()),
                    0xa1 | 0xb1 => Value::String(
                        String::from_utf8(bytes.to_vec())
                            .map_err(|_| err!(Protocol, "AMQP string is not UTF-8"))?,
                    ),
                    _ => Value::Symbol(
                        String::from_utf8(bytes.to_vec())
                            .ok()
                            .filter(|s| s.is_ascii())
                            .ok_or_else(|| err!(Protocol, "AMQP symbol is not ASCII"))?,
                    ),
                }
            }
            0x45 => Value::List(Vec::new()),
            0xc0 | 0xd0 | 0xc1 | 0xd1 => {
                let (size, count) = if ctor & 0xf0 == 0xc0 {
                    (self.u8()? as usize, self.u8()? as usize)
                } else {
                    (self.u32()? as usize, self.u32()? as usize)
                };
                let width = if ctor & 0xf0 == 0xc0 { 1 } else { 4 };
                let end = (self.pos + size)
                    .checked_sub(width)
                    .filter(|e| *e <= self.b.len() && size >= width)
                    .ok_or_else(|| err!(Protocol, "AMQP compound size {size} exceeds the frame"))?;
                // Every element takes at least a byte.
                if count > end - self.pos {
                    bail_kind!(
                        Protocol,
                        "AMQP compound claims {count} elements in {} bytes",
                        end - self.pos
                    );
                }
                let mut items = Vec::with_capacity(count);
                for _ in 0..count {
                    items.push(self.value(depth + 1)?);
                }
                if self.pos != end {
                    bail_kind!(Protocol, "AMQP compound size doesn't match its contents");
                }
                if ctor & 0x0f == 0x01 {
                    if count % 2 != 0 {
                        bail_kind!(Protocol, "AMQP map with an odd number of elements");
                    }
                    let mut pairs = Vec::with_capacity(count / 2);
                    let mut it = items.into_iter();
                    while let (Some(k), Some(v)) = (it.next(), it.next()) {
                        pairs.push((k, v));
                    }
                    Value::Map(pairs)
                } else {
                    Value::List(items)
                }
            }
            0xe0 | 0xf0 => {
                let (size, count) = if ctor == 0xe0 {
                    (self.u8()? as usize, self.u8()? as usize)
                } else {
                    (self.u32()? as usize, self.u32()? as usize)
                };
                let width = if ctor == 0xe0 { 1 } else { 4 };
                let end = (self.pos + size)
                    .checked_sub(width)
                    .filter(|e| *e <= self.b.len() && size >= width)
                    .ok_or_else(|| err!(Protocol, "AMQP array size {size} exceeds the frame"))?;
                // The element constructor, possibly described.
                let mut elem = self.u8()?;
                let descriptor_start = self.pos;
                let descriptor = if elem == 0x00 {
                    let d = self.value(depth + 1)?;
                    elem = self.u8()?;
                    Some(d)
                } else {
                    None
                };
                if self.pos > end {
                    bail_kind!(
                        Protocol,
                        "AMQP array size {size} is smaller than its header"
                    );
                }
                let fixed_zero = matches!(elem, 0x40..=0x45);
                if !fixed_zero && count > end - self.pos {
                    bail_kind!(
                        Protocol,
                        "AMQP array claims {count} elements in {} bytes",
                        end - self.pos
                    );
                }
                if count > 1 << 20 {
                    bail_kind!(Protocol, "AMQP array of {count} elements");
                }
                // Each element costs a copy of the descriptor, and nothing on
                // the wire when it is zero-width.
                let per_element =
                    (self.pos - descriptor_start).saturating_sub(1) + usize::from(fixed_zero);
                let cost = per_element.saturating_mul(count);
                if cost > self.budget {
                    bail_kind!(
                        Protocol,
                        "AMQP array of {count} elements would decode far larger than its {} bytes",
                        self.b.len()
                    );
                }
                self.budget -= cost;
                let mut items = Vec::with_capacity(count.min(4096));
                for _ in 0..count {
                    let v = self.body(elem, depth + 1)?;
                    items.push(match &descriptor {
                        Some(d) => Value::Described(Box::new(d.clone()), Box::new(v)),
                        None => v,
                    });
                }
                if self.pos != end {
                    bail_kind!(Protocol, "AMQP array size doesn't match its contents");
                }
                Value::Array(items)
            }
            other => bail_kind!(Protocol, "unknown AMQP type constructor {other:#04x}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(v: &Value) -> Vec<u8> {
        v.to_bytes().unwrap()
    }

    fn round(v: Value) {
        let b = bytes(&v);
        let (back, n) = Value::decode(&b).unwrap();
        assert_eq!(n, b.len(), "{v:?}");
        assert_eq!(back, v);
    }

    #[test]
    fn compact_encodings() {
        assert_eq!(bytes(&Value::Uint(0)), [0x43]);
        assert_eq!(bytes(&Value::Uint(7)), [0x52, 7]);
        assert_eq!(bytes(&Value::Uint(256)), [0x70, 0, 0, 1, 0]);
        assert_eq!(bytes(&Value::Ulong(0x10)), [0x53, 0x10]);
        assert_eq!(bytes(&Value::Int(-1)), [0x54, 0xff]);
        assert_eq!(
            bytes(&Value::Long(1000)),
            [0x81, 0, 0, 0, 0, 0, 0, 0x03, 0xe8]
        );
        assert_eq!(bytes(&Value::String("ab".into())), [0xa1, 2, b'a', b'b']);
        assert_eq!(
            bytes(&Value::symbol("PLAIN")),
            [0xa3, 5, b'P', b'L', b'A', b'I', b'N']
        );
        assert_eq!(bytes(&Value::List(vec![])), [0x45]);
        assert_eq!(
            bytes(&Value::List(vec![Value::Null, Value::Bool(true)])),
            [0xc0, 3, 2, 0x40, 0x41]
        );
        // A described list: open with just a container id.
        assert_eq!(
            bytes(&Value::described(
                0x10,
                Value::List(vec![Value::String("c".into())])
            )),
            [0x00, 0x53, 0x10, 0xc0, 4, 1, 0xa1, 1, b'c']
        );
        assert_eq!(
            bytes(&Value::Array(vec![Value::symbol("a"), Value::symbol("bc")])),
            [0xe0, 13, 2, 0xb3, 0, 0, 0, 1, b'a', 0, 0, 0, 2, b'b', b'c']
        );
        let long = "x".repeat(300);
        assert_eq!(
            bytes(&Value::String(long.clone()))[..5],
            [0xb1, 0, 0, 1, 44]
        );
    }

    #[test]
    fn round_trips() {
        for v in [
            Value::Null,
            Value::Bool(false),
            Value::Ubyte(200),
            Value::Ushort(65535),
            Value::Uint(u32::MAX),
            Value::Ulong(u64::MAX),
            Value::Byte(-5),
            Value::Short(-300),
            Value::Int(i32::MIN),
            Value::Long(i64::MIN),
            Value::Float(1.5),
            Value::Double(-0.25),
            Value::Decimal64([1; 8]),
            Value::Char('😀'),
            Value::Timestamp(1790998128281),
            Value::Uuid([7; 16]),
            Value::Binary(vec![0, 255]),
            Value::Binary(vec![1; 70000]),
            Value::String("héllo".into()),
            Value::Map(vec![
                (Value::symbol("k"), Value::Int(1)),
                (Value::String("s".into()), Value::Null),
            ]),
            Value::List((0..300).map(Value::Uint).collect()),
            Value::Array(vec![Value::Ulong(1), Value::Ulong(2)]),
            Value::Array(vec![]),
            Value::described(0x77, Value::String("body".into())),
            Value::Described(
                Box::new(Value::symbol("apache.org:selector-filter:string")),
                Box::new(Value::String("a = 1".into())),
            ),
        ] {
            round(v);
        }
    }

    #[test]
    fn reads_other_encodings() {
        // Non-compact forms other implementations send.
        assert_eq!(
            Value::decode(&[0x70, 0, 0, 0, 5]).unwrap().0,
            Value::Uint(5)
        );
        assert_eq!(Value::decode(&[0x56, 1]).unwrap().0, Value::Bool(true));
        assert_eq!(
            Value::decode(&[0xd0, 0, 0, 0, 5, 0, 0, 0, 1, 0x40])
                .unwrap()
                .0,
            Value::List(vec![Value::Null])
        );
        assert_eq!(
            Value::decode(&[0xb3, 0, 0, 0, 1, b'x']).unwrap().0,
            Value::symbol("x")
        );
        // An array of described values.
        let arr = [0xe0, 7, 2, 0x00, 0x53, 0x01, 0x50, 9, 8];
        assert_eq!(
            Value::decode(&arr).unwrap().0,
            Value::Array(vec![
                Value::described(1, Value::Ubyte(9)),
                Value::described(1, Value::Ubyte(8)),
            ])
        );
    }

    #[test]
    fn rejects_bad_input() {
        for bad in [
            &[0xa1, 5, b'a'][..],      // truncated string
            &[0xc0, 200, 1, 0x40],     // size beyond input
            &[0xc0, 2, 5, 0x40],       // more elements than bytes
            &[0xc1, 2, 1, 0x40],       // odd map
            &[0xa1, 1, 0xff],          // not UTF-8
            &[0x01],                   // unknown constructor
            &[0xc0, 3, 1, 0x40, 0x40], // size doesn't match contents
        ] {
            assert!(Value::decode(bad).is_err(), "{bad:02x?}");
        }
        // Deep nesting is refused, not a stack overflow.
        let mut deep = vec![];
        for _ in 0..100 {
            deep.extend_from_slice(&[0x00, 0x53, 0x01]);
        }
        deep.push(0x40);
        assert!(Value::decode(&deep).is_err());
        assert!(Value::symbol("é").to_bytes().is_err());
    }

    #[test]
    fn arrays_cannot_decode_far_larger_than_their_input() {
        // array32 of 2^18 nulls, each described by a 4 KiB binary.
        let count: u32 = 1 << 18;
        let descriptor = Value::Binary(vec![0; 4096]).to_bytes().unwrap();
        let mut body = vec![0x00];
        body.extend_from_slice(&descriptor);
        body.push(0x40);
        let mut b = vec![0xf0];
        b.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
        b.extend_from_slice(&count.to_be_bytes());
        b.extend_from_slice(&body);
        let e = Value::decode(&b).unwrap_err();
        assert!(format!("{e:#}").contains("far larger"), "{e:#}");
        // A plain array of a million nulls in a few bytes, likewise.
        let mut b = vec![0xf0, 0, 0, 0, 5];
        b.extend_from_slice(&(1u32 << 20).to_be_bytes());
        b.push(0x40);
        assert!(Value::decode(&b).is_err());
        // An array whose size is smaller than its own header.
        assert!(Value::decode(&[0xe0, 1, 1, 0x50, 0x00]).is_err());
        // Small arrays of nulls are still fine.
        let (v, _) = Value::decode(&[0xe0, 2, 3, 0x40]).unwrap();
        assert_eq!(v, Value::Array(vec![Value::Null; 3]));
    }
}
