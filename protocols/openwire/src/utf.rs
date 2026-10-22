//! Strings on the wire. Java writes "modified UTF-8" (`DataOutput.writeUTF`,
//! ActiveMQ's `MarshallingSupport.writeUTF8`): like UTF-8 except that U+0000 is
//! two bytes (`C0 80`) and characters outside the BMP are written as their two
//! UTF-16 surrogates, three bytes each.
//!
//! Reading accepts that and standard UTF-8 alike (4-byte sequences, a raw
//! NUL), so messages from non-Java clients decode too. Writing still produces
//! modified UTF-8, which today's Java clients require (see docs/src/development/backlog.md).

use pub_core::{bail_kind, Result};

/// Encode a string; the caller writes the length prefix (u16 or i32).
pub fn encode(s: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(s.len());
    for unit in s.encode_utf16() {
        let c = unit as u32;
        match c {
            0x0001..=0x007F => out.push(c as u8),
            0x0000 | 0x0080..=0x07FF => {
                out.push(0xC0 | ((c >> 6) & 0x1F) as u8);
                out.push(0x80 | (c & 0x3F) as u8);
            }
            _ => {
                out.push(0xE0 | ((c >> 12) & 0x0F) as u8);
                out.push(0x80 | ((c >> 6) & 0x3F) as u8);
                out.push(0x80 | (c & 0x3F) as u8);
            }
        }
    }
    out
}

/// True when every UTF-16 unit is in 1..=127 — tight encoding then writes the
/// string as raw ASCII bytes instead of modified UTF-8.
pub fn is_ascii_only(s: &str) -> bool {
    s.bytes().all(|b| (1..=0x7F).contains(&b))
}

/// Decode modified UTF-8 or standard UTF-8 (or a mix of the two).
pub fn decode(bytes: &[u8]) -> Result<String> {
    let mut units: Vec<u16> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    let bad = |i: usize| pub_core::err!(Protocol, "malformed modified UTF-8 at byte {i}");
    while i < bytes.len() {
        let b = bytes[i];
        if b & 0x80 == 0 {
            units.push(b as u16);
            i += 1;
        } else if b & 0xE0 == 0xC0 {
            let b2 = *bytes.get(i + 1).ok_or_else(|| bad(i))?;
            if b2 & 0xC0 != 0x80 {
                return Err(bad(i));
            }
            units.push((((b & 0x1F) as u16) << 6) | (b2 & 0x3F) as u16);
            i += 2;
        } else if b & 0xF0 == 0xE0 {
            let (b2, b3) = (
                *bytes.get(i + 1).ok_or_else(|| bad(i))?,
                *bytes.get(i + 2).ok_or_else(|| bad(i))?,
            );
            if b2 & 0xC0 != 0x80 || b3 & 0xC0 != 0x80 {
                return Err(bad(i));
            }
            units.push(
                (((b & 0x0F) as u16) << 12) | (((b2 & 0x3F) as u16) << 6) | (b3 & 0x3F) as u16,
            );
            i += 3;
        } else if b & 0xF8 == 0xF0 {
            // Standard UTF-8 for a character outside the BMP.
            let tail = bytes.get(i + 1..i + 4).ok_or_else(|| bad(i))?;
            if tail.iter().any(|t| t & 0xC0 != 0x80) {
                return Err(bad(i));
            }
            let c = (((b & 0x07) as u32) << 18)
                | (((tail[0] & 0x3F) as u32) << 12)
                | (((tail[1] & 0x3F) as u32) << 6)
                | (tail[2] & 0x3F) as u32;
            let c = char::from_u32(c)
                .filter(|c| (*c as u32) >= 0x10000)
                .ok_or_else(|| bad(i))?;
            let mut pair = [0u16; 2];
            units.extend_from_slice(c.encode_utf16(&mut pair));
            i += 4;
        } else {
            return Err(bad(i));
        }
    }
    match String::from_utf16(&units) {
        Ok(s) => Ok(s),
        Err(_) => bail_kind!(Protocol, "string contains an unpaired UTF-16 surrogate"),
    }
}

/// Latin-1 bytes to a string — how Java reads tight encoding's "ASCII only" strings.
pub fn decode_ascii(bytes: &[u8]) -> String {
    bytes.iter().map(|&b| b as char).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vectors() {
        for (s, bytes) in [
            ("A", vec![0x41]),
            ("\0", vec![0xC0, 0x80]),
            ("é", vec![0xC3, 0xA9]),
            ("€", vec![0xE2, 0x82, 0xAC]),
            // U+1F600 as surrogates D83D DE00, three bytes each.
            ("😀", vec![0xED, 0xA0, 0xBD, 0xED, 0xB8, 0x80]),
        ] {
            assert_eq!(encode(s), bytes, "{s:?}");
            assert_eq!(decode(&bytes).unwrap(), s);
        }
        // Standard UTF-8 from non-Java clients: 4-byte sequences, a raw NUL,
        // and both forms in one string.
        assert_eq!(decode("😀".as_bytes()).unwrap(), "😀");
        assert_eq!(decode(b"a\0b").unwrap(), "a\0b");
        let mut mixed = "x😀".as_bytes().to_vec();
        mixed.extend_from_slice(&[0xED, 0xA0, 0xBD, 0xED, 0xB8, 0x80]);
        assert_eq!(decode(&mixed).unwrap(), "x😀😀");
        assert!(decode(&[0xF0, 0x9F, 0x98]).is_err()); // truncated
        assert!(decode(&[0xF4, 0x90, 0x80, 0x80]).is_err()); // above U+10FFFF
        assert!(decode(&[0xF0, 0x80, 0x80, 0x80]).is_err()); // overlong, not a 4-byte char
        assert!(decode(&[0xC3]).is_err());
        assert!(decode(&[0xED, 0xA0, 0xBD]).is_err()); // lone surrogate
        assert!(is_ascii_only("queue.orders") && !is_ascii_only("a\0b") && !is_ascii_only("é"));
    }
}
