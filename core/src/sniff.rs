//! Recognising the wrong protocol on the other end, from the first bytes a server sends, so that
//! an error can say "this is an OpenWire port" instead of "malformed frame".

/// What `first` (the start of what the server sent) looks like, if it's a protocol pub knows.
pub fn identify(first: &[u8]) -> Option<&'static str> {
    // OpenWire: a WireFormatInfo, length (4), type 1, then the magic "ActiveMQ".
    if first.len() >= 8
        && first[4] == 0x01
        && b"ActiveMQ".starts_with(&first[5..first.len().min(13)])
    {
        return Some("an ActiveMQ OpenWire port; try amq:// or amqs://");
    }
    if let Some(v) = first.strip_prefix(b"AMQP") {
        return Some(match v {
            [0, 0, 9, 1, ..] | [1, 1, 0, 9, ..] => {
                "a RabbitMQ AMQP 0-9-1 port; try ramqp:// or ramqps://"
            }
            [0 | 2 | 3, 1, 0, 0, ..] => "an AMQP 1.0 port; try amqp:// or amqps://",
            _ => "an AMQP port",
        });
    }
    if first.starts_with(b"CONNECTED") || first.starts_with(b"ERROR\n") {
        return Some("a STOMP port; try stomp:// or stomps://");
    }
    if first.starts_with(b"HTTP/") {
        return Some("an HTTP server");
    }
    if first.starts_with(b"SSH-") {
        return Some("an SSH server");
    }
    // A TLS record: handshake (0x16) or alert (0x15), version 3.x.
    if let [0x15 | 0x16, 0x03, 0..=0x04, ..] = first {
        return Some(
            "a TLS port; try the TLS scheme (amqs://, amqps://, mqtts://, ...) or tls=true",
        );
    }
    None
}

/// `": the server looks like …"` to append to an error message, or `""`.
pub fn hint(first: &[u8]) -> String {
    identify(first)
        .map(|what| format!(": the server looks like {what}"))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognises_other_protocols() {
        // What ActiveMQ's OpenWire port sends first.
        let openwire = [
            0, 0, 1, 0x51, 1, b'A', b'c', b't', b'i', b'v', b'e', b'M', b'Q', 0,
        ];
        assert!(identify(&openwire).unwrap().contains("OpenWire"));
        assert!(identify(&openwire[..8]).unwrap().contains("OpenWire"));
        assert!(identify(b"AMQP\x00\x00\x09\x01").unwrap().contains("0-9-1"));
        assert!(identify(b"AMQP\x03\x01\x00\x00")
            .unwrap()
            .contains("AMQP 1.0"));
        assert!(identify(b"CONNECTED\nversion:1.2\n")
            .unwrap()
            .contains("STOMP"));
        assert!(identify(b"HTTP/1.1 400 Bad Request")
            .unwrap()
            .contains("HTTP"));
        assert!(identify(&[0x15, 0x03, 0x03, 0x00, 0x02])
            .unwrap()
            .contains("TLS"));
    }

    #[test]
    fn leaves_everything_else_alone() {
        for bytes in [
            &b""[..],
            b"\x20\x02\x00\x00",
            b"\x00\x00\x00\x08",
            b"RANDOM",
            &[0, 0, 0, 9, 1, b'X', 0, 0],
        ] {
            assert_eq!(identify(bytes), None, "{bytes:?}");
            assert_eq!(hint(bytes), "");
        }
    }
}
