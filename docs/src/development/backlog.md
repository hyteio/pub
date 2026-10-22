# Backlog

Work that is decided but waiting on something outside pub.

## OpenWire: send standard UTF-8

**Waiting on:** the ActiveMQ release that fixes UTF-8 handling in the Java client.

Java's OpenWire marshalling writes "modified UTF-8": U+0000 as `C0 80`, and characters outside the
BMP (emoji, many CJK extensions) as two 3-byte surrogates instead of one 4-byte sequence. Non-Java
clients write standard UTF-8.

- **Read side: done.** `protocols/openwire/src/utf.rs` `decode` accepts both forms, so messages from
  non-Java clients decode correctly.
- **Send side: to do.** `utf::encode` still writes modified UTF-8, because the Java client in current
  releases mis-decodes 4-byte sequences. The ActiveMQ team is fixing this in the next release.

Once the fixed version is known:

1. Write standard UTF-8 when the broker (or a setting) says the peers can read it. Options: a
   setting such as `wireFormat.standardUtf8`, or keying off the broker version in `BrokerInfo` /
   `WireFormatInfo` if the fix ships with a negotiable marker.
2. Keep modified UTF-8 as the default for older brokers and clients.
3. Add an interop test with the fixed Java client: pub sends a supplementary character (e.g. U+1F600)
   in a TextMessage body and a property; Java reads it back intact.
