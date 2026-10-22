# Release notes

Downloads for every release are on [GitHub](https://github.com/hyteio/pub/releases); see
[Installation](installation.md).

## 1.0.2

### Installing

- An install script for Linux and macOS: `curl -fsSL https://hyteio.github.io/pub/install.sh | sh`.
- Homebrew: `brew install hyteio/tap/pub`.
- Signed build provenance for every archive and binary: `gh attestation verify FILE --repo
  hyteio/pub` (see [Verifying a download](installation.md#verifying-a-download)).

### Fixes

- MQTT: settings after a `#` wildcard in a URL apply, so `mqtt://broker/sensors/#?version=3.1.1`
  subscribes to `sensors/#` with 3.1.1. They used to become part of the topic.
- MQTT: topic filters are checked before subscribing (`#` only as the whole last level, `+` only as
  a whole level).

### Clearer errors

- A URL pointed at another protocol's port names it, for example an `amqp://` URL on ActiveMQ's
  OpenWire port: "the server looks like an ActiveMQ OpenWire port; try amq:// or amqs://".
  OpenWire, AMQP 1.0, RabbitMQ AMQP 0-9-1, STOMP, HTTP and SSH are recognised.
- A TLS URL pointed at a plain-text port says so.
- A broker that hangs up on MQTT 5 gets a hint to try `-O version=3.1.1` (ActiveMQ speaks 3.1.1
  only).

### Changes

- OpenWire's `broker-type` is `activemq` or `artemis`; `classic` still works as `activemq`.

## 1.0.1

No changes to `pub` itself.

- The documentation adds these release notes.
- Releases attach only the binaries and their checksums (1.0.0 also carried the documentation
  bundle, `artifact.tar`).

## 1.0.0

The first release.

### Protocols

- [ActiveMQ OpenWire](protocols/openwire.md) v12 (`amq://`, `amqs://`), tight and loose encoding,
  for ActiveMQ 6.x and 5.17+.
- [AMQP 1.0](protocols/amqp.md) (`amqp://`, `amqps://`) for ActiveMQ, RabbitMQ 4, Qpid and
  [Azure Service Bus](protocols/servicebus.md).
- [MQTT](protocols/mqtt.md) 5.0 and 3.1.1 (`mqtt://`, `mqtts://`), with will messages, shared
  subscriptions and topic aliases.
- [RabbitMQ AMQP 0-9-1](protocols/ramqp.md) (`ramqp://`, `ramqps://`) for RabbitMQ and LavinMQ.
- [STOMP](protocols/stomp.md) 1.0–1.2 (`stomp://`, `stomps://`), with heart-beats.

### Commands

- `send`, `recv`, `browse`, `move`, `unsub` and `dest` (list, create and delete destinations),
  where the protocol supports them; `pub modules` shows which.
- Selectors, durable subscriptions and transactions on `recv`; transactional `move`.
- `ping` to check a connection, handshake and server details.
- `alias` and `config` for git-style configuration with aliases.

### Messages

- Bodies from the command line, a file or stdin, with headers and typed properties.
- [Formats](formats.md): raw, text, JSON / JSON Lines and XML, for output and for sending captured
  messages.
- [Replay](replay.md) captured messages as new messages or exact copies.

### Security

- TLS 1.2 and 1.3 with rustls and the `ring` provider; no OpenSSL.
- Mutual TLS, SPIFFE ID verification, and configurable versions, cipher suites and groups.
- Servers are verified against the system's trusted roots, or only `ca-file` when it's set.
- Passwords from files, environment variables or a prompt, masked in traces and errors.

### Builds

Linux x86_64 and arm64 (static, musl) and macOS Intel and Apple Silicon, about 2 MB each.

### Known limitations

- OpenWire sends Java's modified UTF-8, so characters outside the Basic Multilingual Plane (emoji,
  for instance) need a Java client that reads it; reading accepts both forms. See the
  [backlog](development/backlog.md).
- On Azure Service Bus, selectors, `--durable`, `unsub`, `move` and destination management are
  refused: Service Bus can't do them over AMQP. See [Azure Service Bus](protocols/servicebus.md).
- No post-quantum key exchange yet: `ring` has no ML-KEM.
