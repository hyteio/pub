# Installation

pub is built from source with [Rust](https://rustup.rs) (stable).

```bash
cargo install --git https://github.com/hyteio/pub pub-cli
```

This installs the `pub` binary into `~/.cargo/bin`. To build from a checkout instead:

```bash
git clone https://github.com/hyteio/pub
cd pub
cargo build --release      # target/release/pub
```

## Choosing what's included

Each protocol and message format is a Cargo feature, all on by default:

| Feature | Adds |
|---|---|
| `openwire` | ActiveMQ OpenWire |
| `amqp` | AMQP 1.0 (and Azure Service Bus) |
| `mqtt` | MQTT |
| `rampq` | RabbitMQ AMQP 0-9-1 |
| `stomp` | STOMP |
| `json` | the `json` message format |
| `xml` | the `xml` message format |

The `raw` and `text` formats are always included. A binary for one protocol only:

```bash
cargo build --release -p pub-cli --no-default-features --features mqtt
```

`pub modules` lists what a binary contains, and a scheme it doesn't support is refused clearly:
`no module handles 'stomp://' (available: mqtt, mqtts, mqtt+ssl)`.

## Smaller binaries

A full release build is about 6 MB on macOS (arm64). Building for size brings it to about 2.2 MB,
and to about 1.7 MB for MQTT alone:

```bash
CARGO_PROFILE_RELEASE_OPT_LEVEL=z \
CARGO_PROFILE_RELEASE_LTO=true \
CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1 \
CARGO_PROFILE_RELEASE_PANIC=abort \
CARGO_PROFILE_RELEASE_STRIP=true \
cargo build --release -p pub-cli --no-default-features --features mqtt
```

`opt-level = "z"` favours size over speed; `"s"` is a middle ground.

## TLS

pub uses [rustls](https://github.com/rustls/rustls) with the `ring` crypto provider. It doesn't
link OpenSSL or aws-lc, so there are no system TLS libraries to install. Trusted roots come from
the operating system, falling back to the bundled Mozilla roots (see [TLS](tls.md)).
