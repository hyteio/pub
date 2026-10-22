# Installation

## Install script

On Linux (x86_64 and arm64) and macOS (Intel and Apple Silicon):

```bash
curl -fsSL https://hyteio.github.io/pub/install.sh | sh
```

It downloads the latest release for your platform, checks its SHA-256 checksum and installs `pub`
into `~/.local/bin`. `PUB_VERSION=1.0.1` picks a version, `PUB_INSTALL_DIR=/usr/local/bin` another
directory. [Read the script](install.sh) before running it, if you like.

## Homebrew

```bash
brew install hyteio/tap/pub
```

## Prebuilt binaries

Each [release](https://github.com/hyteio/pub/releases) has binaries for Linux (x86_64 and arm64,
statically linked, for any distribution) and macOS (Intel and Apple Silicon):

```bash
tar -xzf pub-v1.0.1-aarch64-apple-darwin.tar.gz
sudo mv pub-v1.0.1-aarch64-apple-darwin/pub /usr/local/bin/
```

Each archive has a `.sha256` file to check it against (`shasum -a 256 -c FILE.sha256`).

### Verifying a download

Releases from 1.0.2 on carry signed build provenance: proof that an archive, and the binary in it,
was built by pub's release workflow from the tagged commit. With the
[GitHub CLI](https://cli.github.com):

```bash
gh attestation verify pub-v1.0.2-aarch64-apple-darwin.tar.gz --repo hyteio/pub
gh attestation verify "$(command -v pub)" --repo hyteio/pub
```

## From source

With [Rust](https://rustup.rs) (stable):

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
| `ramqp` | RabbitMQ AMQP 0-9-1 |
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

The release binaries are built with the `dist` profile, which favours size over speed (pub waits on
the network, not the CPU): about 2 MB with everything, 1.5 MB for MQTT alone (macOS arm64):

```bash
cargo build --profile dist -p pub-cli --no-default-features --features mqtt   # target/dist/pub
```

## TLS

pub uses [rustls](https://github.com/rustls/rustls) with the `ring` crypto provider. It doesn't
link OpenSSL or aws-lc, so there are no system TLS libraries to install. Trusted roots come from
the operating system, or from a `ca-file` you give (see [TLS](tls.md)).
