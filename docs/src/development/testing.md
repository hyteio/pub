# Testing and CI

```bash
cargo test            # unit tests, and end-to-end tests against in-process mock brokers
cargo build --release # target/release/pub
cargo deny check      # RustSec advisories, licenses, banned crates (deny.toml)
interop/run.sh        # real brokers (needs Docker, a JDK and openssl)
docs/generate.sh      # regenerate the generated reference pages of this book
```

## Mock brokers

Each protocol crate has an in-process mock broker behind its `mock` feature. The CLI's end-to-end
tests (`cli/tests/cli.rs`) start the real `pub` binary against them, so they need every module; a
build with fewer features skips them.

## The interop suite

`interop/run.sh` starts real brokers in Docker, generates certificates for the run, compiles the
Java helpers in `interop/java` against ActiveMQ's own client jars, and runs the ignored
tests in `cli/tests/brokers.rs`:

| Protocol | Brokers | Covers |
|---|---|---|
| OpenWire | ActiveMQ | TLS 1.3/1.2 and trust, mutual TLS, Java producer → pub and pub → Java consumer (all property types, compression), prefetch and redelivery, selectors, pulls, topics, scheduled delivery, replay strategies, browse, durable subscriptions, transactions, `move`, destinations, XML |
| STOMP | ActiveMQ | headers, properties and binary bodies, selectors, topics, text vs bytes as Java sees them, TLS, refused logins, heart-beats, durable subscriptions, browse, transactions, `move` |
| AMQP 1.0 | ActiveMQ (with `transport.transformer=jms`), RabbitMQ 4 | typed properties and multi-frame messages, selectors, pub ⇄ Java through ActiveMQ's JMS transformer, topics, durable subscriptions, browse, scheduled delivery, transactions and `move` (refused on RabbitMQ), AMQP over TLS |
| MQTT | Mosquitto, ActiveMQ (3.1.1) | 5 and 3.1.1 publish/subscribe, MQTT 5 properties, retained messages, persistent sessions and `--durable`, will messages, shared subscriptions, topic aliases, TLS and passwords, MQTT ⇄ OpenWire |
| AMQP 0-9-1 | RabbitMQ 4 | logins and vhosts, queues, exchanges and topics, unroutable messages, durable subscriptions, transactions, `move`, declaring and deleting, 0-9-1 ⇄ 1.0 |
| Azure Service Bus | Microsoft's emulator (opt-in) | typed properties and multi-frame messages, subscription rules, peek-based browse, transactions, scheduled delivery, the refusals |

- `interop/run.sh mqtt` runs only the tests whose names contain `mqtt`; extra arguments go to the
  test runner.
- `KEEP=1` leaves the brokers running afterwards.
- ActiveMQ is pinned to `apache/activemq:6.3.2`. `ACTIVEMQ_IMAGE`, `MOSQUITTO_IMAGE`,
  `RABBITMQ_IMAGE`, `AZURE_SERVICEBUS_IMAGE` and `AZURE_MSSQL_IMAGE` pick other images.
- **Azure Service Bus** is opt-in: `AZURE_SERVICEBUS=1 interop/run.sh`. That accepts the emulator's
  and SQL Server's license terms. SQL Server is amd64-only, so on arm64 it runs emulated and the
  first start takes a few minutes. The emulator accepts any key, so logins aren't tested there.

## Coverage

With `cargo-llvm-cov` installed:

```bash
cargo llvm-cov --workspace --all-features --html \
    --ignore-filename-regex '(mock\.rs|/tests/|tests\.rs)'
```

To include the real-broker suite, run it under the coverage environment:

```bash
eval "$(cargo llvm-cov show-env --sh)"
cargo llvm-cov clean --workspace
cargo test --workspace --all-features
interop/run.sh
cargo llvm-cov report --html --ignore-filename-regex '(mock\.rs|/tests/|tests\.rs)'
```

## CI

`.github/workflows/ci.yml` runs on pushes to `main`, on pull requests, and weekly:

| Job | Does |
|---|---|
| lint | rustfmt, and clippy with warnings as errors |
| test | the tests on Linux and macOS |
| features | `pub` with no modules, then each module and format alone, warnings as errors |
| deny | `cargo deny check`: RustSec advisories, licenses, the ban on aws-lc and OpenSSL |
| interop | the real-broker suite in Docker |
| docs | regenerates the reference pages, checks they're committed, and builds this book |

The weekly run catches new advisories even when the code hasn't changed. Run the workflow by hand
with *azure_servicebus* ticked to include the Service Bus emulator.

## Releases

Before tagging, add the version to the [release notes](../release-notes.md).
`.github/workflows/release.yml` runs when a version tag is pushed:

```bash
git tag -m "pub 1.0.0" v1.0.0 && git push origin v1.0.0
```

1. It checks the tag matches the workspace version in `Cargo.toml`, and runs the tests.
2. It builds `pub` with the `dist` profile (link-time optimisation, stripped) for Linux x86_64 and
   arm64 (statically linked with musl, so it runs on any distribution) and for macOS Intel and Apple
   Silicon, and signs build provenance for each archive and binary
   ([`gh attestation verify`](../installation.md#verifying-a-download)).
3. It creates the GitHub release, with a `pub-<tag>-<target>.tar.gz` per platform (the binary,
   `LICENSE` and `README.md`), its SHA-256 checksum, and notes generated from the commits.
4. It publishes this book to GitHub Pages with `.github/workflows/docs.yml`. CI builds the book but
   doesn't publish it. To publish documentation changes between releases, run the **Docs** workflow
   from the Actions tab (Run workflow, on `main`).
5. It writes `Formula/pub.rb` to the [hyteio/homebrew-tap](https://github.com/hyteio/homebrew-tap)
   repository, from `.github/scripts/homebrew-formula.sh` and the release's checksums. This needs
   the `HOMEBREW_TAP_TOKEN` secret in the `homebrew` environment (limited to `v*` tags): a
   fine-grained token with Contents read and write on that repository only. Without it the step is
   skipped.

The install script, `docs/src/install.sh`, is served from the documentation site
(`https://hyteio.github.io/pub/install.sh`), so changes to it go live when the docs are published.
