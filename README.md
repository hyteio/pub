# pub — the curl for messaging

`pub` sends, receives, browses and moves messages on message brokers from the command line.

```bash
pub send stomp://mq01:61613/queue/orders -d '{"id":1}' -t application/json
pub recv jms.company.com/queue/orders -n 10 -m 5s -i
pub ping jms.company.com -v
pub recv 'mqtt://broker/sensors/#' -O qos=1 -i
pub send mqtt://broker/sensors/kitchen -d 21.5 -H 'unit: C' -O retain=true
```

**Documentation: <https://hyteio.github.io/pub/>**

## Protocols

| Protocol | Schemes |
|---|---|
| ActiveMQ OpenWire v12 | `amq://`, `amqs://` |
| AMQP 1.0, including Azure Service Bus | `amqp://`, `amqps://` |
| MQTT 5.0 and 3.1.1 | `mqtt://`, `mqtts://` |
| RabbitMQ AMQP 0-9-1 | `rampq://`, `rampqs://` |
| STOMP 1.0–1.2 | `stomp://`, `stomps://` |

Each protocol and message format is an optional Cargo feature, so a build can include only what it
needs.

## Install

```bash
cargo install --git https://github.com/hyteio/pub pub-cli
```

See [Installation](https://hyteio.github.io/pub/installation.html) for slimmer builds.

## Development

```bash
cargo test            # unit tests, and end-to-end tests against in-process mock brokers
interop/run.sh        # real brokers in Docker
cargo deny check      # advisories, licenses, banned crates
docs/generate.sh      # regenerate the reference pages of the documentation
mdbook serve docs     # preview the documentation at http://localhost:3000
```

The documentation's source is in [`docs/`](docs/src/SUMMARY.md); see
[Testing and CI](https://hyteio.github.io/pub/development/testing.html) and
[Architecture](https://hyteio.github.io/pub/development/architecture.html).

## License

Apache-2.0
