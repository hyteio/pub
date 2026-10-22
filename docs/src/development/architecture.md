# Architecture

pub is a Cargo workspace. Protocols and message formats are separate crates that depend only on
`pub-core`; the CLI registers whichever of them are compiled in.

```text
core/                (pub-core) configuration (git syntax), aliases, settings schema, sockets,
                     TLS (rustls + ring), the framed Connection<Codec>, the message model,
                     formats and header strategies, tracing, the Module/Session API
protocols/stomp/     (pub-stomp) STOMP 1.0–1.2: frame codec and session
protocols/mqtt/      (pub-mqtt) MQTT 5.0 and 3.1.1: packet codec, message mapping, session
protocols/openwire/  (pub-openwire) ActiveMQ OpenWire v12: tight and loose marshalling, session
protocols/amqp/      (pub-amqp) AMQP 1.0: type system, frames, SASL, sessions and links; Azure Service Bus
protocols/ramqp/     (pub-ramqp) RabbitMQ AMQP 0-9-1: field tables, method codec, channel session
formats/json/        (pub-format-json) the JSON / JSON Lines format (raw and text are in core)
formats/xml/         (pub-format-xml) the XML format
cli/                 (pub-cli) the `pub` binary: arguments (clap), commands, the module registry
docs/                this book (mdBook)
interop/             the real-broker test harness
```

## How a command runs

1. The CLI resolves the target into an `Endpoint`: an alias or URL, with settings layered from
   the config files, the URL and the command line, and checked against the schema.
2. The `Registry` picks the module for the endpoint's scheme and checks it supports the command
   (a `Verb`).
3. The module's `connect` opens a `Connection<Codec>`, which handles DNS, timeouts, TLS, buffering
   and `-v` tracing, and runs the protocol handshake. It returns a `Session`.
4. The CLI calls the session: `send`, `receive`, `browse`, `move_messages`, transactions,
   destination management. Messages cross the boundary in pub's protocol-neutral `Message`.
5. Formats turn `Message`s into output (and documents back into `Message`s for `send`); header
   strategies decide what a replayed message keeps.

Errors carry an `ErrorKind`, which becomes the [exit code](../reference/exit-codes.md).

## Principles

- **Everything read is bounded**: frame sizes, bodies, headers, buffered frames, nesting depth,
  documents and files. A misbehaving broker or input gets a clear error, not unbounded memory.
- **Nothing is silently dropped**: a field or property a protocol can't carry, an unknown setting
  or a selector the broker ignored is an error (or, outside strict mode, a warning).
- **At least once by default**: a message is acknowledged after it has been written out.
- **Secrets stay out of output**: passwords are masked in traces, errors and listings.
- **TLS is rustls with ring only**: no OpenSSL or aws-lc (enforced by `cargo deny`).
