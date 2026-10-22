# Adding a protocol module

1. Create the crate `protocols/<proto>` (package `pub-<proto>`), depending on `pub-core`.

2. Implement `pub_core::Codec`: `encode(frame, &mut Vec<u8>)`, and
   `decode(&[u8]) -> NeedMore | Skip(n) | Frame(f, n)`. Then
   `Connection::<YourCodec>::open(ep, &ctx.scheme, ctx.trace.clone(), codec)` gives you DNS,
   connect timeouts, TLS, buffering, read deadlines and `-v` tracing. Check every length field
   against the frame size before allocating, and bound nesting depth.

3. Implement `pub_core::Session`, overriding what the protocol supports: `send`, `receive`,
   `browse`, `unsubscribe`, `begin`/`commit`/`rollback`, `move_messages`, the destination methods,
   `info`. Anything left out reports the command as unsupported.

4. Implement `pub_core::Module`: its name, the schemes it handles (`SchemeInfo { scheme,
   default_port, tls }`), the `Verb`s it supports, and `connect`. Declare every setting the module
   reads in `Module::settings()`, so it's validated and documented (the
   [Settings reference](../reference/settings.md) is generated from these). Optional hooks:

   | Hook | For |
   |---|---|
   | `destination_name` | how a URL becomes a destination (MQTT strips the leading `/` and keeps `#`) |
   | `destination_options` | provider options passed through to the broker |
   | `message_fields`, `message_capabilities` | which standard fields it can send; capabilities that depend on settings (MQTT 3.1.1 vs 5) |
   | `typed_properties`, `reserved_property_names`, `protocol_headers` | how properties and raw headers map |
   | `setting_aliases`, `inert_settings` | other clients' names for pub's settings; options accepted for compatibility |
   | `durable_subscriptions`, `transactions`, `prepare_durable` | what `--durable` and `--transaction` mean here |
   | `provider_properties` | properties the broker adds, which header strategies drop on replay |

5. For tests, ship an in-process mock broker behind a `mock` feature (see
   `protocols/*/src/mock.rs`), and add the broker to the [interop suite](testing.md).

6. In `cli/`: add an optional dependency and a feature of the same name, and one `r.register(...)`
   line in `main.rs`.

A new command is added once in `pub_core::Verb` (with a default "unsupported" method on `Session`),
plus its arguments in the CLI; modules opt in through `verbs()`.
