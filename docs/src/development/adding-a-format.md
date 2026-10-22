# Adding a message format

1. Create the crate `formats/<name>` (package `pub-format-<name>`), depending on `pub-core`.

2. Implement `pub_core::Format`:
   - `name` and `description`;
   - `writer(out, ctx)`, returning a `MessageWriter` (`begin`, `write` per message, `finish`). Flush
     after each message, so `recv -n 0` streams.
   - If the format can be read: `can_read() = true`, and `reader(input, ctx)` yielding `Record`s (a
     message and the destination the document names). Bound what a reader buffers by
     `ctx.document_limit()`, and bound nesting depth: input documents are untrusted.
   - Declare options as `format.<name>.*` in `settings()`; they're validated for every protocol.

3. Map the whole of pub's message model (fields, typed properties, protocol headers, the body type)
   so documents round-trip. See `formats/json` and its tests.

4. In `cli/`: an optional dependency and feature, and one `r.register_format(...)` line in
   `main.rs`.

## Adding a header strategy

Implement `pub_core::strategy::HeaderStrategy` (`name`, `description`,
`apply(&mut Message, &StrategyContext) -> StrategyReport`) and register it with
`Registry::register_strategy`. Modules declare the properties brokers add with
`Module::provider_properties`; strategies receive them in the context.
