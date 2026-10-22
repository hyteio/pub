# Messages

pub has one message model for every protocol. It keeps JMS's distinctions, so each module can map
them faithfully.

| Part | How to set it | Examples |
|---|---|---|
| **standard fields** (protocol-neutral; `pub config headers` lists them) | `-H 'name: value'`, by name or JMS name | `-H 'JMSCorrelationID: c-1'`, `-H 'priority: 7'`, `-H 'ttl: 5m'`, `-H 'persistent: true'`, `-t text/plain` |
| **properties** (application properties) | `-H` with any other name (a string), or `-P name[:type]=value` | `-H 'region: EU'`, `-P orderId:long=42`, `-P type=gold` |
| **protocol headers** | `-H` with a name the module claims as a raw header | AMQP `x-opt-*` annotations |
| **destination options** | URL query, alias config or `-O`, matched by the module's patterns, case preserved | `jms.company.com/queue/FOO?activemq.prefetchSize=10` |

- Fields the broker sets (`message-id`, `timestamp`, `expiration`, `redelivered`, …) can't be sent.
  Use `ttl` rather than an absolute expiration. A `ttl` of 0 means the message never expires.
- `recv -i` prints `# standard`, `# properties` and `# protocol` groups before the body. Typed
  properties show their type, such as `orderId (long): 42`.
- Modules declare what they can carry. Asking for something they can't, such as a field they don't
  map (`delivery-delay` on STOMP) or a typed property on a protocol with string properties only, is
  an error in strict mode and a warning otherwise.
- A property named like one of a protocol's headers (`-P priority=1` on STOMP) is always an error:
  it would silently become JMSPriority.
- Destination options never hide typos. A key is a setting if the schema knows it, a destination
  option if it matches a pattern the module declares (see `pub config settings MODULE`), and an
  error otherwise. Options are filtered by direction (`activemq.*` only goes on a subscription, for
  example). `--dest` is always passed through as given.

## Selectors

`-s/--selector` sets a JMS message selector on `recv`, `browse` and `move`. `-sf/--selector-file
FILE` reads it from a file (`-` = stdin; UTF-8, up to 4 KiB). Lines starting with `#` are comments,
and line breaks are kept as whitespace. `-sf` must be its own word: `-sfoo` still means `-s foo`.

Not every protocol has selectors: MQTT and AMQP 0-9-1 don't, and Azure Service Bus filters with
subscription rules instead. pub refuses a selector the broker won't apply, rather than receive
everything.

## Text or bytes

`message-type = text | bytes` (default **bytes**, for every protocol) says whether a body is text,
which must then be valid UTF-8, or opaque bytes. Set it globally, per module, per scheme, per alias
or per command:

```ini
[module "stomp"]
	message-type = text      # all STOMP sends become JMS TextMessages on ActiveMQ
```

| Protocol | Text | Bytes |
|---|---|---|
| STOMP | SEND without `content-length`: ActiveMQ makes a **TextMessage** (the body can't contain NUL) | with `content-length`: a **BytesMessage** |
| MQTT 5 | Payload Format Indicator 1 (UTF-8) | no indicator |
| MQTT 3.1.1 | can't be expressed: reported (an error in strict mode), sent as bytes | — |
| OpenWire | TextMessage | BytesMessage |
| AMQP 1.0 | an `amqp-value` string, or with `text-body=data` a data section marked `text/plain; charset=utf-8` | a data section |
| AMQP 0-9-1 | `content-type: text/plain; charset=utf-8` | no content type |

On receive, `-i` shows `message-type` when the protocol says which it is.

## Transactions

On protocols with transactions (STOMP, OpenWire, AMQP 1.0, AMQP 0-9-1):

- `send --transaction` sends everything in one transaction, all or nothing; `--batch N` commits
  every N messages. If a send fails, the error says how many were committed before it.
- `recv --transaction` acknowledges in a transaction committed after the messages are written,
  so a failure (a full disk, a closed pipe) leaves them on the broker; `--batch N` commits every N.
- `move` copies each message to the target and removes the original together, so a message is
  never lost or doubled (see each protocol for how).
