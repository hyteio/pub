# ActiveMQ OpenWire

`amq://host[:61616]/destination`, `amqs://host[:61617]` (TLS). OpenWire version 12: ActiveMQ
6.x and 5.17+. An older broker is refused with a clear message.

Commands: `send`, `recv`, `browse`, `unsub`, `move`, `dest`, `ping`.

pub sends its WireFormatInfo immediately (some brokers wait for the client to speak first), prefers
the compact **tight** encoding and falls back to loose if the broker declines, never uses the
marshal cache, and runs ActiveMQ's inactivity monitor: keep-alives every third of
`wireFormat.maxInactivityDuration`, and a broker silent for the whole period is a timeout
(exit 28). Broker exceptions map to exit codes: `SecurityException` → 67,
`InvalidDestinationException` → 2, anything else → 8.

## Settings

Settings use the **Java client's names and units (milliseconds)**, so options from an ActiveMQ URI
work as they are:

```bash
pub ping 'amq://broker?jms.userName=app&jms.password=secret&connectionTimeout=3000'
```

| Setting | Default | |
|---|---|---|
| `wireFormat.tightEncodingEnabled` | `true` | prefer tight encoding |
| `wireFormat.stackTraceEnabled` | `false` | Java stack traces in broker errors (Java's default: true) |
| `wireFormat.maxInactivityDuration`, `…InitalDelay` | `30000`, `10000` | ms; 0 turns the monitor off (Java's spelling, "Inital") |
| `jms.connectResponseTimeout` | `0` | ms to wait for the connection to be accepted (0 = `receive-timeout`) |
| `jms.closeTimeout` | `15000` | ms to wait for the broker to acknowledge close |
| `client-id` / `jms.clientID` | generated | the JMS client ID |
| `client-id-prefix` / `jms.clientIDPrefix` | – | prefix for generated client IDs, built as Java does: `<prefix>-<pid>-<start ms>-0:<n>` (Java uses a port where pub uses its pid); ignored when a client ID is set |
| `connection-id-prefix` / `jms.connectionIDPrefix` | – | prefix for connection IDs, as Java does: `<prefix>-<pid>-<start ms>-1:<n>`; producer and message IDs (`<connection id>:1:1:<seq>`) start with it, and so does the default client ID |
| `jms.useAsyncSend` | `false` | send persistent messages without waiting for the broker |
| `jms.alwaysSyncSend` | `false` | wait for the broker on every send |
| `jms.sendTimeout` | `0` | ms; above 0 makes every send synchronous, bounded by it (0 = `receive-timeout`) |
| `jms.producerWindowSize` | `0` | bytes of async sends allowed unacknowledged (producer flow control) |
| `jms.useCompression` | `false` | deflate bodies (any ActiveMQ client inflates them) |
| `jms.disableTimeStampsByDefault` | `false` | leave JMSTimestamp unset |
| `prefetch-follows-count` | `true` | without an explicit prefetch, prefetch only what `-n` asks for |
| `jms.prefetchPolicy.queuePrefetch`, `topicPrefetch`, `all` | 1000, 32767 | explicit prefetch (0 = pull one at a time) |
| `jms.prefetchPolicy.maximumPendingMessageLimit` | `0` | slow topic consumer limit |
| `jms.dispatchAsync`, `jms.exclusiveConsumer`, `jms.useRetroactiveConsumer` | `true`, `false`, `false` | ConsumerInfo flags |
| `jms.sendAcksAsync` | `true` | acknowledge without waiting for the broker |
| `jms.consumerExpiryCheckEnabled` | `true` | drop (and ack as expired) messages that expired on the way |
| `broker-type` | `auto` | which broker, for destination management; `auto` decides from the broker's id |

Other names (`pub config settings openwire` has them all): `connectionTimeout` and `soTimeout`
(ms) for `connect-timeout` and `receive-timeout`; `tcpNoDelay`, `keepAlive`, `socketBufferSize`
and `wireFormat.maxFrameSize` for pub's socket settings; `jms.userName`, `jms.password`,
`jms.clientID`, `jms.clientIDPrefix` and `jms.connectionIDPrefix` for credentials, the client ID
and the ID prefixes. Java options that mean nothing to a CLI (`jms.copyMessageOnSend`,
`jms.watchTopicAdvisories`, `jms.redeliveryPolicy.*`, …) are accepted and noted under `-v`.

## Sending

Destinations are `/queue/X`, `/topic/X`, `queue://X`, `topic://X`, or a bare name (a queue);
composite names (`queue://a,b`) pass through. `message-type = text` sends a TextMessage, otherwise
a BytesMessage. Properties keep their types (`-P n:int=5`). The JMS headers are native fields:
`correlation-id`, `reply-to`, `type`, `priority` (default 4), `persistent` (default true), `ttl`
(JMSExpiration) and `group-id`/`group-seq`. `delivery-delay` becomes ActiveMQ's
`AMQ_SCHEDULED_DELAY` (ActiveMQ needs `schedulerSupport="true"`). There is no content-type header
in JMS, and property names Java routes to headers (`JMSPriority`, `JMSXGroupID`, …) are refused.

Java's send rules apply:

- Persistent messages wait for the broker's answer; non-persistent ones don't (`jms.useAsyncSend`,
  `jms.alwaysSyncSend` and `jms.sendTimeout` change that).
- Async sends stay within `jms.producerWindowSize`, released by the broker's ProducerAck.
- Destination options `producer.windowSize` and `producer.dispatchAsync` set the producer
  (`queue://orders?producer.windowSize=65536`); unknown `producer.*` options are an error, as in
  Java.
- After asynchronous sends, pub waits for the broker to acknowledge the connection close before
  exiting (the only sign the broker got them), and fails (exit 28) if it doesn't.
- As with the Java client, a broker that *refuses* an asynchronous message (not authorized, say)
  only logs it; use synchronous sends when that matters.

## Receiving

A consumer per receive, non-durable for topics. The prefetch follows `-n` unless it's set
(`jms.prefetchPolicy.*`, or `consumer.prefetchSize` on the destination), so
`pub recv amq://b/queue/q` takes one message off the queue rather than 1000; prefetch 0 pulls each
message (MessagePull).

- Destination options `consumer.*` set the consumer as in Java (`prefetchSize`, `selector`,
  `exclusive`, `noLocal`, `priority`, `retroactive`, `dispatchAsync`,
  `maximumPendingMessageLimit`, `noRangeAcks`; unknown ones are an error).
- `-s` is a JMS selector; a bad one is exit 2.
- Each message is acknowledged after it has been written (with `--no-ack`, before: at most once).
  Undelivered prefetched messages go back without counting as redelivered.
- Messages map back natively: message id, destination (`queue://X`), timestamp, the JMS headers,
  `redelivered` and `delivery-count`, and typed properties. TextMessage → `message-type: text`,
  BytesMessage → bytes. Compressed bodies are inflated as Java does, including Java's own quirks and
  unterminated streams. Map, Stream and Object messages keep their raw content, with
  `message-class` in the protocol section alongside `original-destination` (dead-letter queues),
  `user-id` and the broker's in and out times.

Strings are read as Java's modified UTF-8 or as standard UTF-8, so messages from non-Java clients
decode too. pub still writes modified UTF-8, which current Java clients need.

**A message pub can't decode** (a damaged property map or body, text that isn't UTF-8 in either
form) is handled as the Java client handles a message it can't deliver: pub warns with its id and
sends a poison ack, and the broker moves it to the dead-letter queue (`ActiveMQ.DLQ` by default)
with the reason. `browse` warns and skips it. A message only larger than pub's own `max-payload` is
not a bad message: the receive stops with exit 2 and the message stays on the queue.

## Browsing

`pub browse amq://b/queue/q` shows a queue's messages without consuming them (a JMS
`QueueBrowser`). The broker sends a snapshot and then marks the end, so no `-m` is needed; an empty
queue is an empty answer (exit 0). Selectors and `consumer.*` options apply; the prefetch is
`jms.prefetchPolicy.queueBrowserPrefetch` (500), lowered to `-n`. Browsing doesn't mark messages
redelivered. Topics can't be browsed.

## Durable subscriptions

`pub recv amq://b/topic/news --durable audit -O client-id=app` receives through the durable
subscription `audit` of client `app`, creating it the first time; between runs it keeps the topic's
messages. A subscription is identified by client ID and name, so a fixed `client-id` (or
`jms.clientID`) is required. Java's `?consumer.subscriptionName=audit` works too. The prefetch is
`jms.prefetchPolicy.durableTopicPrefetch` (100), lowered to `-n`.

`pub unsub amq://b --durable audit -O client-id=app` removes it. A missing one is exit 2, and
one in use is an error too.

## Transactions

`send --transaction` sends everything in one local transaction; `--batch N` commits every N.
`recv --transaction` acknowledges in a transaction committed after the messages are written, so a
failure rolls back and they stay on the broker, marked redelivered. Transacted sends don't wait for
the broker message by message, as with Java; the commit confirms them.

## Moving messages

```bash
pub move amq://b/queue/DLQ.orders --to /queue/orders -s "region IS NULL"
```

Moves messages to another destination on the same broker as exact copies (message id, timestamp,
expiration, headers, properties, and any body type, Map and Object messages included), sent and
acknowledged in one transaction per batch (`--batch`, 100), so a message is never lost or doubled.
Each copy records the queue it came from as its original destination, as ActiveMQ's own move does.

It stops when the source has been quiet for `-m` (1s) or after `-n`; `--durable NAME` moves what a
durable subscription holds. A summary goes to stderr; `-F json` also writes each moved message.
Moving a destination onto itself is refused.

## Destinations

```bash
pub dest list amq://b                       # queue://NAME and topic://NAME, one per line
pub dest create amq://b/queue/orders
pub dest delete amq://b/queue/orders        # refused while it has consumers (exit 8)
```

ActiveMQ announces destinations on its advisory topics (`ActiveMQ.Advisory.Queue` and
`.Topic`), which pub listens to for `-m` (1s; there is no end marker). Brokers that don't are asked
through their management API (`activemq.management`, with replies on a temporary queue; the user
needs `manage` permission). pub tells them apart by the broker's id; `broker-type` overrides that.
Broker internals (`ActiveMQ.Advisory.*`, `$sys.*`, `activemq.*`, temporary queues) are hidden unless
`--all`.

The wire format pub implements, derived from the ActiveMQ source, is described in
[OpenWire wire format](../development/openwire/README.md).
