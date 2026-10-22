# STOMP

`stomp://host[:61613]/destination`, `stomps://host[:61612]` and `stomp+ssl://` (TLS). STOMP 1.0,
1.1 and 1.2; pub offers all three and uses the version the broker picks.

Commands: `send`, `recv`, `browse` (ActiveMQ), `unsub`, `move`, `ping`.

Destinations are the broker's own names, typically `/queue/orders` and `/topic/news` on ActiveMQ.

| Setting | Default | |
|---|---|---|
| `vhost` | the URL host | the `host` header sent on CONNECT |
| `client-id` | – | sent on CONNECT; durable subscriptions need one |
| `receipts` | `true` | wait for the broker's RECEIPT after each SEND |
| `heart-beat` | `10s` | offered both ways; `0` = none |

## Messages

Fields map to the headers ActiveMQ turns into JMS headers: `correlation-id`, `reply-to`, `type`,
`priority`, `persistent`, `expires` (from `ttl`), `JMSXGroupID` / `JMSXGroupSeq` and
`content-type`. Properties travel as string headers. A SEND without `content-length` becomes a
TextMessage on ActiveMQ, and one with it a BytesMessage (see
[Text or bytes](../messages.md#text-or-bytes)). STOMP can't carry `delivery-delay` or typed
properties.

Destination options: `activemq.*` and `prefetch-count` on SUBSCRIBE; `x-*` (RabbitMQ queue
arguments) on SEND and SUBSCRIBE.

pub's own `receipt` and `transaction` headers always win over any given with `-H`, and NUL bytes are
refused in headers: one would end the frame early.

## Heart-beats

With STOMP 1.1 and later, pub offers `heart-beat` in both directions. While waiting, it sends a
heart-beat whenever it has been quiet for the negotiated interval, and gives up (exit 28) when the
broker has sent nothing for two of its intervals.

## Durable subscriptions

`--durable NAME` with a fixed `client-id` on a `/topic/...` subscribes with both
`activemq.subscriptionName` and `durable-subscription-name`. Between runs the broker keeps
the topic's messages for it.

```bash
pub recv stomp://broker/topic/news --durable audit -O client-id=app -n 0 -m 5s
pub unsub stomp://broker/topic/news --durable audit -O client-id=app
```

## Transactions and move

`--transaction [--batch N]` on `send` and `recv` uses BEGIN, COMMIT and ABORT, each confirmed by
a receipt. After an error, the ABORT waits at most 2 seconds for its receipt.

`pub move` subscribes to the source and, per message, SENDs a copy and ACKs the original, a batch
per transaction:

```bash
pub move stomp://broker/queue/DLQ.orders --to /queue/orders -s "region = 'EU'"
```

Copies keep the headers and properties as they arrived, and stay text or bytes. STOMP can't set a
message id, though, so the broker gives each copy a new one. Moving a destination onto itself is
refused.

## Browsing

`pub browse` uses ActiveMQ's `browser:true` subscription, which ends with a `browser: end`
message. Other brokers would consume the messages instead, so pub refuses (exit 1).
