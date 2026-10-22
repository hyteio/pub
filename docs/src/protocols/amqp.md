# AMQP 1.0

`amqp://host[:5672]/address`, `amqps://host[:5671]`. Written from the OASIS AMQP 1.0
specification, and tested against ActiveMQ, RabbitMQ 4 and the Azure Service Bus emulator.

Commands: `send`, `recv`, `browse`, `unsub`, `move`, `ping`.

| Setting | Default | |
|---|---|---|
| `client-id` | generated | the container id; durable subscriptions need a fixed one |
| `vhost` | the URL host | the hostname sent in SASL and open |
| `idle-timeout` | `60s` | give up after this much silence; pub sends empty frames at half the broker's timeout |
| `sasl-mechanisms` | auto | `PLAIN`, `ANONYMOUS`, `EXTERNAL`, in order of preference |
| `text-body` | `value` | `value` (an `amqp-value` string) or `data` (a data section marked text/plain) |
| `credit`, `prefetch-follows-count` | `1000`, `true` | link credit for receiving |
| `broker-type` | `auto` | `servicebus` or `other`, to override the [Service Bus](servicebus.md) detection |

## Logging in

SASL: EXTERNAL with a client certificate, PLAIN with a user (`-u`), otherwise ANONYMOUS;
`sasl-mechanisms` sets the order. `vhost` sets the hostname sent in SASL and open (RabbitMQ vhosts,
Azure).

## Addresses

`/queue/X` or `queue://X`, and `/topic/X` or `topic://X`, attach with the `queue` or `topic`
capability and, when the broker advertises them, its address prefixes. ActiveMQ sends
`queue-prefix` and `topic-prefix`, and ignores capabilities.

RabbitMQ's addresses are used as given: `/queues/orders`, `/exchanges/amq.topic/a.b`. RabbitMQ
never creates queues over AMQP 1.0, so a missing one is exit 2 (`amqp:not-found`).

## Messages

Messages follow ActiveMQ's JMS transformer, so JMS clients see what Qpid JMS would have sent:

- Text is an `amqp-value` string (a TextMessage), or with `text-body=data` a data section marked
  `text/plain; charset=utf-8`, for non-JMS consumers. Bytes are a data section.
- Correlation id, type (`subject`), priority, persistence (`durable`), ttl (the header ttl and an
  absolute expiry), reply-to, groups, content type and typed properties map natively.
  `delivery-delay` is `x-opt-delivery-delay`.
- On receive, an `amqp-value` string, or a data section with a textual UTF-8 content type, is
  `message-type: text`. Message annotations appear under `# protocol`; maps and sequences are kept
  raw, with `body-section`.

## Sending and receiving

Each send waits for the broker's disposition (at least once); rejected or released is an error.
Messages larger than the broker's frame size go in several transfer frames, within the broker's
session window.

Receiving, credit follows `-n` (`credit`, 1000; `prefetch-follows-count`). Each message is settled
after it has been written; with `--no-ack` the broker settles first, and credit never exceeds `-n`.

`-s` is a JMS selector (`apache.org:selector-filter:string`). A broker that leaves the selector out
of its attach reply hasn't applied it, so pub refuses (exit 1) rather than receive everything.

`--durable NAME` with a fixed `client-id` (the container id) is a durable subscription;
`pub unsub` removes it.

## Transactions and move

Transactions use part 4 of the specification: a link to the broker's coordinator declares and
discharges them, and sends and accepts in between carry the transaction id. `--transaction
[--batch N]` works on `send` and `recv`.

`pub move` receives from the source and, per message, sends a copy and accepts the original, a
batch per transaction. The copy is the message as received, with the same message id, properties
and body, and only `to` changed. ActiveMQ supports this. RabbitMQ doesn't, and refuses with
exit 1 (`amqp:not-implemented`); use [`ramqp://`](ramqp.md) for transactions there.

## Browsing

Browsing uses copy distribution, drained in batches to find the end. A broker that doesn't agree to
copy (RabbitMQ's classic queues) would consume instead, so pub refuses.

## ActiveMQ

ActiveMQ converts AMQP messages for JMS clients only with `transport.transformer=jms` on its `amqp`
connector. Its default is `native`, where OpenWire and JMS consumers see the raw AMQP bytes as a
BytesMessage. Its scheduler (`schedulerSupport="true"`) must be on for `delivery-delay`.
