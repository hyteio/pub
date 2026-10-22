# RabbitMQ AMQP 0-9-1

`rampq://host[:5672]/destination`, `rampqs://host[:5671]` (TLS). "RabbitMQ AMQP 0.9.1": written
from the 0-9-1 specification plus RabbitMQ's extensions, and tested against RabbitMQ 4.

Commands: `send`, `recv`, `unsub`, `move`, `dest create` and `dest delete`, `ping`.

| Setting | Default | |
|---|---|---|
| `vhost` | `/` | the virtual host (an unknown one is exit 67) |
| `heartbeat` | `60s` | the smaller of ours and the broker's; `0` takes the broker's |
| `confirms` | `true` | publisher confirms |
| `mandatory` | queues yes, exchanges no | an unroutable message is an error |
| `prefetch`, `prefetch-follows-count` | `1000`, `true` | `basic.qos` for receiving |
| `sasl-mechanisms` | auto | EXTERNAL with a client certificate, PLAIN with a user, otherwise ANONYMOUS |
| `exchange-type`, `queue-type` | `topic`, the broker's | for what pub declares |
| `connection-name` | – | shown in RabbitMQ's management UI |

## Destinations

- `/queue/NAME` is a queue, published to through the default exchange.
- `/exchange/NAME/KEY` publishes to an exchange with a routing key; `/topic/KEY` is shorthand for
  `/exchange/amq.topic/KEY`. A `#` in a key is kept, though URLs treat it as a fragment, so quote
  such URLs.
- Receiving from an exchange binds a private, exclusive queue with the key.
- The vhost is the `vhost` setting, not part of the path.

## Sending

With publisher confirms (the default), each send waits for the broker's ack. Messages to queues are
`mandatory`, so a missing queue comes back as `NO_ROUTE` (exit 2); messages to exchanges aren't,
unless `-O mandatory=true`. Fields map to AMQP basic properties (content type, correlation id,
reply-to, expiration from `ttl`, message id, timestamp, type, priority, delivery mode); `-H`/`-P`
properties go in the headers table with their types.

## Receiving

`basic.consume`, with the prefetch following `-n`. Each message is acked after it has been written;
with `--no-ack`, as it arrives. pub always acks messages itself, since RabbitMQ ignores the
prefetch for auto-ack consumers and would push the whole queue. There are no selectors and no
browse.

## Durable subscriptions

`--durable NAME` on an exchange or topic declares the durable queue `NAME` (`queue-type` picks
classic, quorum or stream) and binds it. `pub unsub` deletes it, and exits 2 if it doesn't
exist.

## Transactions and move

`--transaction [--batch N]` uses `tx.select`, `tx.commit` and `tx.rollback`.

`pub move` doesn't use transactions: RabbitMQ reports an unroutable publish in a transaction only at
commit, after the source has already been acknowledged. Instead it publishes each copy as mandatory,
waits for the broker's confirm, and only then acknowledges the original. A move to a queue that
doesn't exist therefore fails and leaves the messages where they were. `--batch` has no effect.

## Destinations

`pub dest create` and `pub dest delete` declare or delete `/queue/NAME` and `/exchange/NAME`
(`exchange-type`, default `topic`). AMQP 0-9-1 can't list them; use RabbitMQ's management API.

## With AMQP 1.0 clients

RabbitMQ 4 serves 0-9-1 and 1.0 on one port. An AMQP 1.0 `amqp-value` string reaches 0-9-1
consumers as encoded bytes, so send with `-O text-body=data` on `amqp://` when 0-9-1 clients read
the same queues.
