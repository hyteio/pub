# MQTT

`mqtt://host[:1883]/topic`, `mqtts://host[:8883]/topic` and `mqtt+ssl://` (TLS). MQTT 5.0 by
default; `-O version=3.1.1` selects 3.1.1. ActiveMQ (5.x and 6.x) speaks only 3.1.1, so set
`version = 3.1.1` for it, in an alias or a `[module "mqtt"]` section; a broker that hangs up on
MQTT 5 gets a hint saying so.

A topic's `#` wildcard is kept even though URLs treat `#` as the start of a fragment, and a query
after it still sets options: `mqtt://broker/sensors/#?version=3.1.1` subscribes to `sensors/#`.
Topic filters are checked before subscribing: `#` only as the whole last level, `+` only as a
whole level.

Commands: `send`, `recv`, `unsub` (persistent sessions), `ping`.

The URL path is the topic without its leading `/` (use `//` or `--dest` for a topic that starts with
one). `+` and `#` work in URLs, so `pub recv 'mqtt://broker/sensors/#'` subscribes to
`sensors/#`. Quote URLs containing `#`.

| Setting | Default | |
|---|---|---|
| `version` | `5` | `5` or `3.1.1` |
| `qos` | `1` | 0, 1 or 2 for send and subscribe; checked against the broker's maximum QoS |
| `retain` | `false` | publish as a retained message (send an empty body to clear one) |
| `retain-handling` | `0` | on subscribe: 0 send retained messages, 1 only for a new subscription, 2 don't |
| `clean-start`, `session-expiry` | `true`, `0` | persistent sessions (below) |
| `keep-alive` | `60s` | pub pings while waiting, and fails (exit 28) if the broker stops answering |
| `client-id` | assigned by the broker | |

## Messages

`content-type`, `reply-to` (Response Topic), `correlation-id` (Correlation Data) and `ttl` (Message
Expiry, whole seconds) are MQTT 5 properties. `-H`/`-P` properties become user properties: their
own namespace, so no names are reserved, and repeats are kept on receive. MQTT has no priority,
type, persistence flag, groups, delivery delay or selectors; asking for them is an error in strict
mode.

`recv -i` shows the topic as `destination`, and QoS, retain, dup and the remaining expiry under
`# protocol`. pub tells the broker its `max-frame-size` as the Maximum Packet Size, and refuses to
send a message larger than the broker's.

## Durable subscriptions

A durable subscription is a persistent session. `pub recv mqtt://broker/t --durable NAME`
connects with client id `NAME` and `clean-start = false`, plus (MQTT 5) a session that never expires
unless `session-expiry` says otherwise. The broker keeps the subscription, and queues messages while
pub is away.

`pub unsub mqtt://broker --durable NAME` connects clean, which discards the session and
everything queued for it. MQTT doesn't say whether a session existed, so this always succeeds.

A durable name is one consumer's session, so use it with one topic filter. The session keeps every
subscription it has made: receive `orders` and later `alerts` under the same name, and the broker
delivers both. pub hands both on, so nothing is lost, and warns on the first message that doesn't
match the filter it was asked for.

## Will messages

`-O will-topic=status/app -O will-message=offline` (plus `will-qos`, `will-retain`, and on MQTT 5
`will-delay`). The broker publishes the will if pub's connection ends without a normal DISCONNECT,
for example when pub is killed or the network goes away.

On MQTT 5, `will-on-disconnect=true` publishes it even when pub exits normally (DISCONNECT reason
0x04). Mosquitto honours that; not every broker does.

## Shared subscriptions

`-O share-group=workers` subscribes to `$share/workers/<filter>`, so each message goes to one
member of the group. A `$share/GROUP/filter` written in the URL works too. A broker that advertises
no shared subscriptions is exit 1. Mosquitto supports them; ActiveMQ doesn't.

## Topic aliases

On MQTT 5, when the broker allows them, repeated sends to a topic carry a 2-byte alias instead of
its name (`topic-aliases=false` turns this off). pub lets the broker use up to
`topic-alias-maximum` (16, at most 1024) aliases toward it and resolves them, so received messages
always show the full topic.

## MQTT 3.1.1

3.1.1 has no properties, so it carries only the topic and body. Fields and `-H`/`-P` properties are
reported as "cannot carry" (errors in strict mode, otherwise dropped), and `session-expiry`,
`retain-handling`, will delays and subscription headers are refused. A session persists with
`clean-start = false`, until the next clean connect.

3.1.1 brokers can't refuse a PUBLISH: a message to a topic you may not publish to is silently
dropped. They refuse subscriptions only with a generic `0x80 Failure`.

Mosquitto accepts a subscription its ACL denies and simply delivers nothing, so that case shows up
as a receive timeout (exit 28), not a refusal.
