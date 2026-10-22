# Replaying messages

Messages captured with `recv -F json` (or `-F xml`) can be sent again with `send -F json`. A
**header strategy** decides what they keep: `--header-strategy NAME`, or the `header-strategy`
setting. `pub formats` lists the strategies.

| Strategy | Message id, timestamp | Expiry | Use |
|---|---|---|---|
| `default` | new (assigned at send) | the original time to live, counted from now: `ttl = expiration - timestamp` | replaying, testing, copying traffic |
| `administrative` (`admin`) | exact | the exact original expiration | moving or restoring messages between brokers |

```bash
pub recv broker-a/queue/orders -n 0 -m 5s -F json > orders.jsonl
pub send broker-b/queue/orders -F json -f orders.jsonl                            # as new messages
pub send broker-b/queue/orders -F json -f orders.jsonl --header-strategy admin    # exact copies
```

Both strategies drop:

- **consumer-only** fields: `redelivered`, `delivery-count`;
- **provider-only** headers: the document's `protocol` section, and properties brokers add
  themselves (the JMS provider-set `JMSXUserID`, `JMSXAppID`, …, and broker-internal properties such
  as `__AMQ_*`, `_AMQ_*`, `__HDR_*`).

`-v` lists what was dropped. A `ttl` in the document or from `-H` wins over the computed one. A
message with an expiration but no timestamp (sent with `jms.disableTimeStampsByDefault`) has no ttl
to recover, and is sent without one (noted under `-v`).

`administrative` needs a protocol that lets the client set those fields. OpenWire does: ActiveMQ ids
are parsed into producer and sequence as Java does, and other ids, like UUIDs, travel as the id's
text. STOMP and MQTT can't, so in strict mode it's an error naming the field.

ActiveMQ's duplicate detection silently drops a message whose id it has already seen on that
destination, so an administrative replay of the same capture twice into an ActiveMQ queue
delivers it once.

To move messages within one broker (a dead-letter queue back to its source, say), `pub move` is
simpler and transactional; see each protocol's chapter.
