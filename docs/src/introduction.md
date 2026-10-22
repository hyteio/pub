<img class="banner" src="images/banner.svg" alt="pub — the curl for messaging">

`pub` sends, receives, browses and moves messages on message brokers from the command line, the way
`curl` talks to web servers: one command, a URL, and flags.

```bash
pub send amq://mq01:61616/queue/orders -d '{"id":1}' -P orderId:long=1
pub recv jms.company.com/queue/orders -n 10 -m 5s -i
pub ping jms.company.com -v
pub recv 'mqtt://broker/sensors/#' -O qos=1 -i
pub send mqtt://broker/sensors/kitchen -d 21.5 -H 'unit: C' -O retain=true
```

## Protocols

| Protocol | Schemes | Brokers |
|---|---|---|
| [ActiveMQ OpenWire](protocols/openwire.md) v12 | `amq://`, `amqs://` | ActiveMQ 6.x and 5.17+ |
| [AMQP 1.0](protocols/amqp.md) | `amqp://`, `amqps://` | ActiveMQ, RabbitMQ 4, Qpid, [Azure Service Bus](protocols/servicebus.md) |
| [MQTT](protocols/mqtt.md) 5.0 and 3.1.1 | `mqtt://`, `mqtts://`, `mqtt+ssl://` | ActiveMQ, Mosquitto, EMQX, HiveMQ, … |
| [RabbitMQ AMQP 0-9-1](protocols/ramqp.md) | `ramqp://`, `ramqps://` | RabbitMQ, LavinMQ |
| [STOMP](protocols/stomp.md) 1.0–1.2 | `stomp://`, `stomps://`, `stomp+ssl://` | ActiveMQ, RabbitMQ, any STOMP broker |

Every protocol is an optional module: a build can include only the ones it needs (see
[Installation](installation.md)).

## What it does

- **Send** a body from the command line, a file or stdin, with headers and typed properties; or
  whole captured messages from a JSON or XML document.
- **Receive** with selectors, durable subscriptions and transactions, and write messages as raw
  bodies, a readable layout, JSON Lines or XML.
- **Browse** a queue without consuming it, **move** messages between destinations (a dead-letter
  queue back to its source, say), and **list, create and delete** destinations where the protocol
  allows.
- **Replay** captured messages as new ones, or as exact copies when moving them between brokers.
- **Connect securely**: TLS 1.2/1.3, mutual TLS, SPIFFE identities, and passwords from files,
  environment variables or a prompt.

Configuration uses git-style files with aliases, so `pub send prod/queue/orders` can stand for a
full URL with TLS settings and credentials. No configuration is needed to start: a URL carries
everything.

Start with [Getting started](getting-started.md).
