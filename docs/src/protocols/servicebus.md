# Azure Service Bus

Azure Service Bus speaks AMQP 1.0, so pub reaches it through the [AMQP 1.0](amqp.md) module:
`amqps://NAMESPACE.servicebus.windows.net`, or Microsoft's emulator on `amqp://localhost`.

Log in with a shared access key as user and password, ideally from a password file:

```ini
[alias "sb"]
	url = amqps://mynamespace.servicebus.windows.net
	user = RootManageSharedAccessKey
	password-file = ~/.pub/sb.key
```

```bash
pub send sb/orders -d hello
pub recv sb/events/Subscriptions/audit -n 0 -m 5s
pub browse sb/orders
```

## What's different

pub recognises Service Bus from the host name (`*.servicebus.*`) or the container id its gateway
reports; `broker-type = servicebus` or `other` overrides that. It then adapts:

- **Addresses are entity paths**: a queue `orders`, a topic `events`, a subscription
  `events/Subscriptions/audit`.
- **`browse` peeks** through the entity's `$management` node (`com.microsoft:peek-message`), so
  nothing is locked or consumed. Service Bus would otherwise accept AMQP's copy distribution and
  delete the messages.
- **`delivery-delay`** is sent as `x-opt-scheduled-enqueue-time`, which Service Bus honours.
- **Transactions** work within one entity.

What Service Bus can't do is refused with exit 1, rather than done halfway:

| Asked for | Instead |
|---|---|
| a selector (`-s`) | filter with a subscription rule, and receive from `TOPIC/Subscriptions/NAME` |
| `--durable`, `unsub` | subscriptions are entities: create them in the portal, the CLI or the emulator's config |
| `move` | Service Bus transactions can't span two entities |

Creating and listing entities is also done in the portal, the CLI or the emulator's config, not
over AMQP.

## The emulator

The interop suite can run against Microsoft's emulator (see [Testing and CI](../development/testing.md)).
The emulator accepts any key and speaks only plain AMQP, so logins and TLS are tested only against
a real namespace.
