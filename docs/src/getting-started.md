# Getting started

## Send and receive

A target is a URL: the scheme picks the protocol, the path is the destination.

```bash
pub send stomp://localhost:61613/queue/orders -d 'hello'
pub recv stomp://localhost:61613/queue/orders
```

`recv` (or `receive`) waits for one message, writes its body to stdout and acknowledges it.
Useful flags:

| Flag | Meaning |
|---|---|
| `-n N` | receive N messages (`-n 0`: until `-m` expires) |
| `-m 5s` | stop waiting after 5 seconds (exit 28 if nothing arrived) |
| `-i` | show headers and properties before each body |
| `-l` | a newline after each body |
| `-s "region = 'EU'"` | a JMS selector |
| `-F json` | whole messages as JSON Lines (see [Message formats](formats.md)) |

```bash
pub recv amq://broker/queue/orders -n 0 -m 5s -i
```

## Bodies, headers and properties

```bash
pub send amq://broker/queue/orders -f order.json -t application/json \
    -H 'correlation-id: c-1' -H 'ttl: 5m' -P orderId:long=42 -P region=EU
echo 'from stdin' | pub send mqtt://broker/sensors/kitchen
```

- `-d DATA`, `-d @file`, `-f file`, `-f -` or piped stdin give the body.
- `-H 'name: value'` sets a standard field (`correlation-id`, `priority`, `ttl`, `persistent`, …, or
  its JMS name such as `JMSCorrelationID`), or else a string property.
- `-P name[:type]=value` sets a typed property (`int`, `long`, `bool`, `double`, …).
- `-n N` sends the message N times.

See [Messages](messages.md) for how each part maps onto each protocol.

## Credentials

```bash
pub ping amqs://broker -u app            # prompts for the password, echo off
pub ping amqs://app:secret@broker        # in the URL (visible in shell history)
```

Better, keep them in configuration (see [Credentials](credentials.md)).

## Aliases

```bash
pub alias add prod stomp+ssl://mq01.company.com:61612 --set user=app --set password-env=MQ_PASSWORD
pub send prod/queue/orders -d hi
pub alias resolve prod/queue/orders
```

An alias stands for a URL plus settings; anything after its name is appended to the URL's path.
See [Targets and aliases](targets.md).

## Seeing what happens

`-v` traces the conversation on stderr, curl-style: `*` for information, `>` for bytes sent, `<`
for bytes received. Passwords are masked, in hex dumps too. `-vv` shows whole frames.

```bash
pub ping prod -v
```

`pub modules` lists the protocols compiled in and the commands each supports; `pub config settings`
lists every setting (also in the [Settings reference](reference/settings.md)).

## Exit codes

Scripts can branch on the exit code: 0 success, 2 a usage error, 7 can't connect, 28 a timeout
(including `recv` with no message), 67 a refused login, and so on (see
[Exit codes](reference/exit-codes.md)).
