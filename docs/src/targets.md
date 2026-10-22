# Targets and aliases

A target is a URL or an alias.

## URLs

```text
scheme://[user[:password]@]host[:port][/destination][?setting=value&...]
```

The scheme picks the module (`pub modules` lists them); the module decides which commands it
supports and how the path becomes a destination. The query sets any setting:

```bash
pub ping 'stomps://app:pw@mq01:61612?receive-timeout=2s&insecure=true'
```

No configuration file is needed: a URL carries everything.

## Aliases

An alias is a named URL with settings, kept in a git-style config file:

```ini
# ~/.pub/config.d/jms.company.com.conf
[alias "jms.company.com"]
	url = stomp+ssl://mq01.company.com:61612
	user = app
	password-env = JMS_PASSWORD
```

`pub alias add jms.company.com stomp+ssl://mq01.company.com:61612 --set user=app` writes that file.
Anything after the alias name is appended to its URL's path, and a query after it overrides the
alias's settings:

```bash
pub send jms.company.com/queue/orders -f order.json
# → stomp+ssl://app@mq01.company.com:61612/queue/orders

pub send 'jms.company.com/queue/orders?receipts=false' -d hi
```

The path after an alias can't contain `.` or `..` segments, and a `#` is kept as the URL's
fragment (MQTT reads it as the multi-level wildcard).

| Command | Does |
|---|---|
| `pub alias list` | names and URLs (passwords masked) |
| `pub alias show NAME` | the alias's settings, with the file and line each came from |
| `pub alias add NAME URL [--set k=v]...` | writes `config.d/NAME.conf` (mode 0600 when it holds a secret) |
| `pub alias remove NAME` | deletes it |
| `pub alias resolve TARGET` | what a target resolves to: URL, module, destination, settings and where each came from |

`alias add` validates the URL and settings first; `--force` writes them anyway.

## A default scheme

With `default-scheme` set in the `[core]` section, a bare `host:port` that isn't an alias uses it:

```ini
[core]
	default-scheme = stomp
```
