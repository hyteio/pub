# Network and sockets

These apply to every module's connections. `0` turns off a timeout or a buffer override.

| Setting | Default | Why |
|---|---|---|
| `connect-timeout` | `5s` | per address for the TCP connect, then again for the TLS handshake; a healthy broker answers in milliseconds |
| `send-timeout` | `10s` | a socket write blocked this long means the broker has stopped reading |
| `receive-timeout` | `10s` | how long to wait for a reply the server owes (handshake, receipt); not the wait for messages, which is `recv -m` |
| `tcp-nodelay` | `true` | messaging is small request/reply frames; Nagle's algorithm only adds latency |
| `tcp-keepalive` | `true` | probes idle connections, so a NAT or firewall dropping one during a long `recv` is noticed (`--no-keepalive` to turn off) |
| `tcp-keepalive-time` | `30s` | idle time before the first probe (`--keepalive-time`); `0` = the OS default (2 hours on Linux and macOS) |
| `tcp-keepalive-interval` | `10s` | between unanswered probes (`--keepalive-interval`); `0` = the OS default (75s) |
| `tcp-keepalive-count` | `3` | unanswered probes before the connection is declared dead (`--keepalive-count`); `0` = the OS default (9 on Linux) |
| `receive-buffer`, `send-buffer` | the OS's | sizes such as `256k` or `4m`; see below |

With these keepalive defaults, a silently dead connection is detected after about
30s + 3 × 10s = 1 minute, rather than over 2 hours. They're set per socket, so system-wide settings
are untouched. Interval and count can be set on Linux, macOS, Windows, FreeBSD, NetBSD, DragonFly
and illumos; elsewhere `-v` warns and the OS values apply.

Protocols have their own liveness checks on top: STOMP heart-beats, MQTT keep-alive, OpenWire's
inactivity monitor and AMQP idle timeouts, described in each protocol's chapter.

## Buffers

**Socket buffers are left to the OS by default.** Linux, macOS and Windows all tune socket buffers
and the TCP window automatically. Setting `receive-buffer` or `send-buffer` pins the size and turns
that tuning off for the socket, so only do it for a reason, such as a high-latency link where the
automatic ceiling is too low.

Overrides are applied before connecting, so the window scale is negotiated to match. The kernel may
still cap them (`net.core.rmem_max` / `wmem_max`), and Linux reports double the requested value.
`-v` shows requested and applied sizes when overridden; `-vv` always shows what the OS chose.

## DNS

Names are resolved by the system resolver, with its own timeouts; `connect-timeout` starts once
addresses are known. Up to 16 addresses are tried in turn.
