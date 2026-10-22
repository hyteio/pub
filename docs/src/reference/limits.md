# Limits and permissions

Everything pub reads is bounded, and every limit's error names the limit and how to raise it.

| What | Default | Setting |
|---|---|---|
| a frame from the server (all protocols; STOMP `content-length` is checked before the body arrives) | 64 MiB | `max-frame-size` |
| a message body from `-d`, `-f` or stdin | 64 MiB | `max-payload` |
| headers + properties per message, and subscription headers | 256 | `max-headers` |
| a header or property value / name | 64 KiB / 256 bytes | `max-header-value` / fixed |
| frames set aside while waiting for a reply | 10,000 | fixed |
| messages read by one `send` | 1,000,000 or 1 GiB | fixed |
| messages in one JSON array | 100,000 | fixed (use JSON Lines) |
| STOMP header lines per frame / bytes per line | 1024 / 64 KiB | fixed |
| `password-file` / TLS `key` / `cert` and `ca-file` | 4 KiB / 64 KiB / 4 MiB | fixed |
| DNS addresses tried / selector / destination | 16 / 4 KiB / 1 KiB | fixed |
| config files under `config.d` / bytes per file / bytes in total | 1024 / 64 KiB / 4 MiB | `core.config.max-files`, `max-file-size`, `max-total-size` |
| config depth | 16 | `core.config.max-depth` |
| config format: settings per section / in total, key, alias name, value, line | 256 / 16,384, 128 B, 253 B, 4 KiB, 8 KiB | fixed |
| any duration setting / any size setting | 150 years / 1 TiB | fixed |

Runtime limits are ordinary settings, so one alias can raise `max-frame-size` for a broker that
sends large messages. The `core.config.*` volume limits and `max-depth` govern how `config.d` is
read, so they're honoured only in the main `config`. Config files may not contain control
characters (tab and CRLF are fine).

## Config permissions

Configuration can turn off TLS verification or redirect credentials, so, like ssh, pub refuses
`$PUB_HOME`, `config` and everything under `config.d` unless it's owned by you or root and not
writable by group or others. Kubernetes ConfigMaps (root-owned, 0644) pass. To relax the check:

- `core.config.check-permissions = false` in the main `config` skips it for `config.d`;
- `PUB_CHECK_PERMISSIONS=false` in the environment skips it entirely.

The main config can't exempt itself: a writable config turning the check off is exactly the attack
being prevented.

For password and key files, see [Credentials](../credentials.md).
