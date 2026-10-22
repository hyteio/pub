# Configuration

pub reads `$PUB_HOME` (default `~/.pub`): the file `config`, then every file under `config.d/`,
recursively, in order of relative path (`a.conf` < `team/x.conf` < `z.conf`). Later files win.
`pub config init` creates the directory with a commented starter `config`; `pub config path`
prints where it is.

- Subdirectories are read up to `core.config.max-depth` (default 16; 0 = flat). Deeper nesting is an
  error naming the path.
- Hidden files and directories are skipped, as are editor leftovers (`~`, `.swp`, `.bak`).
  Symlinks are followed, and a directory reached twice is read once.
- Kubernetes ConfigMap volumes work as they are: mount one at, say, `~/.pub/config.d/brokers`, and
  each key is read once, through its top-level symlink (kubelet's `..data` directories are hidden).

## Layers

A setting can be given in several places. The more specific wins:

```text
[defaults]  <  [module "<name>"]  <  [protocol "<scheme>"]  <  [alias "<name>"]  <  URL query  <  command line
```

`[module "stomp"]` applies to every scheme that module handles (`stomp`, `stomps`, `stomp+ssl`);
`[protocol "stomps"]` to one scheme. On the command line, `-O key=value` sets any setting, and many
have flags of their own (`--connect-timeout`, `--cacert`, …).

```ini
[defaults]
	connect-timeout = 3s
[module "openwire"]
	jms.prefetchPolicy.queuePrefetch = 10
[alias "prod"]
	url = amqs://mq01.company.com:61617
	user = app
	password-file = ~/.pub/prod.password
```

`pub config list` shows every setting with the file and line it came from; `pub alias resolve
TARGET` shows the result for one target.

## Validation

Every setting is declared in a schema with its type, default and description; `pub config settings
[module]` prints it, and the [Settings reference](reference/settings.md) has it all. Values are
checked with the same parsers used when connecting:

| Where the setting came from | Unknown key | Invalid value |
|---|---|---|
| `-O`, flags, URL query | error (exit 3) with "did you mean" | error (exit 3) |
| `pub alias add --set` | error unless `--force` | error unless `--force` |
| config files | error in strict mode (the default), else a warning; with file:line and "did you mean" | error (exit 3) |
| a `[defaults]` key another module understands | fine | error |

**Strict mode is on by default** (`core.config.strict`): unknown keys and sections in config files
are errors, so a typo can't silently fall back to a default. To only warn:

```ini
[core "config"]
	strict = false
```

`pub config check` validates everything at once (sections, keys, types, alias URLs and schemes, and
that `ca-file`, `cert` and `key` files exist) and exits 3 if there are errors. It's handy in CI or
after editing.

## Other names for settings

A module can accept other names for pub's settings, so configuration pasted from another client
works. The OpenWire module accepts Java's URI options, such as `connectionTimeout=30000`
(milliseconds) for `connect-timeout`. These names are matched case-insensitively and rewritten to
pub's before validation. The value from the more specific layer wins, whichever name it used;
setting both names in one place uses pub's name, with a warning. `pub alias resolve` marks values
that came from another name (`connect-timeout 30000ms (from connectionTimeout)`).

A module can also declare **inert settings**: options that matter to another client but not to a
CLI, such as Java's `jms.copyMessageOnSend` or the whole `jms.redeliveryPolicy.*` family. They are
accepted so pasted configuration keeps working, never cause an error (even in strict mode), and
`-v` notes each one with the reason.

`pub config settings <module>` lists a module's other names and inert settings.

## Permissions

Configuration can turn off TLS verification or redirect credentials, so, like ssh, pub refuses
`$PUB_HOME`, `config` and everything under `config.d` unless it's owned by you or root and not
writable by group or others. Kubernetes ConfigMaps (root-owned, 0644) pass. See
[Limits and permissions](reference/limits.md) for how to relax the check.
