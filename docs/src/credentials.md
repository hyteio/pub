# Credentials

| Setting | Meaning |
|---|---|
| `user` (or `username`) | the login name |
| `password` | the password itself; prefer one of the next three |
| `password-file` | a file holding the password (one trailing newline is removed) |
| `password-env` | the name of an environment variable holding it |
| `password-prompt` | ask on the terminal when no password is configured |

Precedence: URL userinfo or `-u` > `password` > `password-file` > `password-env`. A password from a
higher source means the lower ones are never read or checked, so `-u user:pass` works even when an
alias's `password-file` is missing.

## The prompt

As with curl, `-u user` without `:password` asks for the password on the terminal, with echo off,
so it never lands in shell history or `ps`. `-u user:` means an empty password.

- The prompt reads the terminal itself, so a payload piped on stdin still works
  (`echo hi | pub send … -u app`).
- `password-prompt = true` on an alias asks whenever no password is configured.
- With no terminal (scripts, CI), `-u user` falls back to configured passwords, and
  `password-prompt` is an error.
- Ctrl-D cancels. Ctrl-C ends pub with the terminal restored.

## Password files

```ini
[alias "prod"]
	url = amqs://mq01:61617
	user = app
	password-file = ~/.pub/prod.password
```

Sensitive files are checked the way ssh checks private keys; the error says how to fix it, and
`pub config check` applies the same rules:

| File | Must be owned by you | Most permissive mode | Symlinks | Loosen with (in `[core "config"]`) |
|---|---|---|---|---|
| `password-file` | yes | `0400` | refused | `secret-file-check-owner`, `secret-file-max-mode`, `secret-file-follow-symlinks` |
| TLS `key` | yes | `0600` | followed | `tls-key-check-owner`, `tls-key-max-mode`, `tls-follow-symlinks` |
| TLS `cert`, `ca-file` | – | – | followed | `tls-follow-symlinks` |

Each file is opened once (with `O_NOFOLLOW` when symlinks are refused) and checked on the open
handle, so the file checked is the file read. A named pipe or other special file is refused rather
than waited on.

Kubernetes-mounted secrets are symlinks, usually root-owned and mode 0644, so they need:

```ini
[core "config"]
	secret-file-follow-symlinks = true
	secret-file-check-owner = false
	secret-file-max-mode = 0644
```

## Where passwords don't appear

pub masks passwords wherever it prints: `-v` traces (in text and in hex dumps), error messages,
`pub alias list`, `alias show` and `config list`, both in URL userinfo and in `?password=` settings.
