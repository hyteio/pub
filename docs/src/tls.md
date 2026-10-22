# TLS

TLS is on for the TLS schemes (`stomps://`, `stomp+ssl://`, `mqtts://`, `amqs://`, `amqps://`,
`ramqps://`), or with `tls = true` on any other. pub uses rustls with the `ring` crypto provider; it
has no SSLv3, TLS 1.0 or 1.1, no CBC suites and no RSA key exchange.

| Setting | Flag | Meaning |
|---|---|---|
| `ca-file` | `--cacert` | CA certificates (PEM) to trust **instead of** the system's |
| `cert`, `key` | `--cert`, `--key` | a client certificate and key for mutual TLS |
| `insecure` | `-k`, `--insecure` | skip certificate verification (testing only) |
| `server-name` | | the name to verify, when it differs from the URL's host |
| `server-spiffe-id` | | verify a SPIFFE ID instead of the host name (below) |

## Trust

Without `ca-file`, servers are verified against the operating system's trusted roots; pub carries
no certificates of its own. A system with none (a `FROM scratch` container, say) is a TLS error
(exit 35) until you install them (the `ca-certificates` package) or set `ca-file`.

**`ca-file` replaces the system's roots**, as curl's
`--cacert` does: a server whose certificate comes from a public CA is refused unless that CA is in
the file too. That's what you want for a private CA: no public CA can vouch for your broker's name.

## Mutual TLS

`cert` and `key` present a client certificate. Certificate files may hold a full chain; keys may be
PKCS#8, PKCS#1 or SEC1 PEM. Files are read on every connection, so certificates rotated on disk by
an agent (cert-manager, spiffe-helper, Vault agent, an ACME client) are picked up by the next
command. The key must be yours and not readable by others (see [Credentials](credentials.md)).

## Versions, cipher suites and groups

Like every setting, these work in `[defaults]`, per protocol, per alias, in the URL or as flags, so
a strict default can be relaxed for one broker:

```ini
[defaults]
	tls-min-version = 1.3            # the default
[alias "legacy-mq"]
	url = stomps://old-broker:61612
	tls-min-version = 1.2
```

| Setting | Flag | Default |
|---|---|---|
| `tls-min-version` | `--tlsv1.2`, `--tlsv1.3` | `1.3` (lowered to match when only `tls-max-version` is set) |
| `tls-max-version` | `--tls-max` | `1.3` |
| `tls-cipher-suites` (in order) | `--ciphers` | the library's |
| `tls-groups` (key exchange, in order) | `--curves` | the library's (X25519, secp256r1, secp384r1) |

`pub config settings` lists the supported names; `-v` shows the negotiated version, suite and group.
With TLS 1.2, `tls-groups` also limits which ECDSA certificate curves are accepted, so keep the
server certificate's curve in the list.

Post-quantum key exchange isn't available yet: the `ring` library has no ML-KEM.

## SPIFFE

`server-spiffe-id = spiffe://example.org/ns/prod/sa/broker` (or `spiffe://example.org/*` for any
workload in the trust domain) verifies the broker's X.509-SVID instead of its host name. The chain
is validated against `ca-file` only (the trust bundle, such as spiffe-helper's `bundle.pem`), and
the certificate's single `spiffe://` URI SAN must match. Combine it with `cert` and `key` pointing at
your own SVID:

```ini
[alias "jms"]
	url = stomps://mq01.internal:61612
	server-spiffe-id = spiffe://example.org/ns/prod/sa/broker
	ca-file = /run/spiffe/bundle.pem
	cert = /run/spiffe/svid.pem
	key = /run/spiffe/svid_key.pem
```
