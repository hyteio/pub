# Exit codes

Borrowed from curl where they line up, so scripts can branch on them.

| Code | Meaning |
|---|---|
| 0 | success |
| 1 | the protocol or command isn't supported (by the module, or by the broker) |
| 2 | usage: a bad command line, an unknown destination, a refused selector |
| 3 | a bad URL, alias, setting or configuration file |
| 7 | couldn't connect |
| 8 | a protocol error: the broker sent something unexpected or reported an error |
| 26 | local or socket I/O failed |
| 28 | a timeout, including `receive` with no message within `-m` |
| 35 | TLS: a handshake or certificate failure |
| 67 | the broker refused the login |

Errors go to stderr as `pub: …`; warnings as `pub: warning: …`.
