# Message formats

`-F` (or the `format` setting, layered like any other) picks how `recv` writes messages and how
`send` reads them. Formats serialise pub's protocol-neutral message, so any format works with any
protocol. `pub formats` lists what's compiled in.

| Format | `recv` writes | `send` reads |
|---|---|---|
| `raw` (default) | bodies only; `-l` adds a newline after each | the whole input as one body (`-d`, `-f`, stdin) |
| `text` | `-i`'s layout: `# standard`, `# properties`, `# protocol`, a blank line, the body | — |
| `json` | one object per message per line (JSON Lines) | objects, JSON Lines, pretty-printed objects, arrays |
| `xml` | a `<messages version="1">` document, one `<message>` each | those documents, one after another, or single `<message>`s |

`-o FILE` appends to a file, created readable only by you, since captured messages may be sensitive.
The file is opened only after the command has been checked, so a mistake doesn't leave an empty one.

## JSON

A document, version 1:

```json
{"pub":1,"destination":"queue://orders",
 "fields":{"message-id":"ID:…","correlation-id":"c-1","priority":7,"ttl":90000,"timestamp":1790998128281},
 "properties":{"region":"EU","ok":true,"ratio":0.5,"n":{"int":5},"big":{"long":1099511627776}},
 "protocol":{"message-class":"MapMessage"},
 "body":{"type":"text","data":"hello"}}
```

- **fields**: pub's standard fields by name (JMS names like `JMSCorrelationID` accepted on input).
  Numbers and booleans are JSON numbers and booleans. Durations (`ttl`, `delivery-delay`) are whole
  milliseconds, or a string like `"30s"`; times are milliseconds since the epoch.
- **properties**: strings, booleans and doubles plain; other types say so: `{"int":5}`,
  `{"long":…}`, `{"short":…}`, `{"byte":…}`, `{"float":0.1}`, `{"double":"NaN"}`. A plain integer on
  input is a `long`.
- **body**: `type` is `text` or `bytes` (absent when the protocol doesn't say); the content is
  `data` (a string) or `base64`. `format.json.body=auto` embeds text bodies that are JSON as
  `"json": <value>`: readable, but not byte-exact on the way back. `format.json.pretty=true`
  indents.
- Unknown keys, fields or property types are errors that name the message
  (`message 3: unknown key 'bdy'`).
- A top-level array may hold up to 100,000 messages; use JSON Lines for more.

## XML

The same information, as elements:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<messages version="1">
  <message destination="queue://orders">
    <fields>
      <field name="correlation-id">c-1</field>
      <field name="ttl">90000ms</field>
    </fields>
    <properties>
      <property name="region">EU</property>
      <property name="n" type="int">5</property>
    </properties>
    <protocol>
      <header name="message-class">MapMessage</header>
    </protocol>
    <body type="text">hello</body>
  </message>
</messages>
```

- Properties are strings unless `type` says otherwise; durations carry a unit; protocol headers keep
  repeats.
- Bodies are byte-exact: `\r` is written as `&#13;` (XML parsers turn line ends into `\n`), nothing
  inside `<body>` is indented, and text XML can't hold (control characters), like bytes, is
  `encoding="base64"`. Attribute values keep tabs and line feeds the same way.
- On input, CDATA, comments and character entities are fine. A DTD is ignored and a custom entity
  refused, so a document can't expand or pull in anything. Unknown elements or attributes are
  errors naming the message.

## Sending documents

`send -F json` (or `-F xml`) reads every message in the input and checks them all before
connecting, so a bad record sends nothing. It reads at most a million messages or 1 GiB in one
send; split larger inputs.

- `-H`, `-P` and `-t` override what the documents say; `-n N` sends the whole set N times.
- The document's destination is ignored unless `--dest-from-input` is given; the URL path or
  `--dest` is then the fallback. Destination names are protocol-specific (`queue://x` for
  OpenWire, `/queue/x` for STOMP), so replaying across protocols may need `--dest`.
- What a captured message keeps (its id, timestamp and expiry) depends on the
  [header strategy](replay.md).
