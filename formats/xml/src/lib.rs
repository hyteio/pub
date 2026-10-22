//! XML message format for pub (`-F xml`): the same information as the JSON
//! format (`formats/json`), as elements.
//!
//! ```xml
//! <?xml version="1.0" encoding="UTF-8"?>
//! <messages version="1">
//!   <message destination="queue://orders">
//!     <fields>
//!       <field name="correlation-id">c-1</field>
//!       <field name="ttl">90000ms</field>
//!     </fields>
//!     <properties>
//!       <property name="region">EU</property>
//!       <property name="n" type="int">5</property>
//!     </properties>
//!     <protocol>
//!       <header name="message-class">MapMessage</header>
//!     </protocol>
//!     <body type="text">hello</body>
//!   </message>
//! </messages>
//! ```
//!
//! - `field`: pub's standard fields by name (aliases accepted on input);
//!   durations carry a unit (`90000ms`), times are milliseconds since the epoch.
//! - `property`: `type` is one of string (the default), bool, byte, short, int,
//!   long, float, double.
//! - `header`: protocol headers, repeats kept.
//! - `body`: `type` text or bytes (absent when the protocol doesn't say);
//!   `encoding="base64"` for bytes and for text XML can't hold (control
//!   characters). Any value element may use `encoding="base64"` likewise.
//! - Bodies are byte-exact: `\r` is written as `&#13;` (XML would otherwise
//!   turn line ends into `\n`), and nothing inside `<body>` is indented.
//!
//! Reading accepts `<messages>` documents or single `<message>`s, one after
//! another (as `receive -F xml -o FILE` appends them); it reads one message at
//! a time.

use std::cell::Cell;
use std::io::{self, BufReader, Read, Write};
use std::rc::Rc;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use pub_core::format::{write_error, Format, FormatContext, MessageWriter, Record, RecordReader};
use pub_core::message::{FieldKind, Property};
use pub_core::payload::is_printable;
use pub_core::{bail_kind, err, BodyType, Field, Message, Result, Value};
use quick_xml::events::{BytesStart, Event};

/// The document version written as `<messages version="1">`.
pub const VERSION: &str = "1";

pub struct XmlFormat;

pub fn format() -> Box<dyn Format> {
    Box::new(XmlFormat)
}

// ---- writing

/// Characters XML 1.0 documents may contain.
fn xml_char(c: char) -> bool {
    matches!(c, '\t' | '\n' | '\r' | '\u{20}'..='\u{D7FF}' | '\u{E000}'..='\u{FFFD}' | '\u{10000}'..='\u{10FFFF}')
}

/// Escaped for element text or an attribute value; `\r` as a character
/// reference so it survives end-of-line normalization.
fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\r' => out.push_str("&#13;"),
            c => out.push(c),
        }
    }
    out
}

/// Escaped for an attribute value: tabs and line feeds too, which attribute
/// value normalization would otherwise turn into spaces.
fn escape_attr(s: &str) -> String {
    escape(s).replace('\t', "&#9;").replace('\n', "&#10;")
}

/// `<tag attrs>value</tag>`, base64 when XML can't hold the value.
fn element(out: &mut String, indent: &str, tag: &str, attrs: &[(&str, &str)], value: &str) {
    out.push_str(indent);
    out.push('<');
    out.push_str(tag);
    for (k, v) in attrs {
        out.push_str(&format!(" {k}=\"{}\"", escape_attr(v)));
    }
    if value.chars().all(xml_char) {
        out.push('>');
        out.push_str(&escape(value));
    } else {
        out.push_str(" encoding=\"base64\">");
        out.push_str(&BASE64.encode(value.as_bytes()));
    }
    out.push_str(&format!("</{tag}>\n"));
}

fn attr_ok(s: &str) -> Result<()> {
    if s.chars().all(xml_char) {
        Ok(())
    } else {
        bail_kind!(Usage, "name '{}' can't be written in XML", s.escape_debug())
    }
}

/// A float in its shortest form (0.1f32 as 0.1).
fn value_text(v: &Value) -> (Option<&'static str>, String) {
    match v {
        Value::String(s) => (None, s.clone()),
        Value::Float(f) => (Some("float"), f.to_string()),
        other => (Some(other.type_name()), other.to_string()),
    }
}

/// One message as a `<message>` element (two-space indented, inside `<messages>`).
pub fn to_xml(msg: &Message) -> Result<String> {
    let mut out = String::new();
    match msg.field(Field::Destination) {
        Some(d) => {
            attr_ok(d)?;
            out.push_str(&format!("  <message destination=\"{}\">\n", escape_attr(d)));
        }
        None => out.push_str("  <message>\n"),
    }
    let fields: Vec<_> = msg
        .fields
        .iter()
        .filter(|(f, _)| **f != Field::Destination)
        .collect();
    if !fields.is_empty() {
        out.push_str("    <fields>\n");
        for (f, v) in fields {
            let v = if f.info().kind == FieldKind::Duration {
                format!("{v}ms")
            } else {
                v.clone()
            };
            element(&mut out, "      ", "field", &[("name", f.name())], &v);
        }
        out.push_str("    </fields>\n");
    }
    if !msg.properties.is_empty() {
        out.push_str("    <properties>\n");
        for p in &msg.properties {
            attr_ok(&p.name)?;
            let (ty, text) = value_text(&p.value);
            let mut attrs = vec![("name", p.name.as_str())];
            if let Some(t) = ty {
                attrs.push(("type", t));
            }
            element(&mut out, "      ", "property", &attrs, &text);
        }
        out.push_str("    </properties>\n");
    }
    if !msg.protocol.is_empty() {
        out.push_str("    <protocol>\n");
        for (k, v) in &msg.protocol {
            attr_ok(k)?;
            element(&mut out, "      ", "header", &[("name", k.as_str())], v);
        }
        out.push_str("    </protocol>\n");
    }
    let mut attrs: Vec<(&str, &str)> = Vec::new();
    if let Some(t) = msg.body_type {
        attrs.push(("type", t.name()));
    }
    let text = match msg.body_type {
        Some(BodyType::Bytes) => None,
        Some(BodyType::Text) => std::str::from_utf8(&msg.body).ok(),
        None => std::str::from_utf8(&msg.body)
            .ok()
            .filter(|_| is_printable(&msg.body)),
    }
    .filter(|s| s.chars().all(xml_char));
    out.push_str("    <body");
    for (k, v) in &attrs {
        out.push_str(&format!(" {k}=\"{v}\""));
    }
    match text {
        Some(s) => {
            out.push('>');
            out.push_str(&escape(s));
        }
        None => {
            out.push_str(" encoding=\"base64\">");
            out.push_str(&BASE64.encode(&msg.body));
        }
    }
    out.push_str("</body>\n  </message>\n");
    Ok(out)
}

struct XmlWriter {
    out: Box<dyn Write>,
}

impl XmlWriter {
    fn put(&mut self, s: &str) -> Result<()> {
        self.out.write_all(s.as_bytes()).map_err(write_error)?;
        self.out.flush().map_err(write_error)
    }
}

impl MessageWriter for XmlWriter {
    fn begin(&mut self) -> Result<()> {
        self.put(&format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<messages version=\"{VERSION}\">\n"
        ))
    }

    fn write(&mut self, msg: &Message) -> Result<()> {
        let xml = to_xml(msg)?;
        self.put(&xml)
    }

    fn finish(&mut self) -> Result<()> {
        self.put("</messages>\n")
    }
}

// ---- reading

/// A parsed element: just what pub's documents use.
#[derive(Debug, Default)]
struct Elem {
    name: String,
    attrs: Vec<(String, String)>,
    children: Vec<Elem>,
    text: String,
}

impl Elem {
    fn attr(&self, name: &str) -> Option<&str> {
        self.attrs
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    /// The element's text, decoded from base64 when it says so.
    fn value(&self) -> Result<String> {
        match self.attr("encoding") {
            None => Ok(self.text.clone()),
            Some("base64") => {
                let bytes = BASE64
                    .decode(self.text.trim())
                    .map_err(|e| err!(Usage, "<{}>: invalid base64: {e}", self.name))?;
                String::from_utf8(bytes)
                    .map_err(|_| err!(Usage, "<{}>: base64 value is not UTF-8", self.name))
            }
            Some(other) => bail_kind!(Usage, "<{}>: unknown encoding '{other}'", self.name),
        }
    }

    fn check_attrs(&self, allowed: &[&str]) -> Result<()> {
        for (k, _) in &self.attrs {
            if !allowed.contains(&k.as_str()) {
                bail_kind!(Usage, "unknown attribute '{k}' on <{}>", self.name);
            }
        }
        Ok(())
    }

    fn children_named(&self, tag: &str) -> Result<&[Elem]> {
        for c in &self.children {
            if c.name != tag {
                bail_kind!(
                    Usage,
                    "unexpected <{}> in <{}> (expected <{tag}>)",
                    c.name,
                    self.name
                );
            }
        }
        Ok(&self.children)
    }

    fn name_attr(&self) -> Result<&str> {
        self.attr("name")
            .ok_or_else(|| err!(Usage, "<{}> needs a name attribute", self.name))
    }
}

/// A `<message>` element as a message.
fn from_elem(e: &Elem) -> Result<Record> {
    if e.name != "message" {
        bail_kind!(Usage, "expected <message>, got <{}>", e.name);
    }
    e.check_attrs(&["destination"])?;
    let mut record = Record {
        destination: e.attr("destination").map(str::to_string),
        ..Default::default()
    };
    let msg = &mut record.message;
    for c in &e.children {
        match c.name.as_str() {
            "fields" => {
                for f in c.children_named("field")? {
                    f.check_attrs(&["name", "encoding"])?;
                    let name = f.name_attr()?;
                    let field = Field::lookup(name)
                        .ok_or_else(|| err!(Usage, "unknown field '{name}' (see pub help fields)"))?;
                    msg.set_field(field, &f.value()?)?;
                }
            }
            "properties" => {
                for p in c.children_named("property")? {
                    p.check_attrs(&["name", "type", "encoding"])?;
                    let name = p.name_attr()?;
                    let ty = p.attr("type").unwrap_or("string");
                    let raw = p.value()?;
                    let value = match ty {
                        "string" => Value::String(raw),
                        t if Value::TYPES.contains(&t) => Value::parse(t, &raw)
                            .map_err(|why| err!(Usage, "property '{name}': {why}"))?,
                        t => bail_kind!(
                            Usage,
                            "property '{name}': unknown type '{t}' (types: {})",
                            Value::TYPES.join(", ")
                        ),
                    };
                    msg.properties.push(Property {
                        name: name.to_string(),
                        value,
                    });
                }
            }
            "protocol" => {
                for h in c.children_named("header")? {
                    h.check_attrs(&["name", "encoding"])?;
                    msg.protocol.push((h.name_attr()?.to_string(), h.value()?));
                }
            }
            "body" => {
                c.check_attrs(&["type", "encoding"])?;
                if !c.children.is_empty() {
                    bail_kind!(Usage, "<body> holds text, not elements (escape < as &lt;)");
                }
                if let Some(t) = c.attr("type") {
                    msg.body_type = Some(BodyType::parse(t).ok_or_else(|| {
                        err!(Usage, "body type must be text or bytes, got '{t}'")
                    })?);
                }
                msg.body = match c.attr("encoding") {
                    None => c.text.as_bytes().to_vec(),
                    Some("base64") => BASE64
                        .decode(c.text.trim())
                        .map_err(|e| err!(Usage, "<body>: invalid base64: {e}"))?,
                    Some(other) => bail_kind!(Usage, "<body>: unknown encoding '{other}'"),
                };
                if msg.body_type == Some(BodyType::Text) && std::str::from_utf8(&msg.body).is_err() {
                    bail_kind!(Usage, "body type is text but the content is not UTF-8");
                }
            }
            other => bail_kind!(
                Usage,
                "unknown element <{other}> in <message> (expected fields, properties, protocol, body)"
            ),
        }
    }
    Ok(record)
}

/// Counts bytes taken from the input, failing once one document exceeds its limit.
struct Bounded {
    inner: Box<dyn Read>,
    used: Rc<Cell<u64>>,
    limit: u64,
}

impl Read for Bounded {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.used.get() > self.limit {
            return Err(io::Error::other(format!(
                "an XML message is larger than {} bytes (2 × max-payload + 16 MiB)",
                self.limit
            )));
        }
        let n = self.inner.read(buf)?;
        self.used.set(self.used.get() + n as u64);
        if self.used.get() > self.limit {
            return Err(io::Error::other(format!(
                "an XML message is larger than {} bytes (2 × max-payload + 16 MiB)",
                self.limit
            )));
        }
        Ok(n)
    }
}

/// Elements nested inside `<message>` (pub's documents use 2).
const MAX_DEPTH: usize = 8;

struct XmlReader {
    reader: quick_xml::Reader<BufReader<Bounded>>,
    buf: Vec<u8>,
    used: Rc<Cell<u64>>,
    index: usize,
    /// Inside `<messages>`, or reading a lone `<message>`.
    state: ReadState,
    done: bool,
}

#[derive(PartialEq)]
enum ReadState {
    Start,
    InMessages,
    Single,
}

fn qname(start: &BytesStart) -> String {
    start.name().as_ref().to_string()
}

impl XmlReader {
    fn xml_err(&self, e: impl std::fmt::Display) -> anyhow::Error {
        err!(
            Usage,
            "invalid XML after message {} (byte {}): {e}",
            self.index,
            self.reader.buffer_position()
        )
    }

    fn elem_from_start(&self, start: &BytesStart) -> Result<Elem> {
        let mut e = Elem {
            name: qname(start),
            ..Default::default()
        };
        for a in start.attributes() {
            let a = a.map_err(|x| self.xml_err(x))?;
            let key = a.key.as_ref().to_string();
            if key == "xmlns" || key.starts_with("xmlns:") {
                continue;
            }
            let value = a
                .normalized_value(quick_xml::XmlVersion::Implicit1_0)
                .map_err(|x| self.xml_err(x))?
                .into_owned();
            e.attrs.push((key, value));
        }
        Ok(e)
    }

    /// The rest of an element whose start tag was just read.
    fn read_elem(&mut self, mut e: Elem, depth: usize) -> Result<Elem> {
        if depth > MAX_DEPTH {
            return Err(self.xml_err(format!("elements nested more than {MAX_DEPTH} deep")));
        }
        loop {
            self.buf.clear();
            let read = self
                .reader
                .read_event_into(&mut self.buf)
                .map(|e| e.into_owned());
            let ev = read.map_err(|x| self.xml_err(x))?;
            match ev {
                Event::Start(s) => {
                    let child = self.elem_from_start(&s)?;
                    let child = self.read_elem(child, depth + 1)?;
                    e.children.push(child);
                }
                Event::Empty(s) => {
                    let child = self.elem_from_start(&s)?;
                    e.children.push(child);
                }
                Event::Text(t) => {
                    // XML 1.0 end-of-line handling (a written \r is a &#13; reference).
                    e.text.push_str(&t.xml10_content());
                }
                Event::CData(c) => {
                    e.text.push_str(&c.into_inner());
                }
                Event::GeneralRef(r) => {
                    if let Some(c) = r.resolve_char_ref().map_err(|x| self.xml_err(x))? {
                        e.text.push(c);
                    } else {
                        let name = r.xml10_content();
                        match quick_xml::escape::resolve_predefined_entity(&name) {
                            Some(v) => e.text.push_str(v),
                            None => bail_kind!(Usage, "unknown entity &{name};"),
                        }
                    }
                }
                Event::End(_) => return Ok(e),
                Event::Eof => return Err(self.xml_err(format!("unclosed <{}>", e.name))),
                Event::Comment(_) | Event::PI(_) | Event::Decl(_) | Event::DocType(_) => {}
            }
        }
    }

    fn next_record(&mut self) -> Result<Option<Record>> {
        loop {
            if self.done {
                return Ok(None);
            }
            self.buf.clear();
            let read = self
                .reader
                .read_event_into(&mut self.buf)
                .map(|e| e.into_owned());
            let ev = read.map_err(|x| self.xml_err(x))?;
            let empty = matches!(ev, Event::Empty(_));
            match ev {
                Event::Start(s) | Event::Empty(s)
                    if self.state == ReadState::Start && qname(&s) == "messages" =>
                {
                    let root = self.elem_from_start(&s)?;
                    root.check_attrs(&["version"])?;
                    if let Some(v) = root.attr("version") {
                        if v != VERSION {
                            bail_kind!(Usage, "unsupported pub document version {v} (this pub reads version {VERSION})");
                        }
                    }
                    if !empty {
                        self.state = ReadState::InMessages;
                    }
                }
                Event::Start(s) if qname(&s) == "message" && self.state != ReadState::Single => {
                    if self.state == ReadState::Start {
                        self.state = ReadState::Single;
                    }
                    let e = self.elem_from_start(&s)?;
                    let e = self.read_elem(e, 0)?;
                    self.used.set(0);
                    self.index += 1;
                    if self.state == ReadState::Single {
                        self.state = ReadState::Start;
                    }
                    return from_elem(&e).map(Some);
                }
                Event::Empty(s) if qname(&s) == "message" && self.state != ReadState::Single => {
                    let e = self.elem_from_start(&s)?;
                    self.index += 1;
                    return from_elem(&e).map(Some);
                }
                Event::Start(s) | Event::Empty(s) => {
                    bail_kind!(
                        Usage,
                        "unexpected <{}> (expected <messages> or <message>)",
                        qname(&s)
                    )
                }
                // `</messages>`: another document may follow.
                Event::End(_) => self.state = ReadState::Start,
                Event::Text(t) => {
                    let s = t.xml10_content();
                    if !s.trim().is_empty() {
                        bail_kind!(Usage, "unexpected text between messages: {}", s.trim());
                    }
                }
                Event::Eof => {
                    if self.state == ReadState::InMessages {
                        return Err(self.xml_err("unclosed <messages>"));
                    }
                    self.done = true;
                }
                _ => {}
            }
        }
    }
}

impl Iterator for XmlReader {
    type Item = Result<Record>;

    fn next(&mut self) -> Option<Result<Record>> {
        let index = self.index + 1;
        match self.next_record() {
            Ok(Some(r)) => Some(Ok(r)),
            Ok(None) => None,
            Err(e) => {
                self.done = true;
                Some(Err(e.context(format!("message {index}"))))
            }
        }
    }
}

impl Format for XmlFormat {
    fn name(&self) -> &'static str {
        "xml"
    }

    fn description(&self) -> &'static str {
        "a <messages> document, one <message> each; reads that or a single <message>"
    }

    fn can_read(&self) -> bool {
        true
    }

    fn writer(&self, out: Box<dyn Write>, _ctx: &FormatContext) -> Result<Box<dyn MessageWriter>> {
        Ok(Box::new(XmlWriter { out }))
    }

    fn reader(&self, input: Box<dyn Read>, ctx: &FormatContext) -> Result<RecordReader> {
        let used = Rc::new(Cell::new(0));
        let bounded = Bounded {
            inner: input,
            used: used.clone(),
            limit: ctx.document_limit(),
        };
        let mut reader = quick_xml::Reader::from_reader(BufReader::new(bounded));
        reader.config_mut().trim_text(false);
        Ok(Box::new(XmlReader {
            reader,
            buf: Vec::new(),
            used,
            index: 0,
            state: ReadState::Start,
            done: false,
        }))
    }
}

#[cfg(test)]
mod tests;
