//! JSON message format for pub (`-F json`).
//!
//! One JSON object per message, written as JSON Lines (one object per line;
//! `format.json.pretty` indents them). Reading accepts JSON Lines,
//! concatenated or pretty-printed objects, and arrays of objects.
//!
//! ```json
//! {"pub":1,"destination":"queue://orders",
//!  "fields":{"correlation-id":"c-1","priority":7},
//!  "properties":{"region":"EU","n":{"int":5}},
//!  "protocol":{"message-class":"MapMessage"},
//!  "body":{"type":"text","data":"hello"}}
//! ```
//!
//! - `fields`: pub's standard fields by name; numbers and booleans as JSON
//!   numbers and booleans. Durations (`ttl`, `delivery-delay`) are numbers of
//!   milliseconds, or strings like `"30s"`; times are milliseconds since the epoch.
//! - `properties`: strings, booleans and doubles as plain JSON; every other type
//!   explicitly, `{"<type>": value}` (`int`, `long`, `short`, `byte`, `float`, or
//!   `double` for NaN/infinity). On input a plain integer is a `long`.
//! - `body`: `type` is `text` or `bytes` (absent when the protocol doesn't say);
//!   the content is `data` (a string), `base64`, or with `format.json.body=auto`
//!   `json` (a text body that is itself JSON, embedded as a value).

use std::cell::Cell;
use std::collections::VecDeque;
use std::io::{self, BufReader, Read, Write};
use std::rc::Rc;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use pub_core::format::{write_error, Format, FormatContext, MessageWriter, Record, RecordReader};
use pub_core::message::{FieldKind, Property};
use pub_core::payload::is_printable;
use pub_core::{bail_kind, err, BodyType, Field, Message, Result, Setting, SettingType, Value};
use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value as Json};

/// The document version written as `"pub": 1`.
pub const VERSION: u64 = 1;

const SETTINGS: &[Setting] = &[
    Setting {
        key: "format.json.pretty",
        ty: SettingType::Bool,
        default: Some("false"),
        help: "indent JSON output (one object per message, no longer one per line)",
    },
    Setting {
        key: "format.json.body",
        ty: SettingType::OneOf(&["string", "auto"]),
        default: Some("string"),
        help: "auto: embed text bodies that are JSON as JSON values (not byte-exact); string: always a string",
    },
];

pub struct JsonFormat;

pub fn format() -> Box<dyn Format> {
    Box::new(JsonFormat)
}

/// How bodies are written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyMode {
    /// Text as a string, bytes as base64: byte-exact.
    String,
    /// Text that parses as JSON is embedded as a value.
    Auto,
}

// ---- writing

fn field_json(f: Field, v: &str) -> Json {
    let number = || {
        v.parse::<i64>()
            .map(Json::from)
            .unwrap_or_else(|_| v.into())
    };
    match f.info().kind {
        FieldKind::Text => v.into(),
        FieldKind::Bool => Json::Bool(v == "true"),
        FieldKind::Priority | FieldKind::Int | FieldKind::Duration | FieldKind::EpochMillis => {
            number()
        }
    }
}

fn typed(ty: &str, v: Json) -> Json {
    let mut m = Map::new();
    m.insert(ty.to_string(), v);
    Json::Object(m)
}

/// A float written in its shortest form (0.1f32 as 0.1, not 0.10000000149011612).
fn float_json(f: f32) -> Json {
    match f.to_string().parse::<f64>().ok().and_then(Number::from_f64) {
        Some(n) => typed("float", Json::Number(n)),
        None => typed("float", f.to_string().into()),
    }
}

fn property_json(v: &Value) -> Json {
    match v {
        Value::String(s) => s.as_str().into(),
        Value::Bool(b) => Json::Bool(*b),
        Value::Double(d) => match Number::from_f64(*d) {
            Some(n) => Json::Number(n),
            None => typed("double", d.to_string().into()), // NaN, inf, -inf
        },
        Value::Float(f) => float_json(*f),
        Value::Byte(n) => typed("byte", (*n).into()),
        Value::Short(n) => typed("short", (*n).into()),
        Value::Int(n) => typed("int", (*n).into()),
        Value::Long(n) => typed("long", (*n).into()),
    }
}

fn body_json(msg: &Message, mode: BodyMode) -> Json {
    let mut b = Map::new();
    if let Some(t) = msg.body_type {
        b.insert("type".into(), t.name().into());
    }
    let text = match msg.body_type {
        Some(BodyType::Bytes) => None,
        Some(BodyType::Text) => std::str::from_utf8(&msg.body).ok(),
        None => std::str::from_utf8(&msg.body)
            .ok()
            .filter(|_| is_printable(&msg.body)),
    };
    match text {
        Some(s) => {
            let embedded = (mode == BodyMode::Auto)
                .then(|| serde_json::from_str::<Json>(s).ok())
                .flatten();
            match embedded {
                Some(v) => b.insert("json".into(), v),
                None => b.insert("data".into(), s.into()),
            };
        }
        None => {
            b.insert("base64".into(), BASE64.encode(&msg.body).into());
        }
    }
    Json::Object(b)
}

/// A message as a pub JSON document.
pub fn to_json(msg: &Message, mode: BodyMode) -> Json {
    let mut doc = Map::new();
    doc.insert("pub".into(), VERSION.into());
    if let Some(d) = msg.field(Field::Destination) {
        doc.insert("destination".into(), d.into());
    }
    let fields: Map<String, Json> = msg
        .fields
        .iter()
        .filter(|(f, _)| **f != Field::Destination)
        .map(|(f, v)| (f.name().to_string(), field_json(*f, v)))
        .collect();
    if !fields.is_empty() {
        doc.insert("fields".into(), Json::Object(fields));
    }
    if !msg.properties.is_empty() {
        let props: Map<String, Json> = msg
            .properties
            .iter()
            .map(|p| (p.name.clone(), property_json(&p.value)))
            .collect();
        doc.insert("properties".into(), Json::Object(props));
    }
    if !msg.protocol.is_empty() {
        let mut proto = Map::new();
        for (k, v) in &msg.protocol {
            // Repeated headers (STOMP allows them): the first one wins, as in STOMP.
            proto.entry(k.clone()).or_insert_with(|| v.as_str().into());
        }
        doc.insert("protocol".into(), Json::Object(proto));
    }
    doc.insert("body".into(), body_json(msg, mode));
    Json::Object(doc)
}

struct JsonWriter {
    out: Box<dyn Write>,
    pretty: bool,
    mode: BodyMode,
}

impl MessageWriter for JsonWriter {
    fn write(&mut self, msg: &Message) -> Result<()> {
        let doc = to_json(msg, self.mode);
        let r = if self.pretty {
            serde_json::to_writer_pretty(&mut self.out, &doc)
        } else {
            serde_json::to_writer(&mut self.out, &doc)
        };
        r.map_err(|e| err!(Io, "write failed: {e}"))?;
        self.out.write_all(b"\n").map_err(write_error)?;
        self.out.flush().map_err(write_error)
    }
}

// ---- reading

fn scalar_text(v: &Json) -> Option<String> {
    match v {
        Json::String(s) => Some(s.clone()),
        Json::Number(n) => Some(n.to_string()),
        Json::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

fn property_value(name: &str, v: &Json) -> Result<Value> {
    Ok(match v {
        Json::String(s) => Value::String(s.clone()),
        Json::Bool(b) => Value::Bool(*b),
        Json::Number(n) if n.is_f64() => Value::Double(n.as_f64().unwrap_or(f64::NAN)),
        Json::Number(n) => Value::Long(
            n.as_i64()
                .ok_or_else(|| err!(Usage, "property '{name}': {n} is out of range for a long"))?,
        ),
        Json::Object(m) if m.len() == 1 => {
            let (ty, inner) = m.iter().next().unwrap();
            if !Value::TYPES.contains(&ty.as_str()) {
                bail_kind!(
                    Usage,
                    "property '{name}': unknown type '{ty}' (types: {})",
                    Value::TYPES.join(", ")
                );
            }
            let raw = scalar_text(inner).ok_or_else(|| {
                err!(
                    Usage,
                    "property '{name}': a {ty} must be a number or string"
                )
            })?;
            Value::parse(ty, &raw).map_err(|why| err!(Usage, "property '{name}': {why}"))?
        }
        _ => bail_kind!(
            Usage,
            "property '{name}': expected a string, boolean, number or {{\"<type>\": value}}"
        ),
    })
}

fn object<'a>(v: &'a Json, what: &str) -> Result<&'a Map<String, Json>> {
    v.as_object()
        .ok_or_else(|| err!(Usage, "'{what}' must be an object"))
}

fn body_from_json(v: &Json, msg: &mut Message) -> Result<()> {
    let b = object(v, "body")?;
    for key in b.keys() {
        if !matches!(key.as_str(), "type" | "data" | "base64" | "json") {
            bail_kind!(
                Usage,
                "unknown key '{key}' in body (expected type, data, base64 or json)"
            );
        }
    }
    if let Some(t) = b.get("type") {
        let t = t
            .as_str()
            .and_then(BodyType::parse)
            .ok_or_else(|| err!(Usage, "body type must be \"text\" or \"bytes\", got {t}"))?;
        msg.body_type = Some(t);
    }
    let contents: Vec<&str> = ["data", "base64", "json"]
        .into_iter()
        .filter(|k| b.contains_key(*k))
        .collect();
    if contents.len() > 1 {
        bail_kind!(Usage, "body has {}; use only one", contents.join(" and "));
    }
    msg.body = match contents.first().copied() {
        Some("data") => b["data"]
            .as_str()
            .ok_or_else(|| err!(Usage, "body data must be a string"))?
            .as_bytes()
            .to_vec(),
        Some("base64") => {
            let s = b["base64"]
                .as_str()
                .ok_or_else(|| err!(Usage, "body base64 must be a string"))?;
            BASE64
                .decode(s.trim())
                .map_err(|e| err!(Usage, "body base64 is invalid: {e}"))?
        }
        Some(_) => serde_json::to_vec(&b["json"]).map_err(|e| err!(Usage, "body json: {e}"))?,
        None => Vec::new(),
    };
    if msg.body_type == Some(BodyType::Text) && std::str::from_utf8(&msg.body).is_err() {
        bail_kind!(Usage, "body type is text but the content is not UTF-8");
    }
    Ok(())
}

/// A pub JSON document as a message.
pub fn from_json(doc: &Json) -> Result<Record> {
    let doc = doc
        .as_object()
        .ok_or_else(|| err!(Usage, "expected a JSON object per message"))?;
    let mut record = Record::default();
    let msg = &mut record.message;
    for (key, v) in doc {
        match key.as_str() {
            "pub" => {
                if v.as_u64() != Some(VERSION) {
                    bail_kind!(Usage, "unsupported pub document version {v} (this pub reads version {VERSION})");
                }
            }
            "destination" => {
                record.destination = Some(
                    v.as_str()
                        .ok_or_else(|| err!(Usage, "'destination' must be a string"))?
                        .to_string(),
                )
            }
            "fields" => {
                for (name, fv) in object(v, "fields")? {
                    let f = Field::lookup(name)
                        .ok_or_else(|| err!(Usage, "unknown field '{name}' (see pub help fields)"))?;
                    let mut raw = scalar_text(fv)
                        .ok_or_else(|| err!(Usage, "field '{name}' must be a string, number or boolean"))?;
                    // Durations are written as milliseconds; a string uses the
                    // usual syntax ("30s"), where a bare number means seconds.
                    if f.info().kind == FieldKind::Duration && fv.is_number() {
                        let ms = fv.as_u64().ok_or_else(|| {
                            err!(Usage, "field '{name}' as a number is whole milliseconds, got {fv}")
                        })?;
                        raw = format!("{ms}ms");
                    }
                    msg.set_field(f, &raw)?;
                }
            }
            "properties" => {
                for (name, pv) in object(v, "properties")? {
                    msg.properties.push(Property {
                        name: name.clone(),
                        value: property_value(name, pv)?,
                    });
                }
            }
            "protocol" => {
                for (name, hv) in object(v, "protocol")? {
                    let value = scalar_text(hv)
                        .ok_or_else(|| err!(Usage, "protocol header '{name}' must be a string"))?;
                    msg.protocol.push((name.clone(), value));
                }
            }
            "body" => body_from_json(v, msg)?,
            other => bail_kind!(
                Usage,
                "unknown key '{other}' (expected pub, destination, fields, properties, protocol, body)"
            ),
        }
    }
    Ok(record)
}

/// Counts bytes taken from the input, failing once one document exceeds its limit.
struct Bounded {
    inner: BufReader<Box<dyn Read>>,
    used: Rc<Cell<u64>>,
    limit: u64,
}

impl Read for Bounded {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.used.get() > self.limit {
            return Err(io::Error::other(format!(
                "a JSON document is larger than {} bytes (2 × max-payload + 16 MiB)",
                self.limit
            )));
        }
        // serde_json reads a byte at a time; keep each read small so the
        // count tracks what it actually consumed.
        let len = buf.len().min(4096);
        let n = self.inner.read(&mut buf[..len])?;
        self.used.set(self.used.get() + n as u64);
        if self.used.get() > self.limit {
            return Err(io::Error::other(format!(
                "a JSON document is larger than {} bytes (2 × max-payload + 16 MiB)",
                self.limit
            )));
        }
        Ok(n)
    }
}

/// Messages in one top-level JSON array; longer inputs should be JSON Lines.
const MAX_ARRAY: usize = 100_000;

/// A top-level JSON value. Arrays are counted while they are parsed, so a long
/// one is refused before it is all in memory.
enum Top {
    One(Json),
    Many(Vec<Json>),
}

impl<'de> Deserialize<'de> for Top {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        d.deserialize_any(TopVisitor)
    }
}

struct TopVisitor;

impl<'de> Visitor<'de> for TopVisitor {
    type Value = Top;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("a message object or an array of them")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> std::result::Result<Top, A::Error> {
        let mut items = Vec::new();
        while let Some(v) = seq.next_element::<Json>()? {
            if items.len() == MAX_ARRAY {
                return Err(de::Error::custom(format!(
                    "more than {MAX_ARRAY} messages in one JSON array (use JSON Lines)"
                )));
            }
            items.push(v);
        }
        Ok(Top::Many(items))
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> std::result::Result<Top, A::Error> {
        Json::deserialize(de::value::MapAccessDeserializer::new(map)).map(Top::One)
    }

    // Anything else is one (invalid) message; `from_json` says why.
    fn visit_bool<E>(self, v: bool) -> std::result::Result<Top, E> {
        Ok(Top::One(Json::Bool(v)))
    }
    fn visit_i64<E>(self, v: i64) -> std::result::Result<Top, E> {
        Ok(Top::One(Json::from(v)))
    }
    fn visit_u64<E>(self, v: u64) -> std::result::Result<Top, E> {
        Ok(Top::One(Json::from(v)))
    }
    fn visit_f64<E>(self, v: f64) -> std::result::Result<Top, E> {
        Ok(Top::One(Json::from(v)))
    }
    fn visit_str<E>(self, v: &str) -> std::result::Result<Top, E> {
        Ok(Top::One(Json::from(v)))
    }
    fn visit_unit<E>(self) -> std::result::Result<Top, E> {
        Ok(Top::One(Json::Null))
    }
}

struct JsonReader {
    stream: serde_json::StreamDeserializer<'static, serde_json::de::IoRead<Bounded>, Top>,
    used: Rc<Cell<u64>>,
    queued: VecDeque<Json>,
    index: usize,
}

impl Iterator for JsonReader {
    type Item = Result<Record>;

    fn next(&mut self) -> Option<Result<Record>> {
        // An empty array yields nothing: read on until a message (or the end).
        let doc = loop {
            if let Some(d) = self.queued.pop_front() {
                break d;
            }
            let v = match self.stream.next()? {
                Ok(v) => v,
                Err(e) => {
                    return Some(Err(err!(
                        Usage,
                        "invalid JSON after message {}: {e}",
                        self.index
                    )))
                }
            };
            self.used.set(0);
            match v {
                Top::Many(items) => self.queued.extend(items),
                Top::One(doc) => break doc,
            }
        };
        self.index += 1;
        let index = self.index;
        Some(from_json(&doc).map_err(|e| e.context(format!("message {index}"))))
    }
}

impl Format for JsonFormat {
    fn name(&self) -> &'static str {
        "json"
    }

    fn description(&self) -> &'static str {
        "one JSON object per message (JSON Lines); reads objects, JSON Lines and arrays"
    }

    fn settings(&self) -> &'static [Setting] {
        SETTINGS
    }

    fn can_read(&self) -> bool {
        true
    }

    fn writer(&self, out: Box<dyn Write>, ctx: &FormatContext) -> Result<Box<dyn MessageWriter>> {
        let mode = match ctx.ep.prop("format.json.body") {
            Some(v) if v.eq_ignore_ascii_case("auto") => BodyMode::Auto,
            _ => BodyMode::String,
        };
        Ok(Box::new(JsonWriter {
            out,
            pretty: ctx.ep.prop_bool("format.json.pretty", false)?,
            mode,
        }))
    }

    fn reader(&self, input: Box<dyn Read>, ctx: &FormatContext) -> Result<RecordReader> {
        let used = Rc::new(Cell::new(0));
        let bounded = Bounded {
            inner: BufReader::new(input),
            used: used.clone(),
            limit: ctx.document_limit(),
        };
        Ok(Box::new(JsonReader {
            stream: serde_json::Deserializer::from_reader(bounded).into_iter::<Top>(),
            used,
            queued: VecDeque::new(),
            index: 0,
        }))
    }
}

#[cfg(test)]
mod tests;
