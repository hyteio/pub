use std::io::Cursor;
use std::sync::{Arc, Mutex};

use pub_core::limits::Limits;
use pub_core::Endpoint;
use serde_json::json;

use super::*;

#[derive(Clone, Default)]
struct Shared(Arc<Mutex<Vec<u8>>>);

impl Write for Shared {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn ep(query: &str) -> Endpoint {
    Endpoint::parse(&format!("stomp://h/q{query}")).unwrap()
}

fn ctx(ep: &Endpoint) -> FormatContext<'_> {
    FormatContext {
        ep,
        limits: Limits::default(),
        lines: false,
    }
}

fn write_all(msgs: &[Message], query: &str) -> String {
    let e = ep(query);
    let out = Shared::default();
    let mut w = JsonFormat.writer(Box::new(out.clone()), &ctx(&e)).unwrap();
    w.begin().unwrap();
    for m in msgs {
        w.write(m).unwrap();
    }
    w.finish().unwrap();
    let s = String::from_utf8(out.0.lock().unwrap().clone()).unwrap();
    s
}

fn read_all(input: &str) -> Result<Vec<Record>> {
    let e = ep("");
    JsonFormat
        .reader(Box::new(Cursor::new(input.as_bytes().to_vec())), &ctx(&e))?
        .collect()
}

fn sample() -> Message {
    let mut m = Message::new("héllo 😀".as_bytes().to_vec());
    m.body_type = Some(BodyType::Text);
    m.set_field(Field::Destination, "queue://orders").unwrap();
    m.set_field(Field::CorrelationId, "c-1").unwrap();
    m.set_field(Field::Priority, "7").unwrap();
    m.set_field(Field::Persistent, "true").unwrap();
    m.set_field(Field::Timestamp, "1790998128281").unwrap();
    m.set_field(Field::Ttl, "90s").unwrap();
    for (name, value) in [
        ("s", Value::String("x".into())),
        ("b", Value::Bool(true)),
        ("d", Value::Double(2.5)),
        ("whole", Value::Double(2.0)),
        ("f", Value::Float(0.1)),
        ("i", Value::Int(42)),
        ("l", Value::Long(1 << 40)),
        ("sh", Value::Short(-7)),
        ("y", Value::Byte(-1)),
        ("nan", Value::Double(f64::NAN)),
    ] {
        m.properties.push(Property {
            name: name.into(),
            value,
        });
    }
    m.protocol
        .push(("message-class".into(), "MapMessage".into()));
    m
}

#[test]
fn writes_the_documented_shape() {
    let line = write_all(&[sample()], "");
    assert!(line.ends_with('\n') && line.lines().count() == 1, "{line}");
    let v: Json = serde_json::from_str(&line).unwrap();
    assert_eq!(
        v,
        json!({
            "pub": 1,
            "destination": "queue://orders",
            "fields": {"timestamp": 1790998128281u64, "correlation-id": "c-1", "priority": 7, "persistent": true, "ttl": 90000},
            "properties": {"s": "x", "b": true, "d": 2.5, "whole": 2.0, "f": {"float": 0.1},
                           "i": {"int": 42}, "l": {"long": 1u64 << 40}, "sh": {"short": -7},
                           "y": {"byte": -1}, "nan": {"double": "NaN"}},
            "protocol": {"message-class": "MapMessage"},
            "body": {"type": "text", "data": "héllo 😀"}
        })
    );
    // Key order follows the message (preserve_order).
    assert!(
        line.starts_with(r#"{"pub":1,"destination":"queue://orders","fields":{"#),
        "{line}"
    );
}

#[test]
fn round_trips_exactly() {
    let mut bytes = Message::new(vec![0u8, 1, 2, 255]);
    bytes.body_type = Some(BodyType::Bytes);
    let unknown = Message::new("plain");
    let binary_unknown = Message::new(vec![0xffu8, 0]);
    let msgs = [sample(), bytes, unknown, binary_unknown];
    let text = write_all(&msgs, "");
    let back = read_all(&text).unwrap();
    assert_eq!(back.len(), 4);
    for (orig, rec) in msgs.iter().zip(&back) {
        let mut want = orig.clone();
        want.fields.remove(&Field::Destination);
        // NaN != NaN; compare it separately.
        let strip = |m: &mut Message| m.properties.retain(|p| p.name != "nan");
        let mut got = rec.message.clone();
        strip(&mut want);
        strip(&mut got);
        assert_eq!(got, want);
    }
    assert_eq!(back[0].destination.as_deref(), Some("queue://orders"));
    assert!(matches!(back[0].message.property("nan"), Some(Value::Double(d)) if d.is_nan()));
    assert!(
        text.contains(r#""body":{"type":"bytes","base64":"AAEC/w=="}"#),
        "{text}"
    );
    assert!(text.contains(r#""body":{"data":"plain"}"#), "{text}");
    assert!(text.contains(r#""body":{"base64":"/wA="}"#), "{text}");
}

#[test]
fn pretty_and_embedded_json_bodies() {
    let mut m = Message::new(r#"{"order": 1, "items": [1,2]}"#);
    m.body_type = Some(BodyType::Text);
    let auto = write_all(&[m.clone()], "?format.json.body=auto");
    assert!(
        auto.contains(r#""json":{"order":1,"items":[1,2]}"#),
        "{auto}"
    );
    let string = write_all(&[m.clone()], "");
    assert!(string.contains(r#""data":"{\"order\": 1"#), "{string}");
    let pretty = write_all(&[m.clone(), m], "?format.json.pretty=true");
    assert!(pretty.lines().count() > 4, "{pretty}");
    // Pretty output reads back (concatenated objects), and an embedded body
    // comes back as compact JSON text.
    assert_eq!(read_all(&pretty).unwrap().len(), 2);
    let back = read_all(&auto).unwrap();
    assert_eq!(back[0].message.body, br#"{"order":1,"items":[1,2]}"#);
}

#[test]
fn reads_hand_written_documents() {
    let input = r#"
        [{"body": {"data": "a"}, "properties": {"n": 5, "x": 1.5, "s": {"short": "7"}}},
         {"fields": {"JMSCorrelationID": "c", "ttl": "30s", "priority": "3"}}]
        {"destination": "/queue/b", "body": {"type": "text", "data": "b"}}
    "#;
    let r = read_all(input).unwrap();
    assert_eq!(r.len(), 3);
    assert_eq!(r[0].message.body, b"a");
    assert_eq!(r[0].message.body_type, None);
    assert_eq!(r[0].message.property("n"), Some(&Value::Long(5))); // plain integer = long
    assert_eq!(r[0].message.property("x"), Some(&Value::Double(1.5)));
    assert_eq!(r[0].message.property("s"), Some(&Value::Short(7)));
    let f = &r[1].message;
    assert_eq!(f.field(Field::CorrelationId), Some("c")); // aliases accepted
    assert_eq!(f.field(Field::Ttl), Some("30000")); // durations normalized
    assert!(f.body.is_empty());
    assert_eq!(r[2].destination.as_deref(), Some("/queue/b"));
}

#[test]
fn errors_say_which_message_and_why() {
    let err = |input: &str| read_all(input).unwrap_err().to_string();
    let chain = |input: &str| format!("{:#}", read_all(input).unwrap_err());
    assert!(
        chain("{}\n{\"bdy\": {}}").contains("message 2: unknown key 'bdy'"),
        "{}",
        chain("{}\n{\"bdy\": {}}")
    );
    assert!(chain(r#"{"pub": 2}"#).contains("unsupported pub document version 2"));
    assert!(chain(r#"{"fields": {"colour": "x"}}"#).contains("unknown field 'colour'"));
    assert!(chain(r#"{"fields": {"priority": 12}}"#).contains("priority"));
    assert!(chain(r#"{"properties": {"n": {"int": 3000000000}}}"#).contains("property 'n'"));
    assert!(chain(r#"{"properties": {"n": {"uint": 3}}}"#).contains("unknown type 'uint'"));
    assert!(chain(r#"{"body": {"data": "a", "base64": "YQ=="}}"#).contains("use only one"));
    assert!(chain(r#"{"body": {"base64": "!!"}}"#).contains("base64 is invalid"));
    assert!(chain(r#"{"body": {"type": "text", "base64": "/w=="}}"#).contains("not UTF-8"));
    assert!(err("{} {").contains("invalid JSON after message 1"));
    assert!(err("42").contains("message 1"));
}

#[test]
fn documents_are_bounded() {
    let e = ep("?max-payload=10");
    let limits = Limits {
        max_payload: 10,
        ..Default::default()
    };
    let c = FormatContext {
        ep: &e,
        limits,
        lines: false,
    };
    let limit = c.document_limit() as usize;
    let huge = format!(r#"{{"body": {{"data": "{}"}}}}"#, "a".repeat(limit + 10));
    let r: Result<Vec<Record>> = JsonFormat
        .reader(Box::new(Cursor::new(huge.into_bytes())), &c)
        .unwrap()
        .collect();
    assert!(r.unwrap_err().to_string().contains("larger than"));
}

#[test]
fn empty_and_long_top_level_arrays() {
    // Many empty arrays: no messages, and no recursion per array.
    let empties = "[]".repeat(200_000);
    assert!(read_all(&empties).unwrap().is_empty());
    let mixed = format!(
        "[]{}[{{\"body\":{{\"type\":\"text\",\"data\":\"a\"}}}}]",
        "[]".repeat(1000)
    );
    assert_eq!(read_all(&mixed).unwrap().len(), 1);
    // An array longer than MAX_ARRAY is refused while it is parsed.
    let long = format!("[{}{{}}]", "{},".repeat(MAX_ARRAY));
    let e = read_all(&long).unwrap_err();
    assert!(format!("{e:#}").contains("JSON Lines"), "{e:#}");
}
