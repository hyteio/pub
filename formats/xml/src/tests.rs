use std::io::Cursor;
use std::sync::{Arc, Mutex};

use pub_core::limits::Limits;
use pub_core::Endpoint;

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

fn ctx(ep: &Endpoint) -> FormatContext<'_> {
    FormatContext {
        ep,
        limits: Limits::default(),
        lines: false,
    }
}

fn write_all(msgs: &[Message]) -> String {
    let ep = Endpoint::parse("stomp://h/q").unwrap();
    let out = Shared::default();
    let mut w = XmlFormat.writer(Box::new(out.clone()), &ctx(&ep)).unwrap();
    w.begin().unwrap();
    for m in msgs {
        w.write(m).unwrap();
    }
    w.finish().unwrap();
    let s = String::from_utf8(out.0.lock().unwrap().clone()).unwrap();
    s
}

fn read_all(input: &str) -> Result<Vec<Record>> {
    let ep = Endpoint::parse("stomp://h/q").unwrap();
    XmlFormat
        .reader(Box::new(Cursor::new(input.as_bytes().to_vec())), &ctx(&ep))?
        .collect()
}

fn sample() -> Message {
    let mut m = Message::new("héllo <&> \"quoted\" 😀".as_bytes().to_vec());
    m.body_type = Some(BodyType::Text);
    for (f, v) in [
        (Field::Destination, "queue://orders"),
        (Field::CorrelationId, "c-1"),
        (Field::Priority, "7"),
        (Field::Persistent, "true"),
        (Field::Timestamp, "1790998128281"),
        (Field::Ttl, "90s"),
    ] {
        m.set_field(f, v).unwrap();
    }
    for (name, value) in [
        ("s", Value::String("x <y>".into())),
        ("b", Value::Bool(true)),
        ("d", Value::Double(2.5)),
        ("f", Value::Float(0.1)),
        ("i", Value::Int(42)),
        ("l", Value::Long(1 << 40)),
        ("sh", Value::Short(-7)),
        ("y", Value::Byte(-1)),
    ] {
        m.properties.push(Property {
            name: name.into(),
            value,
        });
    }
    m.protocol.push(("subscription".into(), "1".into()));
    m.protocol.push(("subscription".into(), "2".into())); // repeats survive
    m
}

#[test]
fn writes_the_documented_shape() {
    let xml = write_all(&[sample()]);
    let expected = r#"<?xml version="1.0" encoding="UTF-8"?>
<messages version="1">
  <message destination="queue://orders">
    <fields>
      <field name="timestamp">1790998128281</field>
      <field name="correlation-id">c-1</field>
      <field name="priority">7</field>
      <field name="persistent">true</field>
      <field name="ttl">90000ms</field>
    </fields>
    <properties>
      <property name="s">x &lt;y&gt;</property>
      <property name="b" type="bool">true</property>
      <property name="d" type="double">2.5</property>
      <property name="f" type="float">0.1</property>
      <property name="i" type="int">42</property>
      <property name="l" type="long">1099511627776</property>
      <property name="sh" type="short">-7</property>
      <property name="y" type="byte">-1</property>
    </properties>
    <protocol>
      <header name="subscription">1</header>
      <header name="subscription">2</header>
    </protocol>
    <body type="text">héllo &lt;&amp;&gt; &quot;quoted&quot; 😀</body>
  </message>
</messages>
"#;
    assert_eq!(xml, expected);
}

#[test]
fn round_trips_exactly() {
    let mut bytes = Message::new(vec![0u8, 1, 2, 255]);
    bytes.body_type = Some(BodyType::Bytes);
    let mut crlf = Message::new("  line one\r\nline two\r\n\ttabbed  ");
    crlf.body_type = Some(BodyType::Text);
    let mut control = Message::new("bell \u{7} and nul \0");
    control.body_type = Some(BodyType::Text);
    let unknown = Message::new("plain");
    let binary_unknown = Message::new(vec![0xffu8, 0]);
    let mut nan = Message::new("");
    nan.properties.push(Property {
        name: "n".into(),
        value: Value::Double(f64::INFINITY),
    });
    nan.set_field(Field::CorrelationId, "has\rcarriage")
        .unwrap();
    let msgs = [sample(), bytes, crlf, control, unknown, binary_unknown, nan];
    let xml = write_all(&msgs);
    let back = read_all(&xml).unwrap();
    assert_eq!(back.len(), msgs.len());
    for (orig, rec) in msgs.iter().zip(&back) {
        let mut want = orig.clone();
        want.fields.remove(&Field::Destination);
        assert_eq!(rec.message, want);
    }
    assert_eq!(back[0].destination.as_deref(), Some("queue://orders"));
    assert!(
        xml.contains(r#"<body type="bytes" encoding="base64">AAEC/w==</body>"#),
        "{xml}"
    );
    assert!(xml.contains("line one&#13;\nline two"), "{xml}");
    assert!(
        xml.contains(r#"<body type="text" encoding="base64">"#),
        "{xml}"
    ); // control characters
    assert!(xml.contains(r#"<body>plain</body>"#), "{xml}");
    assert!(
        xml.contains(r#"<body encoding="base64">/wA=</body>"#),
        "{xml}"
    );
    assert!(
        xml.contains(r#"<property name="n" type="double">inf</property>"#),
        "{xml}"
    );
}

#[test]
fn reads_hand_written_documents() {
    let single = r#"<?xml version="1.0"?>
<!-- one message, no wrapper -->
<message destination="/queue/b">
  <fields><field name="JMSCorrelationID">c</field><field name="ttl">30s</field></fields>
  <properties><property name="n" type="short">7</property><property name="plain">x</property></properties>
  <body type="text"><![CDATA[<raw> & unescaped]]></body>
</message>"#;
    let r = read_all(single).unwrap();
    assert_eq!(r.len(), 1);
    let m = &r[0].message;
    assert_eq!(r[0].destination.as_deref(), Some("/queue/b"));
    assert_eq!(m.field(Field::CorrelationId), Some("c"));
    assert_eq!(m.field(Field::Ttl), Some("30000"));
    assert_eq!(m.property("n"), Some(&Value::Short(7)));
    assert_eq!(m.property("plain"), Some(&Value::String("x".into())));
    assert_eq!(m.body, b"<raw> & unescaped");

    let several = r#"<messages><message><body>a</body></message><message/><message><body>c</body></message></messages>"#;
    let r = read_all(several).unwrap();
    assert_eq!(r.len(), 3);
    assert!(r[1].message.body.is_empty());
    assert!(read_all("<messages/>").unwrap().is_empty());
}

#[test]
fn errors_say_which_message_and_why() {
    let chain = |input: &str| format!("{:#}", read_all(input).unwrap_err());
    let e = chain("<messages><message/><message><bdy/></message></messages>");
    assert!(
        e.contains("message 2") && e.contains("unknown element <bdy>"),
        "{e}"
    );
    assert!(chain(r#"<messages version="2"/>"#).contains("unsupported pub document version 2"));
    assert!(
        chain(r#"<message><fields><field name="colour">x</field></fields></message>"#)
            .contains("unknown field 'colour'")
    );
    assert!(chain(
        r#"<message><properties><property name="n" type="int">x</property></properties></message>"#
    )
    .contains("property 'n'"));
    assert!(chain(
        r#"<message><properties><property name="n" type="uint">1</property></properties></message>"#
    )
    .contains("unknown type 'uint'"));
    assert!(
        chain(r#"<message><body encoding="base64">!!</body></message>"#).contains("invalid base64")
    );
    assert!(chain(r#"<message><body><b/></body></message>"#).contains("holds text"));
    assert!(chain("<messages><message>").contains("unclosed"));
    assert!(chain("<orders/>").contains("expected <messages> or <message>"));
}

#[test]
fn documents_are_bounded() {
    let ep = Endpoint::parse("stomp://h/q").unwrap();
    let c = FormatContext {
        ep: &ep,
        limits: Limits {
            max_payload: 10,
            ..Default::default()
        },
        lines: false,
    };
    let limit = c.document_limit() as usize;
    let huge = format!(
        "<messages><message><body>{}</body></message></messages>",
        "a".repeat(limit + 10)
    );
    let r: Result<Vec<Record>> = XmlFormat
        .reader(Box::new(Cursor::new(huge.into_bytes())), &c)
        .unwrap()
        .collect();
    match r {
        Err(e) => assert!(format!("{e:#}").contains("larger than"), "{e:#}"),
        Ok(v) => panic!("read {} oversized message(s)", v.len()),
    }
}

#[test]
fn nesting_is_bounded_and_appended_documents_are_all_read() {
    let deep = format!("<message><body>{}", "<a>".repeat(100_000));
    let e = read_all(&deep).unwrap_err();
    assert!(format!("{e:#}").contains("nested more than"), "{e:#}");

    // Two captures appended to one file: both documents are read.
    let one = write_all(&[Message::new("a")]);
    let two = write_all(&[Message::new("b"), Message::new("c")]);
    let got = read_all(&format!("{one}{two}")).unwrap();
    assert_eq!(got.len(), 3);
    // Lone messages one after another, likewise.
    let lone = "<message><body type=\"text\">x</body></message>\n";
    assert_eq!(read_all(&lone.repeat(3)).unwrap().len(), 3);
    // Junk after a document is an error, not ignored.
    assert!(read_all(&format!("{one}<oops/>")).is_err());
}

#[test]
fn attributes_keep_tabs_and_line_feeds() {
    let mut m = Message::new("x");
    m.properties.push(Property {
        name: "a\tb".into(),
        value: Value::String("v".into()),
    });
    let doc = write_all(&[m]);
    assert!(doc.contains("name=\"a&#9;b\""), "{doc}");
    let got = read_all(&doc).unwrap();
    assert_eq!(got[0].message.properties[0].name, "a\tb");
}
