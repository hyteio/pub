//! `receive` against the mock broker: prefetch, acks, pulls, expiry,
//! redelivery, selectors and the message mapping.

use std::time::Duration;

use pub_core::module::{Destination, ReceiveOptions};
use pub_core::{
    exit_code, BodyType, Context, Endpoint, Field, Message, Module, Session, Trace, Value,
};
use pub_openwire::mock::{MockBroker, MockConfig};
use pub_openwire::primitive::{encode_map, encode_text, Primitive};
use pub_openwire::types::{self, kind, DataStructure};
use pub_openwire::{OpenWireModule, OpenWireSession};

fn ctx() -> Context {
    Context {
        trace: Trace::default(),
        scheme: OpenWireModule.schemes()[0],
    }
}

fn connect(url: &str) -> OpenWireSession {
    OpenWireSession::connect(&Endpoint::parse(url).unwrap(), &ctx()).unwrap()
}

fn opts(count: usize) -> ReceiveOptions {
    ReceiveOptions {
        count: Some(count),
        timeout: Some(Duration::from_secs(2)),
        ack: true,
        ..Default::default()
    }
}

fn recv(s: &mut OpenWireSession, dest: &str, o: &ReceiveOptions) -> pub_core::Result<Vec<Message>> {
    let mut got = Vec::new();
    s.receive(&Destination::new(dest), o, &mut |m| {
        got.push(m);
        Ok(())
    })?;
    Ok(got)
}

/// A message as a Java producer would send it.
fn java_text(text: &str) -> types::Message {
    types::Message {
        kind: kind::TEXT,
        fields: types::MessageFields {
            content: Some(encode_text(text)),
            persistent: true,
            priority: 4,
            ..Default::default()
        },
        blob: None,
    }
}

fn consumer_infos(b: &MockBroker) -> Vec<types::ConsumerInfo> {
    b.commands("ConsumerInfo")
        .into_iter()
        .map(|d| match d {
            DataStructure::ConsumerInfo(c) => *c,
            other => panic!("{other:?}"),
        })
        .collect()
}

#[test]
fn receives_what_send_sent() {
    let broker = MockBroker::start(MockConfig::default());
    let mut s = connect(&broker.url(""));
    let mut m = Message::new("héllo 😀".as_bytes().to_vec());
    m.body_type = Some(BodyType::Text);
    m.fields.insert(Field::CorrelationId, "c-1".into());
    m.properties.push(pub_core::message::Property {
        name: "n".into(),
        value: Value::Int(5),
    });
    s.send(&Destination::new("/queue/q"), &m).unwrap();
    s.send(&Destination::new("/queue/q"), &Message::new(vec![0u8, 255]))
        .unwrap();

    let got = recv(&mut s, "queue://q", &opts(2)).unwrap();
    s.close().unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].body, m.body);
    assert_eq!(got[0].body_type, Some(BodyType::Text));
    let f = |i: usize, x: Field| got[i].fields.get(&x).cloned();
    assert_eq!(f(0, Field::CorrelationId).as_deref(), Some("c-1"));
    assert_eq!(f(0, Field::Destination).as_deref(), Some("queue://q"));
    assert!(f(0, Field::MessageId).unwrap().starts_with("ID:pub-"));
    assert_eq!(got[0].properties, m.properties);
    assert_eq!(
        (got[1].body.as_slice(), got[1].body_type),
        (&[0u8, 255][..], Some(BodyType::Bytes))
    );

    // Prefetch followed -n; each message was acknowledged.
    assert_eq!(consumer_infos(&broker)[0].prefetch_size, 2);
    assert_eq!(broker.commands("MessageAck").len(), 2);
    assert!(broker.queued("queue://q").is_empty());
}

#[test]
fn prefetch_follows_count_and_leftovers_are_not_redelivered() {
    let broker = MockBroker::start(MockConfig::default());
    for i in 0..5 {
        broker.enqueue("queue://q", java_text(&format!("m{i}")));
    }
    // An explicit, larger prefetch: three dispatched, one consumed.
    let mut s = connect(&broker.url("?jms.prefetchPolicy.queuePrefetch=3"));
    let got = recv(&mut s, "/queue/q", &opts(1)).unwrap();
    s.close().unwrap();
    assert_eq!(got[0].body, b"m0");
    assert_eq!(consumer_infos(&broker)[0].prefetch_size, 3);
    // The two prefetched but undelivered messages went back without counting
    // as redelivered (lastDeliveredSequenceId).
    let left = broker.queued("queue://q");
    assert_eq!(left.len(), 4);
    assert!(left.iter().all(|m| m.fields.redelivery_counter == 0));

    // Following -n (the default) takes only what is asked for.
    let mut s = connect(&broker.url(""));
    let got = recv(&mut s, "q", &opts(2)).unwrap();
    s.close().unwrap();
    assert_eq!(
        (got[0].body.as_slice(), got[1].body.as_slice()),
        (&b"m1"[..], &b"m2"[..])
    );
    assert_eq!(consumer_infos(&broker)[1].prefetch_size, 2);

    // prefetch-follows-count = false: Java's 1000.
    let mut s = connect(&broker.url("?prefetch-follows-count=false"));
    recv(&mut s, "q", &opts(1)).unwrap();
    s.close().unwrap();
    assert_eq!(consumer_infos(&broker)[2].prefetch_size, 1000);

    // consumer.prefetchSize on the destination wins over everything.
    let mut s = connect(&broker.url("?jms.prefetchPolicy.all=50"));
    recv(
        &mut s,
        "queue://q?consumer.prefetchSize=7&consumer.exclusive=true",
        &opts(1),
    )
    .unwrap();
    s.close().unwrap();
    let ci = &consumer_infos(&broker)[3];
    assert_eq!((ci.prefetch_size, ci.exclusive), (7, true));
    assert!(broker.queued("queue://q").is_empty());
}

#[test]
fn zero_prefetch_pulls() {
    let broker = MockBroker::start(MockConfig::default());
    broker.enqueue("queue://q", java_text("a"));
    broker.enqueue("queue://q", java_text("b"));
    let mut s = connect(&broker.url("?jms.prefetchPolicy.all=0"));
    let got = recv(&mut s, "q", &opts(2)).unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(broker.commands("MessagePull").len(), 2);
    // A pull that finds nothing times out with an empty dispatch.
    let o = ReceiveOptions {
        timeout: Some(Duration::from_millis(300)),
        ..opts(1)
    };
    assert!(recv(&mut s, "q", &o).unwrap().is_empty());
    s.close().unwrap();
}

#[test]
fn no_ack_and_errors_leave_messages_as_expected() {
    let broker = MockBroker::start(MockConfig::default());
    broker.enqueue("queue://q", java_text("a"));
    broker.enqueue("queue://q", java_text("b"));

    // A sink failure before the ack: the message goes back, redelivered.
    let mut s = connect(&broker.url(""));
    let e = s
        .receive(&Destination::new("q"), &opts(1), &mut |_| {
            Err(pub_core::err!(Io, "disk full"))
        })
        .unwrap_err();
    assert_eq!(exit_code(&e), 26);
    s.close().unwrap();
    let left = broker.queued("queue://q");
    assert_eq!(left.len(), 2);
    assert_eq!(left[0].fields.redelivery_counter, 1);

    // The redelivered message says so.
    let mut s = connect(&broker.url(""));
    let got = recv(&mut s, "q", &opts(1)).unwrap();
    assert_eq!(
        got[0].fields.get(&Field::Redelivered).map(String::as_str),
        Some("true")
    );
    assert_eq!(
        got[0].fields.get(&Field::DeliveryCount).map(String::as_str),
        Some("2")
    );

    // --no-ack acknowledges before handing the message on: a failure loses it.
    let o = ReceiveOptions {
        ack: false,
        ..opts(1)
    };
    let e = s.receive(&Destination::new("q"), &o, &mut |_| {
        Err(pub_core::err!(Io, "x"))
    });
    assert!(e.is_err());
    s.close().unwrap();
    assert!(broker.queued("queue://q").is_empty());
}

#[test]
fn expired_messages_are_dropped_with_an_expired_ack() {
    let broker = MockBroker::start(MockConfig::default());
    let mut old = java_text("stale");
    old.fields.expiration = 1;
    broker.enqueue("queue://q", old.clone());
    broker.enqueue("queue://q", java_text("fresh"));
    let mut s = connect(&broker.url(""));
    let got = recv(&mut s, "q", &opts(1)).unwrap();
    s.close().unwrap(); // acks are async; close is the round trip that orders them
    assert_eq!(got[0].body, b"fresh");
    let acks: Vec<u8> = broker
        .commands("MessageAck")
        .iter()
        .map(|a| match a {
            DataStructure::MessageAck(a) => a.ack_type,
            _ => 0,
        })
        .collect();
    assert_eq!(acks, [6, 2]);

    // With the check off, it is delivered.
    broker.enqueue("queue://q", old);
    let mut s = connect(&broker.url("?jms.consumerExpiryCheckEnabled=false"));
    assert_eq!(recv(&mut s, "q", &opts(1)).unwrap()[0].body, b"stale");
}

#[test]
fn java_messages_map_onto_pubs_model() {
    let broker = MockBroker::start(MockConfig::default());
    let mut m = java_text("x");
    m.fields.compressed = true;
    m.fields.content = Some(miniz_compress(&encode_text("compressed text")));
    m.fields.jms_type = Some("order".into());
    m.fields.reply_to = Some(Box::new(DataStructure::Topic(Box::new(
        types::Destination {
            physical_name: Some("replies".into()),
        },
    ))));
    m.fields.marshalled_properties = Some(
        encode_map(&[
            ("b".into(), Primitive::Bool(true)),
            ("d".into(), Primitive::Double(1.5)),
        ])
        .unwrap(),
    );
    m.fields.original_destination = Some(Box::new(DataStructure::Queue(Box::new(
        types::Destination {
            physical_name: Some("orig".into()),
        },
    ))));
    broker.enqueue("queue://q", m);
    let map = types::Message {
        kind: kind::MAP,
        fields: types::MessageFields {
            content: Some(encode_map(&[("k".into(), Primitive::Int(1))]).unwrap()),
            ..Default::default()
        },
        blob: None,
    };
    broker.enqueue("queue://q", map);

    let mut s = connect(&broker.url(""));
    let got = recv(&mut s, "q", &opts(2)).unwrap();
    s.close().unwrap();
    assert_eq!(got[0].body, b"compressed text");
    assert_eq!(
        got[0].fields.get(&Field::Type).map(String::as_str),
        Some("order")
    );
    assert_eq!(
        got[0].fields.get(&Field::ReplyTo).map(String::as_str),
        Some("topic://replies")
    );
    assert_eq!(
        got[0].fields.get(&Field::MessageId).map(String::as_str),
        Some("ID:mock-1")
    );
    assert_eq!(got[0].property("d").cloned(), Some(Value::Double(1.5)));
    assert!(got[0].protocol.contains(&(
        "original-destination".to_string(),
        "queue://orig".to_string()
    )));
    assert!(got[1]
        .protocol
        .contains(&("message-class".to_string(), "MapMessage".to_string())));
}

fn miniz_compress(b: &[u8]) -> Vec<u8> {
    // The same zlib stream java.util.zip produces (any valid zlib stream inflates).
    let mut out = vec![0x78, 0x01];
    // Stored block: final, len, ~len, data; then Adler-32.
    out.push(1);
    out.extend_from_slice(&(b.len() as u16).to_le_bytes());
    out.extend_from_slice(&(!(b.len() as u16)).to_le_bytes());
    out.extend_from_slice(b);
    let (mut a, mut c) = (1u32, 0u32);
    for x in b {
        a = (a + *x as u32) % 65521;
        c = (c + a) % 65521;
    }
    out.extend_from_slice(&((c << 16) | a).to_be_bytes());
    out
}

#[test]
fn selectors_and_refusals() {
    let broker = MockBroker::start(MockConfig::default());
    let mut s = connect(&broker.url(""));
    let o = ReceiveOptions {
        selector: Some("region = 'EU'".into()),
        timeout: Some(Duration::from_millis(100)),
        ..opts(1)
    };
    recv(&mut s, "q", &o).unwrap();
    assert_eq!(
        consumer_infos(&broker)[0].selector.as_deref(),
        Some("region = 'EU'")
    );

    let bad = ReceiveOptions {
        selector: Some("((".into()),
        ..o.clone()
    };
    let e = recv(&mut s, "q", &bad).unwrap_err();
    assert_eq!(exit_code(&e), 2, "{e:#}");
    assert!(
        format!("{e:#}").contains("InvalidSelectorException"),
        "{e:#}"
    );

    let e = recv(&mut s, "queue://q?consumer.prefetchSiz=1", &o).unwrap_err();
    assert_eq!(exit_code(&e), 2, "{e:#}");
    let hdr = ReceiveOptions {
        headers: vec![("x".into(), "1".into())],
        ..o.clone()
    };
    assert_eq!(exit_code(&recv(&mut s, "q", &hdr).unwrap_err()), 2);

    // Consumers are removed again (the two that registered).
    s.close().unwrap();
    let removed = broker
        .commands("RemoveInfo")
        .iter()
        .filter(|r| matches!(r, DataStructure::RemoveInfo(r) if matches!(r.object_id.as_deref(), Some(DataStructure::ConsumerId(_)))))
        .count();
    assert_eq!(removed, 1);
}

#[test]
fn undecodable_messages_are_poison_acked_and_skipped() {
    let broker = MockBroker::start(MockConfig::default());
    let mut bad = java_text("x");
    // A TextMessage whose text is not UTF-8 in any form.
    bad.fields.content = Some(vec![0, 0, 0, 2, 0xFF, 0xFE]);
    broker.enqueue("queue://q", java_text("a"));
    broker.enqueue("queue://q", bad);
    broker.enqueue("queue://q", java_text("b"));
    let mut s = connect(&broker.url(""));
    let got = recv(&mut s, "queue://q", &opts(2)).unwrap();
    s.close().unwrap();
    assert_eq!(
        got.iter().map(|m| m.body.clone()).collect::<Vec<_>>(),
        [b"a".to_vec(), b"b".to_vec()]
    );
    let poison: Vec<_> = broker
        .commands("MessageAck")
        .into_iter()
        .filter_map(|c| match c {
            DataStructure::MessageAck(a) if a.ack_type == 1 => Some(a),
            _ => None,
        })
        .collect();
    assert_eq!(poison.len(), 1);
    let cause = poison[0].poison_cause.as_ref().unwrap();
    assert!(
        cause
            .message
            .as_deref()
            .unwrap()
            .contains("could not decode"),
        "{cause:?}"
    );
}

#[test]
fn text_from_non_java_clients_decodes() {
    let broker = MockBroker::start(MockConfig::default());
    let mut m = java_text("x");
    // Standard UTF-8, 4-byte sequence and all, as a non-Java client writes it.
    let text = "hi 😀".as_bytes();
    let mut content = (text.len() as i32).to_be_bytes().to_vec();
    content.extend_from_slice(text);
    m.fields.content = Some(content);
    broker.enqueue("queue://q", m);
    let mut s = connect(&broker.url(""));
    let got = recv(&mut s, "queue://q", &opts(1)).unwrap();
    assert_eq!(String::from_utf8(got[0].body.clone()).unwrap(), "hi 😀");
}

#[test]
fn a_message_over_max_payload_is_left_alone_not_dead_lettered() {
    let broker = MockBroker::start(MockConfig::default());
    let mut big = java_text("x");
    big.kind = kind::BYTES;
    let body = vec![b'z'; 10_000];
    let mut content = (body.len() as i32).to_be_bytes().to_vec();
    content.extend(miniz_oxide::deflate::compress_to_vec_zlib(&body, 6));
    big.fields.content = Some(content);
    big.fields.compressed = true;
    broker.enqueue("queue://q", big);
    let mut s = connect(&broker.url("?max-payload=1k"));
    let e = recv(&mut s, "queue://q", &opts(1)).unwrap_err();
    assert_eq!(exit_code(&e), 2, "{e:#}");
    assert!(broker
        .commands("MessageAck")
        .iter()
        .all(|c| !matches!(c, DataStructure::MessageAck(a) if a.ack_type == 1)));
}
