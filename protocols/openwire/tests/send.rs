//! `send` against the mock broker: producers, message mapping, Java's
//! sync/async rules, the producer window and refusals.

use pub_core::module::Destination;
use pub_core::{exit_code, BodyType, Context, Endpoint, Field, Message, Module, Session, Trace};
use pub_openwire::mock::{MockBroker, MockConfig};
use pub_openwire::primitive::{decode_map, decode_text, Primitive};
use pub_openwire::producer::destination_text;
use pub_openwire::types::kind;
use pub_openwire::{DataStructure, OpenWireModule, OpenWireSession};

fn ctx() -> Context {
    Context {
        trace: Trace::default(),
        scheme: OpenWireModule.schemes()[0],
    }
}

fn connect(url: &str) -> pub_core::Result<OpenWireSession> {
    OpenWireSession::connect(&Endpoint::parse(url).unwrap(), &ctx())
}

fn text(body: &str) -> Message {
    let mut m = Message::new(body.as_bytes().to_vec());
    m.body_type = Some(BodyType::Text);
    m
}

fn non_persistent(body: &[u8]) -> Message {
    let mut m = Message::new(body.to_vec());
    m.fields.insert(Field::Persistent, "false".into());
    m
}

#[test]
fn persistent_sends_are_synchronous_and_mapped() {
    let broker = MockBroker::start(MockConfig::default());
    let mut s = connect(&broker.url("")).unwrap();
    let dest = Destination::new("/queue/orders");
    let mut m = text("hello");
    m.fields.insert(Field::CorrelationId, "c-1".into());
    m.properties.push(pub_core::message::Property {
        name: "n".into(),
        value: pub_core::Value::Long(7),
    });
    s.send(&dest, &m).unwrap();
    s.send(&dest, &Message::new(b"\x00\x01".to_vec())).unwrap();
    s.send(&Destination::new("topic://prices"), &m).unwrap();
    s.close().unwrap();

    // One producer per destination, registered synchronously.
    let producers = broker.commands("ProducerInfo");
    assert_eq!(producers.len(), 2);
    assert!(
        matches!(&producers[0], DataStructure::ProducerInfo(p) if p.response_required && p.window_size == 0)
    );

    let msgs = broker.messages();
    assert_eq!(msgs.len(), 3);
    let f = &msgs[0].fields;
    assert_eq!(msgs[0].kind, kind::TEXT);
    assert!(f.response_required && f.persistent);
    assert_eq!(
        destination_text(f.destination.as_deref().unwrap()),
        "queue://orders"
    );
    assert_eq!(
        decode_text(f.content.as_deref().unwrap())
            .unwrap()
            .as_deref(),
        Some("hello")
    );
    assert_eq!(f.correlation_id.as_deref(), Some("c-1"));
    assert_eq!(
        decode_map(f.marshalled_properties.as_deref().unwrap())
            .unwrap()
            .unwrap(),
        vec![("n".to_string(), Primitive::Long(7))]
    );
    assert!(f.timestamp > 0);
    assert_eq!(msgs[1].kind, kind::BYTES);
    assert_eq!(msgs[1].fields.content.as_deref(), Some(&b"\x00\x01"[..]));
    // Message ids: the producer's id and a per-producer sequence.
    let seq = |i: usize| match msgs[i].fields.message_id.as_deref() {
        Some(DataStructure::MessageId(id)) => id.producer_sequence_id,
        other => panic!("{other:?}"),
    };
    assert_eq!((seq(0), seq(1), seq(2)), (1, 2, 1));
    assert_eq!(
        destination_text(msgs[2].fields.destination.as_deref().unwrap()),
        "topic://prices"
    );

    // Close removes both producers, the session and the connection.
    assert_eq!(broker.commands("RemoveInfo").len(), 4);
}

#[test]
fn async_sends_follow_java_rules() {
    let broker = MockBroker::start(MockConfig::default());
    // Non-persistent: async by default.
    let mut s = connect(&broker.url("")).unwrap();
    s.send(&Destination::new("q"), &non_persistent(b"a"))
        .unwrap();
    s.close().unwrap();
    // useAsyncSend: persistent too.
    let mut s = connect(&broker.url("?jms.useAsyncSend=true")).unwrap();
    s.send(&Destination::new("q"), &Message::new(b"b".to_vec()))
        .unwrap();
    s.close().unwrap();
    // alwaysSyncSend / sendTimeout: never async.
    for q in ["?jms.alwaysSyncSend=true", "?jms.sendTimeout=5000"] {
        let mut s = connect(&broker.url(q)).unwrap();
        s.send(&Destination::new("q"), &non_persistent(b"c"))
            .unwrap();
        s.close().unwrap();
    }
    let sync: Vec<bool> = broker
        .messages()
        .iter()
        .map(|m| m.fields.response_required)
        .collect();
    assert_eq!(sync, [false, false, true, true]);
}

#[test]
fn producer_window_flow_control() {
    // The window counts 1024 + content bytes per message; acks release it.
    let broker = MockBroker::start(MockConfig::default());
    let mut s = connect(&broker.url("?jms.producerWindowSize=2048")).unwrap();
    for _ in 0..10 {
        s.send(&Destination::new("q"), &non_persistent(&[0; 1000]))
            .unwrap();
    }
    s.close().unwrap();
    assert_eq!(broker.messages().len(), 10);
    assert!(
        matches!(&broker.commands("ProducerInfo")[0], DataStructure::ProducerInfo(p) if p.window_size == 2048)
    );

    // No acks: the second send finds the window full and gives up.
    let stingy = MockBroker::start(MockConfig {
        producer_acks: false,
        ..Default::default()
    });
    let mut s = connect(&stingy.url("?jms.producerWindowSize=1500&receive-timeout=300ms")).unwrap();
    s.send(&Destination::new("q"), &non_persistent(&[0; 1000]))
        .unwrap();
    let e = s
        .send(&Destination::new("q"), &non_persistent(&[0; 1000]))
        .unwrap_err();
    assert_eq!(exit_code(&e), 28, "{e:#}");
    assert!(format!("{e:#}").contains("jms.producerWindowSize"), "{e:#}");

    // producer.windowSize on the destination overrides the factory setting.
    let mut s = connect(&broker.url("")).unwrap();
    let mut dest = Destination::new("q2");
    dest.options
        .push(("producer.windowSize".into(), "4096".into()));
    s.send(&dest, &non_persistent(b"x")).unwrap();
    s.send(
        &Destination::new("queue://q3?producer.dispatchAsync=true"),
        &non_persistent(b"x"),
    )
    .unwrap();
    let mut bad = Destination::new("q4");
    bad.options.push(("producer.windowSiz".into(), "1".into()));
    assert_eq!(
        exit_code(&s.send(&bad, &non_persistent(b"x")).unwrap_err()),
        2
    );
    s.close().unwrap();
    let infos = broker.commands("ProducerInfo");
    let last = |i: usize| match &infos[infos.len() - i] {
        DataStructure::ProducerInfo(p) => (p.window_size, p.dispatch_async),
        other => panic!("{other:?}"),
    };
    assert_eq!(last(2), (4096, false));
    assert_eq!(last(1), (0, true));
}

#[test]
fn refused_producer_is_an_auth_error() {
    let broker = MockBroker::start(MockConfig {
        deny_producer: Some("queue://secret".into()),
        ..Default::default()
    });
    let mut s = connect(&broker.url("")).unwrap();
    let e = s
        .send(&Destination::new("/queue/secret"), &text("x"))
        .unwrap_err();
    assert_eq!(exit_code(&e), 67, "{e:#}");
    assert!(
        format!("{e:#}").contains("not authorized to write to: queue://secret"),
        "{e:#}"
    );
    assert!(broker.messages().is_empty());
    s.send(&Destination::new("/queue/open"), &text("x"))
        .unwrap();
    s.close().unwrap();
}

#[test]
fn compression_and_options() {
    let broker = MockBroker::start(MockConfig::default());
    let mut s =
        connect(&broker.url("?jms.useCompression=true&jms.disableTimeStampsByDefault=true"))
            .unwrap();
    s.send(&Destination::new("q"), &text(&"z".repeat(10_000)))
        .unwrap();
    s.close().unwrap();
    let m = &broker.messages()[0];
    assert!(m.fields.compressed);
    assert!(m.fields.content.as_ref().unwrap().len() < 200);
    assert_eq!(m.fields.timestamp, 0);
}

#[test]
fn async_sends_are_confirmed_on_close() {
    // The broker goes quiet after registering the producer: the async send
    // itself succeeds, but close cannot confirm it and says so.
    let broker = MockBroker::start(MockConfig {
        silent_after_producer: true,
        ..Default::default()
    });
    let url = broker
        .url("?jms.closeTimeout=300&receive-timeout=300ms&wireFormat.maxInactivityDuration=0");
    let mut s = connect(&url).unwrap();
    s.send(&Destination::new("q"), &non_persistent(b"x"))
        .unwrap();
    let e = s.close().unwrap_err();
    assert_eq!(exit_code(&e), 28, "{e:#}");
    assert!(
        format!("{e:#}").contains("1 message(s) sent asynchronously were not confirmed"),
        "{e:#}"
    );

    // Without async sends, an unanswered close is not an error.
    let mut s = connect(&url).unwrap();
    s.send(&Destination::new("q"), &Message::new(b"x".to_vec()))
        .unwrap_err(); // sync: no answer
    s.close().unwrap();
}
