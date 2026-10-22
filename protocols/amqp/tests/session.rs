//! The AMQP 1.0 session against the mock broker.

use std::time::Duration;

use pub_amqp::message::{to_amqp, Body, Prefixes, TextBody};
use pub_amqp::mock::{MockBroker, MockConfig};
use pub_amqp::types::Value;
use pub_amqp::{AmqpModule, AmqpSession};
use pub_core::message::Property;
use pub_core::module::{Destination, ReceiveOptions};
use pub_core::{
    exit_code, BodyType, Context, Endpoint, Field, Message, Module, Session, Trace,
    Value as PubValue,
};

fn ctx() -> Context {
    Context {
        trace: Trace::default(),
        scheme: AmqpModule.schemes()[0],
    }
}

fn connect(url: &str) -> pub_core::Result<AmqpSession> {
    let mut ep = Endpoint::parse(url).unwrap();
    ep.apply_credentials().unwrap();
    AmqpSession::connect(&ep, &ctx())
}

fn opts(count: Option<usize>) -> ReceiveOptions {
    ReceiveOptions {
        count,
        timeout: Some(Duration::from_secs(2)),
        ack: true,
        ..Default::default()
    }
}

fn bodies(
    f: impl FnOnce(&mut dyn FnMut(Message) -> pub_core::Result<()>) -> pub_core::Result<usize>,
) -> pub_core::Result<Vec<String>> {
    let mut got = Vec::new();
    f(&mut |m| {
        got.push(String::from_utf8(m.body).unwrap());
        Ok(())
    })?;
    Ok(got)
}

fn text(body: &str) -> Message {
    let mut m = Message::new(body.as_bytes().to_vec());
    m.body_type = Some(BodyType::Text);
    m
}

#[test]
fn connects_with_sasl_and_reports_the_broker() {
    let broker = MockBroker::start(MockConfig::default());
    let mut s = connect(&broker.url("")).unwrap();
    let info = s.info();
    let get = |k: &str| info.iter().find(|(n, _)| n == k).map(|(_, v)| v.clone());
    assert_eq!(get("product").as_deref(), Some("mock 1.0"));
    assert_eq!(get("sasl").as_deref(), Some("ANONYMOUS"));
    s.close().unwrap();

    let secured = MockBroker::start(MockConfig {
        credentials: Some(("app".into(), "s3cret".into())),
        ..Default::default()
    });
    let url = secured.url("");
    let s = connect(&url.replace("amqp://", "amqp://app:s3cret@")).unwrap();
    assert_eq!(
        s.info().iter().find(|(k, _)| k == "sasl").unwrap().1,
        "PLAIN"
    );
    let e = connect(&url.replace("amqp://", "amqp://app:nope@"))
        .err()
        .unwrap();
    assert_eq!(exit_code(&e), 67, "{e:#}");
    // No user, and the broker only offers PLAIN.
    let e = connect(&url).err().unwrap();
    assert_eq!(exit_code(&e), 67, "{e:#}");
    assert!(format!("{e:#}").contains("-u user"), "{e:#}");
}

#[test]
fn send_and_receive_round_trip() {
    let broker = MockBroker::start(MockConfig::default());
    let mut s = connect(&broker.url("")).unwrap();
    let q = Destination::new("/queue/orders");
    let mut m = text("héllo");
    m.fields.insert(Field::CorrelationId, "c-1".into());
    m.fields.insert(Field::Priority, "7".into());
    m.properties.push(Property {
        name: "n".into(),
        value: PubValue::Int(5),
    });
    s.send(&q, &m).unwrap();
    // A message bigger than the broker's 16 KiB frames goes in several transfers.
    let big = Message::new(vec![7u8; 40_000]);
    s.send(&q, &big).unwrap();
    assert_eq!(broker.queued("orders"), 2);
    let sent = broker.messages();
    assert_eq!(sent[0].body, Body::Value(Value::String("héllo".into())));
    assert_eq!(sent[1].body, Body::Data(vec![vec![7u8; 40_000]]));
    // The target asked for a queue.
    let target = broker.attaches()[0]
        .target
        .clone()
        .unwrap()
        .node()
        .cloned()
        .unwrap();
    assert_eq!(target.address.as_deref(), Some("orders"));
    assert!(target.capabilities.unwrap().contains("queue"));

    let mut got = Vec::new();
    s.receive(&q, &opts(Some(2)), &mut |m| {
        got.push(m);
        Ok(())
    })
    .unwrap();
    s.close().unwrap();
    assert_eq!(got[0].body, "héllo".as_bytes());
    assert_eq!(got[0].body_type, Some(BodyType::Text));
    assert_eq!(got[0].field(Field::CorrelationId), Some("c-1"));
    assert_eq!(got[0].property("n"), Some(&PubValue::Int(5)));
    assert_eq!(got[1].body.len(), 40_000); // reassembled from several transfers
    assert_eq!(broker.queued("orders"), 0);
}

#[test]
fn credit_follows_count_and_unsettled_messages_return() {
    let broker = MockBroker::start(MockConfig::default());
    for i in 0..3 {
        let m = to_amqp(
            &text(&format!("m{i}")),
            "q",
            None,
            TextBody::Value,
            &Prefixes::default(),
        )
        .unwrap();
        broker.enqueue("q", &m);
    }
    let mut s = connect(&broker.url("")).unwrap();
    let q = Destination::new("q");
    assert_eq!(
        bodies(|k| s.receive(&q, &opts(Some(1)), k)).unwrap(),
        ["m0"]
    );
    // A failure before the message was settled: it goes back.
    let e = s
        .receive(&q, &opts(Some(1)), &mut |_| {
            Err(pub_core::err!(Io, "disk full"))
        })
        .unwrap_err();
    assert_eq!(exit_code(&e), 26);
    assert_eq!(broker.queued("q"), 2);
    assert_eq!(
        bodies(|k| s.receive(&q, &opts(None), k)).unwrap(),
        ["m1", "m2"]
    );
    s.close().unwrap();
}

#[test]
fn browse_and_brokers_that_cannot() {
    let broker = MockBroker::start(MockConfig::default());
    let mut s = connect(&broker.url("?credit=2")).unwrap();
    let q = Destination::new("/queue/b");
    for i in 0..5 {
        s.send(&q, &text(&format!("m{i}"))).unwrap();
    }
    let all = ReceiveOptions {
        timeout: None,
        ..opts(None)
    };
    // Batches of two, drained to the end.
    assert_eq!(
        bodies(|k| s.browse(&q, &all, k)).unwrap(),
        ["m0", "m1", "m2", "m3", "m4"]
    );
    assert_eq!(
        bodies(|k| s.browse(&q, &opts(Some(3)), k)).unwrap(),
        ["m0", "m1", "m2"]
    );
    assert_eq!(broker.queued("b"), 5);
    assert!(
        bodies(|k| s.browse(&Destination::new("/queue/empty"), &all, k))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        exit_code(&bodies(|k| s.browse(&Destination::new("/topic/t"), &all, k)).unwrap_err()),
        2
    );

    // A broker that ignores copy distribution would consume: refused.
    let rabbit = MockBroker::start(MockConfig {
        no_copy: true,
        ..Default::default()
    });
    let mut s = connect(&rabbit.url("")).unwrap();
    s.send(&q, &text("keep")).unwrap();
    let e = bodies(|k| s.browse(&q, &all, k)).unwrap_err();
    assert_eq!(exit_code(&e), 1, "{e:#}");
    assert!(format!("{e:#}").contains("copy distribution"), "{e:#}");
    assert_eq!(rabbit.queued("b"), 1);
}

#[test]
fn selectors_durable_subscriptions_and_prefixes() {
    // ActiveMQ Classic style: advertised prefixes go on queue and topic addresses.
    let broker = MockBroker::start(MockConfig {
        prefixes: true,
        ..Default::default()
    });
    let mut s = connect(&broker.url("?client-id=app")).unwrap();
    let t = Destination::new("/topic/news");
    let o = ReceiveOptions {
        subscription: Some("audit".into()),
        selector: Some("region = 'EU'".into()),
        timeout: Some(Duration::from_millis(200)),
        ..opts(Some(1))
    };
    let _ = bodies(|k| s.receive(&t, &o, k));
    let a = broker.attaches().pop().unwrap();
    let src = a.source.unwrap();
    assert_eq!(a.name, "audit");
    assert_eq!(src.address.as_deref(), Some("topic://news"));
    assert_eq!(
        (src.durable, src.expiry_policy.map(|e| e.0)),
        (Some(2), Some("never".into()))
    );
    let filter = src.filter.unwrap();
    assert_eq!(
        filter.get("jms-selector").and_then(|v| v.descriptor_code()),
        Some(0x0000_468C_0000_0004)
    );
    assert_eq!(
        broker.subscriptions(),
        [("app".to_string(), "audit".to_string())]
    );
    s.send(&Destination::new("/queue/orders"), &text("x"))
        .unwrap();
    assert_eq!(broker.queued("queue://orders"), 1);

    s.unsubscribe(&t, "audit").unwrap();
    assert!(broker.subscriptions().is_empty());
    let e = s.unsubscribe(&t, "audit").unwrap_err();
    assert_eq!(exit_code(&e), 2, "{e:#}");
    s.close().unwrap();

    // Durable subscriptions need a fixed container id.
    let mut anon = connect(&broker.url("")).unwrap();
    let e = bodies(|k| anon.receive(&t, &o, k)).unwrap_err();
    assert!(format!("{e:#}").contains("client-id"), "{e:#}");
}

#[test]
fn refused_links_explain_why() {
    let broker = MockBroker::start(MockConfig {
        missing: Some("nowhere".into()),
        ..Default::default()
    });
    let mut s = connect(&broker.url("")).unwrap();
    let e = s
        .send(&Destination::new("/queue/nowhere"), &text("x"))
        .unwrap_err();
    assert_eq!(exit_code(&e), 2, "{e:#}");
    assert!(
        format!("{e:#}").contains("no node nowhere (amqp:not-found)"),
        "{e:#}"
    );
}

#[test]
fn transactions_hold_sends_and_accepts_until_discharge() {
    let broker = MockBroker::start(MockConfig::default());
    let mut s = connect(&broker.url("")).unwrap();
    let q = Destination::new("/queue/tx");
    s.begin().unwrap();
    s.send(&q, &text("a")).unwrap();
    s.send(&q, &text("b")).unwrap();
    assert_eq!(broker.queued("tx"), 0, "visible before commit");
    assert_eq!(exit_code(&s.begin().unwrap_err()), 2);
    s.commit().unwrap();
    assert_eq!(broker.queued("tx"), 2);
    s.begin().unwrap();
    s.send(&q, &text("dropped")).unwrap();
    s.rollback().unwrap();
    assert_eq!(broker.queued("tx"), 2);
    assert_eq!(exit_code(&s.commit().unwrap_err()), 2);
    // The coordinator link asked for local transactions.
    let coord = broker.attaches().into_iter().find_map(|a| match a.target {
        Some(pub_amqp::described::Terminus::Coordinator(c)) => Some(c),
        _ => None,
    });
    assert!(coord
        .unwrap()
        .capabilities
        .unwrap()
        .contains("amqp:local-transactions"));

    // A transactional receive whose writer fails rolls back: the message stays.
    let o = ReceiveOptions {
        transaction: Some(1),
        ..opts(Some(2))
    };
    let e = s
        .receive(&q, &o, &mut |_| Err(pub_core::err!(Io, "disk full")))
        .unwrap_err();
    assert_eq!(exit_code(&e), 26);
    assert_eq!(broker.queued("tx"), 2);
    assert_eq!(bodies(|k| s.receive(&q, &o, k)).unwrap(), ["a", "b"]);
    s.close().unwrap();
    assert_eq!(broker.queued("tx"), 0);
    // send; rollback; failed receive (rolled back); two batches + the final one
    assert_eq!(broker.transactions(), (1 + 3, 2));
}

#[test]
fn move_sends_copies_and_accepts_originals_per_batch() {
    let broker = MockBroker::start(MockConfig::default());
    let mut s = connect(&broker.url("")).unwrap();
    for i in 0..3 {
        let mut m = text(&format!("m{i}"));
        m.fields.insert(Field::CorrelationId, format!("c-{i}"));
        m.properties.push(Property {
            name: "n".into(),
            value: PubValue::Int(i),
        });
        s.send(&Destination::new("/queue/dlq"), &m).unwrap();
    }
    let o = ReceiveOptions {
        idle: Some(Duration::from_millis(300)),
        transaction: Some(2),
        ..Default::default()
    };
    let moved = s
        .move_messages(
            &Destination::new("/queue/dlq"),
            &Destination::new("/queue/back"),
            &o,
            &mut |_| Ok(()),
        )
        .unwrap();
    s.close().unwrap();
    assert_eq!(moved, 3);
    assert_eq!((broker.queued("dlq"), broker.queued("back")), (0, 3));
    assert_eq!(broker.transactions(), (2, 0));
    let copies: Vec<_> = broker.messages().into_iter().skip(3).collect();
    assert_eq!(copies.len(), 3);
    let p = copies[1].properties.clone().unwrap();
    assert_eq!(p.to.as_deref(), Some("back"));
    assert_eq!(p.correlation_id, Some(Value::String("c-1".into())));
    // The message id is the original's: an exact copy.
    let original = broker.messages()[1].properties.clone().unwrap();
    assert_eq!(p.message_id, original.message_id);

    let mut s = connect(&broker.url("")).unwrap();
    let e = s
        .move_messages(
            &Destination::new("/queue/back"),
            &Destination::new("/queue/back"),
            &o,
            &mut |_| Ok(()),
        )
        .unwrap_err();
    assert_eq!(exit_code(&e), 2);
}

#[test]
fn a_selector_the_broker_does_not_apply_is_refused() {
    let broker = MockBroker::start(MockConfig {
        ignore_filters: true,
        ..Default::default()
    });
    let mut s = connect(&broker.url("")).unwrap();
    s.send(&Destination::new("/queue/q"), &text("x")).unwrap();
    let o = ReceiveOptions {
        selector: Some("a = 1".into()),
        ..opts(Some(1))
    };
    let e = bodies(|k| s.receive(&Destination::new("/queue/q"), &o, k)).unwrap_err();
    assert_eq!(exit_code(&e), 1, "{e:#}");
    assert!(
        format!("{e:#}").contains("didn't apply the selector"),
        "{e:#}"
    );
    // Nothing was taken.
    assert_eq!(
        bodies(|k| s.receive(&Destination::new("/queue/q"), &opts(Some(1)), k)).unwrap(),
        ["x"]
    );
}
