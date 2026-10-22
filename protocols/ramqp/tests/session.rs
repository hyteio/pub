//! The AMQP 0-9-1 session against the mock broker.

use std::time::Duration;

use pub_core::message::Property;
use pub_core::module::{Destination, DestinationKinds, ReceiveOptions};
use pub_core::{
    exit_code, BodyType, Context, Endpoint, Field, Message, Module, Session, Trace, Value,
};
use pub_ramqp::mock::{MockBroker, MockConfig};
use pub_ramqp::{RamqpModule, RamqpSession};

fn connect(url: &str) -> pub_core::Result<RamqpSession> {
    let mut ep = Endpoint::parse(url).unwrap();
    ep.apply_credentials().unwrap();
    let ctx = Context {
        trace: Trace::default(),
        scheme: RamqpModule.schemes()[0],
    };
    RamqpSession::connect(&ep, &ctx)
}

fn opts(count: Option<usize>) -> ReceiveOptions {
    ReceiveOptions {
        count,
        timeout: Some(Duration::from_secs(2)),
        ack: true,
        ..Default::default()
    }
}

fn text(body: &str) -> Message {
    let mut m = Message::new(body.as_bytes().to_vec());
    m.body_type = Some(BodyType::Text);
    m
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

#[test]
fn login_vhost_and_info() {
    let broker = MockBroker::start(MockConfig {
        credentials: Some(("app".into(), "s3cret".into())),
    });
    let url = broker.url("");
    let s = connect(&url.replace("ramqp://", "ramqp://app:s3cret@")).unwrap();
    let info = s.info();
    assert!(
        info.contains(&("product".into(), "mock 1.0".into())),
        "{info:?}"
    );
    assert!(info.contains(&("sasl".into(), "PLAIN".into())), "{info:?}");
    assert_eq!(
        exit_code(
            &connect(&url.replace("ramqp://", "ramqp://app:nope@"))
                .err()
                .unwrap()
        ),
        67
    );
    let e = connect(&url).err().unwrap(); // no user; only PLAIN offered
    assert!(format!("{e:#}").contains("-u user"), "{e:#}");
    let e = connect(&format!(
        "{}?vhost=prod",
        url.replace("ramqp://", "ramqp://app:s3cret@")
    ))
    .err()
    .unwrap();
    assert_eq!(exit_code(&e), 67, "{e:#}");
    assert!(format!("{e:#}").contains("vhost prod not found"), "{e:#}");
}

#[test]
fn queues_round_trip_with_confirms() {
    let broker = MockBroker::start(MockConfig::default());
    broker.declare_queue("orders");
    let mut s = connect(&broker.url("")).unwrap();
    let q = Destination::new("/queue/orders");
    let mut m = text("héllo");
    m.fields.insert(Field::CorrelationId, "c-1".into());
    m.fields.insert(Field::Ttl, "60000".into());
    m.properties.push(Property {
        name: "n".into(),
        value: Value::Int(5),
    });
    s.send(&q, &m).unwrap();
    s.send(&q, &Message::new(vec![7u8; 40_000])).unwrap(); // several body frames
    let p = &broker.published()[0].properties;
    assert_eq!(p.content_type.as_deref(), Some("text/plain; charset=utf-8"));
    assert_eq!(
        (p.delivery_mode, p.expiration.as_deref()),
        (Some(2), Some("60000"))
    );

    // Unroutable: no such queue.
    let e = s
        .send(&Destination::new("/queue/missing"), &text("x"))
        .unwrap_err();
    assert_eq!(exit_code(&e), 2, "{e:#}");
    assert!(format!("{e:#}").contains("NO_ROUTE (312)"), "{e:#}");

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
    assert_eq!(got[0].property("n"), Some(&Value::Int(5)));
    assert_eq!(got[1].body.len(), 40_000);
    assert!(broker.queued("orders").is_empty());
}

#[test]
fn unacked_messages_come_back_redelivered() {
    let broker = MockBroker::start(MockConfig::default());
    broker.declare_queue("q");
    let mut s = connect(&broker.url("")).unwrap();
    for i in 0..3 {
        s.send(&Destination::new("q"), &text(&format!("m{i}")))
            .unwrap();
    }
    let e = s
        .receive(&Destination::new("q"), &opts(Some(1)), &mut |_| {
            Err(pub_core::err!(Io, "disk full"))
        })
        .unwrap_err();
    assert_eq!(exit_code(&e), 26);
    s.close().unwrap();
    let left = broker.queued("q");
    assert_eq!(left.len(), 3);
    assert!(left[0].redelivered);
    let mut s = connect(&broker.url("")).unwrap();
    let mut got = Vec::new();
    s.receive(&Destination::new("q"), &opts(None), &mut |m| {
        got.push(m);
        Ok(())
    })
    .unwrap();
    assert_eq!(got.len(), 3);
    assert_eq!(got[0].field(Field::Redelivered), Some("true"));
}

#[test]
fn exchanges_topics_and_durable_subscriptions() {
    let broker = MockBroker::start(MockConfig::default());
    let mut s = connect(&broker.url("")).unwrap();
    // A topic nobody listens to is not an error (exchanges aren't mandatory by default).
    s.send(&Destination::new("/topic/nobody"), &text("x"))
        .unwrap();
    // A durable subscription: a named queue bound to amq.topic.
    let t = Destination::new("/topic/audit.#");
    let sub = ReceiveOptions {
        subscription: Some("audit-q".into()),
        timeout: Some(Duration::from_millis(200)),
        ..opts(Some(1))
    };
    assert!(bodies(|k| s.receive(&t, &sub, k)).unwrap().is_empty());
    s.send(&Destination::new("/topic/audit.x"), &text("d1"))
        .unwrap();
    s.send(&Destination::new("/topic/other"), &text("no"))
        .unwrap();
    s.send(
        &Destination::new("/exchange/amq.topic/audit.y.z"),
        &text("d2"),
    )
    .unwrap();
    let all = ReceiveOptions {
        count: Some(2),
        ..sub.clone()
    };
    assert_eq!(bodies(|k| s.receive(&t, &all, k)).unwrap(), ["d1", "d2"]);
    s.unsubscribe(&t, "audit-q").unwrap();
    assert!(!broker.has_queue("audit-q"));
    s.close().unwrap();
    let mut s = connect(&broker.url("")).unwrap();
    assert_eq!(exit_code(&s.unsubscribe(&t, "audit-q").unwrap_err()), 2);
    // --durable on a queue is a mistake.
    let mut s = connect(&broker.url("")).unwrap();
    broker.declare_queue("q");
    assert_eq!(
        exit_code(&bodies(|k| s.receive(&Destination::new("q"), &sub, k)).unwrap_err()),
        2
    );
}

#[test]
fn transactions_move_and_destinations() {
    let broker = MockBroker::start(MockConfig::default());
    let mut s = connect(&broker.url("")).unwrap();
    s.create_destination(&Destination::new("/queue/dlq"))
        .unwrap();
    s.create_destination(&Destination::new("/queue/back"))
        .unwrap();
    s.begin().unwrap();
    for i in 0..3 {
        s.send(&Destination::new("/queue/dlq"), &text(&format!("m{i}")))
            .unwrap();
    }
    assert!(broker.queued("dlq").is_empty(), "visible before commit");
    s.commit().unwrap();
    assert_eq!(broker.queued("dlq").len(), 3);
    s.begin().unwrap();
    s.send(&Destination::new("/queue/dlq"), &text("dropped"))
        .unwrap();
    s.rollback().unwrap();
    s.close().unwrap();

    let mut s = connect(&broker.url("")).unwrap();
    let moved = s
        .move_messages(
            &Destination::new("/queue/dlq"),
            &Destination::new("/queue/back"),
            &ReceiveOptions {
                idle: Some(Duration::from_millis(300)),
                transaction: Some(2),
                ..Default::default()
            },
            &mut |_| Ok(()),
        )
        .unwrap();
    s.close().unwrap();
    assert_eq!(moved, 3);
    assert!(broker.queued("dlq").is_empty());
    assert_eq!(
        broker
            .queued("back")
            .iter()
            .map(|m| m.body.clone())
            .collect::<Vec<_>>(),
        [b"m0".to_vec(), b"m1".to_vec(), b"m2".to_vec()]
    );
    // The send's transaction and the rollback; move uses publisher confirms.
    assert_eq!(broker.transactions(), (1, 1));

    let mut s = connect(&broker.url("")).unwrap();
    s.create_destination(&Destination::new("/exchange/logs"))
        .unwrap();
    s.delete_destination(&Destination::new("/queue/back"))
        .unwrap();
    assert!(!broker.has_queue("back"));
    let kinds = DestinationKinds {
        queues: true,
        topics: true,
        internal: false,
    };
    assert_eq!(
        exit_code(
            &s.list_destinations(kinds, Duration::from_millis(100))
                .unwrap_err()
        ),
        1
    );
    let sel = ReceiveOptions {
        selector: Some("a = 1".into()),
        ..opts(Some(1))
    };
    assert_eq!(
        exit_code(&bodies(|k| s.receive(&Destination::new("dlq"), &sel, k)).unwrap_err()),
        2
    );
}
