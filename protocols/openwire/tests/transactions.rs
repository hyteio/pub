//! Local transactions against the mock broker: transacted sends and
//! receives, rollback on failure, and `move`.

use std::time::Duration;

use pub_core::module::{Destination, ReceiveOptions};
use pub_core::{exit_code, Context, Endpoint, Message, Module, Session, Trace};
use pub_openwire::consumer::message_id_string;
use pub_openwire::mock::{MockBroker, MockConfig};
use pub_openwire::primitive::{encode_map, encode_text, Primitive};
use pub_openwire::types::{self, kind, DataStructure};
use pub_openwire::{OpenWireModule, OpenWireSession};

fn connect(url: &str) -> OpenWireSession {
    let ctx = Context {
        trace: Trace::default(),
        scheme: OpenWireModule.schemes()[0],
    };
    OpenWireSession::connect(&Endpoint::parse(url).unwrap(), &ctx).unwrap()
}

fn opts(count: Option<usize>, transaction: Option<usize>) -> ReceiveOptions {
    ReceiveOptions {
        count,
        timeout: Some(Duration::from_secs(2)),
        ack: true,
        transaction,
        ..Default::default()
    }
}

fn text(body: &str, id: &str) -> types::Message {
    types::Message {
        kind: kind::TEXT,
        fields: types::MessageFields {
            content: Some(encode_text(body)),
            persistent: true,
            message_id: Some(Box::new(DataStructure::MessageId(Box::new(
                types::MessageId {
                    text_view: Some(id.into()),
                    ..Default::default()
                },
            )))),
            ..Default::default()
        },
        blob: None,
    }
}

fn bodies(b: &MockBroker, dest: &str) -> Vec<String> {
    b.queued(dest)
        .iter()
        .map(|m| {
            pub_openwire::primitive::decode_text(m.fields.content.as_deref().unwrap())
                .ok()
                .flatten()
                .unwrap_or_else(|| format!("{:?}", m.fields.content))
        })
        .collect()
}

#[test]
fn transacted_sends_appear_on_commit() {
    let broker = MockBroker::start(MockConfig::default());
    let mut s = connect(&broker.url(""));
    let q = Destination::new("q");
    s.begin().unwrap();
    for i in 0..3 {
        s.send(&q, &Message::new(format!("m{i}"))).unwrap();
    }
    assert!(
        broker.queued("queue://q").is_empty(),
        "visible before commit"
    );
    s.commit().unwrap();
    assert_eq!(broker.queued("queue://q").len(), 3);
    // In a transaction, sends don't wait for the broker even when persistent.
    assert!(broker
        .messages()
        .iter()
        .all(|m| !m.fields.response_required && m.fields.transaction_id.is_some()));

    s.begin().unwrap();
    s.send(&q, &Message::new("dropped")).unwrap();
    s.rollback().unwrap();
    // Closing with a transaction open rolls it back, as a JMS session does.
    s.begin().unwrap();
    s.send(&q, &Message::new("also dropped")).unwrap();
    s.close().unwrap();
    assert_eq!(broker.queued("queue://q").len(), 3);
    assert_eq!(broker.transactions(), (1, 2));

    let mut s = connect(&broker.url(""));
    assert_eq!(exit_code(&s.commit().unwrap_err()), 2); // nothing open
    s.begin().unwrap();
    assert_eq!(exit_code(&s.begin().unwrap_err()), 2); // already open
}

#[test]
fn transacted_receive_commits_in_batches_and_rolls_back_on_failure() {
    let broker = MockBroker::start(MockConfig::default());
    for i in 0..5 {
        broker.enqueue("queue://q", text(&format!("m{i}"), &format!("ID:{i}")));
    }
    let q = Destination::new("q");
    // A failure on the third message: the open transaction (messages 3) rolls back;
    // the batch of two before it was committed.
    let mut s = connect(&broker.url(""));
    let mut n = 0;
    let e = s
        .receive(&q, &opts(None, Some(2)), &mut |_| {
            n += 1;
            if n == 3 {
                Err(pub_core::err!(Io, "disk full"))
            } else {
                Ok(())
            }
        })
        .unwrap_err();
    assert_eq!(exit_code(&e), 26);
    s.close().unwrap();
    assert_eq!(broker.transactions(), (1, 1));
    let left = bodies(&broker, "queue://q");
    assert_eq!(left, ["m2", "m3", "m4"]);
    assert_eq!(broker.queued("queue://q")[0].fields.redelivery_counter, 1);

    // The rest, committed in batches of two: 2 + 1 (the end).
    let mut s = connect(&broker.url(""));
    let got = s
        .receive(&q, &opts(Some(3), Some(2)), &mut |_| Ok(()))
        .unwrap();
    s.close().unwrap();
    assert_eq!(got, 3);
    assert!(broker.queued("queue://q").is_empty());
    assert_eq!(broker.transactions(), (3, 1));
}

#[test]
fn move_makes_exact_copies_transactionally() {
    let broker = MockBroker::start(MockConfig::default());
    broker.enqueue("queue://dlq", text("one", "ID:orig-1"));
    let mut map = text("", "ID:orig-2");
    map.kind = kind::MAP;
    map.fields.content = Some(encode_map(&[("k".into(), Primitive::Int(1))]).unwrap());
    map.fields.marshalled_properties =
        Some(encode_map(&[("reason".into(), Primitive::String("poison".into()))]).unwrap());
    map.fields.timestamp = 1234;
    map.fields.expiration = i64::MAX - 1;
    broker.enqueue("queue://dlq", map);
    broker.enqueue("queue://dlq", text("three", "ID:orig-3"));

    let mut s = connect(&broker.url(""));
    let mut seen = Vec::new();
    let moved = s
        .move_messages(
            &Destination::new("queue://dlq"),
            &Destination::new("/queue/orders"),
            &ReceiveOptions {
                idle: Some(Duration::from_millis(300)),
                transaction: Some(2),
                ..Default::default()
            },
            &mut |m| {
                seen.push(m.fields.get(&pub_core::Field::MessageId).cloned());
                Ok(())
            },
        )
        .unwrap();
    s.close().unwrap();
    assert_eq!(moved, 3);
    assert_eq!(seen.len(), 3);
    assert!(broker.queued("queue://dlq").is_empty());
    let copies = broker.queued("queue://orders");
    let ids: Vec<String> = copies
        .iter()
        .map(|m| message_id_string(m.fields.message_id.as_deref().unwrap()).unwrap())
        .collect();
    assert_eq!(ids, ["ID:orig-1", "ID:orig-2", "ID:orig-3"]);
    let m = &copies[1];
    assert_eq!(m.kind, kind::MAP);
    assert_eq!(
        (m.fields.timestamp, m.fields.expiration),
        (1234, i64::MAX - 1)
    );
    assert!(m.fields.marshalled_properties.is_some());
    // Batches of two: one full, then the rest.
    assert_eq!(broker.transactions(), (2, 0));
}

#[test]
fn move_refuses_what_it_cannot_do() {
    let broker = MockBroker::start(MockConfig {
        deny_producer: Some("queue://secret".into()),
        ..Default::default()
    });
    broker.enqueue("queue://dlq", text("one", "ID:1"));
    let mut s = connect(&broker.url(""));
    let o = ReceiveOptions {
        idle: Some(Duration::from_millis(200)),
        ..Default::default()
    };
    let mut nothing = |_: Message| Ok(());
    // The target producer is refused before anything is consumed.
    let e = s
        .move_messages(
            &Destination::new("dlq"),
            &Destination::new("queue://secret"),
            &o,
            &mut nothing,
        )
        .unwrap_err();
    assert_eq!(exit_code(&e), 67);
    // A plain topic has nothing to move.
    let e = s
        .move_messages(
            &Destination::new("/topic/t"),
            &Destination::new("q"),
            &o,
            &mut nothing,
        )
        .unwrap_err();
    assert_eq!(exit_code(&e), 2);
    s.close().unwrap();
    assert_eq!(broker.queued("queue://dlq").len(), 1);
}
