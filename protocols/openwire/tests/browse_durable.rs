//! Browsing queues and durable topic subscriptions against the mock broker.

use std::time::Duration;

use pub_core::module::{Destination, ReceiveOptions};
use pub_core::{exit_code, Context, Endpoint, Message, Module, Session, Trace};
use pub_openwire::mock::{MockBroker, MockConfig};
use pub_openwire::primitive::encode_text;
use pub_openwire::types::{self, kind, DataStructure};
use pub_openwire::{OpenWireModule, OpenWireSession};

fn connect(url: &str) -> OpenWireSession {
    let ctx = Context {
        trace: Trace::default(),
        scheme: OpenWireModule.schemes()[0],
    };
    OpenWireSession::connect(&Endpoint::parse(url).unwrap(), &ctx).unwrap()
}

fn opts(count: Option<usize>) -> ReceiveOptions {
    ReceiveOptions {
        count,
        timeout: Some(Duration::from_secs(2)),
        ack: true,
        ..Default::default()
    }
}

fn text(body: &str) -> types::Message {
    types::Message {
        kind: kind::TEXT,
        fields: types::MessageFields {
            content: Some(encode_text(body)),
            persistent: true,
            ..Default::default()
        },
        blob: None,
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
fn browsing_leaves_the_queue_alone() {
    let broker = MockBroker::start(MockConfig::default());
    for b in ["a", "b", "c"] {
        broker.enqueue("queue://q", text(b));
    }
    let mut s = connect(&broker.url(""));
    let dest = Destination::new("/queue/q");
    // The whole queue, ended by the broker's empty dispatch (no timeout needed).
    let no_limit = ReceiveOptions {
        timeout: None,
        ..opts(None)
    };
    assert_eq!(
        bodies(|k| s.browse(&dest, &no_limit, k)).unwrap(),
        ["a", "b", "c"]
    );
    // At most two.
    assert_eq!(
        bodies(|k| s.browse(&dest, &opts(Some(2)), k)).unwrap(),
        ["a", "b"]
    );
    s.close().unwrap();
    assert_eq!(broker.queued("queue://q").len(), 3);
    assert!(broker
        .queued("queue://q")
        .iter()
        .all(|m| m.fields.redelivery_counter == 0));

    let infos = consumer_infos(&broker);
    assert!(infos[0].browser && infos[1].browser);
    assert_eq!((infos[0].prefetch_size, infos[1].prefetch_size), (500, 2));

    // Receiving afterwards still gets everything.
    let mut s = connect(&broker.url(""));
    assert_eq!(
        bodies(|k| s.receive(&dest, &opts(Some(3)), k)).unwrap(),
        ["a", "b", "c"]
    );
}

#[test]
fn browse_edge_cases() {
    let broker = MockBroker::start(MockConfig::default());
    let mut s = connect(&broker.url("?jms.prefetchPolicy.queueBrowserPrefetch=7"));
    // An empty queue is an empty answer, not an error.
    let o = opts(None);
    assert!(bodies(|k| s.browse(&Destination::new("empty"), &o, k))
        .unwrap()
        .is_empty());
    assert_eq!(consumer_infos(&broker)[0].prefetch_size, 7);
    // Topics can't be browsed.
    let e = bodies(|k| s.browse(&Destination::new("/topic/t"), &o, k)).unwrap_err();
    assert_eq!(exit_code(&e), 2, "{e:#}");
    // Selectors and consumer options apply.
    let sel = ReceiveOptions {
        selector: Some("region = 'EU'".into()),
        ..opts(None)
    };
    bodies(|k| {
        s.browse(
            &Destination::new("queue://q?consumer.prefetchSize=3"),
            &sel,
            k,
        )
    })
    .unwrap();
    let ci = consumer_infos(&broker).pop().unwrap();
    assert_eq!(
        (ci.selector.as_deref(), ci.prefetch_size),
        (Some("region = 'EU'"), 3)
    );
}

#[test]
fn durable_subscriptions() {
    let broker = MockBroker::start(MockConfig::default());
    let topic = Destination::new("/topic/news");
    let durable = |name: &str| ReceiveOptions {
        subscription: Some(name.into()),
        timeout: Some(Duration::from_millis(300)),
        ..opts(Some(1))
    };

    // Needs a fixed client ID: a generated one changes every time.
    let mut s = connect(&broker.url(""));
    let e = bodies(|k| s.receive(&topic, &durable("audit"), k)).unwrap_err();
    assert_eq!(exit_code(&e), 2);
    assert!(format!("{e:#}").contains("client-id"), "{e:#}");
    // And a topic.
    let mut s = connect(&broker.url("?client-id=app"));
    let e = bodies(|k| s.receive(&Destination::new("q"), &durable("audit"), k)).unwrap_err();
    assert!(format!("{e:#}").contains("for topics"), "{e:#}");

    broker.enqueue("topic://news", text("n1"));
    assert_eq!(
        bodies(|k| s.receive(&topic, &durable("audit"), k)).unwrap(),
        ["n1"]
    );
    let ci = consumer_infos(&broker).pop().unwrap();
    assert_eq!(ci.subscription_name.as_deref(), Some("audit"));
    assert_eq!(ci.client_id.as_deref(), Some("app"));
    assert_eq!(ci.prefetch_size, 1); // follows -n; Java's durable default is 100
                                     // Java's way: a consumer.subscriptionName destination option.
    bodies(|k| {
        s.receive(
            &Destination::new("topic://news?consumer.subscriptionName=other"),
            &opts(Some(1)),
            k,
        )
    })
    .unwrap();
    assert_eq!(
        broker.subscriptions(),
        [
            ("app".to_string(), "audit".to_string()),
            ("app".into(), "other".into())
        ]
    );

    // Unsubscribe removes it; an unknown one is a usage error.
    s.unsubscribe(&topic, "audit").unwrap();
    assert_eq!(
        broker.subscriptions(),
        [("app".to_string(), "other".to_string())]
    );
    let e = s.unsubscribe(&topic, "audit").unwrap_err();
    assert_eq!(exit_code(&e), 2, "{e:#}");
    assert!(
        format!("{e:#}").contains("No durable subscription exists"),
        "{e:#}"
    );
    let mut anon = connect(&broker.url(""));
    assert_eq!(
        exit_code(&anon.unsubscribe(&topic, "other").unwrap_err()),
        2
    );
    s.close().unwrap();
}
