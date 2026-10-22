use std::thread;
use std::time::{Duration, Instant};

use pub_core::{
    exit_code, Context, Destination, Endpoint, Field, Message, Module, ReceiveOptions, Trace, Value,
};
use pub_mqtt::mock::{MockBroker, MockConfig};
use pub_mqtt::{MqttModule, Packet};

fn ctx() -> Context {
    Context {
        trace: Trace::default(),
        scheme: MqttModule.schemes()[0],
    }
}

fn ep(url: &str) -> Endpoint {
    Endpoint::parse(url).unwrap()
}

fn dest(name: &str) -> Destination {
    Destination::new(name)
}

fn opts(count: usize, wait_ms: u64) -> ReceiveOptions {
    ReceiveOptions {
        count: Some(count),
        timeout: Some(Duration::from_millis(wait_ms)),
        ack: true,
        ..Default::default()
    }
}

/// Subscribe in a thread; returns once the broker has the SUBSCRIBE.
fn subscriber(
    broker: &MockBroker,
    url: String,
    filter: &str,
    o: ReceiveOptions,
) -> thread::JoinHandle<pub_core::Result<Vec<Message>>> {
    let before = broker.packets("SUBSCRIBE").len();
    let filter = filter.to_string();
    let h = thread::spawn(move || {
        let mut s = MqttModule.connect(&ep(&url), &ctx())?;
        let mut got = Vec::new();
        s.receive(&dest(&filter), &o, &mut |m| {
            got.push(m);
            Ok(())
        })?;
        s.close()?;
        Ok(got)
    });
    let start = Instant::now();
    while broker.packets("SUBSCRIBE").len() == before {
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "subscriber never subscribed"
        );
        thread::sleep(Duration::from_millis(10));
    }
    thread::sleep(Duration::from_millis(50)); // let SUBACK reach the subscriber
    h
}

fn publish(url: &str, topic: &str, msg: &Message) -> pub_core::Result<()> {
    let mut s = MqttModule.connect(&ep(url), &ctx())?;
    s.send(&dest(topic), msg)?;
    s.close()
}

#[test]
fn connect_reports_broker_details() {
    let broker = MockBroker::start(MockConfig {
        max_qos: 1,
        server_keep_alive: Some(30),
        ..Default::default()
    });
    let mut s = MqttModule.connect(&ep(&broker.url("")), &ctx()).unwrap();
    let info = s.info();
    let get = |k: &str| info.iter().find(|(n, _)| n == k).map(|(_, v)| v.as_str());
    assert_eq!(get("version"), Some("5"));
    assert!(get("assigned-client-id").unwrap().starts_with("mock-"));
    assert_eq!(get("maximum-qos"), Some("1"));
    assert_eq!(get("keep-alive"), Some("30s")); // server override wins
    s.close().unwrap();

    // CONNECT advertises our max packet size (from max-frame-size).
    match &broker.packets("CONNECT")[0] {
        Packet::Connect(c) => {
            assert!(c.clean_start);
            assert_eq!(
                c.props.num(pub_mqtt::codec::prop::MAXIMUM_PACKET_SIZE),
                Some(64 * 1024 * 1024)
            );
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn publish_and_subscribe_at_each_qos() {
    let broker = MockBroker::start(MockConfig::default());
    for qos in ["0", "1", "2"] {
        let topic = format!("orders/{qos}");
        let url = broker.url(&format!("?qos={qos}"));
        let sub = subscriber(&broker, url.clone(), &topic, opts(1, 3000));

        let mut msg = Message::new(format!("qos {qos}"));
        msg.set_field(Field::ContentType, "text/plain").unwrap();
        msg.set_field(Field::CorrelationId, "c-1").unwrap();
        msg.set_field(Field::ReplyTo, "replies/1").unwrap();
        msg.set_property("region", Value::String("EU".into()));
        publish(&url, &topic, &msg).unwrap();

        let got = sub.join().unwrap().unwrap();
        assert_eq!(got.len(), 1, "qos {qos}");
        let m = &got[0];
        assert_eq!(m.body, format!("qos {qos}").as_bytes());
        assert_eq!(m.field(Field::Destination), Some(topic.as_str()));
        assert_eq!(m.field(Field::ContentType), Some("text/plain"));
        assert_eq!(m.field(Field::CorrelationId), Some("c-1"));
        assert_eq!(m.field(Field::ReplyTo), Some("replies/1"));
        assert_eq!(m.property("region"), Some(&Value::String("EU".into())));
        assert!(
            m.protocol.contains(&("qos".into(), qos.into())),
            "{:?}",
            m.protocol
        );
    }
    // The handshakes happened: client acked QoS 1, and ran QoS 2's PUBREC/PUBREL/PUBCOMP both ways.
    assert!(!broker.packets("PUBACK").is_empty());
    assert!(!broker.packets("PUBREL").is_empty()); // from the publisher
    assert!(!broker.packets("PUBREC").is_empty()); // from the subscriber
    assert!(!broker.packets("PUBCOMP").is_empty()); // from the subscriber
}

#[test]
fn wildcards_and_retained_messages() {
    let broker = MockBroker::start(MockConfig::default());
    let url = broker.url("");

    let sub = subscriber(&broker, url.clone(), "sensors/+/temp", opts(1, 3000));
    publish(&url, "sensors/kitchen/temp", &Message::new("21.5")).unwrap();
    let got = sub.join().unwrap().unwrap();
    assert_eq!(
        got[0].field(Field::Destination),
        Some("sensors/kitchen/temp")
    );

    // Retained: stored by the broker and replayed to a later subscriber.
    publish(
        &broker.url("?retain=true"),
        "status/door",
        &Message::new("open"),
    )
    .unwrap();
    assert_eq!(
        broker.retained("status/door").as_deref(),
        Some(&b"open"[..])
    );
    let mut s = MqttModule.connect(&ep(&url), &ctx()).unwrap();
    let mut got = Vec::new();
    s.receive(&dest("status/#"), &opts(1, 2000), &mut |m| {
        got.push(m);
        Ok(())
    })
    .unwrap();
    assert!(got[0].protocol.contains(&("retain".into(), "true".into())));

    // retain-handling = 2: retained messages are not sent.
    let mut s = MqttModule
        .connect(&ep(&broker.url("?retain-handling=2")), &ctx())
        .unwrap();
    let n = s
        .receive(&dest("status/#"), &opts(1, 300), &mut |_| Ok(()))
        .unwrap();
    assert_eq!(n, 0);
}

#[test]
fn broker_refusals_map_to_exit_codes() {
    let broker = MockBroker::start(MockConfig {
        credentials: Some(("app".into(), "s3cret".into())),
        deny_prefix: Some("secret/".into()),
        max_qos: 1,
        ..Default::default()
    });
    let addr = broker.addr;
    let code = |r: pub_core::Result<()>| exit_code(&r.unwrap_err());

    let bad_login = MqttModule
        .connect(&ep(&format!("mqtt://app:wrong@{addr}")), &ctx())
        .map(|_| ());
    let e = bad_login.unwrap_err();
    assert_eq!(exit_code(&e), 67);
    assert!(
        e.to_string()
            .contains("Bad User Name or Password (0x86): bad credentials"),
        "{e}"
    );

    let good = format!("mqtt://app:s3cret@{addr}");
    // Denied publish (PUBACK 0x87) and subscribe (SUBACK 0x87).
    assert_eq!(code(publish(&good, "secret/x", &Message::new("x"))), 67);
    let mut s = MqttModule.connect(&ep(&good), &ctx()).unwrap();
    let e = s
        .receive(&dest("secret/#"), &opts(1, 500), &mut |_| Ok(()))
        .unwrap_err();
    assert_eq!(exit_code(&e), 67, "{e:#}");
    // QoS above the broker's maximum.
    assert_eq!(
        code(publish(&format!("{good}?qos=2"), "a", &Message::new("x"))),
        1
    );
    // Wildcards are not publishable; selectors don't exist in MQTT.
    assert_eq!(code(publish(&good, "a/#", &Message::new("x"))), 2);
    let mut s = MqttModule.connect(&ep(&good), &ctx()).unwrap();
    let o = ReceiveOptions {
        selector: Some("x = 1".into()),
        ..opts(1, 100)
    };
    assert_eq!(
        exit_code(&s.receive(&dest("a"), &o, &mut |_| Ok(())).unwrap_err()),
        1
    );
}

#[test]
fn keep_alive_pings_while_waiting() {
    let broker = MockBroker::start(MockConfig::default());
    let mut s = MqttModule
        .connect(&ep(&broker.url("?keep-alive=1s")), &ctx())
        .unwrap();
    let n = s
        .receive(&dest("quiet"), &opts(1, 2500), &mut |_| Ok(()))
        .unwrap();
    assert_eq!(n, 0);
    assert!(
        broker.packets("PINGREQ").len() >= 2,
        "{} pings",
        broker.packets("PINGREQ").len()
    );
}

#[test]
fn dead_broker_is_detected_by_keep_alive() {
    let broker = MockBroker::start(MockConfig {
        ignore_pings: true,
        ..Default::default()
    });
    let mut s = MqttModule
        .connect(&ep(&broker.url("?keep-alive=1s")), &ctx())
        .unwrap();
    let start = Instant::now();
    let e = s
        .receive(&dest("quiet"), &opts(1, 10_000), &mut |_| Ok(()))
        .unwrap_err();
    assert_eq!(exit_code(&e), 28, "{e:#}");
    assert!(e.to_string().contains("did not answer PINGREQ"), "{e}");
    assert!(
        start.elapsed() < Duration::from_secs(4),
        "{:?}",
        start.elapsed()
    );
}

#[test]
fn deliveries_that_beat_the_suback_are_kept() {
    let broker = MockBroker::start(MockConfig {
        replay_before_suback: true,
        ..Default::default()
    });
    publish(
        &broker.url("?retain=true"),
        "early/a",
        &Message::new("first"),
    )
    .unwrap();
    let mut s = MqttModule.connect(&ep(&broker.url("")), &ctx()).unwrap();
    let mut got = Vec::new();
    let n = s
        .receive(&dest("early/#"), &opts(1, 2000), &mut |m| {
            got.push(m);
            Ok(())
        })
        .unwrap();
    assert_eq!(n, 1);
    assert_eq!(got[0].body, b"first");
}

#[test]
fn persistent_sessions_keep_their_subscription() {
    let broker = MockBroker::start(MockConfig::default());
    let url = broker.url("?client-id=w1&clean-start=false&session-expiry=5m");
    let mut s = MqttModule.connect(&ep(&url), &ctx()).unwrap();
    s.receive(&dest("jobs"), &opts(1, 200), &mut |_| Ok(()))
        .unwrap();
    s.close().unwrap();
    assert!(broker.packets("UNSUBSCRIBE").is_empty());
    match &broker.packets("CONNECT")[0] {
        Packet::Connect(c) => assert!(!c.clean_start && c.client_id == "w1"),
        other => panic!("{other:?}"),
    }

    // A non-persistent session cleans up after itself.
    let mut s = MqttModule.connect(&ep(&broker.url("")), &ctx()).unwrap();
    s.receive(&dest("jobs"), &opts(1, 200), &mut |_| Ok(()))
        .unwrap();
    s.close().unwrap();
    let start = Instant::now();
    while broker.packets("UNSUBSCRIBE").is_empty() && start.elapsed() < Duration::from_secs(2) {
        thread::sleep(Duration::from_millis(10)); // close() does not wait for the broker
    }
    assert_eq!(broker.packets("UNSUBSCRIBE").len(), 1);

    // Resuming needs a client id.
    let e = MqttModule
        .connect(&ep(&broker.url("?clean-start=false")), &ctx())
        .map(|_| ())
        .unwrap_err();
    assert!(e.to_string().contains("needs a client-id"), "{e}");
}

#[test]
fn mqtt_311_end_to_end() {
    let broker = MockBroker::start(MockConfig {
        credentials: Some(("app".into(), "pw".into())),
        deny_prefix: Some("secret/".into()),
        ..Default::default()
    });
    let addr = broker.addr;
    let v4 = format!("mqtt://app:pw@{addr}?version=3.1.1");

    let mut s = MqttModule.connect(&ep(&v4), &ctx()).unwrap();
    assert!(s.info().contains(&("version".into(), "3.1.1".into())));
    s.close().unwrap();
    match &broker.packets("CONNECT")[0] {
        Packet::Connect(c) => assert!(c.protocol_level == 4 && c.props.is_empty()),
        other => panic!("{other:?}"),
    }

    // QoS 0/1/2 between a 3.1.1 subscriber and an MQTT 5 publisher: properties don't reach 3.1.1.
    for qos in ["0", "1", "2"] {
        let topic = format!("t/{qos}");
        let sub = subscriber(&broker, format!("{v4}&qos={qos}"), &topic, opts(1, 3000));
        let mut msg = Message::new("hello");
        msg.set_property("unit", Value::String("C".into()));
        publish(&format!("mqtt://app:pw@{addr}?qos={qos}"), &topic, &msg).unwrap();
        let got = sub.join().unwrap().unwrap();
        assert_eq!(got[0].body, b"hello");
        assert!(got[0].properties.is_empty());
        assert!(got[0].protocol.contains(&("qos".into(), qos.into())));
    }

    // Refusals use 3.1.1's codes.
    let e = MqttModule
        .connect(&ep(&format!("mqtt://app:bad@{addr}?version=3.1.1")), &ctx())
        .map(|_| ())
        .unwrap_err();
    assert_eq!(exit_code(&e), 67);
    assert!(
        e.to_string().contains("bad user name or password (4)"),
        "{e}"
    );
    let mut s = MqttModule.connect(&ep(&v4), &ctx()).unwrap();
    let e = s
        .receive(&dest("secret/#"), &opts(1, 500), &mut |_| Ok(()))
        .unwrap_err();
    assert!(e.to_string().contains("0x80 Failure"), "{e}");
    // A denied 3.1.1 publish cannot be refused by the broker; it just disappears.
    publish(&v4, "secret/x", &Message::new("x")).unwrap();

    // MQTT 5-only settings are rejected up front.
    for q in ["session-expiry=1m", "retain-handling=2"] {
        let e = MqttModule
            .connect(&ep(&format!("{v4}&{q}")), &ctx())
            .map(|_| ())
            .unwrap_err();
        assert!(e.to_string().contains("needs MQTT 5"), "{q}: {e}");
    }
    let mut s = MqttModule.connect(&ep(&v4), &ctx()).unwrap();
    let o = ReceiveOptions {
        headers: vec![("k".into(), "v".into())],
        ..opts(1, 100)
    };
    assert_eq!(
        exit_code(&s.receive(&dest("a"), &o, &mut |_| Ok(())).unwrap_err()),
        1
    );

    // 3.1.1 persistence: clean-start = false keeps the subscription (no UNSUBSCRIBE).
    let before = broker.packets("UNSUBSCRIBE").len();
    let mut s = MqttModule
        .connect(
            &ep(&format!("{v4}&client-id=w311&clean-start=false")),
            &ctx(),
        )
        .unwrap();
    s.receive(&dest("jobs"), &opts(1, 200), &mut |_| Ok(()))
        .unwrap();
    s.close().unwrap();
    thread::sleep(Duration::from_millis(100));
    assert_eq!(broker.packets("UNSUBSCRIBE").len(), before);
}

#[test]
fn capabilities_depend_on_version() {
    let caps5 = MqttModule.message_capabilities(&ep("mqtt://h"));
    assert!(caps5.properties && caps5.fields.contains(&Field::CorrelationId));
    let caps4 = MqttModule.message_capabilities(&ep("mqtt://h?version=3.1.1"));
    assert!(!caps4.properties && caps4.fields.is_empty());
    assert_eq!(caps4.label, Some("MQTT 3.1.1"));

    let mut msg = Message::new("x");
    msg.set_field(Field::CorrelationId, "c").unwrap();
    msg.set_property("unit", Value::String("C".into()));
    let issues = pub_core::message::check_message(&MqttModule, &ep("mqtt://h?version=3.1.1"), &msg);
    let text: Vec<String> = issues.iter().map(|i| i.message.clone()).collect();
    assert_eq!(
        text,
        [
            "the mqtt module (MQTT 3.1.1) cannot carry 'correlation-id'; it will not be sent",
            "the mqtt module (MQTT 3.1.1) cannot carry properties; 'unit' will not be sent",
        ]
    );
    assert!(pub_core::message::check_message(&MqttModule, &ep("mqtt://h"), &msg).is_empty());
}

#[test]
fn will_messages() {
    let broker = MockBroker::start(MockConfig::default());
    let watch = |b: &MockBroker| subscriber(b, b.url(""), "status/#", opts(1, 2000));
    let will =
        "?will-topic=status/w1&will-message=offline&will-qos=1&will-retain=true&will-delay=5s";

    // A normal disconnect discards the will...
    let w = watch(&broker);
    let mut s = MqttModule.connect(&ep(&broker.url(will)), &ctx()).unwrap();
    s.close().unwrap();
    assert!(w.join().unwrap().unwrap().is_empty());
    match &broker.packets("CONNECT")[1] {
        Packet::Connect(c) => {
            let w = c.will.as_ref().unwrap();
            assert_eq!(
                (w.topic.as_str(), &w.payload[..], w.qos, w.retain),
                ("status/w1", &b"offline"[..], 1, true)
            );
            assert_eq!(
                w.props.num(pub_mqtt::codec::prop::WILL_DELAY_INTERVAL),
                Some(5)
            );
        }
        other => panic!("{other:?}"),
    }

    // ...a dropped connection publishes it...
    let w = watch(&broker);
    let s = MqttModule.connect(&ep(&broker.url(will)), &ctx()).unwrap();
    drop(s);
    let got = w.join().unwrap().unwrap();
    assert_eq!(got[0].body, b"offline");
    assert_eq!(
        broker.retained("status/w1").as_deref(),
        Some(&b"offline"[..])
    );

    // ...and so does will-on-disconnect (MQTT 5 reason 0x04).
    let w = subscriber(&broker, broker.url(""), "status/w2", opts(1, 2000));
    let url = broker.url("?will-topic=status/w2&will-message=bye&will-on-disconnect=true");
    let mut s = MqttModule.connect(&ep(&url), &ctx()).unwrap();
    s.close().unwrap();
    assert_eq!(w.join().unwrap().unwrap()[0].body, b"bye");

    // Mistakes are caught before connecting.
    for (q, code) in [
        ("?will-message=x", 2),
        ("?will-topic=a/%23", 2),
        ("?will-on-disconnect=true", 2),
        ("?version=3.1.1&will-topic=a&will-delay=5s", 1),
        ("?version=3.1.1&will-topic=a&will-on-disconnect=true", 1),
    ] {
        let e = MqttModule
            .connect(&ep(&broker.url(q)), &ctx())
            .err()
            .unwrap();
        assert_eq!(exit_code(&e), code, "{q}: {e:#}");
    }
    // 3.1.1 carries a will too (no properties).
    let mut s = MqttModule
        .connect(
            &ep(&broker.url("?version=3.1.1&will-topic=status/w3")),
            &ctx(),
        )
        .unwrap();
    s.close().unwrap();
}

#[test]
fn shared_subscriptions_split_the_messages() {
    let broker = MockBroker::start(MockConfig::default());
    let o = opts(2, 2000);
    let a = subscriber(
        &broker,
        broker.url("?share-group=workers"),
        "jobs",
        o.clone(),
    );
    let b = subscriber(
        &broker,
        broker.url("?share-group=workers"),
        "jobs",
        o.clone(),
    );
    for i in 0..4 {
        publish(&broker.url(""), "jobs", &Message::new(format!("j{i}"))).unwrap();
    }
    let (a, b) = (a.join().unwrap().unwrap(), b.join().unwrap().unwrap());
    assert_eq!((a.len(), b.len()), (2, 2), "each member gets half");
    assert_eq!(a[0].field(Field::Destination), Some("jobs"));
    match &broker.packets("SUBSCRIBE")[0] {
        Packet::Subscribe(s) => assert_eq!(s.filters[0].0, "$share/workers/jobs"),
        other => panic!("{other:?}"),
    }
    let mut s = MqttModule.connect(&ep(&broker.url("")), &ctx()).unwrap();
    for bad in ["$share/jobs", "$share//jobs"] {
        let e = s
            .receive(&dest(bad), &opts(1, 100), &mut |_| Ok(()))
            .unwrap_err();
        assert_eq!(exit_code(&e), 2, "{bad}");
    }
    let e = MqttModule
        .connect(&ep(&broker.url("?share-group=a/b")), &ctx())
        .err()
        .unwrap();
    assert_eq!(exit_code(&e), 2);

    let none = MockBroker::start(MockConfig {
        no_shared: true,
        ..Default::default()
    });
    let mut s = MqttModule
        .connect(&ep(&none.url("?share-group=w")), &ctx())
        .unwrap();
    let e = s
        .receive(&dest("jobs"), &opts(1, 100), &mut |_| Ok(()))
        .unwrap_err();
    assert_eq!(exit_code(&e), 1, "{e:#}");
}

#[test]
fn topic_aliases_both_ways() {
    let broker = MockBroker::start(MockConfig {
        topic_alias_maximum: 2,
        send_aliases: true,
        ..Default::default()
    });
    let sub = subscriber(&broker, broker.url(""), "t/#", opts(4, 3000));
    let mut s = MqttModule.connect(&ep(&broker.url("")), &ctx()).unwrap();
    for t in ["t/a", "t/a", "t/b", "t/c"] {
        s.send(&dest(t), &Message::new(t)).unwrap();
    }
    s.close().unwrap();
    let topics: Vec<(String, Option<u32>)> = broker
        .packets("PUBLISH")
        .into_iter()
        .map(|p| match p {
            Packet::Publish(p) => (p.topic, p.props.num(pub_mqtt::codec::prop::TOPIC_ALIAS)),
            _ => unreachable!(),
        })
        .collect();
    // t/a gets alias 1 and is then sent without its name; t/c finds the aliases used up.
    assert_eq!(
        topics,
        [
            ("t/a".into(), Some(1)),
            ("".into(), Some(1)),
            ("t/b".into(), Some(2)),
            ("t/c".into(), None)
        ]
    );
    // The mock aliased its deliveries to the subscriber; pub resolved them.
    let got = sub.join().unwrap().unwrap();
    let names: Vec<_> = got
        .iter()
        .map(|m| m.field(Field::Destination).unwrap().to_string())
        .collect();
    assert_eq!(names, ["t/a", "t/a", "t/b", "t/c"]);
    assert!(got
        .iter()
        .all(|m| m.protocol.iter().all(|(k, _)| k != "topic-alias")));

    // topic-aliases=false sends names; topic-alias-maximum=0 tells the broker not to alias.
    let quiet = MockBroker::start(MockConfig {
        topic_alias_maximum: 2,
        send_aliases: true,
        ..Default::default()
    });
    let mut s = MqttModule
        .connect(
            &ep(&quiet.url("?topic-aliases=false&topic-alias-maximum=0")),
            &ctx(),
        )
        .unwrap();
    s.send(&dest("t/a"), &Message::new("x")).unwrap();
    s.send(&dest("t/a"), &Message::new("x")).unwrap();
    s.close().unwrap();
    for p in quiet.packets("PUBLISH") {
        let Packet::Publish(p) = p else {
            unreachable!()
        };
        assert_eq!(
            (
                p.topic.as_str(),
                p.props.num(pub_mqtt::codec::prop::TOPIC_ALIAS)
            ),
            ("t/a", None)
        );
    }
    match &quiet.packets("CONNECT")[0] {
        Packet::Connect(c) => assert_eq!(
            c.props.num(pub_mqtt::codec::prop::TOPIC_ALIAS_MAXIMUM),
            None
        ),
        other => panic!("{other:?}"),
    }
}
