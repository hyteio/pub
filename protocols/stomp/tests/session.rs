use std::time::{Duration, Instant};

use pub_core::{
    exit_code, Context, Destination, Endpoint, Field, Message, Module, ReceiveOptions, Trace, Value,
};
use pub_stomp::mock::MockBroker;
use pub_stomp::StompModule;

fn ctx() -> Context {
    Context {
        trace: Trace::default(),
        scheme: StompModule.schemes()[0],
    }
}

#[test]
fn send_then_receive_and_ack() {
    let broker = MockBroker::start(None);
    let ep = Endpoint::parse(&broker.url("/queue/orders")).unwrap();
    let dest = Destination::new("/queue/orders");

    let mut s = StompModule.connect(&ep, &ctx()).unwrap();
    assert!(s.info().contains(&("server".into(), "mock/1.0".into())));
    let mut msg = Message::new("hello");
    msg.set_field(Field::ContentType, "text/plain").unwrap();
    msg.set_field(Field::CorrelationId, "c-1").unwrap();
    msg.set_field(Field::Priority, "7").unwrap();
    msg.set_property("x-colon", Value::String("a:b".into()));
    msg.set_property("orderId", Value::Long(42));
    s.send(&dest, &msg).unwrap();
    s.send(&dest, &Message::new(b"\0bin".to_vec())).unwrap();
    s.close().unwrap();
    assert_eq!(broker.queued("/queue/orders").len(), 2);

    let mut s = StompModule.connect(&ep, &ctx()).unwrap();
    let mut got = Vec::new();
    let opts = ReceiveOptions {
        count: Some(2),
        ack: true,
        ..Default::default()
    };
    let n = s
        .receive(&dest, &opts, &mut |m| {
            got.push(m);
            Ok(())
        })
        .unwrap();
    s.close().unwrap();

    assert_eq!(n, 2);
    let m = &got[0];
    assert_eq!(m.body, b"hello");
    assert_eq!(m.field(Field::ContentType), Some("text/plain"));
    assert_eq!(m.field(Field::CorrelationId), Some("c-1"));
    assert_eq!(m.field(Field::Priority), Some("7"));
    assert!(m.field(Field::MessageId).is_some());
    // STOMP carries properties as strings.
    assert_eq!(m.property("x-colon"), Some(&Value::String("a:b".into())));
    assert_eq!(m.property("orderId"), Some(&Value::String("42".into())));
    assert!(m.protocol.iter().any(|(k, _)| k == "subscription"));
    assert_eq!(got[1].body, b"\0bin");
    assert_eq!(broker.acks(), 2);
}

#[test]
fn destination_options_and_selector_go_on_the_wire() {
    let broker = MockBroker::start(None);
    let ep = Endpoint::parse(&broker.url("")).unwrap();
    let mut dest = Destination::new("/queue/opts");
    dest.options
        .push(("activemq.prefetchSize".into(), "10".into()));

    let mut s = StompModule.connect(&ep, &ctx()).unwrap();
    s.send(&Destination::new("/queue/opts"), &Message::new("x"))
        .unwrap();
    let opts = ReceiveOptions {
        count: Some(1),
        ack: true,
        selector: Some("priority > 4".into()),
        headers: vec![("x-raw".into(), "1".into())],
        ..Default::default()
    };
    s.receive(&dest, &opts, &mut |_| Ok(())).unwrap();
    s.close().unwrap();

    let sub = &broker.frames("SUBSCRIBE")[0];
    assert_eq!(sub.get("activemq.prefetchSize"), Some("10"));
    assert_eq!(sub.get("selector"), Some("priority > 4"));
    assert_eq!(sub.get("x-raw"), Some("1"));
    assert_eq!(sub.get("destination"), Some("/queue/opts"));
}

#[test]
fn ttl_becomes_absolute_expires() {
    let broker = MockBroker::start(None);
    let ep = Endpoint::parse(&broker.url("")).unwrap();
    let mut s = StompModule.connect(&ep, &ctx()).unwrap();
    let mut msg = Message::new("x");
    msg.set_field(Field::Ttl, "1m").unwrap();
    msg.set_field(Field::GroupId, "g1").unwrap();
    s.send(&Destination::new("/queue/ttl"), &msg).unwrap();
    s.close().unwrap();

    let send = &broker.frames("SEND")[0];
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let expires: u128 = send.get("expires").unwrap().parse().unwrap();
    assert!(
        expires > now + 50_000 && expires <= now + 60_000,
        "{expires} vs {now}"
    );
    assert_eq!(send.get("JMSXGroupID"), Some("g1"));
    assert_eq!(send.get("ttl"), None);
}

#[test]
fn receive_times_out_on_empty_queue() {
    let broker = MockBroker::start(None);
    let ep = Endpoint::parse(&broker.url("")).unwrap();
    let mut s = StompModule.connect(&ep, &ctx()).unwrap();
    let start = Instant::now();
    let opts = ReceiveOptions {
        count: Some(1),
        timeout: Some(Duration::from_millis(300)),
        ack: true,
        ..Default::default()
    };
    let n = s
        .receive(&Destination::new("/queue/empty"), &opts, &mut |_| Ok(()))
        .unwrap();
    assert_eq!(n, 0);
    assert!(start.elapsed() >= Duration::from_millis(300));
    s.close().unwrap();
}

#[test]
fn bad_credentials_are_an_auth_error() {
    let broker = MockBroker::start(Some(("app", "secret")));
    let url = broker.url("").replace("stomp://", "stomp://app:wrong@");
    let err = StompModule
        .connect(&Endpoint::parse(&url).unwrap(), &ctx())
        .err()
        .unwrap();
    assert_eq!(exit_code(&err), 67, "{err:#}");
    assert!(err.to_string().contains("Bad credentials"));

    let url = broker.url("").replace("stomp://", "stomp://app:secret@");
    StompModule
        .connect(&Endpoint::parse(&url).unwrap(), &ctx())
        .unwrap()
        .close()
        .unwrap();
}

#[test]
fn connection_refused_is_a_connect_error() {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let ep = Endpoint::parse(&format!("stomp://127.0.0.1:{port}")).unwrap();
    let err = StompModule.connect(&ep, &ctx()).err().unwrap();
    assert_eq!(exit_code(&err), 7, "{err:#}");
}

/// Accepts connections, optionally answers CONNECT, then never reads or writes again.
fn stalled_server(answer_connect: bool) -> std::net::SocketAddr {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for mut s in listener.incoming().flatten() {
            if answer_connect {
                let mut buf = [0u8; 1024];
                let _ = s.read(&mut buf);
                let _ = s.write_all(b"CONNECTED\nversion:1.2\n\n\0");
            }
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_secs(30));
                drop(s);
            });
        }
    });
    addr
}

#[test]
fn receive_timeout_bounds_the_handshake() {
    let addr = stalled_server(false);
    let ep = Endpoint::parse(&format!("stomp://{addr}?receive-timeout=200ms")).unwrap();
    let start = Instant::now();
    let err = StompModule.connect(&ep, &ctx()).err().unwrap();
    assert_eq!(exit_code(&err), 28, "{err:#}");
    assert!(format!("{err:#}").contains("receive-timeout"), "{err:#}");
    assert!(start.elapsed() < Duration::from_secs(2));
}

#[test]
fn send_timeout_fires_when_server_stops_reading() {
    let addr = stalled_server(true);
    let ep = Endpoint::parse(&format!("stomp://{addr}?send-timeout=200ms&receipts=false")).unwrap();
    let mut s = StompModule.connect(&ep, &ctx()).unwrap();
    // Far larger than the socket buffers, so the write must block.
    let big = Message::new(vec![b'x'; 64 * 1024 * 1024]);
    let start = Instant::now();
    let err = s.send(&Destination::new("/queue/big"), &big).err().unwrap();
    assert_eq!(exit_code(&err), 28, "{err:#}");
    assert!(format!("{err:#}").contains("send-timeout"), "{err:#}");
    assert!(start.elapsed() < Duration::from_secs(5));
}

/// Answers CONNECT, then sends `payload` once a SUBSCRIBE arrives.
fn hostile_server(payload: Vec<u8>) -> std::net::SocketAddr {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        if let Ok((mut s, _)) = listener.accept() {
            let mut buf = [0u8; 4096];
            let _ = s.read(&mut buf); // CONNECT
            let _ = s.write_all(b"CONNECTED\nversion:1.2\n\n\0");
            let _ = s.read(&mut buf); // SUBSCRIBE
            let _ = s.write_all(&payload);
            std::thread::sleep(Duration::from_secs(5));
        }
    });
    addr
}

fn receive_one(addr: std::net::SocketAddr, query: &str) -> anyhow::Error {
    let ep = Endpoint::parse(&format!("stomp://{addr}{query}")).unwrap();
    let mut s = StompModule.connect(&ep, &ctx()).unwrap();
    let opts = ReceiveOptions {
        count: Some(1),
        timeout: Some(Duration::from_secs(5)),
        ..Default::default()
    };
    s.receive(&Destination::new("/queue/x"), &opts, &mut |_| Ok(()))
        .unwrap_err()
}

#[test]
fn oversized_content_length_is_refused_before_the_body() {
    let addr =
        hostile_server(b"MESSAGE\ndestination:/queue/x\ncontent-length:100000000\n\n".to_vec());
    let start = Instant::now();
    let err = receive_one(addr, "");
    assert_eq!(exit_code(&err), 8, "{err:#}");
    assert!(
        format!("{err:#}").contains("over max-frame-size (64 MiB)"),
        "{err:#}"
    );
    assert!(start.elapsed() < Duration::from_secs(2)); // did not wait for 100 MB
}

#[test]
fn unterminated_frame_is_cut_off_at_max_frame_size() {
    let mut junk = b"MESSAGE\ndestination:/queue/x\n\n".to_vec();
    junk.extend(std::iter::repeat_n(b'x', 256 * 1024)); // never NUL-terminated
    let addr = hostile_server(junk);
    let err = receive_one(addr, "?max-frame-size=64k");
    assert_eq!(exit_code(&err), 8, "{err:#}");
    assert!(
        format!("{err:#}").contains("larger than max-frame-size (64 KiB)"),
        "{err:#}"
    );
}

mod extensions {
    use std::time::{Duration, Instant};

    use pub_core::module::ReceiveOptions;
    use pub_core::{exit_code, Destination, Message, Session};
    use pub_stomp::mock::{MockBroker, MockConfig};

    fn connect(url: &str) -> pub_core::Result<pub_stomp::StompSession> {
        let mut ep = pub_core::Endpoint::parse(url).unwrap();
        ep.apply_credentials().unwrap();
        let ctx = pub_core::Context {
            trace: pub_core::Trace::default(),
            scheme: pub_core::Module::schemes(&pub_stomp::StompModule)[0],
        };
        pub_stomp::StompSession::connect(&ep, &ctx)
    }

    #[test]
    fn heart_beats_are_negotiated_sent_and_watched() {
        let broker = MockBroker::start_with(MockConfig {
            heart_beat: Some((100, 100)),
            ..Default::default()
        });
        // Waiting 1s with 100ms heart-beats: pub keeps sending them...
        let mut s = connect(&broker.url("?heart-beat=100ms")).unwrap();
        let o = ReceiveOptions {
            count: Some(1),
            timeout: Some(Duration::from_millis(150)),
            ack: true,
            ..Default::default()
        };
        let start = Instant::now();
        // ...but the mock never sends any, so after two missed beats pub gives up.
        let e = s
            .receive(
                &Destination::new("/queue/q"),
                &ReceiveOptions {
                    timeout: Some(Duration::from_secs(5)),
                    ..o.clone()
                },
                &mut |_| Ok(()),
            )
            .unwrap_err();
        assert_eq!(exit_code(&e), 28, "{e:#}");
        assert!(format!("{e:#}").contains("two heart-beats"), "{e:#}");
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "took {:?}",
            start.elapsed()
        );

        // A broker that doesn't do heart-beats: none sent, no deadline.
        let quiet = MockBroker::start(None);
        let mut s = connect(&quiet.url("?heart-beat=100ms")).unwrap();
        assert_eq!(
            s.receive(&Destination::new("/queue/q"), &o, &mut |_| Ok(()))
                .unwrap(),
            0
        );
        s.close().unwrap();
        assert_eq!(quiet.heartbeats(), 0);
    }

    #[test]
    fn pub_sends_heart_beats_while_waiting() {
        // The mock wants heart-beats every 100ms but offers none (sx = 0): pub
        // sends, and doesn't expect any back.
        let broker = MockBroker::start_with(MockConfig {
            heart_beat: Some((0, 100)),
            ..Default::default()
        });
        let mut s = connect(&broker.url("?heart-beat=100ms")).unwrap();
        let o = ReceiveOptions {
            count: Some(1),
            timeout: Some(Duration::from_millis(700)),
            ack: true,
            ..Default::default()
        };
        assert_eq!(
            s.receive(&Destination::new("/queue/q"), &o, &mut |_| Ok(()))
                .unwrap(),
            0
        );
        s.close().unwrap();
        assert!(broker.heartbeats() >= 4, "{}", broker.heartbeats());
    }

    #[test]
    fn browse_on_activemq_classic_only() {
        let classic = MockBroker::start_with(MockConfig {
            server: Some("ActiveMQ/6.2.0".into()),
            ..Default::default()
        });
        let mut s = connect(&classic.url("")).unwrap();
        for b in ["a", "b"] {
            s.send(&Destination::new("/queue/q"), &Message::new(b))
                .unwrap();
        }
        let all = ReceiveOptions {
            ack: true,
            ..Default::default()
        };
        let mut got = Vec::new();
        s.browse(&Destination::new("/queue/q"), &all, &mut |m| {
            got.push(m.body);
            Ok(())
        })
        .unwrap();
        assert_eq!(got, [b"a".to_vec(), b"b".to_vec()]);
        assert_eq!(classic.queued("/queue/q").len(), 2);
        assert_eq!(classic.frames("SUBSCRIBE")[0].get("browser"), Some("true"));
        assert_eq!(classic.acks(), 0);

        let other = MockBroker::start(None);
        let mut s = connect(&other.url("")).unwrap();
        let e = s
            .browse(&Destination::new("/queue/q"), &all, &mut |_| Ok(()))
            .unwrap_err();
        assert_eq!(exit_code(&e), 1, "{e:#}");
    }

    #[test]
    fn durable_subscriptions() {
        let broker = MockBroker::start(None);
        let o = ReceiveOptions {
            subscription: Some("audit".into()),
            timeout: Some(Duration::from_millis(100)),
            count: Some(1),
            ack: true,
            ..Default::default()
        };
        let mut anon = connect(&broker.url("")).unwrap();
        let e = anon
            .receive(&Destination::new("/topic/t"), &o, &mut |_| Ok(()))
            .unwrap_err();
        assert_eq!(exit_code(&e), 2, "{e:#}");
        let mut s = connect(&broker.url("?client-id=app")).unwrap();
        let e = s
            .receive(&Destination::new("/queue/q"), &o, &mut |_| Ok(()))
            .unwrap_err();
        assert!(format!("{e:#}").contains("for topics"), "{e:#}");
        s.receive(&Destination::new("/topic/t"), &o, &mut |_| Ok(()))
            .unwrap();
        s.unsubscribe(&Destination::new("/topic/t"), "audit")
            .unwrap();
        s.close().unwrap();
        let sub = &broker.frames("SUBSCRIBE")[0];
        assert_eq!(sub.get("activemq.subscriptionName"), Some("audit"));
        assert_eq!(sub.get("durable-subscription-name"), Some("audit"));
        // The durable consumer isn't UNSUBSCRIBEd at the end; the unsubscribe names it.
        let unsubs = broker.frames("UNSUBSCRIBE");
        assert_eq!(unsubs.len(), 1);
        assert_eq!(unsubs[0].get("activemq.subscriptionName"), Some("audit"));
        assert_eq!(broker.frames("CONNECT")[1].get("client-id"), Some("app"));
    }

    #[test]
    fn transactions_hold_sends_and_acks_until_commit() {
        let broker = MockBroker::start(None);
        let mut s = connect(&broker.url("")).unwrap();
        let q = Destination::new("/queue/tx");
        s.begin().unwrap();
        for b in ["a", "b"] {
            s.send(&q, &Message::new(b)).unwrap();
        }
        assert!(
            broker.queued("/queue/tx").is_empty(),
            "visible before commit"
        );
        assert_eq!(exit_code(&s.begin().unwrap_err()), 2);
        s.commit().unwrap();
        assert_eq!(broker.queued("/queue/tx").len(), 2);
        s.begin().unwrap();
        s.send(&q, &Message::new("dropped")).unwrap();
        s.rollback().unwrap();
        assert_eq!(broker.queued("/queue/tx").len(), 2);
        assert_eq!(exit_code(&s.commit().unwrap_err()), 2);

        // Receiving in batches of one: each ACK commits on its own.
        let o = ReceiveOptions {
            count: Some(2),
            timeout: Some(Duration::from_secs(2)),
            ack: true,
            transaction: Some(1),
            ..Default::default()
        };
        assert_eq!(s.receive(&q, &o, &mut |_| Ok(())).unwrap(), 2);
        s.close().unwrap();
        // send, then rollback, then 2 batches + the final (empty) one
        assert_eq!(broker.transactions(), (1 + 3, 1));
        assert_eq!(broker.acks(), 2);
        let sends = broker.frames("SEND");
        assert!(sends.iter().all(|f| f.get("transaction").is_some()));
        assert!(broker
            .frames("ACK")
            .iter()
            .all(|f| f.get("transaction").is_some()));
    }

    #[test]
    fn move_copies_headers_in_batches_and_rolls_back_on_error() {
        let broker = MockBroker::start(None);
        let mut s = connect(&broker.url("")).unwrap();
        for i in 0..3 {
            let mut m = Message::new(format!("m{i}"));
            m.properties.push(pub_core::message::Property {
                name: "n".into(),
                value: pub_core::Value::String(i.to_string()),
            });
            s.send(&Destination::new("/queue/dlq"), &m).unwrap();
        }
        let o = ReceiveOptions {
            idle: Some(Duration::from_millis(300)),
            transaction: Some(2),
            ..Default::default()
        };
        let mut seen = Vec::new();
        let moved = s
            .move_messages(
                &Destination::new("/queue/dlq"),
                &Destination::new("/queue/back"),
                &o,
                &mut |m| {
                    seen.push(m.body);
                    Ok(())
                },
            )
            .unwrap();
        s.close().unwrap();
        assert_eq!(moved, 3);
        assert_eq!(seen.len(), 3);
        assert_eq!(
            broker.queued("/queue/back"),
            [b"m0".to_vec(), b"m1".to_vec(), b"m2".to_vec()]
        );
        let back: Vec<_> = broker
            .frames("SEND")
            .into_iter()
            .filter(|f| f.get("destination") == Some("/queue/back"))
            .collect();
        assert_eq!(back[2].get("n"), Some("2"));
        assert!(back
            .iter()
            .all(|f| f.get("subscription").is_none() && f.get("redelivered").is_none()));
        assert!(
            back.iter().all(|f| f.get("message-id").is_none()),
            "the broker gives copies new ids"
        );
        assert_eq!(broker.transactions(), (2, 0)); // batches of 2 + 1
        assert_eq!(broker.acks(), 3);

        // A failing writer rolls the open batch back.
        let mut s = connect(&broker.url("")).unwrap();
        let e = s
            .move_messages(
                &Destination::new("/queue/back"),
                &Destination::new("/queue/dlq"),
                &o,
                &mut |_| Err(pub_core::err!(Io, "disk full")),
            )
            .unwrap_err();
        assert_eq!(exit_code(&e), 26);
        s.close().unwrap();
        assert_eq!(broker.transactions(), (2, 1));
        assert!(
            broker.queued("/queue/dlq").is_empty(),
            "the copy was rolled back"
        );
        let mut s = connect(&broker.url("")).unwrap();
        let e = s
            .move_messages(
                &Destination::new("/queue/a"),
                &Destination::new("/queue/a"),
                &o,
                &mut |_| Ok(()),
            )
            .unwrap_err();
        assert_eq!(exit_code(&e), 2);
    }
}
