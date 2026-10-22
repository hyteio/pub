use std::time::{Duration, Instant};

use pub_core::{exit_code, Context, Endpoint, Module, Trace};
use pub_openwire::mock::{MockBroker, MockConfig};
use pub_openwire::{DataStructure, OpenWireModule, OpenWireSession};

fn ctx() -> Context {
    Context {
        trace: Trace::default(),
        scheme: OpenWireModule.schemes()[0],
    }
}

/// An endpoint with Java-style names normalized, as the CLI does.
fn endpoint(url: &str) -> Endpoint {
    let mut registry = pub_core::Registry::new();
    registry.register(pub_openwire::module());
    let mut ep = Endpoint::parse(url).unwrap();
    registry.normalize_settings(&mut ep);
    ep
}

fn connect(url: &str) -> pub_core::Result<OpenWireSession> {
    OpenWireSession::connect(&Endpoint::parse(url).unwrap(), &ctx())
}

#[test]
fn connects_sending_first_and_negotiates_tight() {
    let broker = MockBroker::start(MockConfig::default());
    let mut s = OpenWireModule
        .connect(&Endpoint::parse(&broker.url("")).unwrap(), &ctx())
        .unwrap();
    let info = s.info();
    let get = |k: &str| info.iter().find(|(n, _)| n == k).map(|(_, v)| v.clone());
    assert_eq!(get("version").as_deref(), Some("12"));
    assert_eq!(get("encoding").as_deref(), Some("tight"));
    assert_eq!(get("provider").as_deref(), Some("mock 1.0"));
    s.close().unwrap();
    assert_eq!(broker.negotiated_tight(), Some(true));

    // ConnectionInfo carried a generated client id and our connection id.
    match &broker.commands("ConnectionInfo")[0] {
        DataStructure::ConnectionInfo(ci) => {
            assert!(ci.response_required);
            assert!(ci.client_id.as_deref().unwrap().starts_with("ID:pub-"));
        }
        other => panic!("{other:?}"),
    }
    // Close: session RemoveInfo, connection RemoveInfo (response required), ShutdownInfo.
    let removes = broker.commands("RemoveInfo");
    assert_eq!(removes.len(), 2);
    assert!(matches!(&removes[1], DataStructure::RemoveInfo(r) if r.response_required));
    let start = Instant::now();
    while broker.commands("ShutdownInfo").is_empty() && start.elapsed() < Duration::from_secs(2) {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(broker.commands("ShutdownInfo").len(), 1);
}

#[test]
fn client_id_prefix_like_java() {
    let broker = MockBroker::start(MockConfig::default());
    let client_id = |query: &str| {
        let mut s = OpenWireModule
            .connect(&endpoint(&broker.url(query)), &ctx())
            .unwrap();
        s.close().unwrap();
        match broker.commands("ConnectionInfo").last() {
            Some(DataStructure::ConnectionInfo(ci)) => ci.client_id.clone().unwrap(),
            other => panic!("{other:?}"),
        }
    };
    // Java's URI name (any case) and pub's own; an explicit client ID wins.
    let id = client_id("?jms.clientIdPrefix=orders-app");
    assert!(
        id.starts_with(&format!("orders-app-{}-", std::process::id())),
        "{id}"
    );
    assert!(id.contains("-0:"), "{id}");
    assert!(client_id("?client-id-prefix=billing").starts_with("billing-"));
    assert_eq!(
        client_id("?jms.clientIDPrefix=x&jms.clientID=fixed"),
        "fixed"
    );
    assert!(client_id("").starts_with("ID:pub-"));
}

#[test]
fn connection_id_prefix_like_java() {
    let broker = MockBroker::start(MockConfig::default());
    let mut s = OpenWireSession::connect(
        &endpoint(&broker.url("?jms.connectionIdPrefix=orders-conn")),
        &ctx(),
    )
    .unwrap();
    let id = s.connection_id().to_string();
    assert!(
        id.starts_with(&format!("orders-conn-{}-", std::process::id())),
        "{id}"
    );
    assert!(id.contains("-1:"), "{id}");
    // Producer and message IDs are built on it, and the client ID defaults to it.
    pub_core::Session::send(
        &mut s,
        &pub_core::module::Destination::new("q"),
        &pub_core::Message::new("x"),
    )
    .unwrap();
    pub_core::Session::close(&mut s).unwrap();
    match broker.commands("ConnectionInfo").last() {
        Some(DataStructure::ConnectionInfo(ci)) => {
            assert_eq!(ci.client_id.as_deref(), Some(id.as_str()))
        }
        other => panic!("{other:?}"),
    }
    let mid = pub_openwire::consumer::message_id_string(
        broker.messages()[0].fields.message_id.as_deref().unwrap(),
    )
    .unwrap();
    assert_eq!(mid, format!("{id}:1:1:1"));
}

#[test]
fn falls_back_to_loose_when_either_side_declines_tight() {
    let loose_broker = MockBroker::start(MockConfig {
        tight: false,
        ..Default::default()
    });
    let s = connect(&loose_broker.url("")).unwrap();
    assert!(!s.negotiated.tight);

    let broker = MockBroker::start(MockConfig::default());
    let s = connect(&broker.url("?wireFormat.tightEncodingEnabled=false")).unwrap();
    assert!(!s.negotiated.tight);
    assert_eq!(broker.negotiated_tight(), Some(false));
}

#[test]
fn refusals_map_to_exit_codes() {
    let broker = MockBroker::start(MockConfig {
        credentials: Some(("app".into(), "s3cret".into())),
        ..Default::default()
    });
    let e = connect(&broker.url("").replace("amq://", "amq://app:wrong@"))
        .err()
        .unwrap();
    assert_eq!(exit_code(&e), 67, "{e:#}");
    assert!(
        format!("{e:#}").contains("User name [app] or password is invalid. (SecurityException)"),
        "{e:#}"
    );
    assert!(connect(&broker.url("").replace("amq://", "amq://app:s3cret@")).is_ok());

    let old = MockBroker::start(MockConfig {
        version: 11,
        ..Default::default()
    });
    let e = connect(&old.url("")).err().unwrap();
    assert_eq!(exit_code(&e), 8);
    assert!(e.to_string().contains("version 11"), "{e}");
}

#[test]
fn keep_alives_hold_a_quiet_connection_open() {
    let broker = MockBroker::start(MockConfig {
        max_inactivity_ms: 900,
        keep_alive_every: Some(Duration::from_millis(250)),
        ..Default::default()
    });
    let mut s = connect(&broker.url("?wireFormat.maxInactivityDurationInitalDelay=0")).unwrap();
    assert_eq!(
        s.negotiated.max_inactivity,
        Some(Duration::from_millis(900))
    );
    s.keep_alive_for(Duration::from_millis(2000)).unwrap();
    // We wrote nothing else, so keep-alives went out every ~300 ms.
    assert!(
        broker.commands("KeepAliveInfo").len() >= 4,
        "{}",
        broker.commands("KeepAliveInfo").len()
    );
}

#[test]
fn a_silent_broker_is_detected() {
    let broker = MockBroker::start(MockConfig {
        max_inactivity_ms: 600,
        silent_after_connect: true,
        ..Default::default()
    });
    let mut s = connect(&broker.url("?wireFormat.maxInactivityDurationInitalDelay=0")).unwrap();
    let start = Instant::now();
    let e = s.keep_alive_for(Duration::from_secs(10)).unwrap_err();
    assert_eq!(exit_code(&e), 28, "{e:#}");
    assert!(
        e.to_string().contains("wireFormat.maxInactivityDuration"),
        "{e}"
    );
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "{:?}",
        start.elapsed()
    );
}
