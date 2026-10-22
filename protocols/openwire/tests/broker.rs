//! Wire-format check against a real broker (opt-in):
//!
//! ```text
//! PUB_OPENWIRE_BROKER=localhost:61616 cargo test -p pub-openwire --test broker -- --ignored --nocapture
//! ```
//!
//! Decodes the broker's own WireFormatInfo, negotiates (preferring tight
//! encoding), decodes its BrokerInfo, and checks the broker accepts pub's
//! ConnectionInfo by answering with a Response.

use std::net::TcpStream;
use std::time::Duration;

use pub_core::transport::Stream;
use pub_core::{Connection, Trace};
use pub_openwire::primitive::{decode_map, encode_map, Primitive};
use pub_openwire::types::*;
use pub_openwire::{DataStructure, OpenWireCodec};

fn prop<'a>(map: &'a [(String, Primitive)], key: &str) -> Option<&'a Primitive> {
    map.iter().find(|(k, _)| k == key).map(|(_, v)| v)
}

#[test]
#[ignore]
fn negotiate_and_connect() {
    let addr = std::env::var("PUB_OPENWIRE_BROKER").unwrap_or_else(|_| "localhost:61616".into());
    let user = std::env::var("PUB_OPENWIRE_USER").ok();
    let pass = std::env::var("PUB_OPENWIRE_PASSWORD").ok();
    let tcp = TcpStream::connect(&addr).expect("connect");
    let mut conn = Connection::new(Stream::Plain(tcp), OpenWireCodec::default(), Trace::new(0));
    let wait = Some(Duration::from_secs(10));

    // 1. Ours first, loose: like the Java client, don't wait for the broker —
    //    Artemis's multi-protocol acceptor detects the protocol from the client's
    //    first bytes and stays silent until then. Version 12, prefer tight,
    //    no cache, no stack traces.
    let ours = vec![
        ("TightEncodingEnabled".to_string(), Primitive::Bool(true)),
        ("CacheEnabled".into(), Primitive::Bool(false)),
        ("StackTraceEnabled".into(), Primitive::Bool(false)),
        ("TcpNoDelayEnabled".into(), Primitive::Bool(true)),
        ("SizePrefixDisabled".into(), Primitive::Bool(false)),
        ("MaxInactivityDuration".into(), Primitive::Long(30_000)),
        (
            "MaxInactivityDurationInitalDelay".into(),
            Primitive::Long(10_000),
        ),
        ("CacheSize".into(), Primitive::Int(0)),
        ("MaxFrameSize".into(), Primitive::Long(i64::MAX)),
        ("ProviderName".into(), Primitive::String("pub".into())),
    ];
    conn.send(&DataStructure::WireFormatInfo(Box::new(WireFormatInfo {
        magic: MAGIC,
        version: 12,
        marshalled_properties: Some(encode_map(&ours).unwrap()),
    })))
    .unwrap();

    // 2. The broker's, loose.
    let broker_wf = match conn.recv(wait).unwrap().expect("broker WireFormatInfo") {
        DataStructure::WireFormatInfo(w) => w,
        other => panic!("expected WireFormatInfo, got {}", other.name()),
    };
    assert_eq!(broker_wf.magic, MAGIC);
    let broker_props = decode_map(broker_wf.marshalled_properties.as_deref().unwrap())
        .unwrap()
        .unwrap();
    println!("broker WireFormatInfo: version {}", broker_wf.version);
    for (k, v) in &broker_props {
        println!("  {k} = {v:?}");
    }

    // Negotiation: version = min, tight = AND.
    let version = broker_wf.version.min(12);
    let tight = prop(&broker_props, "TightEncodingEnabled") == Some(&Primitive::Bool(true));
    println!("negotiated: version {version}, tight encoding {tight}");
    assert_eq!(version, 12, "broker negotiates below OpenWire 12");
    conn.codec.encoding.tight = tight;

    // 3. Now in the negotiated encoding: BrokerInfo, then our ConnectionInfo's Response.
    conn.send(&DataStructure::ConnectionInfo(Box::new(ConnectionInfo {
        command_id: 1,
        response_required: true,
        connection_id: Some(Box::new(DataStructure::ConnectionId(Box::new(
            ConnectionId {
                value: Some("ID:pub-wire-check-1".into()),
            },
        )))),
        client_id: Some("pub-wire-check".into()),
        user_name: user,
        password: pass,
        manageable: false,
        ..Default::default()
    })))
    .unwrap();

    let (mut saw_broker_info, mut response) = (false, None);
    while response.is_none() {
        match conn.recv(wait).unwrap().expect("broker reply") {
            DataStructure::BrokerInfo(b) => {
                println!(
                    "BrokerInfo: name {:?}, url {:?}",
                    b.broker_name, b.broker_url
                );
                saw_broker_info = true;
            }
            DataStructure::Response(r) => {
                response = Some(format!("Response(correlation {})", r.correlation_id))
            }
            DataStructure::ExceptionResponse(e) => {
                panic!("broker refused ConnectionInfo: {:?}", e.exception)
            }
            other => println!("(also received {})", other.name()),
        }
    }
    println!("{}", response.unwrap());
    assert!(saw_broker_info);

    conn.send(&DataStructure::ShutdownInfo(Box::new(ShutdownInfo {
        command_id: 2,
        response_required: false,
    })))
    .unwrap();
}
