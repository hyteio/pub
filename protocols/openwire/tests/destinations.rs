//! Destination management against the mock broker (ActiveMQ Classic style:
//! advisory topics and DestinationInfo).

use std::time::Duration;

use pub_core::module::{Destination, DestinationKinds};
use pub_core::{exit_code, Context, Endpoint, Module, Session, Trace};
use pub_openwire::mock::{MockBroker, MockConfig};
use pub_openwire::session::BrokerType;
use pub_openwire::{OpenWireModule, OpenWireSession};

fn connect(url: &str) -> OpenWireSession {
    let ctx = Context {
        trace: Trace::default(),
        scheme: OpenWireModule.schemes()[0],
    };
    OpenWireSession::connect(&Endpoint::parse(url).unwrap(), &ctx).unwrap()
}

const ALL: DestinationKinds = DestinationKinds {
    queues: true,
    topics: true,
    internal: false,
};
const WAIT: Duration = Duration::from_millis(300);

#[test]
fn create_list_and_delete() {
    let broker = MockBroker::start(MockConfig::default());
    let mut s = connect(&broker.url(""));
    assert_eq!(s.broker_type(), BrokerType::Classic); // "ID:mock-broker"
    s.create_destination(&Destination::new("/queue/orders"))
        .unwrap();
    s.create_destination(&Destination::new("topic://prices"))
        .unwrap();
    s.send(&Destination::new("q2"), &pub_core::Message::new("x"))
        .unwrap();
    assert_eq!(
        s.list_destinations(ALL, WAIT).unwrap(),
        ["queue://orders", "queue://q2", "topic://prices"]
    );
    let queues = DestinationKinds {
        topics: false,
        ..ALL
    };
    assert_eq!(
        s.list_destinations(queues, WAIT).unwrap(),
        ["queue://orders", "queue://q2"]
    );

    s.delete_destination(&Destination::new("queue://q2"))
        .unwrap();
    assert!(broker.queued("queue://q2").is_empty());
    assert_eq!(broker.destinations(), ["queue://orders", "topic://prices"]);
    s.close().unwrap();
}

#[test]
fn artemis_listing_needs_its_management_api() {
    // The mock is not Artemis: with broker-type=artemis nothing answers the
    // management request, and the error says what's needed.
    let broker = MockBroker::start(MockConfig::default());
    let mut s = connect(&broker.url("?broker-type=artemis&receive-timeout=300ms"));
    assert_eq!(s.broker_type(), BrokerType::Artemis);
    let e = s.list_destinations(ALL, WAIT).unwrap_err();
    assert_eq!(exit_code(&e), 28, "{e:#}");
    assert!(format!("{e:#}").contains("manage"), "{e:#}");
    let requests: Vec<_> = broker.messages();
    assert_eq!(requests.len(), 2); // getQueueNames and getAddressNames, sent together
    assert!(requests.iter().all(|m| m.fields.reply_to.is_some()));
    assert_eq!(
        requests[0].fields.correlation_id.as_deref(),
        Some("pub-mgmt-0")
    );
}
