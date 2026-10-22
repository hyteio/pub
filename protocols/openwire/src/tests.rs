//! Codec tests: exact bytes derived by hand from the Java marshallers, and
//! round trips of every data structure in loose and tight encoding.

use pub_core::{Codec, Decoded};

use crate::codec::OpenWireCodec;
use crate::primitive::{encode_map, Primitive};
use crate::types::*;
use crate::wire::Encoding;

const LOOSE: Encoding = Encoding {
    tight: false,
    stack_traces: false,
};
const TIGHT: Encoding = Encoding {
    tight: true,
    stack_traces: false,
};

fn codec(enc: Encoding) -> OpenWireCodec {
    OpenWireCodec {
        encoding: enc,
        ..Default::default()
    }
}

fn encode(enc: Encoding, ds: &DataStructure) -> Vec<u8> {
    let mut out = Vec::new();
    codec(enc).encode(ds, &mut out).unwrap();
    out
}

fn decode(enc: Encoding, bytes: &[u8]) -> DataStructure {
    match codec(enc).decode(bytes).unwrap() {
        Decoded::Frame(ds, n) => {
            assert_eq!(n, bytes.len());
            ds
        }
        other => panic!("{other:?}"),
    }
}

fn b<T>(v: T) -> Option<Box<T>> {
    Some(Box::new(v))
}

fn ds_session_id() -> DataStructure {
    DataStructure::SessionId(Box::new(SessionId {
        connection_id: Some("c".into()),
        value: 1,
    }))
}

#[test]
fn keep_alive_exact_bytes() {
    let ka = DataStructure::KeepAliveInfo(Box::new(KeepAliveInfo {
        command_id: 5,
        response_required: false,
    }));
    // Loose: size 6 = type + i32 + bool byte.
    assert_eq!(encode(LOOSE, &ka), [0, 0, 0, 6, 10, 0, 0, 0, 5, 0]);
    // Tight: size 7 = type + bit-stream prefix + 1 bit byte + i32.
    assert_eq!(encode(TIGHT, &ka), [0, 0, 0, 7, 10, 1, 0x00, 0, 0, 0, 5]);
}

#[test]
fn nested_object_exact_bytes() {
    let si = DataStructure::SessionInfo(Box::new(SessionInfo {
        command_id: 1,
        response_required: true,
        session_id: Some(Box::new(ds_session_id())),
    }));
    // Loose: cmd, resp, present, type 121, string(present, len 1, 'c'), long 1.
    assert_eq!(
        encode(LOOSE, &si),
        [0, 0, 0, 20, 4, 0, 0, 0, 1, 1, 1, 121, 1, 0, 1, b'c', 0, 0, 0, 0, 0, 0, 0, 1]
    );
    // Tight bits, LSB first: resp=1, present=1, string present=1, ascii=1, long size (0,1) = 0b10_1111.
    // Data: cmd, type 121, string len 1 'c', long as u16.
    assert_eq!(
        encode(TIGHT, &si),
        [0, 0, 0, 13, 4, 1, 0x2F, 0, 0, 0, 1, 121, 0, 1, b'c', 0, 1]
    );
    for enc in [LOOSE, TIGHT] {
        assert_eq!(decode(enc, &encode(enc, &si)), si);
    }
}

fn sample_message(kind: u8) -> Message {
    let dest = DataStructure::Queue(Box::new(Destination {
        physical_name: Some("orders".into()),
    }));
    let producer = DataStructure::ProducerId(Box::new(ProducerId {
        connection_id: Some("ID:host-1".into()),
        value: 1,
        session_id: 1,
    }));
    let props = encode_map(&[
        ("region".into(), Primitive::String("EU".into())),
        ("n".into(), Primitive::Long(42)),
    ])
    .unwrap();
    Message {
        kind,
        fields: MessageFields {
            command_id: 7,
            producer_id: Some(Box::new(producer.clone())),
            destination: Some(Box::new(dest)),
            message_id: b(DataStructure::MessageId(Box::new(MessageId {
                text_view: None,
                producer_id: Some(Box::new(producer)),
                producer_sequence_id: 3,
                broker_sequence_id: 0,
            }))),
            correlation_id: Some("c-1".into()),
            persistent: true,
            expiration: 1_790_000_000_000,
            priority: 4,
            reply_to: b(DataStructure::TempQueue(Box::new(Destination {
                physical_name: Some("ID:tmp".into()),
            }))),
            timestamp: 1_789_999_999_000,
            jms_type: Some("order".into()),
            content: Some(crate::primitive::encode_text("héllo")),
            marshalled_properties: Some(props),
            redelivery_counter: 2,
            group_id: Some("g".into()),
            group_sequence: 9,
            ..Default::default()
        },
        blob: (kind == kind::BLOB).then(|| BlobFields {
            remote_blob_url: Some("http://x".into()),
            mime_type: None,
            deleted_by_broker: true,
        }),
    }
}

#[test]
fn nested_messages_carry_the_marshal_aware_bit_in_tight_encoding() {
    let md = DataStructure::MessageDispatch(Box::new(MessageDispatch {
        command_id: 1,
        response_required: false,
        consumer_id: None,
        destination: None,
        message: b(DataStructure::Message(Box::new(sample_message(kind::TEXT)))),
        redelivery_counter: 0,
    }));
    let bytes = encode(TIGHT, &md);
    // Bits: resp=0, consumer absent=0, destination absent=0, message present=1, cached form=0.
    let first_bits = bytes[6];
    assert_eq!(first_bits & 0x1F, 0b0_1000);
    assert_eq!(decode(TIGHT, &bytes), md);

    // Setting the "cached form" bit is refused (pub never negotiates the cache).
    let mut tampered = bytes.clone();
    tampered[6] |= 0b1_0000;
    assert!(codec(TIGHT).decode(&tampered).is_err());
}

fn all_structures() -> Vec<DataStructure> {
    use DataStructure as D;
    let error = BrokerError {
        class: Some("java.lang.SecurityException".into()),
        message: Some("User name [app] or password is invalid.".into()),
        stack: vec![],
        cause: None,
    };
    let cid = || {
        b(D::ConnectionId(Box::new(ConnectionId {
            value: Some("ID:host-1".into()),
        })))
    };
    let consumer = || {
        b(D::ConsumerId(Box::new(ConsumerId {
            connection_id: Some("ID:host-1".into()),
            session_id: 1,
            value: 70_000,
        })))
    };
    let topic = || {
        b(D::Topic(Box::new(Destination {
            physical_name: Some("prices.>".into()),
        })))
    };
    let mut v = vec![
        D::WireFormatInfo(Box::new(WireFormatInfo {
            magic: MAGIC,
            version: 12,
            marshalled_properties: Some(
                encode_map(&[("CacheEnabled".into(), Primitive::Bool(false))]).unwrap(),
            ),
        })),
        D::BrokerInfo(Box::new(BrokerInfo {
            command_id: 1,
            broker_id: b(D::BrokerId(Box::new(BrokerId {
                value: Some("ID:broker".into()),
            }))),
            broker_url: Some("tcp://0.0.0.0:61616".into()),
            peer_broker_infos: Some(vec![]),
            broker_name: Some("localhost".into()),
            connection_id: 5_000_000_000,
            ..Default::default()
        })),
        D::ConnectionInfo(Box::new(ConnectionInfo {
            command_id: 1,
            response_required: true,
            connection_id: cid(),
            client_id: Some("pub-1".into()),
            user_name: Some("app".into()),
            password: Some("pässword".into()),
            manageable: true,
            ..Default::default()
        })),
        D::SessionInfo(Box::new(SessionInfo {
            command_id: 2,
            session_id: Some(Box::new(ds_session_id())),
            ..Default::default()
        })),
        D::ConsumerInfo(Box::new(ConsumerInfo {
            command_id: 3,
            response_required: true,
            consumer_id: consumer(),
            destination: topic(),
            prefetch_size: 1000,
            maximum_pending_message_limit: -1,
            dispatch_async: true,
            selector: Some("region = 'EU'".into()),
            priority: 0,
            ..Default::default()
        })),
        D::ProducerInfo(Box::new(ProducerInfo {
            command_id: 4,
            destination: topic(),
            window_size: 65536,
            ..Default::default()
        })),
        D::DestinationInfo(Box::new(DestinationInfo {
            connection_id: cid(),
            destination: topic(),
            operation_type: 1,
            timeout: 0,
            ..Default::default()
        })),
        D::KeepAliveInfo(Box::new(KeepAliveInfo {
            command_id: 9,
            response_required: true,
        })),
        D::ShutdownInfo(Box::default()),
        D::RemoveInfo(Box::new(RemoveInfo {
            command_id: 10,
            object_id: consumer(),
            last_delivered_sequence_id: -1,
            ..Default::default()
        })),
        D::ConnectionError(Box::new(ConnectionError {
            exception: Some(error.clone()),
            connection_id: cid(),
            ..Default::default()
        })),
        D::ConsumerControl(Box::new(ConsumerControl {
            consumer_id: consumer(),
            prefetch: 5,
            start: true,
            ..Default::default()
        })),
        D::ConnectionControl(Box::new(ConnectionControl {
            reconnect_to: Some("tcp://b:61616".into()),
            token: Some(vec![1, 2]),
            ..Default::default()
        })),
        D::ProducerAck(Box::new(ProducerAck {
            size: 1024,
            ..Default::default()
        })),
        D::TransactionInfo(Box::new(TransactionInfo {
            command_id: 3,
            response_required: true,
            connection_id: Some(Box::new(D::ConnectionId(Box::new(ConnectionId {
                value: Some("ID:c".into()),
            })))),
            transaction_id: Some(Box::new(D::LocalTransactionId(Box::new(
                LocalTransactionId {
                    value: 7,
                    connection_id: Some(Box::new(D::ConnectionId(Box::new(ConnectionId {
                        value: Some("ID:c".into()),
                    })))),
                },
            )))),
            kind: tx::COMMIT_ONE_PHASE,
        })),
        D::RemoveSubscriptionInfo(Box::new(RemoveSubscriptionInfo {
            command_id: 9,
            response_required: true,
            connection_id: Some(Box::new(D::ConnectionId(Box::new(ConnectionId {
                value: Some("ID:c".into()),
            })))),
            subscription_name: Some("audit".into()),
            client_id: Some("app".into()),
        })),
        D::MessagePull(Box::new(MessagePull {
            consumer_id: consumer(),
            timeout: 5000,
            ..Default::default()
        })),
        D::MessageAck(Box::new(MessageAck {
            command_id: 11,
            destination: topic(),
            consumer_id: consumer(),
            ack_type: 4,
            first_message_id: None,
            message_count: 1,
            poison_cause: Some(error.clone()),
            ..Default::default()
        })),
        D::Response(Box::new(Response {
            command_id: 12,
            correlation_id: 1,
            ..Default::default()
        })),
        D::ExceptionResponse(Box::new(ExceptionResponse {
            correlation_id: 1,
            exception: Some(error),
            ..Default::default()
        })),
        D::DataResponse(Box::new(DataResponse {
            correlation_id: 2,
            data: cid(),
            ..Default::default()
        })),
        D::DataArrayResponse(Box::new(DataArrayResponse {
            correlation_id: 3,
            data: Some(vec![*cid().unwrap()]),
            ..Default::default()
        })),
        D::IntegerResponse(Box::new(IntegerResponse {
            correlation_id: 4,
            result: -7,
            ..Default::default()
        })),
        D::LocalTransactionId(Box::new(LocalTransactionId {
            value: 3,
            connection_id: cid(),
        })),
        D::XaTransactionId(Box::new(XaTransactionId {
            format_id: 1,
            global_transaction_id: Some(vec![9]),
            branch_qualifier: None,
        })),
    ];
    for k in kind::MESSAGE..=kind::BLOB {
        v.push(D::Message(Box::new(sample_message(k))));
    }
    v.push(D::MessageDispatch(Box::new(MessageDispatch {
        consumer_id: consumer(),
        destination: topic(),
        message: b(D::Message(Box::new(sample_message(kind::BYTES)))),
        redelivery_counter: 1,
        ..Default::default()
    })));
    v
}

#[test]
fn every_structure_round_trips_in_both_encodings() {
    for enc in [
        LOOSE,
        TIGHT,
        Encoding {
            tight: true,
            stack_traces: true,
        },
        Encoding {
            tight: false,
            stack_traces: true,
        },
    ] {
        for ds in all_structures() {
            let bytes = encode(enc, &ds);
            assert_eq!(decode(enc, &bytes), ds, "{} in {enc:?}", ds.name());
        }
    }
}

#[test]
fn tight_is_smaller_than_loose() {
    let msg = DataStructure::Message(Box::new(sample_message(kind::TEXT)));
    let (loose, tight) = (encode(LOOSE, &msg).len(), encode(TIGHT, &msg).len());
    assert!(tight < loose, "tight {tight} vs loose {loose}");
}

#[test]
fn throwables_with_stack_traces() {
    let enc = Encoding {
        tight: true,
        stack_traces: true,
    };
    let err = BrokerError {
        class: Some("javax.jms.JMSException".into()),
        message: None,
        stack: vec![StackFrame {
            class: Some("A".into()),
            method: Some("m".into()),
            file: None,
            line: 12,
        }],
        cause: Some(Box::new(BrokerError {
            class: Some("java.io.IOException".into()),
            ..Default::default()
        })),
    };
    let ds = DataStructure::ExceptionResponse(Box::new(ExceptionResponse {
        correlation_id: 1,
        exception: Some(err),
        ..Default::default()
    }));
    assert_eq!(decode(enc, &encode(enc, &ds)), ds);
}

#[test]
fn framing() {
    let ds = all_structures().remove(2); // ConnectionInfo
    for enc in [LOOSE, TIGHT] {
        let bytes = encode(enc, &ds);
        for cut in 0..bytes.len() {
            assert_eq!(
                codec(enc).decode(&bytes[..cut]).unwrap(),
                Decoded::NeedMore,
                "cut {cut}"
            );
        }
        // Size prefix is checked before buffering the body.
        let mut small = OpenWireCodec {
            encoding: enc,
            max_frame: 16,
        };
        let e = small.decode(&bytes[..4]).unwrap_err().to_string();
        assert!(e.contains("over max-frame-size (16 bytes)"), "{e}");
    }
    // A null command is skipped.
    assert_eq!(
        codec(LOOSE).decode(&[0, 0, 0, 1, 0, 9]).unwrap(),
        Decoded::Skip(5)
    );
    // A command type pub doesn't use is skipped whole (its length is known);
    // trailing bytes after a known one are an error.
    assert_eq!(
        codec(LOOSE).decode(&[0, 0, 0, 3, 15, 1, 2]).unwrap(),
        Decoded::Skip(7)
    );
    let mut padded = encode(LOOSE, &all_structures().remove(7)); // KeepAliveInfo
    padded.push(0);
    let n = padded.len() as i32 - 4;
    padded[..4].copy_from_slice(&n.to_be_bytes());
    assert!(codec(LOOSE)
        .decode(&padded)
        .unwrap_err()
        .to_string()
        .contains("trailing bytes"));
    assert!(codec(LOOSE).decode(&[0xFF, 0xFF, 0xFF, 0xFF]).is_err()); // negative size
}
