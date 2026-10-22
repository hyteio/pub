//! OpenWire v12 data structures and commands.
//!
//! Field order follows `docs/src/development/openwire/v12-layouts.txt` (extracted from the
//! generated Java marshallers). Each structure is declared once; the same
//! declaration drives loose and tight encoding through [`Field`].

use pub_core::{bail_kind, Result};

use crate::wire::{Field, Reader, Writer};

/// Exception carried by `ExceptionResponse`, `ConnectionError` and `MessageAck`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BrokerError {
    /// Java exception class, e.g. `java.lang.SecurityException`.
    pub class: Option<String>,
    pub message: Option<String>,
    pub stack: Vec<StackFrame>,
    pub cause: Option<Box<BrokerError>>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct StackFrame {
    pub class: Option<String>,
    pub method: Option<String>,
    pub file: Option<String>,
    pub line: i32,
}

/// Declares structs whose fields are encoded in declaration order.
macro_rules! structures {
    ($( $(#[$m:meta])* $name:ident { $( $(#[$fm:meta])* $field:ident : $ty:ty ),* $(,)? } )*) => {
        $(
            $(#[$m])*
            #[derive(Debug, Clone, Default, PartialEq)]
            pub struct $name { $( $(#[$fm])* pub $field: $ty ),* }

            impl $name {
                pub fn marshal(&self, w: &mut Writer) -> Result<()> {
                    $( Field::put(&self.$field, w)?; )*
                    let _ = w;
                    Ok(())
                }

                pub fn unmarshal(r: &mut Reader) -> Result<Self> {
                    let _ = &r;
                    Ok($name { $( $field: Field::get(r)? ),* })
                }
            }
        )*
    };
}

type Nested = Option<Box<DataStructure>>;
type Array = Option<Vec<DataStructure>>;
type Str = Option<String>;
type Bytes = Option<Vec<u8>>;
type Error = Option<BrokerError>;

structures! {
    // ---- identifiers
    ConnectionId { value: Str }
    SessionId { connection_id: Str, value: i64 }
    ProducerId { connection_id: Str, value: i64, session_id: i64 }
    ConsumerId { connection_id: Str, session_id: i64, value: i64 }
    BrokerId { value: Str }
    MessageId { text_view: Str, producer_id: Nested, producer_sequence_id: i64, broker_sequence_id: i64 }
    LocalTransactionId { value: i64, connection_id: Nested }
    XaTransactionId { format_id: i32, global_transaction_id: Bytes, branch_qualifier: Bytes }
    /// Queue, topic, temp queue or temp topic (the kind is the variant).
    Destination { physical_name: Str }

    // ---- commands (every command starts with command_id, response_required)
    BrokerInfo {
        command_id: i32, response_required: bool,
        broker_id: Nested, broker_url: Str, peer_broker_infos: Array, broker_name: Str,
        slave_broker: bool, master_broker: bool, fault_tolerant_configuration: bool,
        duplex_connection: bool, network_connection: bool, connection_id: i64,
        broker_upload_url: Str, network_properties: Str,
    }
    ConnectionInfo {
        command_id: i32, response_required: bool,
        connection_id: Nested, client_id: Str, password: Str, user_name: Str, broker_path: Array,
        broker_master_connector: bool, manageable: bool, client_master: bool, fault_tolerant: bool,
        failover_reconnect: bool, client_ip: Str,
    }
    SessionInfo { command_id: i32, response_required: bool, session_id: Nested }
    ConsumerInfo {
        command_id: i32, response_required: bool,
        consumer_id: Nested, browser: bool, destination: Nested, prefetch_size: i32,
        maximum_pending_message_limit: i32, dispatch_async: bool, selector: Str, client_id: Str,
        subscription_name: Str, no_local: bool, exclusive: bool, retroactive: bool, priority: u8,
        broker_path: Array, additional_predicate: Nested, network_subscription: bool,
        optimized_acknowledge: bool, no_range_acks: bool, network_consumer_path: Array,
    }
    ProducerInfo {
        command_id: i32, response_required: bool,
        producer_id: Nested, destination: Nested, broker_path: Array, dispatch_async: bool, window_size: i32,
    }
    DestinationInfo {
        command_id: i32, response_required: bool,
        connection_id: Nested, destination: Nested, operation_type: u8, timeout: i64, broker_path: Array,
    }
    KeepAliveInfo { command_id: i32, response_required: bool }
    ShutdownInfo { command_id: i32, response_required: bool }
    RemoveInfo { command_id: i32, response_required: bool, object_id: Nested, last_delivered_sequence_id: i64 }
    ConnectionError { command_id: i32, response_required: bool, exception: Error, connection_id: Nested }
    ConsumerControl {
        command_id: i32, response_required: bool,
        destination: Nested, close: bool, consumer_id: Nested, prefetch: i32, flush: bool, start: bool, stop: bool,
    }
    ConnectionControl {
        command_id: i32, response_required: bool,
        close: bool, exit: bool, fault_tolerant: bool, resume: bool, suspend: bool,
        connected_brokers: Str, reconnect_to: Str, rebalance_connection: bool, token: Bytes,
    }
    ProducerAck { command_id: i32, response_required: bool, producer_id: Nested, size: i32 }
    /// Local transaction control: `kind` is one of [`tx`].
    TransactionInfo {
        command_id: i32, response_required: bool,
        connection_id: Nested, transaction_id: Nested, kind: u8,
    }
    /// Removes a durable subscription (Java spells the field `subcriptionName`).
    RemoveSubscriptionInfo {
        command_id: i32, response_required: bool,
        connection_id: Nested, subscription_name: Str, client_id: Str,
    }
    MessagePull {
        command_id: i32, response_required: bool,
        consumer_id: Nested, destination: Nested, timeout: i64, correlation_id: Str, message_id: Nested,
    }
    MessageDispatch {
        command_id: i32, response_required: bool,
        consumer_id: Nested, destination: Nested, message: Nested, redelivery_counter: i32,
    }
    MessageAck {
        command_id: i32, response_required: bool,
        destination: Nested, transaction_id: Nested, consumer_id: Nested, ack_type: u8,
        first_message_id: Nested, last_message_id: Nested, message_count: i32, poison_cause: Error,
    }
    /// Fields shared by every message type (ActiveMQMessage and subclasses).
    MessageFields {
        command_id: i32, response_required: bool,
        producer_id: Nested, destination: Nested, transaction_id: Nested, original_destination: Nested,
        message_id: Nested, original_transaction_id: Nested, group_id: Str, group_sequence: i32,
        correlation_id: Str, persistent: bool, expiration: i64, priority: u8, reply_to: Nested,
        timestamp: i64, jms_type: Str, content: Bytes, marshalled_properties: Bytes,
        data_structure: Nested, target_consumer_id: Nested, compressed: bool, redelivery_counter: i32,
        broker_path: Array, arrival: i64, user_id: Str,
        /// Java spells it `recievedByDFBridge`.
        received_by_df_bridge: bool,
        droppable: bool, cluster: Array, broker_in_time: i64, broker_out_time: i64,
        jmsx_group_first_for_consumer: bool,
    }
    /// Extra fields of ActiveMQBlobMessage (type 29).
    BlobFields { remote_blob_url: Str, mime_type: Str, deleted_by_broker: bool }
    Response { command_id: i32, response_required: bool, correlation_id: i32 }
    ExceptionResponse { command_id: i32, response_required: bool, correlation_id: i32, exception: Error }
    DataResponse { command_id: i32, response_required: bool, correlation_id: i32, data: Nested }
    DataArrayResponse { command_id: i32, response_required: bool, correlation_id: i32, data: Array }
    IntegerResponse { command_id: i32, response_required: bool, correlation_id: i32, result: i32 }
}

pub const MAGIC: [u8; 8] = *b"ActiveMQ";

/// WireFormatInfo has no command id; its body is the magic, the version and the
/// options as a marshalled primitive map (see [`crate::primitive`]).
#[derive(Debug, Clone, PartialEq)]
pub struct WireFormatInfo {
    pub magic: [u8; 8],
    pub version: i32,
    pub marshalled_properties: Bytes,
}

impl WireFormatInfo {
    fn marshal(&self, w: &mut Writer) -> Result<()> {
        w.raw(&self.magic);
        w.i32(self.version);
        w.bytes(self.marshalled_properties.as_deref());
        Ok(())
    }

    fn unmarshal(r: &mut Reader) -> Result<Self> {
        let magic: [u8; 8] = r.raw(8)?.try_into().unwrap();
        Ok(WireFormatInfo {
            magic,
            version: r.i32()?,
            marshalled_properties: r.bytes()?,
        })
    }
}

/// `TransactionInfo.type` values (local transactions use BEGIN, COMMIT_ONE_PHASE, ROLLBACK).
pub mod tx {
    pub const BEGIN: u8 = 0;
    pub const PREPARE: u8 = 1;
    pub const COMMIT_ONE_PHASE: u8 = 2;
    pub const COMMIT_TWO_PHASE: u8 = 3;
    pub const ROLLBACK: u8 = 4;
    pub const RECOVER: u8 = 5;
    pub const FORGET: u8 = 6;
    pub const END: u8 = 7;
}

/// JMS message kinds (the data structure type of a message).
pub mod kind {
    pub const MESSAGE: u8 = 23;
    pub const BYTES: u8 = 24;
    pub const MAP: u8 = 25;
    pub const OBJECT: u8 = 26;
    pub const STREAM: u8 = 27;
    pub const TEXT: u8 = 28;
    pub const BLOB: u8 = 29;
}

/// Any message: the kind (23–29), the common fields, and blob extras for kind 29.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Message {
    pub kind: u8,
    pub fields: MessageFields,
    pub blob: Option<BlobFields>,
}

/// Every type pub can encode or decode. Anything else is refused with its type number.
#[derive(Debug, Clone, PartialEq)]
pub enum DataStructure {
    WireFormatInfo(Box<WireFormatInfo>),
    BrokerInfo(Box<BrokerInfo>),
    ConnectionInfo(Box<ConnectionInfo>),
    SessionInfo(Box<SessionInfo>),
    ConsumerInfo(Box<ConsumerInfo>),
    ProducerInfo(Box<ProducerInfo>),
    DestinationInfo(Box<DestinationInfo>),
    KeepAliveInfo(Box<KeepAliveInfo>),
    ShutdownInfo(Box<ShutdownInfo>),
    RemoveInfo(Box<RemoveInfo>),
    ConnectionError(Box<ConnectionError>),
    ConsumerControl(Box<ConsumerControl>),
    ConnectionControl(Box<ConnectionControl>),
    ProducerAck(Box<ProducerAck>),
    RemoveSubscriptionInfo(Box<RemoveSubscriptionInfo>),
    TransactionInfo(Box<TransactionInfo>),
    MessagePull(Box<MessagePull>),
    MessageDispatch(Box<MessageDispatch>),
    MessageAck(Box<MessageAck>),
    Message(Box<Message>),
    Response(Box<Response>),
    ExceptionResponse(Box<ExceptionResponse>),
    DataResponse(Box<DataResponse>),
    DataArrayResponse(Box<DataArrayResponse>),
    IntegerResponse(Box<IntegerResponse>),
    Queue(Box<Destination>),
    Topic(Box<Destination>),
    TempQueue(Box<Destination>),
    TempTopic(Box<Destination>),
    MessageId(Box<MessageId>),
    LocalTransactionId(Box<LocalTransactionId>),
    XaTransactionId(Box<XaTransactionId>),
    ConnectionId(Box<ConnectionId>),
    SessionId(Box<SessionId>),
    ConsumerId(Box<ConsumerId>),
    ProducerId(Box<ProducerId>),
    BrokerId(Box<BrokerId>),
}

/// `(variant, type id)` pairs, in one place.
macro_rules! dispatch {
    ($self:ident, $inner:ident => $e:expr) => {
        match $self {
            DataStructure::WireFormatInfo($inner) => $e,
            DataStructure::BrokerInfo($inner) => $e,
            DataStructure::ConnectionInfo($inner) => $e,
            DataStructure::SessionInfo($inner) => $e,
            DataStructure::ConsumerInfo($inner) => $e,
            DataStructure::ProducerInfo($inner) => $e,
            DataStructure::DestinationInfo($inner) => $e,
            DataStructure::KeepAliveInfo($inner) => $e,
            DataStructure::ShutdownInfo($inner) => $e,
            DataStructure::RemoveInfo($inner) => $e,
            DataStructure::ConnectionError($inner) => $e,
            DataStructure::ConsumerControl($inner) => $e,
            DataStructure::ConnectionControl($inner) => $e,
            DataStructure::ProducerAck($inner) => $e,
            DataStructure::RemoveSubscriptionInfo($inner) => $e,
            DataStructure::TransactionInfo($inner) => $e,
            DataStructure::MessagePull($inner) => $e,
            DataStructure::MessageDispatch($inner) => $e,
            DataStructure::MessageAck($inner) => $e,
            DataStructure::Message($inner) => $e,
            DataStructure::Response($inner) => $e,
            DataStructure::ExceptionResponse($inner) => $e,
            DataStructure::DataResponse($inner) => $e,
            DataStructure::DataArrayResponse($inner) => $e,
            DataStructure::IntegerResponse($inner) => $e,
            DataStructure::Queue($inner) | DataStructure::Topic($inner) => $e,
            DataStructure::TempQueue($inner) | DataStructure::TempTopic($inner) => $e,
            DataStructure::MessageId($inner) => $e,
            DataStructure::LocalTransactionId($inner) => $e,
            DataStructure::XaTransactionId($inner) => $e,
            DataStructure::ConnectionId($inner) => $e,
            DataStructure::SessionId($inner) => $e,
            DataStructure::ConsumerId($inner) => $e,
            DataStructure::ProducerId($inner) => $e,
            DataStructure::BrokerId($inner) => $e,
        }
    };
}

trait Marshal {
    fn marshal_into(&self, w: &mut Writer) -> Result<()>;
}

macro_rules! impl_marshal {
    ($($t:ty),*) => { $( impl Marshal for $t { fn marshal_into(&self, w: &mut Writer) -> Result<()> { self.marshal(w) } } )* };
}
impl_marshal!(
    WireFormatInfo,
    BrokerInfo,
    ConnectionInfo,
    SessionInfo,
    ConsumerInfo,
    ProducerInfo,
    DestinationInfo,
    KeepAliveInfo,
    ShutdownInfo,
    RemoveInfo,
    ConnectionError,
    ConsumerControl,
    ConnectionControl,
    ProducerAck,
    RemoveSubscriptionInfo,
    TransactionInfo,
    MessagePull,
    MessageDispatch,
    MessageAck,
    Response,
    ExceptionResponse,
    DataResponse,
    DataArrayResponse,
    IntegerResponse,
    Destination,
    MessageId,
    LocalTransactionId,
    XaTransactionId,
    ConnectionId,
    SessionId,
    ConsumerId,
    ProducerId,
    BrokerId
);

impl Marshal for Message {
    fn marshal_into(&self, w: &mut Writer) -> Result<()> {
        self.fields.marshal(w)?;
        if self.kind == kind::BLOB {
            self.blob.clone().unwrap_or_default().marshal(w)?;
        }
        Ok(())
    }
}

impl DataStructure {
    pub fn type_id(&self) -> u8 {
        match self {
            DataStructure::WireFormatInfo(_) => 1,
            DataStructure::BrokerInfo(_) => 2,
            DataStructure::ConnectionInfo(_) => 3,
            DataStructure::SessionInfo(_) => 4,
            DataStructure::ConsumerInfo(_) => 5,
            DataStructure::ProducerInfo(_) => 6,
            DataStructure::DestinationInfo(_) => 8,
            DataStructure::KeepAliveInfo(_) => 10,
            DataStructure::ShutdownInfo(_) => 11,
            DataStructure::RemoveInfo(_) => 12,
            DataStructure::ConnectionError(_) => 16,
            DataStructure::ConsumerControl(_) => 17,
            DataStructure::ConnectionControl(_) => 18,
            DataStructure::ProducerAck(_) => 19,
            DataStructure::RemoveSubscriptionInfo(_) => 9,
            DataStructure::TransactionInfo(_) => 7,
            DataStructure::MessagePull(_) => 20,
            DataStructure::MessageDispatch(_) => 21,
            DataStructure::MessageAck(_) => 22,
            DataStructure::Message(m) => m.kind,
            DataStructure::Response(_) => 30,
            DataStructure::ExceptionResponse(_) => 31,
            DataStructure::DataResponse(_) => 32,
            DataStructure::DataArrayResponse(_) => 33,
            DataStructure::IntegerResponse(_) => 34,
            DataStructure::Queue(_) => 100,
            DataStructure::Topic(_) => 101,
            DataStructure::TempQueue(_) => 102,
            DataStructure::TempTopic(_) => 103,
            DataStructure::MessageId(_) => 110,
            DataStructure::LocalTransactionId(_) => 111,
            DataStructure::XaTransactionId(_) => 112,
            DataStructure::ConnectionId(_) => 120,
            DataStructure::SessionId(_) => 121,
            DataStructure::ConsumerId(_) => 122,
            DataStructure::ProducerId(_) => 123,
            DataStructure::BrokerId(_) => 124,
        }
    }

    /// Java's `isMarshallAware()`: messages and WireFormatInfo. When nested in
    /// tight encoding, these carry one extra "cached form" bit.
    pub fn type_is_marshal_aware(t: u8) -> bool {
        t == 1 || (kind::MESSAGE..=kind::BLOB).contains(&t)
    }

    pub fn is_marshal_aware(&self) -> bool {
        Self::type_is_marshal_aware(self.type_id())
    }

    pub fn marshal_body(&self, w: &mut Writer) -> Result<()> {
        dispatch!(self, inner => inner.marshal_into(w))
    }

    /// Whether pub can decode type `t` (the arms of [`Self::unmarshal_body`]).
    pub fn supports(t: u8) -> bool {
        matches!(
            t,
            1..=12
                | 16..=22
                | kind::MESSAGE..=kind::BLOB
                | 30..=34
                | 100..=103
                | 110..=112
                | 120..=124
        )
    }

    pub fn unmarshal_body(t: u8, r: &mut Reader) -> Result<Self> {
        use DataStructure as D;
        Ok(match t {
            1 => D::WireFormatInfo(Box::new(WireFormatInfo::unmarshal(r)?)),
            2 => D::BrokerInfo(Box::new(BrokerInfo::unmarshal(r)?)),
            3 => D::ConnectionInfo(Box::new(ConnectionInfo::unmarshal(r)?)),
            4 => D::SessionInfo(Box::new(SessionInfo::unmarshal(r)?)),
            5 => D::ConsumerInfo(Box::new(ConsumerInfo::unmarshal(r)?)),
            6 => D::ProducerInfo(Box::new(ProducerInfo::unmarshal(r)?)),
            8 => D::DestinationInfo(Box::new(DestinationInfo::unmarshal(r)?)),
            10 => D::KeepAliveInfo(Box::new(KeepAliveInfo::unmarshal(r)?)),
            11 => D::ShutdownInfo(Box::new(ShutdownInfo::unmarshal(r)?)),
            12 => D::RemoveInfo(Box::new(RemoveInfo::unmarshal(r)?)),
            16 => D::ConnectionError(Box::new(ConnectionError::unmarshal(r)?)),
            17 => D::ConsumerControl(Box::new(ConsumerControl::unmarshal(r)?)),
            18 => D::ConnectionControl(Box::new(ConnectionControl::unmarshal(r)?)),
            19 => D::ProducerAck(Box::new(ProducerAck::unmarshal(r)?)),
            9 => D::RemoveSubscriptionInfo(Box::new(RemoveSubscriptionInfo::unmarshal(r)?)),
            7 => D::TransactionInfo(Box::new(TransactionInfo::unmarshal(r)?)),
            20 => D::MessagePull(Box::new(MessagePull::unmarshal(r)?)),
            21 => D::MessageDispatch(Box::new(MessageDispatch::unmarshal(r)?)),
            22 => D::MessageAck(Box::new(MessageAck::unmarshal(r)?)),
            kind::MESSAGE..=kind::BLOB => {
                let fields = MessageFields::unmarshal(r)?;
                let blob = if t == kind::BLOB {
                    Some(BlobFields::unmarshal(r)?)
                } else {
                    None
                };
                D::Message(Box::new(Message {
                    kind: t,
                    fields,
                    blob,
                }))
            }
            30 => D::Response(Box::new(Response::unmarshal(r)?)),
            31 => D::ExceptionResponse(Box::new(ExceptionResponse::unmarshal(r)?)),
            32 => D::DataResponse(Box::new(DataResponse::unmarshal(r)?)),
            33 => D::DataArrayResponse(Box::new(DataArrayResponse::unmarshal(r)?)),
            34 => D::IntegerResponse(Box::new(IntegerResponse::unmarshal(r)?)),
            100 => D::Queue(Box::new(Destination::unmarshal(r)?)),
            101 => D::Topic(Box::new(Destination::unmarshal(r)?)),
            102 => D::TempQueue(Box::new(Destination::unmarshal(r)?)),
            103 => D::TempTopic(Box::new(Destination::unmarshal(r)?)),
            110 => D::MessageId(Box::new(MessageId::unmarshal(r)?)),
            111 => D::LocalTransactionId(Box::new(LocalTransactionId::unmarshal(r)?)),
            112 => D::XaTransactionId(Box::new(XaTransactionId::unmarshal(r)?)),
            120 => D::ConnectionId(Box::new(ConnectionId::unmarshal(r)?)),
            121 => D::SessionId(Box::new(SessionId::unmarshal(r)?)),
            122 => D::ConsumerId(Box::new(ConsumerId::unmarshal(r)?)),
            123 => D::ProducerId(Box::new(ProducerId::unmarshal(r)?)),
            124 => D::BrokerId(Box::new(BrokerId::unmarshal(r)?)),
            other => bail_kind!(Protocol, "unsupported OpenWire data structure type {other}"),
        })
    }

    /// `(command_id, response_required)` for commands; `None` for plain data structures.
    pub fn command_header(&self) -> Option<(i32, bool)> {
        use DataStructure as D;
        Some(match self {
            D::BrokerInfo(c) => (c.command_id, c.response_required),
            D::ConnectionInfo(c) => (c.command_id, c.response_required),
            D::SessionInfo(c) => (c.command_id, c.response_required),
            D::ConsumerInfo(c) => (c.command_id, c.response_required),
            D::ProducerInfo(c) => (c.command_id, c.response_required),
            D::DestinationInfo(c) => (c.command_id, c.response_required),
            D::KeepAliveInfo(c) => (c.command_id, c.response_required),
            D::ShutdownInfo(c) => (c.command_id, c.response_required),
            D::RemoveInfo(c) => (c.command_id, c.response_required),
            D::ConnectionError(c) => (c.command_id, c.response_required),
            D::ConsumerControl(c) => (c.command_id, c.response_required),
            D::ConnectionControl(c) => (c.command_id, c.response_required),
            D::ProducerAck(c) => (c.command_id, c.response_required),
            D::RemoveSubscriptionInfo(c) => (c.command_id, c.response_required),
            D::TransactionInfo(c) => (c.command_id, c.response_required),
            D::MessagePull(c) => (c.command_id, c.response_required),
            D::MessageDispatch(c) => (c.command_id, c.response_required),
            D::MessageAck(c) => (c.command_id, c.response_required),
            D::Message(m) => (m.fields.command_id, m.fields.response_required),
            D::Response(c) => (c.command_id, c.response_required),
            D::ExceptionResponse(c) => (c.command_id, c.response_required),
            D::DataResponse(c) => (c.command_id, c.response_required),
            D::DataArrayResponse(c) => (c.command_id, c.response_required),
            D::IntegerResponse(c) => (c.command_id, c.response_required),
            _ => return None,
        })
    }

    /// Human name for traces and errors.
    pub fn name(&self) -> &'static str {
        match self {
            DataStructure::WireFormatInfo(_) => "WireFormatInfo",
            DataStructure::BrokerInfo(_) => "BrokerInfo",
            DataStructure::ConnectionInfo(_) => "ConnectionInfo",
            DataStructure::SessionInfo(_) => "SessionInfo",
            DataStructure::ConsumerInfo(_) => "ConsumerInfo",
            DataStructure::ProducerInfo(_) => "ProducerInfo",
            DataStructure::DestinationInfo(_) => "DestinationInfo",
            DataStructure::KeepAliveInfo(_) => "KeepAliveInfo",
            DataStructure::ShutdownInfo(_) => "ShutdownInfo",
            DataStructure::RemoveInfo(_) => "RemoveInfo",
            DataStructure::ConnectionError(_) => "ConnectionError",
            DataStructure::ConsumerControl(_) => "ConsumerControl",
            DataStructure::ConnectionControl(_) => "ConnectionControl",
            DataStructure::ProducerAck(_) => "ProducerAck",
            DataStructure::RemoveSubscriptionInfo(_) => "RemoveSubscriptionInfo",
            DataStructure::TransactionInfo(_) => "TransactionInfo",
            DataStructure::MessagePull(_) => "MessagePull",
            DataStructure::MessageDispatch(_) => "MessageDispatch",
            DataStructure::MessageAck(_) => "MessageAck",
            DataStructure::Message(_) => "Message",
            DataStructure::Response(_) => "Response",
            DataStructure::ExceptionResponse(_) => "ExceptionResponse",
            DataStructure::DataResponse(_) => "DataResponse",
            DataStructure::DataArrayResponse(_) => "DataArrayResponse",
            DataStructure::IntegerResponse(_) => "IntegerResponse",
            DataStructure::Queue(_) => "Queue",
            DataStructure::Topic(_) => "Topic",
            DataStructure::TempQueue(_) => "TempQueue",
            DataStructure::TempTopic(_) => "TempTopic",
            DataStructure::MessageId(_) => "MessageId",
            DataStructure::LocalTransactionId(_) => "LocalTransactionId",
            DataStructure::XaTransactionId(_) => "XATransactionId",
            DataStructure::ConnectionId(_) => "ConnectionId",
            DataStructure::SessionId(_) => "SessionId",
            DataStructure::ConsumerId(_) => "ConsumerId",
            DataStructure::ProducerId(_) => "ProducerId",
            DataStructure::BrokerId(_) => "BrokerId",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::Encoding;

    #[test]
    fn supports_matches_the_decoder() {
        let enc = Encoding {
            tight: false,
            stack_traces: false,
        };
        for t in 0..=255u8 {
            let mut r = Reader::new(enc, &[]).unwrap();
            let e = DataStructure::unmarshal_body(t, &mut r).unwrap_err();
            let unknown = format!("{e:#}").contains("unsupported OpenWire data structure");
            assert_eq!(unknown, !DataStructure::supports(t), "type {t}: {e:#}");
        }
    }
}
