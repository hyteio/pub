//! AMQP 1.0 composite types: performatives (part 2), SASL frames (part 5),
//! sources and targets, delivery states, errors (part 3) and message sections.
//!
//! Each is a described list; [`composite!`] generates the struct and its
//! conversion. Encoding drops trailing nulls (the spec allows it), decoding
//! accepts short lists, and mandatory fields that are missing are errors.

use pub_core::{bail_kind, err, Result};

use crate::types::Value;

/// Conversion of one field of a composite to and from a [`Value`].
pub trait Field: Sized {
    fn to_value(&self) -> Value;
    /// `Null` means absent; only `Option`s accept it.
    fn from_value(v: Value, field: &str) -> Result<Self>;
}

fn wrong(field: &str, want: &str, got: &Value) -> anyhow::Error {
    err!(
        Protocol,
        "AMQP field {field}: expected {want}, got {}",
        got.type_name()
    )
}

impl<T: Field> Field for Option<T> {
    fn to_value(&self) -> Value {
        match self {
            Some(v) => v.to_value(),
            None => Value::Null,
        }
    }

    fn from_value(v: Value, field: &str) -> Result<Self> {
        match v {
            Value::Null => Ok(None),
            v => T::from_value(v, field).map(Some),
        }
    }
}

macro_rules! scalar {
    ($t:ty, $variant:ident, $want:literal) => {
        impl Field for $t {
            fn to_value(&self) -> Value {
                Value::$variant(self.clone())
            }

            fn from_value(v: Value, field: &str) -> Result<Self> {
                match v {
                    Value::$variant(x) => Ok(x),
                    Value::Null => bail_kind!(Protocol, "AMQP field {field} is mandatory"),
                    other => Err(wrong(field, $want, &other)),
                }
            }
        }
    };
}

scalar!(bool, Bool, "boolean");
scalar!(u8, Ubyte, "ubyte");
scalar!(u16, Ushort, "ushort");
scalar!(u32, Uint, "uint");
scalar!(u64, Ulong, "ulong");
scalar!(String, String, "string");

/// Raw values (message ids, annotations, anything typed per use).
impl Field for Value {
    fn to_value(&self) -> Value {
        self.clone()
    }

    fn from_value(v: Value, _: &str) -> Result<Self> {
        Ok(v)
    }
}

/// `binary`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Binary(pub Vec<u8>);

impl Field for Binary {
    fn to_value(&self) -> Value {
        Value::Binary(self.0.clone())
    }

    fn from_value(v: Value, field: &str) -> Result<Self> {
        match v {
            Value::Binary(b) => Ok(Binary(b)),
            Value::Null => bail_kind!(Protocol, "AMQP field {field} is mandatory"),
            other => Err(wrong(field, "binary", &other)),
        }
    }
}

/// `symbol`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Symbol(pub String);

impl Symbol {
    pub fn new(s: &str) -> Self {
        Symbol(s.to_string())
    }
}

impl Field for Symbol {
    fn to_value(&self) -> Value {
        Value::Symbol(self.0.clone())
    }

    fn from_value(v: Value, field: &str) -> Result<Self> {
        match v {
            Value::Symbol(s) => Ok(Symbol(s)),
            Value::Null => bail_kind!(Protocol, "AMQP field {field} is mandatory"),
            other => Err(wrong(field, "symbol", &other)),
        }
    }
}

/// A `multiple="true"` symbol field: one symbol or an array of them.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Symbols(pub Vec<String>);

impl Symbols {
    pub fn of(names: &[&str]) -> Self {
        Symbols(names.iter().map(|s| s.to_string()).collect())
    }

    pub fn contains(&self, name: &str) -> bool {
        self.0.iter().any(|s| s == name)
    }
}

impl Field for Symbols {
    fn to_value(&self) -> Value {
        Value::Array(self.0.iter().cloned().map(Value::Symbol).collect())
    }

    fn from_value(v: Value, field: &str) -> Result<Self> {
        match v {
            Value::Symbol(s) => Ok(Symbols(vec![s])),
            Value::Array(items) | Value::List(items) => items
                .into_iter()
                .map(|i| match i {
                    Value::Symbol(s) => Ok(s),
                    other => Err(wrong(field, "symbol", &other)),
                })
                .collect::<Result<_>>()
                .map(Symbols),
            Value::Null => bail_kind!(Protocol, "AMQP field {field} is mandatory"),
            other => Err(wrong(field, "symbols", &other)),
        }
    }
}

/// `map` (fields, annotations, filter sets): pairs in order.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Map(pub Vec<(Value, Value)>);

impl Map {
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.0
            .iter()
            .find(|(k, _)| k.as_str() == Some(key))
            .map(|(_, v)| v)
    }
}

impl Field for Map {
    fn to_value(&self) -> Value {
        Value::Map(self.0.clone())
    }

    fn from_value(v: Value, field: &str) -> Result<Self> {
        match v {
            Value::Map(m) => Ok(Map(m)),
            Value::Null => bail_kind!(Protocol, "AMQP field {field} is mandatory"),
            other => Err(wrong(field, "map", &other)),
        }
    }
}

/// `timestamp`: milliseconds since the epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timestamp(pub i64);

impl Field for Timestamp {
    fn to_value(&self) -> Value {
        Value::Timestamp(self.0)
    }

    fn from_value(v: Value, field: &str) -> Result<Self> {
        match v {
            Value::Timestamp(t) => Ok(Timestamp(t)),
            Value::Null => bail_kind!(Protocol, "AMQP field {field} is mandatory"),
            other => Err(wrong(field, "timestamp", &other)),
        }
    }
}

/// The fields of a described list, by position.
fn list_fields(v: Value, code: u64, name: &str) -> Result<Vec<Value>> {
    match v {
        Value::Described(d, body) => {
            let matches = match *d {
                Value::Ulong(c) => c == code,
                Value::Symbol(ref s) => symbolic_code(s) == Some(code),
                _ => false,
            };
            if !matches {
                bail_kind!(Protocol, "expected AMQP {name}, got descriptor {d:?}");
            }
            match *body {
                Value::List(items) => Ok(items),
                other => bail_kind!(
                    Protocol,
                    "AMQP {name} is not a list ({})",
                    other.type_name()
                ),
            }
        }
        other => bail_kind!(Protocol, "expected AMQP {name}, got {}", other.type_name()),
    }
}

/// The numeric descriptor of a symbolic one (`amqp:open:list`), for peers
/// that send names.
fn symbolic_code(name: &str) -> Option<u64> {
    Some(match name {
        "amqp:open:list" => 0x10,
        "amqp:begin:list" => 0x11,
        "amqp:attach:list" => 0x12,
        "amqp:flow:list" => 0x13,
        "amqp:transfer:list" => 0x14,
        "amqp:disposition:list" => 0x15,
        "amqp:detach:list" => 0x16,
        "amqp:end:list" => 0x17,
        "amqp:close:list" => 0x18,
        "amqp:error:list" => 0x1d,
        "amqp:source:list" => 0x28,
        "amqp:target:list" => 0x29,
        "amqp:coordinator:list" => 0x30,
        "amqp:declare:list" => 0x31,
        "amqp:discharge:list" => 0x32,
        "amqp:declared:list" => 0x33,
        "amqp:transactional-state:list" => 0x34,
        _ => return None,
    })
}

/// Encode fields as a described list, without trailing nulls.
fn described_list(code: u64, mut fields: Vec<Value>) -> Value {
    while fields.last() == Some(&Value::Null) {
        fields.pop();
    }
    Value::described(code, Value::List(fields))
}

macro_rules! composite {
    ($(#[$doc:meta])* $name:ident = $code:literal { $($(#[$fdoc:meta])* $field:ident : $ty:ty),* $(,)? }) => {
        $(#[$doc])*
        #[derive(Debug, Clone, PartialEq, Default)]
        pub struct $name { $($(#[$fdoc])* pub $field: $ty),* }

        impl $name {
            pub const CODE: u64 = $code;

            pub fn to_value(&self) -> Value {
                described_list($code, vec![$(Field::to_value(&self.$field)),*])
            }

            pub fn from_value(v: Value) -> Result<Self> {
                let mut it = list_fields(v, $code, stringify!($name))?.into_iter();
                Ok($name {
                    $($field: Field::from_value(
                        it.next().unwrap_or(Value::Null),
                        concat!(stringify!($name), ".", stringify!($field)),
                    )?,)*
                })
            }
        }

        impl Field for $name {
            fn to_value(&self) -> Value {
                $name::to_value(self)
            }

            fn from_value(v: Value, _: &str) -> Result<Self> {
                $name::from_value(v)
            }
        }
    };
}

// ---- part 2: transport

composite!(
    /// Connection parameters, exchanged first.
    Open = 0x10 {
        container_id: String,
        hostname: Option<String>,
        max_frame_size: Option<u32>,
        channel_max: Option<u16>,
        /// Milliseconds.
        idle_time_out: Option<u32>,
        outgoing_locales: Option<Symbols>,
        incoming_locales: Option<Symbols>,
        offered_capabilities: Option<Symbols>,
        desired_capabilities: Option<Symbols>,
        properties: Option<Map>,
    }
);

composite!(
    Begin = 0x11 {
        remote_channel: Option<u16>,
        next_outgoing_id: u32,
        incoming_window: u32,
        outgoing_window: u32,
        handle_max: Option<u32>,
        offered_capabilities: Option<Symbols>,
        desired_capabilities: Option<Symbols>,
        properties: Option<Map>,
    }
);

composite!(
    /// `role`: false = sender, true = receiver.
    Attach = 0x12 {
        name: String,
        handle: u32,
        role: bool,
        snd_settle_mode: Option<u8>,
        rcv_settle_mode: Option<u8>,
        source: Option<Source>,
        target: Option<Terminus>,
        unsettled: Option<Map>,
        incomplete_unsettled: Option<bool>,
        initial_delivery_count: Option<u32>,
        max_message_size: Option<u64>,
        offered_capabilities: Option<Symbols>,
        desired_capabilities: Option<Symbols>,
        properties: Option<Map>,
    }
);

composite!(
    Flow = 0x13 {
        next_incoming_id: Option<u32>,
        incoming_window: u32,
        next_outgoing_id: u32,
        outgoing_window: u32,
        handle: Option<u32>,
        delivery_count: Option<u32>,
        link_credit: Option<u32>,
        available: Option<u32>,
        drain: Option<bool>,
        echo: Option<bool>,
        properties: Option<Map>,
    }
);

composite!(
    Transfer = 0x14 {
        handle: u32,
        delivery_id: Option<u32>,
        delivery_tag: Option<Binary>,
        message_format: Option<u32>,
        settled: Option<bool>,
        more: Option<bool>,
        rcv_settle_mode: Option<u8>,
        state: Option<DeliveryState>,
        resume: Option<bool>,
        aborted: Option<bool>,
        batchable: Option<bool>,
    }
);

composite!(
    Disposition = 0x15 {
        role: bool,
        first: u32,
        last: Option<u32>,
        settled: Option<bool>,
        state: Option<DeliveryState>,
        batchable: Option<bool>,
    }
);

composite!(Detach = 0x16 { handle: u32, closed: Option<bool>, error: Option<Error> });
composite!(End = 0x17 { error: Option<Error> });
composite!(Close = 0x18 { error: Option<Error> });

composite!(
    /// `condition` is a symbol such as `amqp:unauthorized-access`.
    Error = 0x1d { condition: Symbol, description: Option<String>, info: Option<Map> }
);

/// `role` values.
pub const SENDER: bool = false;
pub const RECEIVER: bool = true;

/// `snd-settle-mode`: unsettled, settled, mixed; `rcv-settle-mode`: first, second.
pub mod settle {
    pub const SND_UNSETTLED: u8 = 0;
    pub const SND_SETTLED: u8 = 1;
    pub const RCV_FIRST: u8 = 0;
}

// ---- part 3: messaging terminuses and delivery states

composite!(
    Source = 0x28 {
        address: Option<String>,
        /// 0 none, 1 configuration, 2 unsettled-state.
        durable: Option<u32>,
        /// link-detach, session-end, connection-close, never.
        expiry_policy: Option<Symbol>,
        timeout: Option<u32>,
        dynamic: Option<bool>,
        dynamic_node_properties: Option<Map>,
        /// move or copy (browsing).
        distribution_mode: Option<Symbol>,
        filter: Option<Map>,
        default_outcome: Option<Value>,
        outcomes: Option<Symbols>,
        capabilities: Option<Symbols>,
    }
);

composite!(
    Target = 0x29 {
        address: Option<String>,
        durable: Option<u32>,
        expiry_policy: Option<Symbol>,
        timeout: Option<u32>,
        dynamic: Option<bool>,
        dynamic_node_properties: Option<Map>,
        capabilities: Option<Symbols>,
    }
);

/// A link's target: a node, or (part 4) the transaction coordinator.
#[derive(Debug, Clone, PartialEq)]
pub enum Terminus {
    Node(Target),
    Coordinator(Coordinator),
}

impl Terminus {
    pub fn node(&self) -> Option<&Target> {
        match self {
            Terminus::Node(t) => Some(t),
            Terminus::Coordinator(_) => None,
        }
    }

    pub fn address(&self) -> Option<String> {
        self.node().and_then(|t| t.address.clone())
    }
}

impl From<Target> for Terminus {
    fn from(t: Target) -> Self {
        Terminus::Node(t)
    }
}

impl Field for Terminus {
    fn to_value(&self) -> Value {
        match self {
            Terminus::Node(t) => t.to_value(),
            Terminus::Coordinator(c) => c.to_value(),
        }
    }

    fn from_value(v: Value, field: &str) -> Result<Self> {
        match v.descriptor_code() {
            Some(Coordinator::CODE) => Coordinator::from_value(v).map(Terminus::Coordinator),
            _ => Target::from_value(v)
                .map_err(|e| e.context(field.to_string()))
                .map(Terminus::Node),
        }
    }
}

// ---- part 4: transactions

composite!(
    /// The target of the link that declares and discharges transactions.
    Coordinator = 0x30 { capabilities: Option<Symbols> }
);
composite!(Declare = 0x31 { global_id: Option<Value> });
composite!(
    /// `fail`: roll back instead of committing.
    Discharge = 0x32 { txn_id: Binary, fail: Option<bool> }
);

/// Delivery outcomes and states.
#[derive(Debug, Clone, PartialEq)]
pub enum DeliveryState {
    Received {
        section_number: u32,
        section_offset: u64,
    },
    Accepted,
    Rejected(Option<Error>),
    Released,
    Modified {
        delivery_failed: Option<bool>,
        undeliverable_here: Option<bool>,
        message_annotations: Option<Map>,
    },
    /// The coordinator's answer to a declare: the transaction's id.
    Declared(Vec<u8>),
    /// Work in a transaction, with its outcome once known.
    Transactional {
        txn_id: Vec<u8>,
        outcome: Option<Box<DeliveryState>>,
    },
    /// Anything else, kept as is.
    Other(Value),
}

impl DeliveryState {
    pub fn name(&self) -> &'static str {
        match self {
            DeliveryState::Received { .. } => "received",
            DeliveryState::Accepted => "accepted",
            DeliveryState::Rejected(_) => "rejected",
            DeliveryState::Released => "released",
            DeliveryState::Modified { .. } => "modified",
            DeliveryState::Declared(_) => "declared",
            DeliveryState::Transactional { .. } => "transactional",
            DeliveryState::Other(_) => "other",
        }
    }
}

impl Field for DeliveryState {
    fn to_value(&self) -> Value {
        match self {
            DeliveryState::Received {
                section_number,
                section_offset,
            } => described_list(
                0x23,
                vec![Value::Uint(*section_number), Value::Ulong(*section_offset)],
            ),
            DeliveryState::Accepted => described_list(0x24, vec![]),
            DeliveryState::Rejected(e) => described_list(0x25, vec![e.to_value()]),
            DeliveryState::Released => described_list(0x26, vec![]),
            DeliveryState::Modified {
                delivery_failed,
                undeliverable_here,
                message_annotations,
            } => described_list(
                0x27,
                vec![
                    delivery_failed.to_value(),
                    undeliverable_here.to_value(),
                    message_annotations.to_value(),
                ],
            ),
            DeliveryState::Declared(id) => described_list(0x33, vec![Value::Binary(id.clone())]),
            DeliveryState::Transactional { txn_id, outcome } => described_list(
                0x34,
                vec![
                    Value::Binary(txn_id.clone()),
                    outcome.as_ref().map_or(Value::Null, |o| o.to_value()),
                ],
            ),
            DeliveryState::Other(v) => v.clone(),
        }
    }

    fn from_value(v: Value, field: &str) -> Result<Self> {
        let code = v
            .descriptor_code()
            .ok_or_else(|| wrong(field, "delivery state", &v))?;
        let fields = |v: Value| -> Result<Vec<Value>> {
            match v {
                Value::Described(_, b) => match *b {
                    Value::List(items) => Ok(items),
                    other => bail_kind!(
                        Protocol,
                        "AMQP delivery state is not a list ({})",
                        other.type_name()
                    ),
                },
                _ => unreachable!(),
            }
        };
        let nth = |f: &mut Vec<Value>, i: usize| {
            f.get_mut(i)
                .map(|v| std::mem::replace(v, Value::Null))
                .unwrap_or(Value::Null)
        };
        Ok(match code {
            0x23 => {
                let mut f = fields(v)?;
                DeliveryState::Received {
                    section_number: Field::from_value(nth(&mut f, 0), "received.section-number")?,
                    section_offset: Field::from_value(nth(&mut f, 1), "received.section-offset")?,
                }
            }
            0x24 => DeliveryState::Accepted,
            0x25 => {
                let mut f = fields(v)?;
                DeliveryState::Rejected(Field::from_value(nth(&mut f, 0), "rejected.error")?)
            }
            0x26 => DeliveryState::Released,
            0x27 => {
                let mut f = fields(v)?;
                DeliveryState::Modified {
                    delivery_failed: Field::from_value(nth(&mut f, 0), "modified.delivery-failed")?,
                    undeliverable_here: Field::from_value(
                        nth(&mut f, 1),
                        "modified.undeliverable-here",
                    )?,
                    message_annotations: Field::from_value(
                        nth(&mut f, 2),
                        "modified.message-annotations",
                    )?,
                }
            }
            0x33 => {
                let mut f = fields(v)?;
                let Binary(id) = Field::from_value(nth(&mut f, 0), "declared.txn-id")?;
                DeliveryState::Declared(id)
            }
            0x34 => {
                let mut f = fields(v)?;
                let Binary(txn_id) =
                    Field::from_value(nth(&mut f, 0), "transactional-state.txn-id")?;
                let outcome: Option<DeliveryState> =
                    Field::from_value(nth(&mut f, 1), "transactional-state.outcome")?;
                DeliveryState::Transactional {
                    txn_id,
                    outcome: outcome.map(Box::new),
                }
            }
            _ => DeliveryState::Other(v),
        })
    }
}

// ---- part 5: SASL

composite!(SaslMechanisms = 0x40 { sasl_server_mechanisms: Symbols });
composite!(SaslInit = 0x41 { mechanism: Symbol, initial_response: Option<Binary>, hostname: Option<String> });
composite!(SaslChallenge = 0x42 { challenge: Binary });
composite!(SaslResponse = 0x43 { response: Binary });
composite!(
    /// `code`: 0 ok, 1 auth, 2 sys, 3 sys-perm, 4 sys-temp.
    SaslOutcome = 0x44 { code: u8, additional_data: Option<Binary> }
);

// ---- part 3: message sections

composite!(
    Header = 0x70 {
        durable: Option<bool>,
        priority: Option<u8>,
        /// Milliseconds.
        ttl: Option<u32>,
        first_acquirer: Option<bool>,
        delivery_count: Option<u32>,
    }
);

composite!(
    Properties = 0x73 {
        message_id: Option<Value>,
        user_id: Option<Binary>,
        to: Option<String>,
        subject: Option<String>,
        reply_to: Option<String>,
        correlation_id: Option<Value>,
        content_type: Option<Symbol>,
        content_encoding: Option<Symbol>,
        absolute_expiry_time: Option<Timestamp>,
        creation_time: Option<Timestamp>,
        group_id: Option<String>,
        group_sequence: Option<u32>,
        reply_to_group_id: Option<String>,
    }
);

/// Section descriptors.
pub mod section {
    pub const HEADER: u64 = 0x70;
    pub const DELIVERY_ANNOTATIONS: u64 = 0x71;
    pub const MESSAGE_ANNOTATIONS: u64 = 0x72;
    pub const PROPERTIES: u64 = 0x73;
    pub const APPLICATION_PROPERTIES: u64 = 0x74;
    pub const DATA: u64 = 0x75;
    pub const AMQP_SEQUENCE: u64 = 0x76;
    pub const AMQP_VALUE: u64 = 0x77;
    pub const FOOTER: u64 = 0x78;
}

/// A source or target address expiring "never" (durable subscriptions).
pub const EXPIRY_NEVER: &str = "never";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_round_trip_and_trailing_nulls() {
        let open = Open {
            container_id: "pub-1".into(),
            hostname: Some("broker".into()),
            idle_time_out: Some(30_000),
            ..Default::default()
        };
        let v = open.to_value();
        // container-id, hostname, null, null, idle-time-out: nothing after.
        match &v {
            Value::Described(_, body) => match &**body {
                Value::List(items) => assert_eq!(items.len(), 5),
                other => panic!("{other:?}"),
            },
            other => panic!("{other:?}"),
        }
        let back = Open::from_value(Value::decode(&v.to_bytes().unwrap()).unwrap().0).unwrap();
        assert_eq!(back, open);
    }

    #[test]
    fn mandatory_fields_and_descriptors() {
        let begin = Value::described(0x11, Value::List(vec![Value::Null]));
        let e = Begin::from_value(begin).unwrap_err();
        assert!(
            e.to_string()
                .contains("Begin.next_outgoing_id is mandatory"),
            "{e}"
        );
        let wrong = Value::described(0x12, Value::List(vec![]));
        assert!(Open::from_value(wrong).is_err());
        // A symbolic descriptor works too.
        let named = Value::Described(
            Box::new(Value::symbol("amqp:close:list")),
            Box::new(Value::List(vec![])),
        );
        assert_eq!(Close::from_value(named).unwrap(), Close::default());
    }

    #[test]
    fn delivery_states_and_nested_composites() {
        for s in [
            DeliveryState::Accepted,
            DeliveryState::Released,
            DeliveryState::Rejected(Some(Error {
                condition: Symbol::new("amqp:not-found"),
                description: Some("no queue".into()),
                info: None,
            })),
            DeliveryState::Modified {
                delivery_failed: Some(true),
                undeliverable_here: None,
                message_annotations: None,
            },
            DeliveryState::Received {
                section_number: 1,
                section_offset: 9,
            },
        ] {
            let v = Field::to_value(&s);
            assert_eq!(<DeliveryState as Field>::from_value(v, "s").unwrap(), s);
        }
        let attach = Attach {
            name: "pub-sender".into(),
            handle: 0,
            role: SENDER,
            source: Some(Source {
                address: Some("pub".into()),
                ..Default::default()
            }),
            target: Some(
                Target {
                    address: Some("orders".into()),
                    capabilities: Some(Symbols::of(&["queue"])),
                    ..Default::default()
                }
                .into(),
            ),
            initial_delivery_count: Some(0),
            ..Default::default()
        };
        let back = Attach::from_value(
            Value::decode(&attach.to_value().to_bytes().unwrap())
                .unwrap()
                .0,
        )
        .unwrap();
        assert_eq!(back, attach);
        // A single symbol where an array is allowed.
        assert_eq!(
            Symbols::from_value(Value::symbol("queue"), "c").unwrap(),
            Symbols::of(&["queue"])
        );
    }

    #[test]
    fn transaction_types_round_trip() {
        let round = |v: Value| Value::decode(&v.to_bytes().unwrap()).unwrap().0;
        let coord = Terminus::Coordinator(Coordinator {
            capabilities: Some(Symbols::of(&["amqp:local-transactions"])),
        });
        assert_eq!(
            Terminus::from_value(round(coord.to_value()), "t").unwrap(),
            coord
        );
        for state in [
            DeliveryState::Declared(vec![0, 0, 0, 7]),
            DeliveryState::Transactional {
                txn_id: vec![7],
                outcome: Some(Box::new(DeliveryState::Accepted)),
            },
            DeliveryState::Transactional {
                txn_id: vec![7],
                outcome: None,
            },
        ] {
            assert_eq!(
                DeliveryState::from_value(round(state.to_value()), "s").unwrap(),
                state
            );
        }
        let d = Discharge {
            txn_id: Binary(vec![7]),
            fail: Some(true),
        };
        assert_eq!(Discharge::from_value(round(d.to_value())).unwrap(), d);
    }
}
