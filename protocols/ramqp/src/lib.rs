//! AMQP 0-9-1 module for pub — "ramqp", RabbitMQ's AMQP 0.9.1 (`ramqp://`,
//! `ramqps://`): RabbitMQ, LavinMQ and other 0-9-1 brokers.
//!
//! Written from the AMQP 0-9-1 specification with RabbitMQ's errata (field
//! table types) and extensions (publisher confirms, `basic.nack`,
//! `authentication_failure_close`, `connection.blocked`).
//!
//! - [`wire`]: field encodings, tables and packed bits
//! - [`codec`]: frames, methods and content properties
//! - [`message`]: destinations and the mapping to pub's message model
//! - [`session`]: login, tuning, heartbeats; send, receive, transactions, move, destinations

pub mod codec;
pub mod message;
pub mod session;
pub mod wire;

#[cfg(feature = "mock")]
pub mod mock;

use pub_core::{
    Context, Endpoint, Field, Module, Result, SchemeInfo, Session, Setting, SettingType, Verb,
};

pub use session::RamqpSession;

pub struct RamqpModule;

const SCHEMES: &[SchemeInfo] = &[
    SchemeInfo {
        scheme: "ramqp",
        default_port: 5672,
        tls: false,
    },
    SchemeInfo {
        scheme: "ramqps",
        default_port: 5671,
        tls: true,
    },
];

const fn setting(
    key: &'static str,
    ty: SettingType,
    default: Option<&'static str>,
    help: &'static str,
) -> Setting {
    Setting {
        key,
        ty,
        default,
        help,
    }
}

const SETTINGS: &[Setting] = &[
    setting("vhost", SettingType::Text, Some("/"), "the virtual host"),
    setting(
        "heartbeat",
        SettingType::Duration,
        Some("60s"),
        "heartbeat interval asked for (the smaller of ours and the broker's); 0 = the broker's",
    ),
    setting(
        "confirms",
        SettingType::Bool,
        Some("true"),
        "publisher confirms: each send waits for the broker to take the message",
    ),
    setting(
        "mandatory",
        SettingType::Bool,
        None,
        "a message nothing can route is an error (default: for queues yes, for exchanges no)",
    ),
    setting(
        "prefetch",
        SettingType::Count,
        Some("1000"),
        "basic.qos prefetch count for receiving (1-65535)",
    ),
    setting(
        "prefetch-follows-count",
        SettingType::Bool,
        Some("true"),
        "prefetch only as many messages as -n asks for",
    ),
    setting(
        "sasl-mechanisms",
        SettingType::Text,
        None,
        "SASL mechanisms in order of preference (PLAIN, ANONYMOUS, EXTERNAL); default: EXTERNAL with a client certificate, PLAIN with a user, else ANONYMOUS",
    ),
    setting(
        "exchange-type",
        SettingType::OneOf(&["direct", "fanout", "topic", "headers"]),
        Some("topic"),
        "the type of exchange `pub dest create .../exchange/NAME` declares",
    ),
    setting(
        "queue-type",
        SettingType::OneOf(&["classic", "quorum", "stream"]),
        None,
        "x-queue-type for queues pub declares (dest create, durable subscriptions)",
    ),
    setting(
        "connection-name",
        SettingType::Text,
        None,
        "a name for the connection in RabbitMQ's management UI",
    ),
];

/// Fields basic properties carry.
const SEND_FIELDS: &[Field] = &[
    Field::CorrelationId,
    Field::ReplyTo,
    Field::Type,
    Field::Priority,
    Field::Persistent,
    Field::Ttl,
    Field::ContentType,
];

impl Module for RamqpModule {
    fn name(&self) -> &'static str {
        "ramqp"
    }

    fn description(&self) -> &'static str {
        "RabbitMQ AMQP 0-9-1 (RabbitMQ, LavinMQ)"
    }

    fn schemes(&self) -> &'static [SchemeInfo] {
        SCHEMES
    }

    fn verbs(&self) -> &'static [Verb] {
        &[
            Verb::Send,
            Verb::Receive,
            Verb::Unsubscribe,
            Verb::Move,
            Verb::Destinations,
            Verb::Ping,
        ]
    }

    fn settings(&self) -> &'static [Setting] {
        SETTINGS
    }

    fn message_fields(&self) -> &'static [Field] {
        SEND_FIELDS
    }

    fn typed_properties(&self) -> bool {
        true
    }

    fn text_bodies(&self) -> bool {
        true
    }

    fn durable_subscriptions(&self) -> bool {
        true
    }

    fn transactions(&self) -> bool {
        true
    }

    /// The URL path, keeping a `#` wildcard (a URL fragment) in routing keys:
    /// `ramqp://b/topic/sensors.#`.
    fn destination_name(&self, ep: &Endpoint) -> Option<String> {
        let mut name = ep.path.clone();
        if let Some(frag) = &ep.fragment {
            name.push('#');
            name.push_str(frag);
        }
        if name.is_empty() {
            ep.prop("destination").map(str::to_string)
        } else {
            Some(name)
        }
    }

    fn connect(&self, ep: &Endpoint, ctx: &Context) -> Result<Box<dyn Session>> {
        Ok(Box::new(RamqpSession::connect(ep, ctx)?))
    }
}

pub fn module() -> Box<dyn Module> {
    Box::new(RamqpModule)
}
