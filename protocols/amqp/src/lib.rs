//! AMQP 1.0 module for pub (`amqp://`, `amqps://`): ActiveMQ Classic and
//! Artemis, RabbitMQ 4, Qpid, Azure Service Bus, Solace, ...
//!
//! Written from the OASIS AMQP 1.0 specification; message bodies and fields
//! follow ActiveMQ's JMS transformer so JMS clients see what Qpid JMS would send.
//!
//! - [`types`]: the AMQP type system and its encodings
//! - [`described`]: performatives, SASL frames, terminuses, delivery states, message sections
//! - [`codec`]: protocol headers and frames for pub-core's `Connection`
//! - [`message`]: message sections and the mapping to pub's message model
//! - [`session`]: SASL, open, session and links; send, receive, browse, durable subscriptions

pub mod codec;
pub mod described;
pub mod message;
pub mod session;
pub mod types;

#[cfg(feature = "mock")]
pub mod mock;

use pub_core::{
    Context, Endpoint, Field, Module, Result, SchemeInfo, Session, Setting, SettingType, Verb,
};

pub use session::AmqpSession;

pub struct AmqpModule;

const SCHEMES: &[SchemeInfo] = &[
    SchemeInfo {
        scheme: "amqp",
        default_port: 5672,
        tls: false,
    },
    SchemeInfo {
        scheme: "amqps",
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
    setting(
        "broker-type",
        SettingType::OneOf(&["auto", "servicebus", "other"]),
        Some("auto"),
        "servicebus = Azure Service Bus (browse by peeking; refuse selectors, move, --durable); auto = from the host name or the broker's container id",
    ),
    setting(
        "client-id",
        SettingType::Text,
        None,
        "the AMQP container id; default: generated (durable subscriptions need a fixed one)",
    ),
    setting(
        "vhost",
        SettingType::Text,
        None,
        "the hostname sent in SASL and open (virtual host; Azure/RabbitMQ use it); default: the URL host",
    ),
    setting(
        "idle-timeout",
        SettingType::Duration,
        Some("60s"),
        "give up when the broker is silent this long; pub sends empty frames at half the broker's (0 = off)",
    ),
    setting(
        "sasl-mechanisms",
        SettingType::Text,
        None,
        "SASL mechanisms in order of preference (PLAIN, ANONYMOUS, EXTERNAL); default: EXTERNAL with a client certificate, PLAIN with a user, else ANONYMOUS",
    ),
    setting(
        "text-body",
        SettingType::OneOf(&["value", "data"]),
        Some("value"),
        "text bodies: value = amqp-value string (JMS TextMessage); data = data section with text/plain; charset=utf-8",
    ),
    setting(
        "credit",
        SettingType::Count,
        Some("1000"),
        "link credit for receiving (how many messages the broker may send ahead)",
    ),
    setting(
        "prefetch-follows-count",
        SettingType::Bool,
        Some("true"),
        "grant only as much credit as -n asks for",
    ),
];

/// Standard fields an AMQP message can carry.
const SEND_FIELDS: &[Field] = &[
    Field::CorrelationId,
    Field::ReplyTo,
    Field::Type,
    Field::Priority,
    Field::Persistent,
    Field::Ttl,
    Field::DeliveryDelay,
    Field::GroupId,
    Field::GroupSeq,
    Field::ContentType,
];

impl Module for AmqpModule {
    fn name(&self) -> &'static str {
        "amqp"
    }

    fn description(&self) -> &'static str {
        "AMQP 1.0 (ActiveMQ, Artemis, RabbitMQ 4, Qpid, Azure Service Bus)"
    }

    fn schemes(&self) -> &'static [SchemeInfo] {
        SCHEMES
    }

    fn verbs(&self) -> &'static [Verb] {
        &[
            Verb::Send,
            Verb::Receive,
            Verb::Browse,
            Verb::Unsubscribe,
            Verb::Move,
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

    fn connect(&self, ep: &Endpoint, ctx: &Context) -> Result<Box<dyn Session>> {
        Ok(Box::new(AmqpSession::connect(ep, ctx)?))
    }
}

pub fn module() -> Box<dyn Module> {
    Box::new(AmqpModule)
}
