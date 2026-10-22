//! STOMP module for pub: `stomp://`, `stomp+ssl://` and `stomps://`.

pub mod codec;
pub mod mapping;
pub mod session;

use pub_core::{
    Context, DestinationOption, Endpoint, Field, Module, Result, SchemeInfo, Session, Setting,
    SettingType, Verb,
};

pub use codec::{Frame, StompCodec};
pub use session::StompSession;

pub struct StompModule;

const SCHEMES: &[SchemeInfo] = &[
    SchemeInfo {
        scheme: "stomp",
        default_port: 61613,
        tls: false,
    },
    SchemeInfo {
        scheme: "stomp+ssl",
        default_port: 61612,
        tls: true,
    },
    SchemeInfo {
        scheme: "stomps",
        default_port: 61612,
        tls: true,
    },
];

const SETTINGS: &[Setting] = &[
    Setting {
        key: "vhost",
        ty: SettingType::Text,
        default: None,
        help: "CONNECT `host` header (virtual host); defaults to the URL host",
    },
    Setting {
        key: "client-id",
        ty: SettingType::Text,
        default: None,
        help: "CONNECT `client-id` header, for durable subscriptions",
    },
    Setting {
        key: "receipts",
        ty: SettingType::Bool,
        default: Some("true"),
        help: "request a RECEIPT for each SEND and wait for it",
    },
    Setting {
        key: "heart-beat",
        ty: SettingType::Duration,
        default: Some("10s"),
        help: "heart-beat interval offered both ways (STOMP 1.1+); pub fails if the broker goes silent for two (0 = off)",
    },
];

impl Module for StompModule {
    fn name(&self) -> &'static str {
        "stomp"
    }

    fn description(&self) -> &'static str {
        "STOMP 1.0/1.1/1.2 (ActiveMQ, Artemis, RabbitMQ, ...)"
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

    fn destination_options(&self) -> &'static [DestinationOption] {
        mapping::DESTINATION_OPTIONS
    }

    fn message_fields(&self) -> &'static [Field] {
        mapping::SEND_FIELDS
    }

    fn provider_properties(&self) -> &'static [&'static str] {
        mapping::PROVIDER_PROPERTIES
    }

    fn reserved_property_names(&self) -> &'static [&'static str] {
        mapping::RESERVED
    }

    /// Text = SEND without content-length, which ActiveMQ/Artemis make a TextMessage.
    fn text_bodies(&self) -> bool {
        true
    }

    /// ActiveMQ and Artemis: `activemq.subscriptionName` /
    /// `durable-subscription-name` with a client-id.
    fn durable_subscriptions(&self) -> bool {
        true
    }

    fn transactions(&self) -> bool {
        true
    }

    fn connect(&self, ep: &Endpoint, ctx: &Context) -> Result<Box<dyn Session>> {
        Ok(Box::new(StompSession::connect(ep, ctx)?))
    }
}

pub fn module() -> Box<dyn Module> {
    Box::new(StompModule)
}

#[cfg(feature = "mock")]
pub mod mock;
