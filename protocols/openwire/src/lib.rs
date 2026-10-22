//! ActiveMQ OpenWire module for pub (`amq://`, `amqs://`).
//!
//! OpenWire v12, tight encoding preferred (loose as fallback). The format
//! reference, derived from the ActiveMQ client source, is `docs/src/development/openwire/` in
//! the repository; field layouts are generated from the Java marshallers.
//!
//! - [`utf`]: Java modified UTF-8
//! - [`wire`]: loose/tight field encoding (boolean stream, variable-size longs, ...)
//! - [`primitive`]: typed property maps and TextMessage bodies
//! - [`types`]: every data structure and command pub handles
//! - [`codec`]: framing for pub-core's `Connection`
//! - [`producer`]: destination names and pub's message model as an ActiveMQ message
//! - [`consumer`]: consumer settings and an ActiveMQ message as pub's message model
//! - [`session`]: negotiation, connection, keep-alive, send, close
//!
//! Settings use the Java client's names and units (milliseconds), so options
//! from an ActiveMQ URI work as-is: `amq://broker?jms.closeTimeout=5000`.

pub mod codec;
pub mod consumer;
pub mod primitive;
pub mod producer;
pub mod session;
pub mod types;
pub mod utf;
pub mod wire;

#[cfg(feature = "mock")]
pub mod mock;

use pub_core::module::{DestinationOption, OptionScope};
use pub_core::{
    AliasValue, Context, Endpoint, Field, InertSetting, Module, Result, SchemeInfo, Session,
    Setting, SettingAlias, SettingType, Verb,
};

pub use codec::OpenWireCodec;
pub use session::OpenWireSession;
pub use types::DataStructure;
pub use wire::Encoding;

pub struct OpenWireModule;

const SCHEMES: &[SchemeInfo] = &[
    SchemeInfo {
        scheme: "amq",
        default_port: 61616,
        tls: false,
    },
    SchemeInfo {
        scheme: "amqs",
        default_port: 61617,
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
    setting("client-id", SettingType::Text, None, "JMS client ID (jms.clientID); default: generated per connection"),
    setting(
        "client-id-prefix",
        SettingType::Text,
        None,
        "prefix for generated client IDs, as Java's clientIDPrefix: <prefix>-<pid>-<start ms>-0:<n> (ignored when client-id is set)",
    ),
    setting(
        "connection-id-prefix",
        SettingType::Text,
        None,
        "prefix for connection IDs, as Java's connectionIDPrefix: <prefix>-<pid>-<start ms>-1:<n> (producer and message IDs start with it)",
    ),
    setting(
        "wireFormat.tightEncodingEnabled",
        SettingType::Bool,
        Some("true"),
        "prefer the compact tight encoding; used when the broker agrees",
    ),
    setting(
        "wireFormat.stackTraceEnabled",
        SettingType::Bool,
        Some("false"),
        "ask for Java stack traces in broker errors (Java default: true)",
    ),
    setting(
        "wireFormat.maxInactivityDuration",
        SettingType::Count,
        Some("30000"),
        "ms of silence before the connection is presumed dead; keep-alives every third of it (0 = off)",
    ),
    setting(
        "wireFormat.maxInactivityDurationInitalDelay",
        SettingType::Count,
        Some("10000"),
        "ms before the inactivity monitor starts (Java's spelling, 'Inital')",
    ),
    setting(
        "jms.connectResponseTimeout",
        SettingType::Count,
        Some("0"),
        "ms to wait for the broker to accept the connection (0 = receive-timeout)",
    ),
    setting("jms.closeTimeout", SettingType::Count, Some("15000"), "ms to wait for the broker to acknowledge close"),
    setting(
        "broker-type",
        // `classic` is the name 1.0.0 and 1.0.1 used for `activemq`.
        SettingType::OneOf(&["auto", "activemq", "artemis", "classic"]),
        Some("auto"),
        "which broker, for destination management: activemq, artemis, or auto = from the broker's id (classic = activemq)",
    ),
    setting(
        "jms.useAsyncSend",
        SettingType::Bool,
        Some("false"),
        "send persistent messages without waiting for the broker (non-persistent ones always are)",
    ),
    setting(
        "jms.alwaysSyncSend",
        SettingType::Bool,
        Some("false"),
        "wait for the broker on every send, persistent or not",
    ),
    setting(
        "jms.sendTimeout",
        SettingType::Count,
        Some("0"),
        "ms to wait for the broker to accept a message; > 0 makes every send synchronous (0 = receive-timeout)",
    ),
    setting(
        "jms.producerWindowSize",
        SettingType::Count,
        Some("0"),
        "bytes of asynchronous sends allowed unacknowledged (0 = no producer flow control)",
    ),
    setting(
        "jms.useCompression",
        SettingType::Bool,
        Some("false"),
        "deflate message bodies (any ActiveMQ client inflates them)",
    ),
    setting(
        "jms.disableTimeStampsByDefault",
        SettingType::Bool,
        Some("false"),
        "leave JMSTimestamp unset",
    ),
    setting(
        "prefetch-follows-count",
        SettingType::Bool,
        Some("true"),
        "without an explicit prefetch, take only as many messages as -n asks for (Java: 1000 queue / 32767 topic)",
    ),
    setting(
        "jms.prefetchPolicy.all",
        SettingType::Count,
        None,
        "prefetch for queues and topics (0 = pull one message at a time)",
    ),
    setting("jms.prefetchPolicy.queuePrefetch", SettingType::Count, None, "queue prefetch (Java default 1000)"),
    setting("jms.prefetchPolicy.topicPrefetch", SettingType::Count, None, "topic prefetch (Java default 32767)"),
    setting(
        "jms.prefetchPolicy.durableTopicPrefetch",
        SettingType::Count,
        None,
        "durable subscription prefetch (Java default 100)",
    ),
    setting(
        "jms.prefetchPolicy.queueBrowserPrefetch",
        SettingType::Count,
        None,
        "browse prefetch (Java default 500)",
    ),
    setting(
        "jms.prefetchPolicy.maximumPendingMessageLimit",
        SettingType::Count,
        Some("0"),
        "messages a slow topic consumer may fall behind before the broker discards old ones (0 = no limit)",
    ),
    setting("jms.dispatchAsync", SettingType::Bool, Some("true"), "ask the broker to dispatch asynchronously"),
    setting("jms.exclusiveConsumer", SettingType::Bool, Some("false"), "ask to be the queue's only consumer"),
    setting(
        "jms.useRetroactiveConsumer",
        SettingType::Bool,
        Some("false"),
        "topics: also receive recent messages the broker kept (needs a broker recovery policy)",
    ),
    setting(
        "jms.sendAcksAsync",
        SettingType::Bool,
        Some("true"),
        "acknowledge without waiting for the broker",
    ),
    setting(
        "jms.consumerExpiryCheckEnabled",
        SettingType::Bool,
        Some("true"),
        "drop messages that expired on the way (acknowledged as expired)",
    ),
];

/// Options carried on the destination, as in `queue://orders?producer.windowSize=65536`.
const DESTINATION_OPTIONS: &[DestinationOption] = &[
    DestinationOption {
        pattern: "producer.*",
        scope: OptionScope::Produce,
        help: "ProducerInfo options: producer.windowSize, producer.dispatchAsync",
    },
    DestinationOption {
        pattern: "consumer.*",
        scope: OptionScope::Consume,
        help: "ConsumerInfo options, e.g. consumer.prefetchSize, consumer.selector",
    },
];

/// Standard fields with a native OpenWire home (`delivery-delay` becomes
/// ActiveMQ's `AMQ_SCHEDULED_DELAY`; content-type has none).
const SEND_FIELDS: &[Field] = &[
    // Only from a document with the administrative header strategy.
    Field::MessageId,
    Field::Timestamp,
    Field::Expiration,
    Field::CorrelationId,
    Field::ReplyTo,
    Field::Type,
    Field::Priority,
    Field::Persistent,
    Field::Ttl,
    Field::DeliveryDelay,
    Field::GroupId,
    Field::GroupSeq,
];

/// Properties brokers set themselves: the JMS spec's provider-set JMSX names,
/// and Artemis's internal ones (`__AMQ_CID`, `_AMQ_ROUTING_TYPE`, `__HDR_*`).
pub const PROVIDER_PROPERTIES: &[&str] = &[
    "JMSXUserID",
    "JMSXAppID",
    "JMSXProducerTXID",
    "JMSXConsumerTXID",
    "JMSXRcvTimestamp",
    "JMSXState",
    "__AMQ_*",
    "_AMQ_*",
    "__HDR_*",
    // ActiveMQ's AMQP transformer records what the AMQP message looked like.
    "JMS_AMQP_*",
];

/// JMS header names Java routes to message fields when set as properties.
const RESERVED: &[&str] = &[
    "JMSCorrelationID",
    "JMSDeliveryMode",
    "JMSDestination",
    "JMSExpiration",
    "JMSMessageID",
    "JMSPriority",
    "JMSRedelivered",
    "JMSReplyTo",
    "JMSTimestamp",
    "JMSType",
    "JMSXDeliveryCount",
    "JMSXGroupID",
    "JMSXGroupSeq",
];

const fn alias(
    alias: &'static str,
    targets: &'static [&'static str],
    value: AliasValue,
) -> SettingAlias {
    SettingAlias {
        alias,
        targets,
        value,
    }
}

/// ActiveMQ URI / connection-factory names for pub's own settings.
const ALIASES: &[SettingAlias] = &[
    alias(
        "connectionTimeout",
        &["connect-timeout"],
        AliasValue::Millis,
    ),
    alias("soTimeout", &["receive-timeout"], AliasValue::Millis),
    alias("tcpNoDelay", &["tcp-nodelay"], AliasValue::Same),
    alias("keepAlive", &["tcp-keepalive"], AliasValue::Same),
    alias(
        "socketBufferSize",
        &["receive-buffer", "send-buffer"],
        AliasValue::Same,
    ),
    alias(
        "wireFormat.maxFrameSize",
        &["max-frame-size"],
        AliasValue::Same,
    ),
    alias("jms.clientID", &["client-id"], AliasValue::Same),
    alias(
        "jms.clientIDPrefix",
        &["client-id-prefix"],
        AliasValue::Same,
    ),
    alias(
        "jms.connectionIDPrefix",
        &["connection-id-prefix"],
        AliasValue::Same,
    ),
    alias("jms.userName", &["user"], AliasValue::Same),
    alias("jms.password", &["password"], AliasValue::Same),
];

const fn inert(pattern: &'static str, reason: &'static str) -> InertSetting {
    InertSetting { pattern, reason }
}

/// Java client options that mean nothing to a one-shot CLI.
const INERT: &[InertSetting] = &[
    inert(
        "wireFormat.cacheEnabled",
        "pub never uses the marshal cache",
    ),
    inert("wireFormat.cacheSize", "pub never uses the marshal cache"),
    inert(
        "wireFormat.sizePrefixDisabled",
        "pub always uses the size prefix",
    ),
    inert(
        "wireFormat.tcpNoDelayEnabled",
        "use tcpNoDelay / tcp-nodelay",
    ),
    inert("trace", "use -v"),
    inert(
        "jms.watchTopicAdvisories",
        "pub never subscribes to advisory topics",
    ),
    inert("jms.copyMessageOnSend", "pub sends each message once"),
    inert("jms.statsEnabled", "pub keeps no JMS statistics"),
    inert(
        "jms.objectMessageSerializationDefered",
        "pub does not send ObjectMessages",
    ),
    inert(
        "jms.optimizedMessageDispatch",
        "client-side dispatch tuning",
    ),
    inert("jms.useDedicatedTaskRunner", "client-side threading"),
    inert("jms.alwaysSessionAsync", "client-side threading"),
    inert("jms.maxThreadPoolSize", "client-side threading"),
    inert(
        "jms.nestedMapAndListEnabled",
        "pub does not send MapMessages",
    ),
    inert("jms.checkForDuplicates", "client-side duplicate audit"),
    inert("jms.auditDepth", "client-side duplicate audit"),
    inert(
        "jms.auditMaximumProducerNumber",
        "client-side duplicate audit",
    ),
    inert(
        "jms.transactedIndividualAck",
        "pub does not use transactions",
    ),
    inert(
        "jms.nonBlockingRedelivery",
        "pub does not redeliver locally",
    ),
    inert("jms.rmIdFromConnectionId", "XA only"),
    inert("jms.xaAckMode", "XA only"),
    inert(
        "jms.warnAboutUnstartedConnectionTimeout",
        "pub starts connections immediately",
    ),
    inert(
        "jms.trustAllPackages",
        "pub does not deserialize ObjectMessages",
    ),
    inert(
        "jms.trustedPackages",
        "pub does not deserialize ObjectMessages",
    ),
    inert("jms.redeliveryPolicy.*", "pub does not redeliver locally"),
    inert(
        "jms.blobTransferPolicy.*",
        "blob messages are not supported",
    ),
    inert(
        "jms.prefetchPolicy.optimizeDurableTopicPrefetch",
        "pub does not create durable subscriptions",
    ),
    inert(
        "jms.messagePrioritySupported",
        "pub hands messages on in arrival order",
    ),
    inert("jms.optimizeAcknowledge", "pub acknowledges each message"),
    inert(
        "jms.optimizeAcknowledgeTimeOut",
        "pub acknowledges each message",
    ),
    inert(
        "jms.optimizedAckScheduledAckInterval",
        "pub acknowledges each message",
    ),
    inert(
        "jms.consumerFailoverRedeliveryWaitPeriod",
        "pub does not fail over",
    ),
];

impl Module for OpenWireModule {
    fn name(&self) -> &'static str {
        "openwire"
    }

    fn description(&self) -> &'static str {
        "ActiveMQ OpenWire v12 (ActiveMQ 6.x/5.17+, Artemis)"
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
            Verb::Destinations,
            Verb::Ping,
        ]
    }

    fn settings(&self) -> &'static [Setting] {
        SETTINGS
    }

    fn setting_aliases(&self) -> &'static [SettingAlias] {
        ALIASES
    }

    fn inert_settings(&self) -> &'static [InertSetting] {
        INERT
    }

    fn destination_options(&self) -> &'static [DestinationOption] {
        DESTINATION_OPTIONS
    }

    fn message_fields(&self) -> &'static [Field] {
        SEND_FIELDS
    }

    fn typed_properties(&self) -> bool {
        true
    }

    fn durable_subscriptions(&self) -> bool {
        true
    }

    fn transactions(&self) -> bool {
        true
    }

    fn provider_properties(&self) -> &'static [&'static str] {
        PROVIDER_PROPERTIES
    }

    fn text_bodies(&self) -> bool {
        true
    }

    fn reserved_property_names(&self) -> &'static [&'static str] {
        RESERVED
    }

    fn connect(&self, ep: &Endpoint, ctx: &Context) -> Result<Box<dyn Session>> {
        Ok(Box::new(OpenWireSession::connect(ep, ctx)?))
    }
}

pub fn module() -> Box<dyn Module> {
    Box::new(OpenWireModule)
}

#[cfg(test)]
mod tests;
