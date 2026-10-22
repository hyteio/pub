//! MQTT module for pub: `mqtt://`, `mqtts://` and `mqtt+ssl://`.
//!
//! MQTT 5.0 by default; `version = 3.1.1` selects MQTT 3.1.1.

pub mod codec;
pub mod mapping;
pub mod session;

use pub_core::{
    bail_kind, Context, Endpoint, Field, MessageCapabilities, Module, Result, SchemeInfo, Session,
    Setting, SettingType, Verb,
};

pub use codec::{MqttCodec, Packet};
pub use session::MqttSession;

pub struct MqttModule;

const SCHEMES: &[SchemeInfo] = &[
    SchemeInfo {
        scheme: "mqtt",
        default_port: 1883,
        tls: false,
    },
    SchemeInfo {
        scheme: "mqtts",
        default_port: 8883,
        tls: true,
    },
    SchemeInfo {
        scheme: "mqtt+ssl",
        default_port: 8883,
        tls: true,
    },
];

const QOS: &[&str] = &["0", "1", "2"];
const VERSIONS: &[&str] = &["5", "3.1.1"];

const SETTINGS: &[Setting] = &[
    Setting {
        key: "version",
        ty: SettingType::OneOf(VERSIONS),
        default: Some("5"),
        help: "MQTT protocol version: 5 or 3.1.1 (3.1.1 carries no properties, content-type, correlation-id or ttl)",
    },
    Setting {
        key: "qos",
        ty: SettingType::OneOf(QOS),
        default: Some("1"),
        help: "0 = at most once, 1 = at least once (PUBACK), 2 = exactly once; for send and subscribe",
    },
    Setting {
        key: "retain",
        ty: SettingType::Bool,
        default: Some("false"),
        help: "ask the broker to keep the message for future subscribers",
    },
    Setting {
        key: "retain-handling",
        ty: SettingType::OneOf(QOS),
        default: Some("0"),
        help: "retained messages on subscribe: 0 = send, 1 = only for a new subscription, 2 = don't send",
    },
    Setting {
        key: "clean-start",
        ty: SettingType::Bool,
        default: Some("true"),
        help: "start a new session; false resumes one (needs client-id)",
    },
    Setting {
        key: "session-expiry",
        ty: SettingType::Duration,
        default: Some("0"),
        help: "how long the broker keeps the session after disconnect (MQTT 5; --durable: forever)",
    },
    Setting {
        key: "keep-alive",
        ty: SettingType::Duration,
        default: Some("60s"),
        help: "MQTT keep-alive; pub pings while waiting and fails if the broker stops answering (0 = off)",
    },
    Setting {
        key: "will-topic",
        ty: SettingType::Text,
        default: None,
        help: "a will message: the broker publishes it to this topic if pub's connection ends without a normal DISCONNECT",
    },
    Setting {
        key: "will-message",
        ty: SettingType::Text,
        default: Some(""),
        help: "the will message's body",
    },
    Setting {
        key: "will-qos",
        ty: SettingType::OneOf(QOS),
        default: Some("0"),
        help: "the will message's QoS",
    },
    Setting {
        key: "will-retain",
        ty: SettingType::Bool,
        default: Some("false"),
        help: "publish the will message as retained",
    },
    Setting {
        key: "will-delay",
        ty: SettingType::Duration,
        default: Some("0"),
        help: "MQTT 5: wait this long after the connection ends before publishing the will (no will if the session resumes first)",
    },
    Setting {
        key: "will-on-disconnect",
        ty: SettingType::Bool,
        default: Some("false"),
        help: "MQTT 5: publish the will even when pub disconnects normally",
    },
    Setting {
        key: "share-group",
        ty: SettingType::Text,
        default: None,
        help: "receive through a shared subscription ($share/GROUP/filter): each message goes to one member of the group",
    },
    Setting {
        key: "topic-aliases",
        ty: SettingType::Bool,
        default: Some("true"),
        help: "MQTT 5: send repeated topics as topic aliases when the broker allows them",
    },
    Setting {
        key: "topic-alias-maximum",
        ty: SettingType::Count,
        default: Some("16"),
        help: "MQTT 5: how many topic aliases the broker may use when sending to pub (0 = none, at most 1024)",
    },
    Setting {
        key: "client-id",
        ty: SettingType::Text,
        default: None,
        help: "MQTT client identifier; empty lets the broker assign one",
    },
];

impl Module for MqttModule {
    fn name(&self) -> &'static str {
        "mqtt"
    }

    fn description(&self) -> &'static str {
        "MQTT 5.0 and 3.1.1 (ActiveMQ, Mosquitto, EMQX, HiveMQ, ...)"
    }

    fn schemes(&self) -> &'static [SchemeInfo] {
        SCHEMES
    }

    fn verbs(&self) -> &'static [Verb] {
        &[Verb::Send, Verb::Receive, Verb::Unsubscribe, Verb::Ping]
    }

    fn settings(&self) -> &'static [Setting] {
        SETTINGS
    }

    fn message_fields(&self) -> &'static [Field] {
        mapping::SEND_FIELDS
    }

    /// MQTT 3.1.1 has no properties, so nothing but the body and topic travels.
    fn message_capabilities(&self, ep: &Endpoint) -> MessageCapabilities {
        let v311 = session::protocol_level(ep).is_ok_and(|l| l == codec::PROTOCOL_LEVEL_311);
        MessageCapabilities {
            fields: if v311 { &[] } else { mapping::SEND_FIELDS },
            properties: !v311,
            typed_properties: false,
            reserved_property_names: &[],
            // MQTT 5's Payload Format Indicator says "UTF-8 text"; 3.1.1 has no way to.
            text_bodies: !v311,
            label: v311.then_some("MQTT 3.1.1"),
        }
    }

    /// Topics are not paths: `mqtt://host/sensors/+/temp` is the topic
    /// `sensors/+/temp`, and a `#` (which URLs treat as the fragment) is kept,
    /// so `mqtt://host/sensors/#` subscribes to `sensors/#`. A topic that really
    /// starts with `/` needs a double slash or `--dest`.
    fn destination_name(&self, ep: &Endpoint) -> Option<String> {
        let mut topic = ep.path.strip_prefix('/').unwrap_or(&ep.path).to_string();
        if let Some(frag) = &ep.fragment {
            topic.push('#');
            topic.push_str(frag);
        }
        if topic.is_empty() {
            ep.prop("destination").map(str::to_string)
        } else {
            Some(topic)
        }
    }

    /// A durable subscription is a persistent session: `--durable NAME` is
    /// its client-id, with clean-start = false and (MQTT 5) a session that
    /// never expires unless session-expiry says otherwise. Unsubscribing
    /// connects clean, which discards the session and everything queued.
    fn durable_subscriptions(&self) -> bool {
        true
    }

    fn prepare_durable(&self, ep: &mut Endpoint, name: &str, unsubscribe: bool) -> Result<()> {
        if name.is_empty() {
            bail_kind!(Usage, "--durable needs a name (the MQTT client-id)");
        }
        match ep.prop("client-id") {
            Some(id) if id != name => bail_kind!(
                Usage,
                "--durable {name} is the MQTT session's client-id, but client-id is '{id}'"
            ),
            _ => ep.set_prop("client-id", name),
        }
        if unsubscribe {
            ep.set_prop("clean-start", "true");
            ep.set_prop("session-expiry", "0");
            return Ok(());
        }
        if ep.prop("clean-start").is_some() && ep.prop_bool("clean-start", false)? {
            bail_kind!(
                Usage,
                "--durable resumes a session; clean-start = true would discard it"
            );
        }
        ep.set_prop("clean-start", "false");
        if session::protocol_level(ep)? == codec::PROTOCOL_LEVEL_5 {
            match ep.prop_duration("session-expiry")? {
                None => ep.set_prop("session-expiry", format!("{}s", u32::MAX)),
                Some(d) if d.is_zero() => bail_kind!(
                    Usage,
                    "--durable with session-expiry = 0 would end the session at disconnect"
                ),
                Some(_) => {}
            }
        }
        Ok(())
    }

    fn connect(&self, ep: &Endpoint, ctx: &Context) -> Result<Box<dyn Session>> {
        Ok(Box::new(MqttSession::connect(ep, ctx)?))
    }
}

pub fn module() -> Box<dyn Module> {
    Box::new(MqttModule)
}

#[cfg(feature = "mock")]
pub mod mock;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topics_from_urls() {
        let name = |u: &str| MqttModule.destination_name(&Endpoint::parse(u).unwrap());
        assert_eq!(
            name("mqtt://h/sensors/+/temp").as_deref(),
            Some("sensors/+/temp")
        );
        assert_eq!(name("mqtt://h/sensors/#").as_deref(), Some("sensors/#"));
        assert_eq!(name("mqtt://h/#").as_deref(), Some("#"));
        assert_eq!(name("mqtt://h//leading").as_deref(), Some("/leading"));
        assert_eq!(name("mqtt://h/a%2Fb").as_deref(), Some("a/b"));
        assert_eq!(name("mqtt://h").as_deref(), None);
        assert_eq!(name("mqtt://h?destination=x/y").as_deref(), Some("x/y"));
    }
}
