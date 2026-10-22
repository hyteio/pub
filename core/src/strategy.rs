//! Header strategies: what `send` keeps of a message read from a document
//! (`-F json`), i.e. how a captured message is replayed.
//!
//! Registered like formats ([`crate::Registry::register_strategy`]); chosen
//! with `--header-strategy` or the `header-strategy` setting. Built in:
//!
//! - `default` — publish as a new message: the broker assigns a new message-id
//!   and timestamp, and the time-to-live is the original one counted from now
//!   (`ttl = expiration - timestamp`);
//! - `administrative` — publish the exact message-id, timestamp and expiration
//!   (moving or restoring messages between brokers). The protocol module must
//!   be able to set them (OpenWire can; others report what they can't carry).
//!
//! Both drop consumer-only fields (`redelivered`, `delivery-count`) and
//! provider-only headers: the document's `protocol` section and properties the
//! brokers add themselves ([`crate::Module::provider_properties`]).

use crate::message::{Field, Message};
use crate::settings::matches_pattern;

pub const DEFAULT_STRATEGY: &str = "default";

/// Set on delivery to a consumer; meaningless when publishing.
pub const CONSUMER_ONLY: &[Field] = &[Field::Redelivered, Field::DeliveryCount];

/// A message's identity and timing, normally assigned at send time.
pub const IDENTITY: &[Field] = &[Field::MessageId, Field::Timestamp, Field::Expiration];

pub struct StrategyContext<'a> {
    /// Property name patterns brokers add themselves (every module's, since a
    /// document doesn't say which broker it came from).
    pub provider_properties: &'a [&'static str],
}

/// What a strategy changed, for `-v`.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct StrategyReport {
    /// Names of fields, properties and protocol headers removed.
    pub dropped: Vec<String>,
    /// The ttl worked out from expiration - timestamp, in ms.
    pub ttl_restored: Option<i64>,
    /// There was an expiration but the ttl couldn't be worked out (no timestamp).
    pub ttl_lost: bool,
}

pub trait HeaderStrategy: Send + Sync {
    fn name(&self) -> &'static str;

    fn description(&self) -> &'static str;

    fn apply(&self, msg: &mut Message, ctx: &StrategyContext) -> StrategyReport;
}

fn drop_field(msg: &mut Message, f: Field, report: &mut StrategyReport) {
    if msg.fields.remove(&f).is_some() {
        report.dropped.push(f.name().to_string());
    }
}

/// Consumer-only fields, the protocol section and broker-added properties.
fn drop_non_publishable(msg: &mut Message, ctx: &StrategyContext, report: &mut StrategyReport) {
    for f in CONSUMER_ONLY {
        drop_field(msg, *f, report);
    }
    for (name, _) in msg.protocol.drain(..) {
        report.dropped.push(name);
    }
    msg.properties.retain(|p| {
        let provider = ctx
            .provider_properties
            .iter()
            .any(|pat| matches_pattern(pat, &p.name));
        if provider {
            report.dropped.push(p.name.clone());
        }
        !provider
    });
    // The destination is the record's, never a field to send.
    drop_field(msg, Field::Destination, report);
}

pub struct DefaultStrategy;

impl HeaderStrategy for DefaultStrategy {
    fn name(&self) -> &'static str {
        "default"
    }

    fn description(&self) -> &'static str {
        "a new message: new message-id and timestamp, original ttl counted from now"
    }

    fn apply(&self, msg: &mut Message, ctx: &StrategyContext) -> StrategyReport {
        let mut report = StrategyReport::default();
        let had_expiration = msg.field(Field::Expiration).is_some();
        report.ttl_restored = msg.restore_ttl();
        report.ttl_lost =
            had_expiration && report.ttl_restored.is_none() && msg.field(Field::Ttl).is_none();
        for f in IDENTITY {
            drop_field(msg, *f, &mut report);
        }
        drop_non_publishable(msg, ctx, &mut report);
        report
    }
}

pub struct AdministrativeStrategy;

impl HeaderStrategy for AdministrativeStrategy {
    fn name(&self) -> &'static str {
        "administrative"
    }

    fn description(&self) -> &'static str {
        "the exact message-id, timestamp and expiration (needs a protocol that can set them)"
    }

    fn apply(&self, msg: &mut Message, ctx: &StrategyContext) -> StrategyReport {
        let mut report = StrategyReport::default();
        // The absolute expiration is kept, so a ttl would only disagree with it.
        if msg.field(Field::Expiration).is_some() {
            drop_field(msg, Field::Ttl, &mut report);
        }
        drop_non_publishable(msg, ctx, &mut report);
        report
    }
}

pub fn builtin() -> Vec<Box<dyn HeaderStrategy>> {
    vec![Box::new(DefaultStrategy), Box::new(AdministrativeStrategy)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::Property;
    use crate::Value;

    fn captured() -> Message {
        let mut m = Message::new("x");
        for (f, v) in [
            (Field::MessageId, "ID:h-1-2-1:1:1:7"),
            (Field::Destination, "queue://q"),
            (Field::Timestamp, "1000"),
            (Field::Expiration, "61000"),
            (Field::CorrelationId, "c"),
            (Field::Redelivered, "true"),
            (Field::DeliveryCount, "2"),
        ] {
            m.set_field(f, v).unwrap();
        }
        for name in ["region", "__AMQ_CID", "_AMQ_ROUTING_TYPE"] {
            m.properties.push(Property {
                name: name.into(),
                value: Value::String("v".into()),
            });
        }
        m.protocol.push(("broker-in-time".into(), "1".into()));
        m
    }

    const CTX: StrategyContext = StrategyContext {
        provider_properties: &["__AMQ_*", "_AMQ_*"],
    };

    #[test]
    fn default_publishes_a_new_message() {
        let mut m = captured();
        let r = DefaultStrategy.apply(&mut m, &CTX);
        assert_eq!(r.ttl_restored, Some(60_000));
        let fields: Vec<_> = m.fields.keys().map(|f| f.name()).collect();
        assert_eq!(fields, ["correlation-id", "ttl"]);
        assert_eq!(m.properties.len(), 1);
        assert!(m.protocol.is_empty());
        for name in [
            "message-id",
            "timestamp",
            "expiration",
            "redelivered",
            "delivery-count",
            "destination",
            "broker-in-time",
            "__AMQ_CID",
            "_AMQ_ROUTING_TYPE",
        ] {
            assert!(r.dropped.iter().any(|d| d == name), "{name}: {r:?}");
        }

        let mut no_ts = captured();
        no_ts.fields.remove(&Field::Timestamp);
        let r = DefaultStrategy.apply(&mut no_ts, &CTX);
        assert!(r.ttl_lost && no_ts.field(Field::Ttl).is_none());
    }

    #[test]
    fn administrative_keeps_identity_and_timing() {
        let mut m = captured();
        m.set_field(Field::Ttl, "5s").unwrap();
        let r = AdministrativeStrategy.apply(&mut m, &CTX);
        let fields: Vec<_> = m.fields.keys().map(|f| f.name()).collect();
        assert_eq!(
            fields,
            ["message-id", "timestamp", "correlation-id", "expiration"]
        );
        assert!(r.dropped.iter().any(|d| d == "ttl"));
        assert_eq!(m.properties.len(), 1);
        assert!(m.protocol.is_empty());
    }
}
