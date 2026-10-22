//! The module API: protocols register URI schemes and declare which sub-commands
//! (verbs) they support.

use std::fmt;
use std::time::Duration;

use crate::aliases::{InertSetting, SettingAlias};
use crate::endpoint::Endpoint;
use crate::error::Result;
use crate::message::{Field, Message};
use crate::settings::Setting;
use crate::trace::Trace;
use crate::{bail_kind, err};

/// A URI scheme handled by a module.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemeInfo {
    pub scheme: &'static str,
    pub default_port: u16,
    /// Always wrap the socket in TLS.
    pub tls: bool,
}

/// Messaging sub-commands. The CLI owns argument parsing for each; modules declare
/// which ones they implement via [`Module::verbs`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verb {
    Send,
    Receive,
    /// Look at a queue's messages without consuming them.
    Browse,
    /// Remove a durable subscription.
    Unsubscribe,
    /// Move messages between destinations on one broker, transactionally.
    Move,
    /// List, create and delete destinations.
    Destinations,
    Ping,
}

impl Verb {
    pub const ALL: &'static [Verb] = &[
        Verb::Send,
        Verb::Receive,
        Verb::Browse,
        Verb::Unsubscribe,
        Verb::Move,
        Verb::Destinations,
        Verb::Ping,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Verb::Send => "send",
            Verb::Receive => "recv",
            Verb::Browse => "browse",
            Verb::Unsubscribe => "unsub",
            Verb::Move => "move",
            Verb::Destinations => "dest",
            Verb::Ping => "ping",
        }
    }
}

impl fmt::Display for Verb {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// What a module gets besides the endpoint when connecting.
#[derive(Debug, Clone)]
pub struct Context {
    pub trace: Trace,
    /// The scheme entry that matched the endpoint.
    pub scheme: SchemeInfo,
}

#[derive(Debug, Clone, Default)]
pub struct ReceiveOptions {
    /// Stop after this many messages; `None` = until timeout.
    pub count: Option<usize>,
    /// Give up waiting after this long; `None` = wait forever.
    pub timeout: Option<Duration>,
    /// Acknowledge each message after it has been handed to the sink.
    pub ack: bool,
    /// Message selector (JMS/SQL-92 style), if the protocol supports one.
    pub selector: Option<String>,
    /// Extra raw protocol headers for the subscription.
    pub headers: Vec<(String, String)>,
    /// Receive through this durable subscription (modules with
    /// [`Module::durable_subscriptions`]).
    pub subscription: Option<String>,
    /// Acknowledge in local transactions (modules with [`Module::transactions`]),
    /// committing every N messages (0: once, at the end); an error rolls back.
    pub transaction: Option<usize>,
    /// Stop once no message has arrived for this long (`move`).
    pub idle: Option<Duration>,
}

/// Which destinations `list_destinations` reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DestinationKinds {
    pub queues: bool,
    pub topics: bool,
    /// Include the broker's own (e.g. ActiveMQ's `ActiveMQ.Advisory.*` topics).
    pub internal: bool,
}

/// Where a provider-specific destination option applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptionScope {
    /// Receiving (subscribe/consumer).
    Consume,
    /// Sending (producer).
    Produce,
    Both,
}

impl OptionScope {
    pub fn applies_to(self, verb: Verb) -> bool {
        match self {
            OptionScope::Both => true,
            OptionScope::Consume => matches!(verb, Verb::Receive | Verb::Browse | Verb::Move),
            OptionScope::Produce => verb == Verb::Send,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            OptionScope::Consume => "receive",
            OptionScope::Produce => "send",
            OptionScope::Both => "send+receive",
        }
    }
}

/// A key pattern (`consumer.*` or an exact name) that a module accepts as a
/// destination option: passed through to the provider untouched, case preserved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DestinationOption {
    pub pattern: &'static str,
    pub scope: OptionScope,
    pub help: &'static str,
}

/// A destination plus the provider options that apply to it (e.g. ActiveMQ's
/// `consumer.prefetchSize`), already filtered to the current verb's scope.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Destination {
    pub name: String,
    /// In key order, original spelling.
    pub options: Vec<(String, String)>,
}

impl Destination {
    pub fn new(name: impl Into<String>) -> Self {
        Destination {
            name: name.into(),
            options: Vec::new(),
        }
    }
}

impl fmt::Display for Destination {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name)?;
        for (i, (k, v)) in self.options.iter().enumerate() {
            write!(f, "{}{k}={v}", if i == 0 { '?' } else { '&' })?;
        }
        Ok(())
    }
}

/// What a module can carry in a message *for a particular endpoint* — some
/// protocols differ by version (MQTT 3.1.1 has no properties at all).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageCapabilities {
    /// Standard fields that can be sent.
    pub fields: &'static [Field],
    /// Application properties can be sent at all.
    pub properties: bool,
    /// Property types survive the trip (false: everything becomes a string).
    pub typed_properties: bool,
    /// Property names the protocol uses for its own headers.
    pub reserved_property_names: &'static [&'static str],
    /// Can mark a body as text (`message-type = text`); otherwise everything is bytes.
    pub text_bodies: bool,
    /// Qualifies the module name in messages, e.g. "MQTT 3.1.1".
    pub label: Option<&'static str>,
}

/// A protocol plugin.
pub trait Module: Send + Sync {
    fn name(&self) -> &'static str;

    fn description(&self) -> &'static str;

    fn schemes(&self) -> &'static [SchemeInfo];

    fn verbs(&self) -> &'static [Verb];

    /// Settings this module reads, beyond pub-core's own
    /// ([`crate::settings::CORE_SETTINGS`]). Used for validation and `pub config settings`.
    fn settings(&self) -> &'static [Setting] {
        &[]
    }

    /// The destination named by an endpoint: by default its URL path, else the
    /// `destination` setting. Protocols whose names are not path-like override
    /// this (MQTT drops the leading `/` and keeps a `#` wildcard from the fragment).
    fn destination_name(&self, ep: &Endpoint) -> Option<String> {
        ep.destination()
    }

    /// Other names this module accepts for pub's settings, e.g. Java URI options
    /// like `connectionTimeout` (ms) for `connect-timeout`. See [`crate::aliases`].
    fn setting_aliases(&self) -> &'static [SettingAlias] {
        &[]
    }

    /// Settings accepted for compatibility but ignored (e.g. Java client options
    /// that mean nothing to a CLI). See [`crate::aliases::InertSetting`].
    fn inert_settings(&self) -> &'static [InertSetting] {
        &[]
    }

    /// Query/config keys passed through to the provider as destination options.
    fn destination_options(&self) -> &'static [DestinationOption] {
        &[]
    }

    /// `-H` names (patterns) that are raw protocol headers rather than properties.
    fn protocol_headers(&self) -> &'static [&'static str] {
        &[]
    }

    /// Property names (patterns, trailing `*`) that brokers reached over this
    /// protocol add to messages themselves, e.g. Artemis's `__AMQ_CID`. The
    /// default header strategy strips them when replaying a captured message.
    fn provider_properties(&self) -> &'static [&'static str] {
        &[]
    }

    /// Whether `receive` can use a durable subscription (`--durable NAME`).
    fn durable_subscriptions(&self) -> bool {
        false
    }

    /// Adjust settings for `--durable NAME` before connecting, where the
    /// subscription belongs to the connection itself (MQTT sessions).
    /// `unsubscribe` is true when connecting to remove the subscription.
    fn prepare_durable(&self, _ep: &mut Endpoint, _name: &str, _unsubscribe: bool) -> Result<()> {
        Ok(())
    }

    /// Whether sessions support local transactions (`--transaction`, `move`).
    fn transactions(&self) -> bool {
        false
    }

    /// Standard fields this module can send.
    fn message_fields(&self) -> &'static [Field] {
        &[]
    }

    /// Whether property types survive the trip (false: everything is a string).
    /// Whether the protocol can say "this body is text" (`message-type = text`).
    fn text_bodies(&self) -> bool {
        false
    }

    fn typed_properties(&self) -> bool {
        false
    }

    /// Property names the protocol uses for its own headers; sending a property
    /// with one of these names would change its meaning.
    fn reserved_property_names(&self) -> &'static [&'static str] {
        &[]
    }

    /// What can be sent to this endpoint. The default combines
    /// [`message_fields`](Self::message_fields), [`typed_properties`](Self::typed_properties)
    /// and [`reserved_property_names`](Self::reserved_property_names); modules whose
    /// abilities depend on settings (e.g. a protocol version) override this.
    fn message_capabilities(&self, _ep: &Endpoint) -> MessageCapabilities {
        MessageCapabilities {
            fields: self.message_fields(),
            properties: true,
            typed_properties: self.typed_properties(),
            reserved_property_names: self.reserved_property_names(),
            text_bodies: self.text_bodies(),
            label: None,
        }
    }

    fn supports(&self, verb: Verb) -> bool {
        self.verbs().contains(&verb)
    }

    /// Connect and complete the protocol handshake.
    fn connect(&self, ep: &Endpoint, ctx: &Context) -> Result<Box<dyn Session>>;
}

/// A live protocol session. Methods a module does not support keep the default,
/// which reports the verb as unsupported.
pub trait Session {
    /// Server/session details shown by `pub ping`.
    fn info(&self) -> Vec<(String, String)> {
        Vec::new()
    }

    fn send(&mut self, _destination: &Destination, _msg: &Message) -> Result<()> {
        Err(unsupported(Verb::Send))
    }

    /// Deliver messages to `sink` until `opts.count` or `opts.timeout`; returns how many.
    fn receive(
        &mut self,
        _destination: &Destination,
        _opts: &ReceiveOptions,
        _sink: &mut dyn FnMut(Message) -> Result<()>,
    ) -> Result<usize> {
        Err(unsupported(Verb::Receive))
    }

    /// Deliver a queue's messages without consuming them, until the end of the
    /// queue, `opts.count` or `opts.timeout`; returns how many (`opts.ack` and
    /// `opts.subscription` don't apply).
    fn browse(
        &mut self,
        _destination: &Destination,
        _opts: &ReceiveOptions,
        _sink: &mut dyn FnMut(Message) -> Result<()>,
    ) -> Result<usize> {
        Err(unsupported(Verb::Browse))
    }

    /// Remove the durable subscription `name`; an unknown name is an error where the
    /// protocol can tell.
    fn unsubscribe(&mut self, _destination: &Destination, _name: &str) -> Result<()> {
        Err(unsupported(Verb::Unsubscribe))
    }

    /// Start a local transaction: sends until [`commit`](Self::commit) or
    /// [`rollback`](Self::rollback) are part of it.
    fn begin(&mut self) -> Result<()> {
        Err(err!(
            Unsupported,
            "transactions are not supported by this protocol"
        ))
    }

    fn commit(&mut self) -> Result<()> {
        Err(err!(
            Unsupported,
            "transactions are not supported by this protocol"
        ))
    }

    fn rollback(&mut self) -> Result<()> {
        Err(err!(
            Unsupported,
            "transactions are not supported by this protocol"
        ))
    }

    /// The broker's destinations as `queue://name` / `topic://name`, sorted;
    /// `wait` bounds how long to listen when the protocol has no end marker.
    fn list_destinations(
        &mut self,
        _kinds: DestinationKinds,
        _wait: Duration,
    ) -> Result<Vec<String>> {
        Err(unsupported(Verb::Destinations))
    }

    fn create_destination(&mut self, _destination: &Destination) -> Result<()> {
        Err(unsupported(Verb::Destinations))
    }

    /// Fails if the destination is in use (the broker decides).
    fn delete_destination(&mut self, _destination: &Destination) -> Result<()> {
        Err(unsupported(Verb::Destinations))
    }

    /// Move messages from `from` to `to` on this broker as exact copies, each
    /// batch (`opts.transaction`) in one transaction; `moved` sees each message.
    /// Stops at `opts.count`, `opts.timeout`, or after `opts.idle` without one.
    fn move_messages(
        &mut self,
        _from: &Destination,
        _to: &Destination,
        _opts: &ReceiveOptions,
        _moved: &mut dyn FnMut(Message) -> Result<()>,
    ) -> Result<usize> {
        Err(unsupported(Verb::Move))
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

fn unsupported(verb: Verb) -> anyhow::Error {
    err!(Unsupported, "'{verb}' is not supported by this protocol")
}

/// Messages per transaction when moving, unless `--batch` says otherwise.
pub const DEFAULT_MOVE_BATCH: usize = 100;

/// The set of compiled-in modules.
pub struct Registry {
    modules: Vec<Box<dyn Module>>,
    formats: Vec<Box<dyn crate::format::Format>>,
    strategies: Vec<Box<dyn crate::strategy::HeaderStrategy>>,
}

/// Starts with the built-in formats (`raw`, `text`) and header strategies
/// (`default`, `administrative`), and no protocol modules.
impl Default for Registry {
    fn default() -> Self {
        Registry {
            modules: Vec::new(),
            formats: crate::format::builtin(),
            strategies: crate::strategy::builtin(),
        }
    }
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a module. Panics if one of its schemes is already taken — that is a
    /// build-time wiring mistake, not a runtime condition.
    pub fn register(&mut self, module: Box<dyn Module>) {
        for s in module.schemes() {
            if let Some((other, _)) = self.find(s.scheme) {
                panic!(
                    "scheme '{}' registered by both '{}' and '{}'",
                    s.scheme,
                    other.name(),
                    module.name()
                );
            }
        }
        self.modules.push(module);
    }

    /// Add a message format. Panics on a duplicate name (a wiring mistake).
    pub fn register_format(&mut self, format: Box<dyn crate::format::Format>) {
        if self.formats.iter().any(|f| f.name() == format.name()) {
            panic!("format '{}' registered twice", format.name());
        }
        self.formats.push(format);
    }

    pub fn formats(&self) -> impl Iterator<Item = &dyn crate::format::Format> {
        self.formats.iter().map(|f| f.as_ref())
    }

    /// A format by name (case-insensitive), with a suggestion when misspelled.
    pub fn format(&self, name: &str) -> Result<&dyn crate::format::Format> {
        if let Some(f) = self
            .formats()
            .find(|f| f.name().eq_ignore_ascii_case(name.trim()))
        {
            return Ok(f);
        }
        let names: Vec<&str> = self.formats().map(|f| f.name()).collect();
        let hint = crate::settings::suggest_name(name, &names)
            .map(|s| format!("; did you mean '{s}'?"))
            .unwrap_or_default();
        bail_kind!(
            Usage,
            "unknown format '{name}' (available: {}){hint}",
            names.join(", ")
        )
    }

    /// Add a header strategy. Panics on a duplicate name (a wiring mistake).
    pub fn register_strategy(&mut self, s: Box<dyn crate::strategy::HeaderStrategy>) {
        if self.strategies.iter().any(|x| x.name() == s.name()) {
            panic!("header strategy '{}' registered twice", s.name());
        }
        self.strategies.push(s);
    }

    pub fn strategies(&self) -> impl Iterator<Item = &dyn crate::strategy::HeaderStrategy> {
        self.strategies.iter().map(|s| s.as_ref())
    }

    /// A header strategy by name (case-insensitive; `admin` for `administrative`).
    pub fn strategy(&self, name: &str) -> Result<&dyn crate::strategy::HeaderStrategy> {
        let want = match name.trim() {
            n if n.eq_ignore_ascii_case("admin") => "administrative",
            n => n,
        };
        if let Some(s) = self
            .strategies()
            .find(|s| s.name().eq_ignore_ascii_case(want))
        {
            return Ok(s);
        }
        let names: Vec<&str> = self.strategies().map(|s| s.name()).collect();
        let hint = crate::settings::suggest_name(name, &names)
            .map(|s| format!("; did you mean '{s}'?"))
            .unwrap_or_default();
        bail_kind!(
            Usage,
            "unknown header strategy '{name}' (available: {}){hint}",
            names.join(", ")
        )
    }

    /// Property patterns any compiled-in module says brokers add themselves.
    pub fn provider_properties(&self) -> Vec<&'static str> {
        let mut all: Vec<&'static str> = Vec::new();
        for m in self.modules() {
            for p in m.provider_properties() {
                if !all.contains(p) {
                    all.push(p);
                }
            }
        }
        all
    }

    /// Resolve a target with every configuration layer, including `[module]`.
    pub fn resolve_endpoint(&self, target: &str, cfg: &crate::config::Config) -> Result<Endpoint> {
        Endpoint::resolve_with(target, cfg, &|scheme| {
            self.lookup(scheme).ok().map(|(m, _)| m.name())
        })
    }

    pub fn module_named(&self, name: &str) -> Option<&dyn Module> {
        self.modules().find(|m| m.name().eq_ignore_ascii_case(name))
    }

    pub fn modules(&self) -> impl Iterator<Item = &dyn Module> {
        self.modules.iter().map(|m| m.as_ref())
    }

    fn find(&self, scheme: &str) -> Option<(&dyn Module, &SchemeInfo)> {
        self.modules().find_map(|m| {
            m.schemes()
                .iter()
                .find(|s| s.scheme.eq_ignore_ascii_case(scheme))
                .map(|s| (m, s))
        })
    }

    pub fn lookup(&self, scheme: &str) -> Result<(&dyn Module, SchemeInfo)> {
        match self.find(scheme) {
            Some((m, s)) => Ok((m, *s)),
            None => {
                let known: Vec<_> = self
                    .modules()
                    .flat_map(|m| m.schemes().iter().map(|s| s.scheme))
                    .collect();
                bail_kind!(
                    Unsupported,
                    "no module handles '{scheme}://' (available: {})",
                    known.join(", ")
                )
            }
        }
    }

    /// Find the module for an endpoint and check it supports `verb`.
    pub fn for_verb(&self, ep: &Endpoint, verb: Verb) -> Result<(&dyn Module, SchemeInfo)> {
        let (m, s) = self.lookup(&ep.scheme)?;
        if !m.supports(verb) {
            let verbs: Vec<_> = m.verbs().iter().map(|v| v.name()).collect();
            bail_kind!(
                Unsupported,
                "the {} module does not support '{verb}' (supports: {})",
                m.name(),
                verbs.join(", ")
            );
        }
        Ok((m, s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Dummy;
    impl Module for Dummy {
        fn name(&self) -> &'static str {
            "dummy"
        }
        fn description(&self) -> &'static str {
            ""
        }
        fn schemes(&self) -> &'static [SchemeInfo] {
            &[SchemeInfo {
                scheme: "dummy",
                default_port: 1,
                tls: false,
            }]
        }
        fn verbs(&self) -> &'static [Verb] {
            &[Verb::Ping]
        }
        fn connect(&self, _: &Endpoint, _: &Context) -> Result<Box<dyn Session>> {
            unreachable!()
        }
    }

    #[test]
    fn lookup_and_verb_support() {
        let mut r = Registry::new();
        r.register(Box::new(Dummy));
        let ep = Endpoint::parse("DUMMY://h").unwrap();
        assert_eq!(r.for_verb(&ep, Verb::Ping).unwrap().1.default_port, 1);
        let e = r.for_verb(&ep, Verb::Send).err().unwrap();
        assert!(e.to_string().contains("does not support 'send'"));
        assert_eq!(crate::exit_code(&e), 1);
        assert!(r.lookup("amq").is_err());
    }

    #[test]
    #[should_panic(expected = "registered by both")]
    fn duplicate_scheme_panics() {
        let mut r = Registry::new();
        r.register(Box::new(Dummy));
        r.register(Box::new(Dummy));
    }
}
