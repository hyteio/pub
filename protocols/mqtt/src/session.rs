//! The MQTT 5 conversation.
//!
//! - CONNECT/CONNACK, honouring the broker's limits (maximum QoS, retain
//!   availability, maximum packet size, server keep-alive).
//! - send: PUBLISH at QoS 0 (fire and forget), 1 (PUBACK) or 2 (PUBREC/PUBREL/PUBCOMP).
//! - receive: SUBSCRIBE; each message is handed to the sink *before* it is
//!   acknowledged (at-least-once), then PUBACK / PUBREC+PUBCOMP.
//! - Keep-alive: while waiting, a PINGREQ is sent when the connection has been
//!   quiet for 3/4 of the keep-alive interval; no PINGRESP within the interval
//!   means the broker is gone.
//!
//! - Will messages (`will-*`): in CONNECT; the broker publishes the will if
//!   the connection ends without a normal DISCONNECT (or, MQTT 5, with reason
//!   0x04 when `will-on-disconnect` is set).
//! - Shared subscriptions: `share-group` subscribes to `$share/GROUP/filter`.
//! - Topic aliases (MQTT 5): repeated topics are sent as aliases up to the
//!   broker's Topic Alias Maximum; pub accepts `topic-alias-maximum` aliases
//!   from the broker and resolves them before handing messages on.

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

use anyhow::Context as _;
use pub_core::error::ErrorKind;
use pub_core::{
    bail_kind, err, Codec, Connection, Context, Destination, Endpoint, Message, ReceiveOptions,
    Result, Session,
};

use crate::codec::{
    describe_reason, prop, Ack, ConnAck, Connect, MqttCodec, Packet, PropValue, Props, Publish,
    ReasonProps, Subscribe, Unsubscribe, Will,
};
use crate::codec::{PROTOCOL_LEVEL_311, PROTOCOL_LEVEL_5};
use crate::mapping;

const DEFAULT_KEEP_ALIVE: Duration = Duration::from_secs(60);
/// Waiting for the last QoS 2 handshakes or a clean DISCONNECT is a courtesy.
const MAX_WIND_DOWN: Duration = Duration::from_secs(2);

pub struct MqttSession {
    /// Protocol level in use: 5 (MQTT 5.0) or 4 (MQTT 3.1.1).
    level: u8,
    conn: Connection<MqttCodec>,
    connack: ConnAck,
    qos: u8,
    retain: bool,
    retain_handling: u8,
    /// Effective keep-alive (the broker may override ours); `None` = disabled.
    keep_alive: Option<Duration>,
    last_sent: Instant,
    ping_sent: Option<Instant>,
    next_id: u16,
    closed: bool,
    /// The broker keeps the session after we disconnect (session-expiry > 0).
    persistent: bool,
    /// Messages that arrived while waiting for something else (e.g. a resumed
    /// session's queue arriving before SUBACK); delivered before reading more.
    pending: VecDeque<Packet>,
    /// DISCONNECT with reason 0x04 so the broker publishes the will anyway.
    will_on_disconnect: bool,
    share_group: Option<String>,
    /// Topic aliases we send (topic -> alias), up to the broker's maximum.
    aliases_out: HashMap<String, u16>,
    alias_max_out: u16,
    /// Topic aliases the broker sends (alias -> topic), up to ours.
    aliases_in: HashMap<u16, String>,
    alias_max_in: u16,
    /// The client id in use (ours, or the one the broker assigned).
    client_id: String,
}

fn choice(ep: &Endpoint, key: &str, default: u8) -> Result<u8> {
    match ep.prop(key) {
        None => Ok(default),
        Some(v) => v
            .trim()
            .parse::<u8>()
            .ok()
            .filter(|n| *n <= 2)
            .ok_or_else(|| err!(Config, "{key} must be 0, 1 or 2, got '{v}'")),
    }
}

fn seconds(ep: &Endpoint, key: &str, default: Duration, max: u64) -> Result<u64> {
    let secs = ep
        .prop_duration(key)?
        .unwrap_or(default)
        .as_secs_f64()
        .ceil() as u64;
    if secs > max {
        bail_kind!(Config, "{key} must be at most {max} seconds");
    }
    Ok(secs)
}

/// The protocol level selected by the `version` setting.
pub fn protocol_level(ep: &Endpoint) -> Result<u8> {
    match ep.prop("version").map(str::trim) {
        None | Some("5") => Ok(PROTOCOL_LEVEL_5),
        Some("3.1.1") => Ok(PROTOCOL_LEVEL_311),
        Some(v) => bail_kind!(Config, "version must be 5 or 3.1.1, got '{v}'"),
    }
}

/// MQTT 3.1.1 CONNACK return codes (there are no MQTT 5 reason codes or properties).
fn connack_error_311(code: u8) -> anyhow::Error {
    let (kind_auth, text) = match code {
        1 => (
            false,
            "unacceptable protocol version (does the broker support MQTT 3.1.1?)",
        ),
        2 => (false, "client identifier rejected"),
        3 => (false, "server unavailable"),
        4 => (true, "bad user name or password"),
        5 => (true, "not authorized"),
        _ => (false, "unknown return code"),
    };
    let what = format!("connection refused by broker: {text} ({code})");
    match (kind_auth, code) {
        (true, _) => err!(Auth, "{what}"),
        (false, 3) => err!(Connect, "{what}"),
        _ => err!(Protocol, "{what}"),
    }
}

/// CONNACK failures: credentials -> Auth (67), "go elsewhere" -> Connect (7), else Protocol (8).
fn connack_error(code: u8, props: &Props) -> anyhow::Error {
    let what = format!(
        "connection refused by broker: {}",
        describe_reason(code, props)
    );
    match code {
        0x86 | 0x87 | 0x8A | 0x8C => err!(Auth, "{what}"),
        0x88 | 0x89 | 0x9C | 0x9D | 0x9F => err!(Connect, "{what}"),
        _ => err!(Protocol, "{what}"),
    }
}

impl MqttSession {
    pub fn connect(ep: &Endpoint, ctx: &Context) -> Result<Self> {
        let mut trace = ctx.trace.clone();
        if let Some(p) = &ep.password {
            trace = trace.redacting(p);
        }
        let qos = choice(ep, "qos", 1)?;
        let retain_handling = choice(ep, "retain-handling", 0)?;
        let retain = ep.prop_bool("retain", false)?;
        let clean_start = ep.prop_bool("clean-start", true)?;
        let session_expiry = seconds(ep, "session-expiry", Duration::ZERO, u32::MAX as u64)? as u32;
        let keep_alive = seconds(ep, "keep-alive", DEFAULT_KEEP_ALIVE, u16::MAX as u64)? as u16;
        let client_id = ep.prop("client-id").unwrap_or_default().to_string();
        let level = protocol_level(ep)?;
        let v5 = level == PROTOCOL_LEVEL_5;
        if !v5 && session_expiry > 0 {
            bail_kind!(
                Unsupported,
                "session-expiry needs MQTT 5; with 3.1.1, clean-start = false keeps the session until a clean connect"
            );
        }
        if !v5 && retain_handling != 0 {
            bail_kind!(Unsupported, "retain-handling needs MQTT 5");
        }
        if !clean_start && client_id.is_empty() {
            bail_kind!(
                Config,
                "clean-start = false resumes a session, which needs a client-id"
            );
        }

        let will = will(ep, v5)?;
        let will_on_disconnect = ep.prop_bool("will-on-disconnect", false)?;
        if will_on_disconnect && !v5 {
            bail_kind!(Unsupported, "will-on-disconnect needs MQTT 5");
        }
        if will_on_disconnect && will.is_none() {
            bail_kind!(Usage, "will-on-disconnect needs a will (will-topic)");
        }
        let share_group = match ep.prop("share-group").filter(|g| !g.is_empty()) {
            Some(g) if g.contains(['/', '+', '#']) => {
                bail_kind!(Usage, "share-group '{g}' may not contain '/', '+' or '#'")
            }
            g => g.map(str::to_string),
        };
        let use_aliases = v5 && ep.prop_bool("topic-aliases", true)?;
        let alias_max_in = match ep.prop("topic-alias-maximum") {
            None => 16,
            // Each alias can pin a topic of up to 64 KiB.
            Some(v) => v
                .trim()
                .parse::<u16>()
                .ok()
                .filter(|&n| n <= 1024)
                .ok_or_else(|| err!(Config, "topic-alias-maximum must be 0-1024, got '{v}'"))?,
        };

        let mut conn = Connection::open(ep, &ctx.scheme, trace, MqttCodec::default())?;
        conn.codec.max_frame = conn.limits().max_frame_size;
        conn.codec.version = level;

        let mut props = Props::default();
        if v5 && session_expiry > 0 {
            props.push(
                prop::SESSION_EXPIRY_INTERVAL,
                PropValue::U32(session_expiry),
            );
        }
        // Tell the broker the largest packet we accept, so it doesn't send one we'd refuse.
        if v5 {
            let max_packet = conn.limits().max_frame_size.min(u32::MAX as usize) as u32;
            props.push(prop::MAXIMUM_PACKET_SIZE, PropValue::U32(max_packet));
            if alias_max_in > 0 {
                props.push(prop::TOPIC_ALIAS_MAXIMUM, PropValue::U16(alias_max_in));
            }
        }

        let connect = Packet::Connect(Connect {
            protocol_level: level,
            clean_start,
            keep_alive,
            props,
            client_id,
            username: ep.username.clone(),
            password: ep.password.as_ref().map(|p| p.as_bytes().to_vec()),
            will,
        });
        conn.send(&connect)?;
        let reply = conn
            .recv_reply("CONNACK")
            .map_err(|e| match pub_core::error::kind_of(&e) {
                // Brokers that only know 3.1.1 hang up on an MQTT 5 CONNECT without a word.
                Some(ErrorKind::Io) if v5 => err!(
                    Io,
                    "{e}: the broker may not speak MQTT 5 (ActiveMQ 5.x and 6.x speak 3.1.1); \
                     try -O version=3.1.1"
                ),
                _ => e,
            })
            .context("waiting for CONNACK")?;
        let connack = match reply {
            Packet::ConnAck(c) if !v5 && c.reason != 0 => return Err(connack_error_311(c.reason)),
            Packet::ConnAck(c) if c.reason >= 0x80 => {
                return Err(connack_error(c.reason, &c.props))
            }
            Packet::ConnAck(c) => c,
            Packet::Disconnect(r) => return Err(connack_error(r.reason, &r.props)),
            other => bail_kind!(Protocol, "expected CONNACK, got {}", other.name()),
        };

        let keep_alive = connack
            .props
            .num(prop::SERVER_KEEP_ALIVE)
            .unwrap_or(keep_alive as u32);
        let mut session = MqttSession {
            level,
            conn,
            qos,
            retain,
            retain_handling,
            keep_alive: (keep_alive > 0).then(|| Duration::from_secs(keep_alive as u64)),
            last_sent: Instant::now(),
            ping_sent: None,
            // A resumed session may still hold our earlier packet ids (a QoS 2
            // send interrupted before PUBREL): don't start where the last run did.
            next_id: if clean_start { 0 } else { packet_id_offset() },
            // 3.1.1: a non-clean session lasts until the next clean connect.
            persistent: if v5 { session_expiry > 0 } else { !clean_start },
            pending: VecDeque::new(),
            closed: false,
            will_on_disconnect,
            share_group,
            aliases_out: HashMap::new(),
            alias_max_out: if use_aliases {
                connack.props.num(prop::TOPIC_ALIAS_MAXIMUM).unwrap_or(0) as u16
            } else {
                0
            },
            aliases_in: HashMap::new(),
            alias_max_in: if v5 { alias_max_in } else { 0 },
            client_id: String::new(),
            connack,
        };
        session.client_id = session
            .connack
            .props
            .str(prop::ASSIGNED_CLIENT_IDENTIFIER)
            .unwrap_or(ep.prop("client-id").unwrap_or("-"))
            .to_string();
        let id = &session.client_id;
        session.conn.trace().info(format_args!(
            "MQTT {} session established, client id {id}, keep-alive {keep_alive}s",
            if v5 { "5" } else { "3.1.1" }
        ));
        Ok(session)
    }

    fn send_packet(&mut self, p: &Packet) -> Result<()> {
        self.conn.send(p)?;
        self.last_sent = Instant::now();
        Ok(())
    }

    fn next_packet_id(&mut self) -> u16 {
        self.next_id = self.next_id.checked_add(1).unwrap_or(1);
        self.next_id
    }

    /// The topic (or nothing) and alias to publish `topic` with.
    fn alias_for(&mut self, topic: &str) -> (String, Option<u16>) {
        if let Some(&a) = self.aliases_out.get(topic) {
            return (String::new(), Some(a));
        }
        if (self.aliases_out.len() as u16) < self.alias_max_out {
            let a = self.aliases_out.len() as u16 + 1;
            self.aliases_out.insert(topic.to_string(), a);
            return (topic.to_string(), Some(a));
        }
        (topic.to_string(), None)
    }

    /// A PUBLISH from the broker with its topic alias resolved (and removed).
    fn resolve_alias(&mut self, mut p: Publish) -> Result<Publish> {
        let Some(alias) = p.props.num(prop::TOPIC_ALIAS) else {
            return Ok(p);
        };
        if alias == 0 || alias > self.alias_max_in as u32 {
            bail_kind!(
                Protocol,
                "the broker used topic alias {alias}; pub allows 1-{} (topic-alias-maximum)",
                self.alias_max_in
            );
        }
        let alias = alias as u16;
        if p.topic.is_empty() {
            p.topic = self.aliases_in.get(&alias).cloned().ok_or_else(|| {
                err!(
                    Protocol,
                    "the broker used topic alias {alias} before setting it"
                )
            })?;
        } else {
            self.aliases_in.insert(alias, p.topic.clone());
        }
        p.props.0.retain(|(id, _)| *id != prop::TOPIC_ALIAS);
        Ok(p)
    }

    /// A PUBLISH from the broker must name a topic (directly or by alias),
    /// and a topic, not a filter.
    fn check_incoming_topic(p: &Publish) -> Result<()> {
        if p.topic.is_empty() {
            bail_kind!(Protocol, "the broker sent a message without a topic");
        }
        if p.topic.contains(['+', '#']) {
            bail_kind!(
                Protocol,
                "the broker sent a message to a wildcard topic '{}'",
                p.topic
            );
        }
        Ok(())
    }

    fn max_qos(&self) -> u8 {
        self.connack
            .props
            .num(prop::MAXIMUM_QOS)
            .map_or(2, |q| q as u8)
    }

    /// The next packet before `deadline`, keeping the connection alive meanwhile.
    /// PINGRESP is consumed here; a DISCONNECT from the broker is an error.
    fn recv_until(&mut self, deadline: Option<Instant>) -> Result<Option<Packet>> {
        if let Some(p) = self.pending.pop_front() {
            return Ok(Some(p));
        }
        self.recv_wire(deadline)
    }

    /// Like [`recv_until`](Self::recv_until) but straight from the connection,
    /// ignoring `pending` — for waits that set packets aside into `pending`.
    fn recv_wire(&mut self, deadline: Option<Instant>) -> Result<Option<Packet>> {
        loop {
            let ping_at = self.keep_alive.map(|k| self.last_sent + k * 3 / 4);
            let pong_by = self.ping_sent.zip(self.keep_alive).map(|(s, k)| s + k);
            let wake = [
                deadline,
                ping_at.filter(|_| self.ping_sent.is_none()),
                pong_by,
            ]
            .into_iter()
            .flatten()
            .min();

            match self.conn.recv_until(wake)? {
                Some(Packet::PingResp) => self.ping_sent = None,
                Some(Packet::Disconnect(r)) => {
                    bail_kind!(
                        Protocol,
                        "broker disconnected: {}",
                        describe_reason(r.reason, &r.props)
                    )
                }
                Some(Packet::Auth(_)) => bail_kind!(
                    Unsupported,
                    "broker requested enhanced authentication (AUTH), which is not supported"
                ),
                Some(Packet::Publish(p)) => {
                    let p = self.resolve_alias(p)?;
                    Self::check_incoming_topic(&p)?;
                    return Ok(Some(Packet::Publish(p)));
                }
                Some(p) => return Ok(Some(p)),
                None => {
                    // Checked only after reading, so a PINGRESP that arrived
                    // while we were busy still counts.
                    let now = Instant::now();
                    if let (Some(sent), Some(k)) = (self.ping_sent, self.keep_alive) {
                        if now >= sent + k {
                            bail_kind!(
                                Timeout,
                                "broker did not answer PINGREQ within the keep-alive ({}s)",
                                k.as_secs()
                            );
                        }
                    }
                    if deadline.is_some_and(|d| now >= d) {
                        return Ok(None);
                    }
                    if self.ping_sent.is_none() && ping_at.is_some_and(|t| now >= t) {
                        self.send_packet(&Packet::PingReq)?;
                        self.ping_sent = Some(Instant::now());
                    }
                }
            }
        }
    }

    /// Wait (up to receive-timeout) for the acknowledgement `want` of packet `id`.
    fn await_ack(&mut self, want: &str, id: u16) -> Result<Ack> {
        let deadline = self.conn.reply_deadline();
        loop {
            let Some(p) = self.recv_wire(deadline)? else {
                bail_kind!(Timeout, "no {want} for packet {id} within receive-timeout");
            };
            let ack = match (&p, want) {
                (Packet::PubAck(a), "PUBACK")
                | (Packet::PubRec(a), "PUBREC")
                | (Packet::PubComp(a), "PUBCOMP") => a,
                _ => continue,
            };
            if ack.packet_id != id {
                continue;
            }
            if ack.reason >= 0x80 {
                let what = format!(
                    "broker rejected the message: {}",
                    describe_reason(ack.reason, &ack.props)
                );
                // "Not authorized" is an authorization failure, like a refused subscription.
                return Err(if ack.reason == 0x87 {
                    err!(Auth, "{what}")
                } else {
                    err!(Protocol, "{what}")
                });
            }
            if ack.reason == 0x10 {
                self.conn.trace().info("broker: no matching subscribers");
            }
            return Ok(ack.clone());
        }
    }

    fn sub_ack(&mut self, id: u16, unsub: bool) -> Result<Vec<u8>> {
        let deadline = self.conn.reply_deadline();
        let want = if unsub { "UNSUBACK" } else { "SUBACK" };
        loop {
            match self.recv_wire(deadline)? {
                None => bail_kind!(Timeout, "no {want} within receive-timeout"),
                Some(Packet::SubAck(a)) if !unsub && a.packet_id == id => return Ok(a.reasons),
                Some(Packet::UnsubAck(a)) if unsub && a.packet_id == id => return Ok(a.reasons),
                // Deliveries that raced the acknowledgement: keep them for receive().
                Some(p @ (Packet::Publish(_) | Packet::PubRel(_))) => {
                    pub_core::limits::check_held(self.pending.len())?;
                    self.pending.push_back(p)
                }
                Some(_) => {}
            }
        }
    }
}

/// A starting point for packet ids that differs from run to run.
fn packet_id_offset() -> u16 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    ((nanos ^ std::process::id()) % u16::MAX as u32) as u16
}

/// The will message from the `will-*` settings, if `will-topic` is set.
fn will(ep: &Endpoint, v5: bool) -> Result<Option<Will>> {
    let delay = seconds(ep, "will-delay", Duration::ZERO, u32::MAX as u64)? as u32;
    let Some(topic) = ep.prop("will-topic").filter(|t| !t.is_empty()) else {
        for key in ["will-message", "will-qos", "will-retain", "will-delay"] {
            if ep.prop(key).is_some_and(|m| !m.is_empty()) {
                bail_kind!(Usage, "{key} needs a will-topic");
            }
        }
        return Ok(None);
    };
    check_topic_name(topic).map_err(|e| e.context("will-topic"))?;
    if delay > 0 && !v5 {
        bail_kind!(Unsupported, "will-delay needs MQTT 5");
    }
    let mut props = Props::default();
    if v5 {
        // The body comes from a setting, so it is text.
        props.push(prop::PAYLOAD_FORMAT_INDICATOR, PropValue::Byte(1));
        if delay > 0 {
            props.push(prop::WILL_DELAY_INTERVAL, PropValue::U32(delay));
        }
    }
    Ok(Some(Will {
        qos: choice(ep, "will-qos", 0)?,
        retain: ep.prop_bool("will-retain", false)?,
        props,
        topic: topic.to_string(),
        payload: ep
            .prop("will-message")
            .unwrap_or_default()
            .as_bytes()
            .to_vec(),
    }))
}

/// A shared subscription filter must be `$share/GROUP/FILTER`.
fn check_shared(filter: &str) -> Result<()> {
    let Some(rest) = filter.strip_prefix("$share/") else {
        return Ok(());
    };
    match rest.split_once('/') {
        Some((group, f)) if !group.is_empty() && !group.contains(['+', '#']) && !f.is_empty() => {
            Ok(())
        }
        _ => bail_kind!(
            Usage,
            "a shared subscription is $share/GROUP/FILTER; '{filter}' isn't one"
        ),
    }
}

/// MQTT 4.7.1: `#` only as the whole last level, `+` only as a whole level.
fn check_topic_filter(filter: &str) -> Result<()> {
    if filter.is_empty() {
        bail_kind!(Usage, "MQTT topic filter is empty");
    }
    let levels: Vec<&str> = filter.split('/').collect();
    for (i, level) in levels.iter().enumerate() {
        let bad = (level.contains('#') && (*level != "#" || i + 1 != levels.len()))
            || (level.contains('+') && *level != "+");
        if bad {
            bail_kind!(
                Usage,
                "'{filter}' isn't a valid MQTT topic filter: '#' must be a whole last level and '+' a \
                 whole level (in a URL, put settings before the '#' or use -O)"
            );
        }
    }
    Ok(())
}

fn check_topic_name(topic: &str) -> Result<()> {
    if topic.is_empty() {
        bail_kind!(Usage, "MQTT topic is empty");
    }
    if topic.contains(['+', '#']) {
        bail_kind!(
            Usage,
            "wildcards (+, #) are for subscribing; '{topic}' is not a topic you can publish to"
        );
    }
    Ok(())
}

impl Session for MqttSession {
    fn info(&self) -> Vec<(String, String)> {
        let p = &self.connack.props;
        let version = if self.level == PROTOCOL_LEVEL_5 {
            "5"
        } else {
            "3.1.1"
        };
        let mut info = vec![("version".to_string(), version.to_string())];
        let mut add = |k: &str, v: Option<String>| {
            if let Some(v) = v {
                info.push((k.to_string(), v));
            }
        };
        add(
            "assigned-client-id",
            p.str(prop::ASSIGNED_CLIENT_IDENTIFIER).map(str::to_string),
        );
        add(
            "session-present",
            Some(self.connack.session_present.to_string()),
        );
        add(
            "keep-alive",
            Some(
                self.keep_alive
                    .map_or("off".into(), |k| format!("{}s", k.as_secs())),
            ),
        );
        add("maximum-qos", Some(self.max_qos().to_string()));
        add(
            "retain-available",
            Some((p.num(prop::RETAIN_AVAILABLE) != Some(0)).to_string()),
        );
        add(
            "maximum-packet-size",
            p.num(prop::MAXIMUM_PACKET_SIZE).map(|n| n.to_string()),
        );
        add(
            "receive-maximum",
            p.num(prop::RECEIVE_MAXIMUM).map(|n| n.to_string()),
        );
        add(
            "topic-alias-maximum",
            p.num(prop::TOPIC_ALIAS_MAXIMUM).map(|n| n.to_string()),
        );
        add(
            "wildcard-subscriptions",
            Some((p.num(prop::WILDCARD_SUBSCRIPTION_AVAILABLE) != Some(0)).to_string()),
        );
        add(
            "shared-subscriptions",
            Some((p.num(prop::SHARED_SUBSCRIPTION_AVAILABLE) != Some(0)).to_string()),
        );
        add(
            "response-information",
            p.str(prop::RESPONSE_INFORMATION).map(str::to_string),
        );
        add(
            "server-reference",
            p.str(prop::SERVER_REFERENCE).map(str::to_string),
        );
        add("reason", p.reason_string().map(str::to_string));
        if self.level != PROTOCOL_LEVEL_5 {
            // 3.1.1 brokers advertise nothing; don't present MQTT 5 defaults as facts.
            info.retain(|(k, _)| {
                matches!(k.as_str(), "version" | "session-present" | "keep-alive")
            });
        }
        info
    }

    fn send(&mut self, destination: &Destination, msg: &Message) -> Result<()> {
        let topic = destination.name.as_str();
        check_topic_name(topic)?;
        if self.qos > self.max_qos() {
            bail_kind!(
                Unsupported,
                "broker accepts QoS {} at most; use -O qos={}",
                self.max_qos(),
                self.max_qos()
            );
        }
        if self.retain && self.connack.props.num(prop::RETAIN_AVAILABLE) == Some(0) {
            bail_kind!(Unsupported, "broker does not support retained messages");
        }
        let packet_id = (self.qos > 0).then(|| self.next_packet_id());
        // 3.1.1 has nowhere to put fields or properties; the CLI has already
        // reported them as not carried.
        let mut props = if self.level == PROTOCOL_LEVEL_5 {
            mapping::publish_props(msg)
        } else {
            Props::default()
        };
        let (wire_topic, alias) = self.alias_for(topic);
        let new_alias = alias.is_some() && !wire_topic.is_empty();
        if let Some(a) = alias {
            props.push(prop::TOPIC_ALIAS, PropValue::U16(a));
        }
        let publish = Packet::Publish(Publish {
            dup: false,
            qos: self.qos,
            retain: self.retain,
            topic: wire_topic,
            packet_id,
            props,
            payload: msg.body.clone(),
        });
        // 0 is not a valid Maximum Packet Size; treat it as absent.
        if let Some(max) = self
            .connack
            .props
            .num(prop::MAXIMUM_PACKET_SIZE)
            .filter(|&m| m > 0)
        {
            let mut encoded = Vec::new();
            MqttCodec::default().encode(&publish, &mut encoded)?;
            if encoded.len() > max as usize {
                // The broker never sees this PUBLISH, so it doesn't learn a new alias.
                if new_alias {
                    self.aliases_out.remove(topic);
                }
                bail_kind!(
                    Usage,
                    "message is {} bytes as an MQTT packet; the broker accepts at most {max}",
                    encoded.len()
                );
            }
        }
        self.send_packet(&publish)?;
        match (self.qos, packet_id) {
            (1, Some(id)) => {
                self.await_ack("PUBACK", id)?;
            }
            (2, Some(id)) => {
                self.await_ack("PUBREC", id)?;
                self.send_packet(&Packet::PubRel(Ack::new(id)))?;
                self.await_ack("PUBCOMP", id)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn receive(
        &mut self,
        destination: &Destination,
        opts: &ReceiveOptions,
        sink: &mut dyn FnMut(Message) -> Result<()>,
    ) -> Result<usize> {
        if opts.selector.is_some() {
            bail_kind!(
                Unsupported,
                "MQTT has no message selectors; subscribe to a narrower topic filter instead"
            );
        }
        if destination.name.is_empty() {
            bail_kind!(Usage, "MQTT topic filter is empty");
        }
        let filter = match &self.share_group {
            Some(_) if destination.name.starts_with("$share/") => bail_kind!(
                Usage,
                "share-group is set and the topic filter is already shared ({})",
                destination.name
            ),
            Some(g) => format!("$share/{g}/{}", destination.name),
            None => destination.name.clone(),
        };
        check_shared(&filter)?;
        check_topic_filter(
            filter
                .strip_prefix("$share/")
                .map_or(&filter, |r| r.split_once('/').map_or(r, |(_, f)| f)),
        )?;
        if filter.starts_with("$share/")
            && self.connack.props.num(prop::SHARED_SUBSCRIPTION_AVAILABLE) == Some(0)
        {
            bail_kind!(Unsupported, "this broker has no shared subscriptions");
        }
        // --no-ack: ask for QoS 0, where nothing is acknowledged.
        let qos = if opts.ack {
            self.qos.min(self.max_qos())
        } else {
            0
        };
        if self.level != PROTOCOL_LEVEL_5 && !opts.headers.is_empty() {
            bail_kind!(
                Unsupported,
                "subscription headers (MQTT user properties) need MQTT 5"
            );
        }
        let mut props = Props::default();
        for (k, v) in &opts.headers {
            props.push(prop::USER_PROPERTY, PropValue::Pair(k.clone(), v.clone()));
        }
        let id = self.next_packet_id();
        let options = qos | (self.retain_handling << 4);
        self.send_packet(&Packet::Subscribe(Subscribe {
            packet_id: id,
            props,
            filters: vec![(filter.clone(), options)],
        }))?;
        let granted = self.sub_ack(id, false)?;
        match granted.first() {
            Some(&code) if code >= 0x80 && self.level != PROTOCOL_LEVEL_5 => {
                // 3.1.1 has a single failure code; brokers use it for ACL denials too.
                bail_kind!(Protocol, "subscription to '{filter}' refused by the broker (0x80 Failure; often: not authorized)");
            }
            Some(&code) if code >= 0x80 => {
                let what = format!(
                    "subscription to '{filter}' refused: {}",
                    describe_reason(code, &Props::default())
                );
                return Err(if code == 0x87 {
                    err!(Auth, "{what}")
                } else {
                    err!(Protocol, "{what}")
                });
            }
            Some(&g) => self
                .conn
                .trace()
                .info(format_args!("subscribed to {filter} with QoS {g}")),
            None => bail_kind!(Protocol, "SUBACK without a reason code"),
        }

        let deadline = opts.timeout.map(|t| Instant::now() + t);
        let mut received = 0;
        let mut awaiting_rel: HashSet<u16> = HashSet::new();
        let mut warned_other_subscription = false;
        while opts.count.is_none_or(|c| received < c) {
            match self.recv_until(deadline)? {
                None => break,
                Some(Packet::Publish(p)) => {
                    let (qos, packet_id) = (p.qos, p.packet_id);
                    // A QoS 2 message resent before its PUBREL: already handed on.
                    if let (2, Some(pid)) = (qos, packet_id) {
                        if awaiting_rel.contains(&pid) {
                            self.send_packet(&Packet::PubRec(Ack::new(pid)))?;
                            continue;
                        }
                    }
                    // A persistent session keeps every subscription it ever made;
                    // the broker delivers for all of them.
                    if !warned_other_subscription && !mapping::topic_matches(&filter, &p.topic) {
                        warned_other_subscription = true;
                        self.conn.trace().warn(format_args!(
                            "a message on '{}' comes from another subscription in session '{}' \
                             (not '{filter}'): use one --durable name per topic filter, or reset the \
                             session with `pub unsub --durable {}`",
                            p.topic, self.client_id, self.client_id
                        ));
                    }
                    // Hand over first, acknowledge after: at-least-once.
                    sink(mapping::message_from_publish(p))?;
                    received += 1;
                    match (qos, packet_id) {
                        (1, Some(pid)) => self.send_packet(&Packet::PubAck(Ack::new(pid)))?,
                        (2, Some(pid)) => {
                            self.send_packet(&Packet::PubRec(Ack::new(pid)))?;
                            awaiting_rel.insert(pid);
                        }
                        _ => {}
                    }
                }
                Some(Packet::PubRel(a)) => {
                    self.send_packet(&Packet::PubComp(Ack::new(a.packet_id)))?;
                    awaiting_rel.remove(&a.packet_id);
                }
                Some(_) => {}
            }
        }

        // Finish QoS 2 handshakes still in flight, briefly.
        let cap = Instant::now() + MAX_WIND_DOWN;
        let until = self.conn.reply_deadline().map_or(cap, |d| d.min(cap));
        while !awaiting_rel.is_empty() {
            match self.recv_until(Some(until))? {
                Some(Packet::PubRel(a)) => {
                    self.send_packet(&Packet::PubComp(Ack::new(a.packet_id)))?;
                    awaiting_rel.remove(&a.packet_id);
                }
                Some(_) => {}
                None => break,
            }
        }

        // A persistent session keeps its subscription, so the broker queues
        // messages for the next `receive` with the same client-id.
        if !self.persistent {
            let unsub_id = self.next_packet_id();
            self.send_packet(&Packet::Unsubscribe(Unsubscribe {
                packet_id: unsub_id,
                props: Props::default(),
                filters: vec![filter],
            }))?;
        }
        Ok(received)
    }

    /// The clean connect (see `MqttModule::prepare_durable`) already
    /// discarded the session; MQTT doesn't say whether there was one.
    fn unsubscribe(&mut self, _destination: &Destination, name: &str) -> Result<()> {
        if self.persistent {
            bail_kind!(
                Usage,
                "unsubscribing needs a clean connect (clean-start = true)"
            );
        }
        self.conn
            .trace()
            .info(format_args!("session {name} discarded"));
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        // Normal disconnection (or 0x04: publish the will anyway); nothing comes back.
        let reason = if self.will_on_disconnect { 0x04 } else { 0 };
        let _ = self.conn.send(&Packet::Disconnect(ReasonProps {
            reason,
            props: Props::default(),
        }));
        self.conn.shutdown();
        Ok(())
    }
}

impl Drop for MqttSession {
    fn drop(&mut self) {
        if !self.closed {
            self.conn.shutdown();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::check_topic_filter;

    #[test]
    fn topic_filters_follow_the_wildcard_rules() {
        for ok in ["#", "a/#", "a/+/c", "+", "+/+", "a/b", "/#", "a//b"] {
            assert!(check_topic_filter(ok).is_ok(), "{ok}");
        }
        for bad in ["", "a/#/c", "a#", "a/#?version=3.1.1", "a/b+", "+a/b", "##"] {
            assert!(check_topic_filter(bad).is_err(), "{bad}");
        }
    }
}
