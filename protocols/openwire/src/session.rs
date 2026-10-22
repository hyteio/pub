//! The OpenWire conversation (connection, session, keep-alive, close).
//!
//! Mirrors the Java client (`ActiveMQConnection`, `InactivityMonitor`):
//! 1. send our WireFormatInfo immediately (loose) — never wait for the broker's;
//!    Artemis's multi-protocol acceptor stays silent until the client speaks;
//! 2. read the broker's, negotiate (version = min, tight/stack traces = AND),
//!    require version 12, switch the codec to the negotiated encoding;
//! 3. ConnectionInfo, synchronously (bounded by `jms.connectResponseTimeout`,
//!    else `receive-timeout`); SessionInfo asynchronously;
//! 4. while waiting: send KeepAliveInfo when nothing was written for a third of
//!    the negotiated inactivity period, answer keep-alives that ask for a
//!    response, and fail if the broker sends nothing for the whole period;
//! 5. send: one producer per destination (ProducerInfo, sync); each message
//!    sync or async by Java's rules, async ones within the producer window
//!    (released by the broker's ProducerAck);
//! 6. receive: ConsumerInfo (sync) with the prefetch following `-n` unless set
//!    explicitly; MessagePull when the prefetch is 0; each message acknowledged
//!    after it was handed on (before, with `--no-ack`); expired ones dropped
//!    with an expired ack; RemoveInfo(consumer) carries the last delivered
//!    sequence id so the broker doesn't count undelivered prefetched messages
//!    as redelivered;
//! 7. close: RemoveInfo(producers, session) async, RemoveInfo(connection) sync
//!    (bounded by `jms.closeTimeout`), ShutdownInfo async. After asynchronous
//!    sends that round trip is required, since it is the only sign the broker
//!    got them.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Context as _;
use pub_core::module::{Destination, DestinationKinds, ReceiveOptions};
use pub_core::{bail_kind, err, Codec, Connection, Context, Endpoint, Message, Result, Session};

use crate::codec::OpenWireCodec;
use crate::consumer::{self, ConsumerSettings};
use crate::primitive::{decode_map, encode_map, Primitive};
use crate::producer::{self, destination_text, SendOptions};
use crate::types::*;
use crate::wire::Encoding;

pub const VERSION: i32 = 12;

/// What both sides agreed on.
#[derive(Debug, Clone, PartialEq)]
pub struct Negotiated {
    pub version: i32,
    pub tight: bool,
    pub stack_traces: bool,
    /// `None` when either side disabled the inactivity monitor (0).
    pub max_inactivity: Option<Duration>,
    pub initial_delay: Duration,
    /// Largest frame the broker accepts, when it advertises one.
    pub broker_max_frame: Option<u64>,
}

/// pub's side of the negotiation, from settings.
#[derive(Debug, Clone, PartialEq)]
pub struct WireOptions {
    pub tight: bool,
    pub stack_traces: bool,
    pub tcp_no_delay: bool,
    pub max_inactivity_ms: i64,
    pub initial_delay_ms: i64,
    pub max_frame: i64,
}

fn ms_setting(ep: &Endpoint, key: &str, default: i64) -> Result<i64> {
    match ep.prop(key) {
        None => Ok(default),
        Some(v) => v
            .trim()
            .parse::<i64>()
            .ok()
            .filter(|n| *n >= 0 && *n as u128 <= pub_core::endpoint::MAX_DURATION.as_millis())
            .ok_or_else(|| {
                err!(
                    Config,
                    "{key} must be a whole number of milliseconds (up to 150 years), got '{v}'"
                )
            }),
    }
}

impl WireOptions {
    pub fn from_endpoint(ep: &Endpoint, max_frame: usize) -> Result<Self> {
        Ok(WireOptions {
            tight: ep.prop_bool("wireFormat.tightEncodingEnabled", true)?,
            stack_traces: ep.prop_bool("wireFormat.stackTraceEnabled", false)?,
            tcp_no_delay: ep.prop_bool("tcp-nodelay", true)?,
            max_inactivity_ms: ms_setting(ep, "wireFormat.maxInactivityDuration", 30_000)?,
            initial_delay_ms: ms_setting(
                ep,
                "wireFormat.maxInactivityDurationInitalDelay",
                10_000,
            )?,
            max_frame: max_frame.min(i64::MAX as usize) as i64,
        })
    }

    pub fn wire_format_info(&self) -> WireFormatInfo {
        let props = vec![
            (
                "TightEncodingEnabled".to_string(),
                Primitive::Bool(self.tight),
            ),
            ("CacheEnabled".into(), Primitive::Bool(false)),
            ("CacheSize".into(), Primitive::Int(0)),
            (
                "StackTraceEnabled".into(),
                Primitive::Bool(self.stack_traces),
            ),
            (
                "TcpNoDelayEnabled".into(),
                Primitive::Bool(self.tcp_no_delay),
            ),
            ("SizePrefixDisabled".into(), Primitive::Bool(false)),
            (
                "MaxInactivityDuration".into(),
                Primitive::Long(self.max_inactivity_ms),
            ),
            (
                "MaxInactivityDurationInitalDelay".into(),
                Primitive::Long(self.initial_delay_ms),
            ),
            ("MaxFrameSize".into(), Primitive::Long(self.max_frame)),
            ("MaxFrameSizeEnabled".into(), Primitive::Bool(true)),
            ("ProviderName".into(), Primitive::String("pub".into())),
            (
                "ProviderVersion".into(),
                Primitive::String(env!("CARGO_PKG_VERSION").into()),
            ),
            ("PlatformDetails".into(), Primitive::String("Rust".into())),
        ];
        WireFormatInfo {
            magic: MAGIC,
            version: VERSION,
            // Encoding a fixed, small map cannot fail.
            marshalled_properties: Some(encode_map(&props).expect("wire format options")),
        }
    }
}

fn get<'a>(props: &'a [(String, Primitive)], key: &str) -> Option<&'a Primitive> {
    props.iter().find(|(k, _)| k == key).map(|(_, v)| v)
}

fn get_bool(props: &[(String, Primitive)], key: &str) -> bool {
    get(props, key) == Some(&Primitive::Bool(true))
}

fn get_long(props: &[(String, Primitive)], key: &str) -> Option<i64> {
    match get(props, key) {
        Some(Primitive::Long(n)) => Some(*n),
        Some(Primitive::Int(n)) => Some(*n as i64),
        _ => None,
    }
}

/// Java's `OpenWireFormat.min`: the smaller positive value; a non-positive one is ignored.
fn java_min(a: i64, b: i64) -> i64 {
    if (a < b && a > 0) || b <= 0 {
        a
    } else {
        b
    }
}

/// Negotiate per `OpenWireFormat.renegotiateWireFormat` and `InactivityMonitor`.
pub fn negotiate(
    ours: &WireOptions,
    broker: &WireFormatInfo,
) -> Result<(Negotiated, Vec<(String, Primitive)>)> {
    if broker.magic != MAGIC {
        bail_kind!(
            Protocol,
            "not an OpenWire broker (bad WireFormatInfo magic)"
        );
    }
    let props = match broker.marshalled_properties.as_deref() {
        Some(b) => decode_map(b)?.unwrap_or_default(),
        None => Vec::new(),
    };
    let version = java_min(VERSION as i64, broker.version as i64) as i32;
    if version != VERSION {
        bail_kind!(
            Protocol,
            "broker negotiates OpenWire version {version}; pub supports only version {VERSION} (ActiveMQ 6.x/5.17+, Artemis)"
        );
    }
    // InactivityMonitor uses a plain min: either side's 0 disables it.
    let broker_inactivity = get_long(&props, "MaxInactivityDuration").unwrap_or(0);
    let inactivity = ours.max_inactivity_ms.min(broker_inactivity);
    let negotiated = Negotiated {
        version,
        tight: ours.tight && get_bool(&props, "TightEncodingEnabled"),
        stack_traces: ours.stack_traces && get_bool(&props, "StackTraceEnabled"),
        max_inactivity: (inactivity > 0).then(|| Duration::from_millis(inactivity as u64)),
        initial_delay: Duration::from_millis(ours.initial_delay_ms.max(0) as u64),
        broker_max_frame: get_long(&props, "MaxFrameSize")
            .filter(|n| *n > 0)
            .map(|n| n as u64),
    };
    Ok((negotiated, props))
}

/// Exit code for a broker exception: security problems are authentication
/// failures (67); a bad destination or selector is a usage error (2); the rest
/// are protocol (8).
pub fn broker_error(what: &str, e: Option<&BrokerError>) -> anyhow::Error {
    let class = e
        .and_then(|e| e.class.as_deref())
        .unwrap_or("unknown error");
    let message = e.and_then(|e| e.message.as_deref()).unwrap_or("");
    let short = class.rsplit('.').next().unwrap_or(class);
    let text = if message.is_empty() {
        format!("{what}: {short}")
    } else {
        format!("{what}: {message} ({short})")
    };
    if class.ends_with("SecurityException") || class.ends_with("JMSSecurityException") {
        err!(Auth, "{text}")
    } else if class.ends_with("InvalidDestinationException")
        || class.ends_with("InvalidSelectorException")
        // Artemis's name for a bad selector
        || class.ends_with("InvalidFilterExpressionException")
    {
        err!(Usage, "{text}")
    } else {
        err!(Protocol, "{text}")
    }
}

/// An ID from a prefix, built like Java's `IdGenerator(prefix).generateId()`:
/// `<prefix>-<port>-<start ms>-<instance>:<seq>`. Java's port is one it binds
/// once per JVM to tell processes apart; pub uses its pid. The connection
/// factory makes two generators, client IDs first, and they share a static
/// instance counter, so client IDs are instance 0 and connection IDs instance 1.
/// Each sequence starts at 1.
fn java_id(prefix: &str, instance: u32, sequence: &std::sync::atomic::AtomicU64) -> String {
    use std::sync::OnceLock;
    static START: OnceLock<u128> = OnceLock::new();
    let start = START.get_or_init(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    });
    format!(
        "{prefix}-{}-{start}-{instance}:{}",
        std::process::id(),
        sequence.fetch_add(1, Ordering::Relaxed)
    )
}

/// A client ID from `client-id-prefix` (Java's `clientIDPrefix`).
pub fn generated_client_id(prefix: &str) -> String {
    static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    java_id(prefix, 0, &SEQUENCE)
}

/// A connection ID from `connection-id-prefix` (Java's `connectionIDPrefix`).
pub fn generated_connection_id(prefix: &str) -> String {
    static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    java_id(prefix, 1, &SEQUENCE)
}

/// `ID:pub-<pid>-<millis>-<n>`: pid and start time separate processes, `n`
/// separates connections within one.
fn connection_id_value() -> String {
    static NEXT: AtomicU32 = AtomicU32::new(1);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!(
        "ID:pub-{}-{}-{}",
        std::process::id(),
        now.as_millis(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

/// A producer for one destination, with its window (`producerWindowSize`).
struct Producer {
    id: ProducerId,
    destination: DataStructure,
    sequence: i64,
    /// Bytes allowed in flight unacknowledged; 0 = no flow control.
    window: u64,
    in_flight: u64,
}

pub struct OpenWireSession {
    conn: Connection<OpenWireCodec>,
    pub negotiated: Negotiated,
    broker_props: Vec<(String, Primitive)>,
    broker_info: Option<BrokerInfo>,
    connection_id: String,
    session_id: Option<DataStructure>,
    next_command_id: i32,
    last_sent: Instant,
    last_read: Instant,
    monitor_from: Instant,
    close_timeout: Duration,
    pending: VecDeque<DataStructure>,
    closed: bool,
    send_opts: SendOptions,
    consumer_settings: ConsumerSettings,
    next_consumer: i64,
    /// By pub's destination string (name plus options).
    producers: HashMap<String, Producer>,
    next_producer: i64,
    /// Messages sent without waiting for the broker.
    unconfirmed: u64,
    client_id: String,
    /// Set by client-id / jms.clientID rather than generated.
    client_id_explicit: bool,
    /// The open local transaction (a LocalTransactionId).
    tx: Option<DataStructure>,
    next_tx: i64,
    /// `broker-type`, unless auto.
    broker_type: Option<BrokerType>,
    next_temp: i64,
}

impl OpenWireSession {
    pub fn connect(ep: &Endpoint, ctx: &Context) -> Result<Self> {
        let mut trace = ctx.trace.clone();
        if let Some(p) = &ep.password {
            trace = trace.redacting(p);
        }
        let connect_response_ms = ms_setting(ep, "jms.connectResponseTimeout", 0)?;
        let close_ms = ms_setting(ep, "jms.closeTimeout", 15_000)?;
        let send_opts = SendOptions::from_endpoint(ep)?;
        let consumer_settings = ConsumerSettings::from_endpoint(ep)?;

        let mut conn = Connection::open(ep, &ctx.scheme, trace, OpenWireCodec::default())?;
        let max_frame = conn.limits().max_frame_size;
        conn.codec.max_frame = max_frame;
        let ours = WireOptions::from_endpoint(ep, max_frame)?;

        // 1. Speak first.
        conn.send(&DataStructure::WireFormatInfo(Box::new(
            ours.wire_format_info(),
        )))?;
        // 2. The broker's WireFormatInfo, then negotiate.
        let reply = conn
            .recv_reply("WireFormatInfo")
            .context("waiting for the broker's WireFormatInfo")?;
        let DataStructure::WireFormatInfo(broker_wf) = reply else {
            bail_kind!(
                Protocol,
                "expected WireFormatInfo from the broker, got {}",
                reply.name()
            );
        };
        let (negotiated, broker_props) = negotiate(&ours, &broker_wf)?;
        conn.codec.encoding = Encoding {
            tight: negotiated.tight,
            stack_traces: negotiated.stack_traces,
        };
        conn.trace().info(format_args!(
            "OpenWire {} negotiated: {} encoding, inactivity {}",
            negotiated.version,
            if negotiated.tight { "tight" } else { "loose" },
            negotiated
                .max_inactivity
                .map_or("off".to_string(), |d| format!("{}ms", d.as_millis()))
        ));

        let now = Instant::now();
        let mut session = OpenWireSession {
            monitor_from: now + negotiated.initial_delay,
            conn,
            negotiated,
            broker_props,
            broker_info: None,
            connection_id: match ep.prop("connection-id-prefix") {
                Some(prefix) => generated_connection_id(prefix),
                None => connection_id_value(),
            },
            session_id: None,
            next_command_id: 0,
            last_sent: now,
            last_read: now,
            close_timeout: Duration::from_millis(close_ms as u64),
            pending: VecDeque::new(),
            closed: false,
            send_opts,
            consumer_settings,
            next_consumer: 0,
            producers: HashMap::new(),
            next_producer: 0,
            unconfirmed: 0,
            client_id: String::new(),
            client_id_explicit: ep.prop("client-id").is_some(),
            tx: None,
            next_tx: 0,
            broker_type: match ep
                .prop("broker-type")
                .map(str::to_ascii_lowercase)
                .as_deref()
            {
                Some("activemq" | "classic") => Some(BrokerType::ActiveMq),
                Some("artemis") => Some(BrokerType::Artemis),
                _ => None,
            },
            next_temp: 0,
        };

        // 3. ConnectionInfo (sync), then SessionInfo (async).
        // An explicit client ID wins; else one from the prefix (as Java);
        // else the connection ID.
        let client_id = match (ep.prop("client-id"), ep.prop("client-id-prefix")) {
            (Some(id), _) => id.to_string(),
            (None, Some(prefix)) => generated_client_id(prefix),
            (None, None) => session.connection_id.clone(),
        };
        session.client_id = client_id.clone();
        let id = session.next_id();
        let info = DataStructure::ConnectionInfo(Box::new(ConnectionInfo {
            command_id: id,
            response_required: true,
            connection_id: Some(Box::new(session.connection_id_ds())),
            client_id: Some(client_id.clone()),
            user_name: ep.username.clone(),
            password: ep.password.clone(),
            manageable: true,
            ..Default::default()
        }));
        let deadline = if connect_response_ms > 0 {
            Some(Instant::now() + Duration::from_millis(connect_response_ms as u64))
        } else {
            session.conn.reply_deadline()
        };
        session
            .request(info, id, deadline, "ConnectionInfo")
            .context("connecting")?;

        let session_id = DataStructure::SessionId(Box::new(SessionId {
            connection_id: Some(session.connection_id.clone()),
            value: 1,
        }));
        let id = session.next_id();
        session.send(&DataStructure::SessionInfo(Box::new(SessionInfo {
            command_id: id,
            response_required: false,
            session_id: Some(Box::new(session_id.clone())),
        })))?;
        session.session_id = Some(session_id);
        session.conn.trace().info(format_args!(
            "OpenWire connection {} established",
            session.connection_id
        ));
        Ok(session)
    }

    /// Keep the connection open for `d`, servicing keep-alives; fails if the
    /// broker goes silent. Commands that arrive meanwhile are kept for later.
    pub fn keep_alive_for(&mut self, d: Duration) -> Result<()> {
        let deadline = Instant::now() + d;
        while let Some(ds) = self.recv_until(Some(deadline))? {
            if !matches!(ds, DataStructure::ProducerAck(_)) {
                pub_core::limits::check_held(self.pending.len())?;
                self.pending.push_back(ds);
            }
            if Instant::now() >= deadline {
                break;
            }
        }
        Ok(())
    }

    pub fn connection_id(&self) -> &str {
        &self.connection_id
    }

    fn connection_id_ds(&self) -> DataStructure {
        DataStructure::ConnectionId(Box::new(ConnectionId {
            value: Some(self.connection_id.clone()),
        }))
    }

    fn next_id(&mut self) -> i32 {
        self.next_command_id += 1;
        self.next_command_id
    }

    fn send(&mut self, ds: &DataStructure) -> Result<()> {
        self.conn.send(ds)?;
        self.last_sent = Instant::now();
        Ok(())
    }

    /// The next command before `deadline`, keeping the connection alive and
    /// handling broker housekeeping (keep-alives, BrokerInfo, ConnectionError).
    /// ProducerAcks are applied to the producer window, then returned; callers
    /// other than [`Self::wait_for_window`] skip them.
    fn recv_until(&mut self, deadline: Option<Instant>) -> Result<Option<DataStructure>> {
        if let Some(p) = self.pending.pop_front() {
            return Ok(Some(p));
        }
        loop {
            let write_at = self
                .negotiated
                .max_inactivity
                .map(|p| (self.last_sent + p / 3).max(self.monitor_from));
            let read_by = self
                .negotiated
                .max_inactivity
                .map(|p| (self.last_read + p).max(self.monitor_from));
            let wake = [deadline, write_at, read_by].into_iter().flatten().min();

            match self.conn.recv_until(wake)? {
                Some(ds) => {
                    self.last_read = Instant::now();
                    match ds {
                        DataStructure::KeepAliveInfo(k) => {
                            if k.response_required {
                                let ka = KeepAliveInfo {
                                    command_id: k.command_id,
                                    response_required: false,
                                };
                                self.send(&DataStructure::KeepAliveInfo(Box::new(ka)))?;
                            }
                        }
                        DataStructure::BrokerInfo(b) => self.broker_info = Some(*b),
                        DataStructure::ProducerAck(a) => {
                            // Applied here; returned so a sender waiting for window space wakes up.
                            self.producer_ack(&a);
                            return Ok(Some(DataStructure::ProducerAck(a)));
                        }
                        DataStructure::ConnectionError(e) => {
                            return Err(broker_error(
                                "broker closed the connection",
                                e.exception.as_ref(),
                            ))
                        }
                        other => return Ok(Some(other)),
                    }
                }
                None => {
                    // Checked only after reading, so frames that arrived while
                    // we were busy still count.
                    let now = Instant::now();
                    if let (true, Some(period)) =
                        (now >= self.monitor_from, self.negotiated.max_inactivity)
                    {
                        if now.duration_since(self.last_read) >= period {
                            bail_kind!(
                                Timeout,
                                "broker sent nothing for {}ms (wireFormat.maxInactivityDuration); connection presumed dead",
                                period.as_millis()
                            );
                        }
                    }
                    if deadline.is_some_and(|d| now >= d) {
                        return Ok(None);
                    }
                    if write_at.is_some_and(|t| now >= t) {
                        self.send(&DataStructure::KeepAliveInfo(Box::default()))?;
                    }
                }
            }
        }
    }

    /// Send a command that wants a response and wait for it; other commands
    /// arriving meanwhile are kept for later.
    fn request(
        &mut self,
        cmd: DataStructure,
        id: i32,
        deadline: Option<Instant>,
        what: &str,
    ) -> Result<()> {
        self.send(&cmd)?;
        let mut held = Vec::new();
        let result = loop {
            match self.recv_until(deadline)? {
                None => break Err(err!(Timeout, "no response to {what} from the broker")),
                Some(DataStructure::Response(r)) if r.correlation_id == id => break Ok(()),
                Some(DataStructure::ExceptionResponse(r)) if r.correlation_id == id => {
                    break Err(broker_error(
                        &format!("broker refused {what}"),
                        r.exception.as_ref(),
                    ))
                }
                Some(DataStructure::ProducerAck(_)) => {}
                Some(other) => {
                    pub_core::limits::check_held(held.len() + self.pending.len())?;
                    held.push(other)
                }
            }
        };
        self.pending.extend(held);
        result
    }
}

impl OpenWireSession {
    /// Release window space: the broker acknowledged `size` bytes.
    fn producer_ack(&mut self, ack: &ProducerAck) {
        let Some(DataStructure::ProducerId(id)) = ack.producer_id.as_deref() else {
            return;
        };
        if let Some(p) = self.producers.values_mut().find(|p| p.id == **id) {
            p.in_flight = p.in_flight.saturating_sub(ack.size.max(0) as u64);
        }
    }

    /// Deadline for a send: `jms.sendTimeout` if set, else pub's reply timeout
    /// (Java would wait forever; a CLI should not).
    fn send_deadline(&self) -> Option<Instant> {
        match self.send_opts.send_timeout_ms {
            0 => self.conn.reply_deadline(),
            ms => Some(
                Instant::now() + Duration::from_millis(ms).min(pub_core::endpoint::MAX_DURATION),
            ),
        }
    }

    /// The producer for `dest`, registering it with the broker (ProducerInfo, sync) on first use.
    fn producer(&mut self, dest: &Destination) -> Result<String> {
        let key = dest.to_string();
        if self.producers.contains_key(&key) {
            return Ok(key);
        }
        let (destination, mut options) = producer::parse_destination(&dest.name)?;
        options.extend(dest.options.iter().cloned());
        let popts = producer::producer_options(&options, self.send_opts.producer_window_size)?;
        self.register_producer(key, destination, popts)
    }

    /// Register a producer (ProducerInfo, sync) under `key`.
    fn register_producer(
        &mut self,
        key: String,
        destination: DataStructure,
        popts: producer::ProducerOptions,
    ) -> Result<String> {
        if self.producers.contains_key(&key) {
            return Ok(key);
        }
        self.next_producer += 1;
        let id = ProducerId {
            connection_id: Some(self.connection_id.clone()),
            value: self.next_producer,
            session_id: 1,
        };
        let cmd_id = self.next_id();
        let info = DataStructure::ProducerInfo(Box::new(ProducerInfo {
            command_id: cmd_id,
            response_required: true,
            producer_id: Some(Box::new(DataStructure::ProducerId(Box::new(id.clone())))),
            destination: Some(Box::new(destination.clone())),
            broker_path: None,
            dispatch_async: popts.dispatch_async,
            window_size: popts.window_size,
        }));
        let what = format!("producer for {}", destination_text(&destination));
        let deadline = self.conn.reply_deadline();
        self.request(info, cmd_id, deadline, &what)?;
        self.producers.insert(
            key.clone(),
            Producer {
                id,
                destination,
                sequence: 0,
                window: popts.window_size.max(0) as u64,
                in_flight: 0,
            },
        );
        Ok(key)
    }

    /// Block until the producer window has room (`MemoryUsage.waitForSpace`).
    fn wait_for_window(&mut self, key: &str) -> Result<()> {
        let deadline = self.send_deadline();
        let mut held = Vec::new();
        let result = loop {
            let (window, in_flight) = {
                let p = &self.producers[key];
                (p.window, p.in_flight)
            };
            if window == 0 || in_flight < window {
                break Ok(());
            }
            match self.recv_until(deadline)? {
                None => {
                    break Err(err!(
                        Timeout,
                        "producer window full ({} of {} bytes unacknowledged; jms.producerWindowSize): the broker is not accepting messages",
                        in_flight,
                        window
                    ))
                }
                Some(DataStructure::ProducerAck(_)) => {}
                Some(other) => {
                    pub_core::limits::check_held(held.len() + self.pending.len())?;
                    held.push(other)
                }
            }
        };
        self.pending.extend(held);
        result
    }

    /// Refuse a message the broker's MaxFrameSize would reject (the broker
    /// would drop the connection instead of answering).
    fn check_frame_size(&mut self, ds: &DataStructure, payload: u64) -> Result<()> {
        let Some(max) = self.negotiated.broker_max_frame else {
            return Ok(());
        };
        // Headers are small; only encode to measure when it could matter.
        if payload + 64 * 1024 < max {
            return Ok(());
        }
        let mut buf = Vec::new();
        self.conn.codec.encode(ds, &mut buf)?;
        let size = buf.len().saturating_sub(4) as u64;
        if size > max {
            bail_kind!(
                Usage,
                "message frame is {size} bytes; the broker accepts at most {max} (its wireFormat.maxFrameSize)"
            );
        }
        Ok(())
    }
}

impl OpenWireSession {
    /// The client ID for durable subscriptions, which are identified by client
    /// ID + subscription name: it must be set explicitly, since a generated one
    /// changes with every connection.
    fn durable_client_id(&self) -> Result<String> {
        match (&self.client_id, self.client_id_explicit) {
            (id, true) => Ok(id.clone()),
            _ => bail_kind!(
                Usage,
                "durable subscriptions need a fixed client ID: set client-id (or jms.clientID), e.g. -O client-id=orders-app"
            ),
        }
    }

    /// One consumer: `receive` (with a durable subscription when asked) or,
    /// with `browse`, a queue browser (`ActiveMQQueueBrowser`): the broker
    /// sends the queue's messages, then an empty dispatch at the end; acks only
    /// free prefetch space, so nothing is consumed.
    fn consume(
        &mut self,
        dest: &Destination,
        opts: &ReceiveOptions,
        mode: Mode,
        sink: &mut dyn FnMut(Message) -> Result<()>,
    ) -> Result<usize> {
        let browse = matches!(mode, Mode::Browse);
        if !opts.headers.is_empty() {
            bail_kind!(
                Usage,
                "OpenWire subscriptions have no headers; use consumer.* destination options (e.g. ?consumer.exclusive=true)"
            );
        }
        let (destination, mut options) = producer::parse_destination(&dest.name)?;
        options.extend(dest.options.iter().cloned());
        let topic = matches!(destination, DataStructure::Topic(_));
        if browse && topic {
            bail_kind!(
                Usage,
                "only queues can be browsed; {} is a topic",
                destination_text(&destination)
            );
        }
        // Moving: the target's producer, registered before consuming starts.
        let move_to = match mode {
            Mode::Move(to) => {
                if topic && opts.subscription.is_none() {
                    bail_kind!(
                        Usage,
                        "move from a queue or a durable subscription (--durable); {} is a topic",
                        destination_text(&destination)
                    );
                }
                Some(self.producer(to)?)
            }
            _ => None,
        };
        let cs = self.consumer_settings;
        self.next_consumer += 1;
        let consumer_id = DataStructure::ConsumerId(Box::new(ConsumerId {
            connection_id: Some(self.connection_id.clone()),
            session_id: 1,
            value: self.next_consumer,
        }));
        let mut info = ConsumerInfo {
            consumer_id: Some(Box::new(consumer_id.clone())),
            destination: Some(Box::new(destination.clone())),
            browser: browse,
            maximum_pending_message_limit: cs.maximum_pending,
            dispatch_async: cs.dispatch_async,
            exclusive: cs.exclusive,
            retroactive: cs.retroactive,
            subscription_name: if browse {
                None
            } else {
                opts.subscription.clone()
            },
            ..Default::default()
        };
        consumer::apply_consumer_options(&mut info, &options)?;
        let durable = info.subscription_name.is_some();
        if durable {
            if !topic {
                bail_kind!(
                    Usage,
                    "durable subscriptions are for topics; {} is a queue",
                    destination_text(&destination)
                );
            }
            info.client_id = Some(self.durable_client_id()?);
        }
        let kind = match (browse, topic, durable) {
            (true, ..) => consumer::ConsumerKind::Browser,
            (_, true, true) => consumer::ConsumerKind::DurableTopic,
            (_, true, false) => consumer::ConsumerKind::Topic,
            _ => consumer::ConsumerKind::Queue,
        };
        // A consumer.prefetchSize option already set it; otherwise the policy.
        if !options
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case("consumer.prefetchSize"))
        {
            info.prefetch_size = cs.prefetch_for(kind, opts.count);
        }
        if browse {
            // A browser can't pull; it needs room for at least one message.
            info.prefetch_size = info.prefetch_size.max(1);
        }
        if let Some(sel) = opts.selector.as_deref().filter(|s| !s.trim().is_empty()) {
            info.selector = Some(sel.to_string());
        }
        let prefetch = info.prefetch_size;
        let cmd_id = self.next_id();
        info.command_id = cmd_id;
        info.response_required = true;
        let what = format!("consumer for {}", destination_text(&destination));
        let deadline = self.conn.reply_deadline();
        self.request(
            DataStructure::ConsumerInfo(Box::new(info)),
            cmd_id,
            deadline,
            &what,
        )?;
        self.conn.trace().info(format_args!(
            "{} {}{} (prefetch {prefetch})",
            if browse { "browsing" } else { "consuming from" },
            destination_text(&destination),
            opts.subscription
                .as_deref()
                .filter(|_| durable && !browse)
                .map(|n| format!(" as durable subscription '{n}'"))
                .unwrap_or_default()
        ));

        let max_payload = self.conn.limits().max_payload;
        let deadline = opts.timeout.map(|t| Instant::now() + t);
        let mut received = 0;
        let mut last_delivered: i64 = -1;
        let mut pulling = false;
        let mut idle_until = opts.idle.map(|d| Instant::now() + d);
        let batch = if browse { None } else { opts.transaction };
        let result = (|| -> Result<()> {
            if batch.is_some() {
                Session::begin(self)?;
            }
            while opts.count.is_none_or(|c| received < c) {
                if prefetch == 0 && !pulling {
                    // MessagePull: 0 = wait for a message; otherwise the broker
                    // answers with an empty dispatch after `timeout` ms.
                    let timeout = deadline.map_or(0, |d| {
                        d.saturating_duration_since(Instant::now())
                            .as_millis()
                            .max(1) as i64
                    });
                    let id = self.next_id();
                    self.send(&DataStructure::MessagePull(Box::new(MessagePull {
                        command_id: id,
                        consumer_id: Some(Box::new(consumer_id.clone())),
                        destination: Some(Box::new(destination.clone())),
                        timeout,
                        ..Default::default()
                    })))?;
                    pulling = true;
                }
                // A browse with no time limit still gives up if the broker goes
                // quiet before saying the browse is done.
                let wait = match (deadline, browse) {
                    (None, true) => self.conn.reply_deadline(),
                    (d, _) => [d, idle_until].into_iter().flatten().min(),
                };
                let Some(ds) = self.recv_until(wait)? else {
                    if browse && deadline.is_none() {
                        bail_kind!(
                            Timeout,
                            "the broker stopped sending before the end of the browse (receive-timeout)"
                        );
                    }
                    break;
                };
                let DataStructure::MessageDispatch(md) = ds else {
                    continue; // ProducerAcks, stray responses
                };
                if md.consumer_id.as_deref() != Some(&consumer_id) {
                    continue;
                }
                pulling = false;
                let Some(DataStructure::Message(m)) = md.message.as_deref() else {
                    if browse {
                        break; // the end of the queue
                    }
                    continue; // a pull that timed out
                };
                let broker_seq = match m.fields.message_id.as_deref() {
                    Some(DataStructure::MessageId(id)) => id.broker_sequence_id,
                    _ => 0,
                };
                last_delivered = broker_seq;
                idle_until = opts.idle.map(|d| Instant::now() + d);
                let message_id = m.fields.message_id.clone();
                let ack_with =
                    |s: &mut Self, ack_type: u8, poison_cause: Option<BrokerError>| -> Result<()> {
                        let id = s.next_id();
                        let cmd = DataStructure::MessageAck(Box::new(MessageAck {
                            command_id: id,
                            response_required: !cs.send_acks_async,
                            transaction_id: s.tx.clone().map(Box::new),
                            destination: md.destination.clone(),
                            consumer_id: Some(Box::new(consumer_id.clone())),
                            ack_type,
                            first_message_id: message_id.clone(),
                            last_message_id: message_id.clone(),
                            message_count: 1,
                            poison_cause,
                        }));
                        if cs.send_acks_async {
                            s.send(&cmd)
                        } else {
                            // jms.sendAcksAsync=false: wait for the broker to take it.
                            let deadline = s.conn.reply_deadline();
                            s.request(cmd, id, deadline, "the acknowledgement")
                        }
                    };
                let ack = |s: &mut Self, ack_type: u8| ack_with(s, ack_type, None);
                if cs.expiry_check && consumer::is_expired(m) {
                    self.conn.trace().info(format_args!(
                        "{} expired message {}",
                        if browse { "skipping" } else { "dropping" },
                        message_id
                            .as_deref()
                            .and_then(consumer::message_id_string)
                            .unwrap_or_default()
                    ));
                    // A browser leaves the queue alone: a plain ack frees the slot.
                    let t = if browse {
                        consumer::ack::STANDARD
                    } else {
                        consumer::ack::EXPIRED
                    };
                    ack(self, t)?;
                    continue;
                }
                if let Some(key) = &move_to {
                    // An exact copy to the target, and the original's ack, in
                    // one transaction.
                    let copy = moved_copy(m, &self.producers[key]);
                    self.send_built(key, copy, "moved message")?;
                    ack(self, consumer::ack::STANDARD)?;
                    if let Ok(msg) = consumer::to_pub_message(m, md.redelivery_counter, max_payload)
                    {
                        sink(msg)?;
                    }
                } else {
                    let msg = match consumer::to_pub_message(m, md.redelivery_counter, max_payload)
                    {
                        Ok(msg) => msg,
                        // Over pub's own max-payload: stop, and leave the
                        // message where it is.
                        Err(e)
                            if pub_core::error::kind_of(&e) == Some(pub_core::ErrorKind::Usage) =>
                        {
                            return Err(e);
                        }
                        Err(e) => {
                            // As the Java client does with a message it can't
                            // deliver: a poison ack, and the broker moves it to
                            // the dead-letter queue with the cause. A browser
                            // just passes it by.
                            let id = message_id
                                .as_deref()
                                .and_then(consumer::message_id_string)
                                .unwrap_or_default();
                            if browse {
                                self.conn
                                    .trace()
                                    .warn(format_args!("skipping message {id}: {e:#}"));
                                ack(self, consumer::ack::STANDARD)?;
                            } else {
                                self.conn.trace().warn(format_args!(
                                    "message {id} can't be decoded ({e:#}); sent to the dead-letter queue"
                                ));
                                let cause = BrokerError {
                                    class: Some("java.lang.Throwable".into()),
                                    message: Some(format!(
                                        "pub could not decode the message: {e:#}"
                                    )),
                                    ..Default::default()
                                };
                                ack_with(self, consumer::ack::POISON, Some(cause))?;
                            }
                            continue;
                        }
                    };
                    let ack_first = !opts.ack && !browse;
                    if ack_first {
                        ack(self, consumer::ack::STANDARD)?; // at most once
                    }
                    sink(msg)?;
                    if !ack_first {
                        // At least once; for a browser, just room for the next.
                        ack(self, consumer::ack::STANDARD)?;
                    }
                }
                received += 1;
                if let Some(n) = batch.filter(|n| *n > 0) {
                    if received.is_multiple_of(n) {
                        Session::commit(self)?;
                        // Unless that was the last one: no empty transaction to end on.
                        if opts.count.is_none_or(|c| received < c) {
                            Session::begin(self)?;
                        }
                    }
                }
            }
            if batch.is_some() && self.tx.is_some() {
                Session::commit(self)?;
            }
            Ok(())
        })();
        // Whatever the failure, what this transaction did is undone: received
        // messages go back to the queue, moved copies are never seen.
        if result.is_err() && self.tx.is_some() {
            let _ = Session::rollback(self);
        }

        // Remove the consumer even after an error; undelivered prefetched
        // messages go back to the queue without counting as redelivered. A
        // durable subscription stays, offline, collecting messages.
        let id = self.next_id();
        let removed = self.send(&DataStructure::RemoveInfo(Box::new(RemoveInfo {
            command_id: id,
            response_required: false,
            object_id: Some(Box::new(consumer_id)),
            last_delivered_sequence_id: last_delivered,
        })));
        result?;
        removed?;
        Ok(received)
    }
}

/// ActiveMQ's advisory topics announcing destinations (`AdvisorySupport`).
pub const ADVISORY_QUEUE: &str = "ActiveMQ.Advisory.Queue";
pub const ADVISORY_TOPIC: &str = "ActiveMQ.Advisory.Topic";

/// `DestinationInfo.operationType`.
const DESTINATION_ADD: u8 = 0;
const DESTINATION_REMOVE: u8 = 1;

/// Destinations the broker keeps for itself: ActiveMQ's advisory topics.
fn is_internal_destination(name: &str) -> bool {
    let physical = name.split_once("://").map_or(name, |(_, p)| p);
    physical.starts_with("ActiveMQ.Advisory.")
}

/// Which ActiveMQ broker: they differ in how destinations are listed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerType {
    /// ActiveMQ: advisory topics.
    ActiveMq,
    /// ActiveMQ Artemis: its management API (`activemq.management`).
    Artemis,
}

/// One request to Artemis's management API.
enum Management<'a> {
    Operation {
        resource: &'a str,
        name: &'a str,
        params: &'a str,
    },
    Attribute {
        resource: String,
        name: &'a str,
    },
}

/// Names out of a management reply: the result wrapped in an array, e.g.
/// `[["a","b"]]`.
pub fn management_names(reply: &str) -> Result<Vec<String>> {
    let v: serde_json::Value = serde_json::from_str(reply)
        .map_err(|e| err!(Protocol, "unreadable management reply ({e}): {reply}"))?;
    let list = match &v {
        serde_json::Value::Array(outer) if outer.len() == 1 && outer[0].is_array() => &outer[0],
        other => other,
    };
    list.as_array()
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect()
        })
        .ok_or_else(|| err!(Protocol, "unexpected management reply: {reply}"))
}

/// Artemis's own addresses and queues (`$sys.*`, `activemq.*`), and temporary
/// queues (Java names them `<connection id>:<n>`), which ActiveMQ doesn't list.
fn is_artemis_internal(name: &str) -> bool {
    let temporary = name.starts_with("ID:")
        && name
            .rsplit_once(':')
            .is_some_and(|(_, n)| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()));
    name.starts_with('$') || name.starts_with("activemq.") || temporary
}

/// What a consumer is for.
#[derive(Clone, Copy)]
enum Mode<'a> {
    Receive,
    Browse,
    /// Move each message to this destination.
    Move(&'a Destination),
}

/// The copy `move` sends: the same message (id, timestamp, expiration,
/// headers, properties, body of any kind) from our producer to the target,
/// without the broker's bookkeeping from its first delivery.
fn moved_copy(m: &crate::types::Message, producer: &Producer) -> crate::types::Message {
    let mut copy = m.clone();
    let f = &mut copy.fields;
    // As ActiveMQ's own move does: keep where the message first came from.
    if f.original_destination.is_none() {
        f.original_destination = f.destination.take();
    }
    f.destination = Some(Box::new(producer.destination.clone()));
    f.producer_id = Some(Box::new(DataStructure::ProducerId(Box::new(
        producer.id.clone(),
    ))));
    if let Some(DataStructure::MessageId(id)) = f.message_id.as_deref_mut() {
        id.broker_sequence_id = 0;
    }
    f.redelivery_counter = 0;
    f.broker_in_time = 0;
    f.broker_out_time = 0;
    f.arrival = 0;
    f.broker_path = None;
    f.cluster = None;
    f.target_consumer_id = None;
    copy
}

impl OpenWireSession {
    /// The producer's next message id: (producer id, message id, destination).
    fn next_message_ids(&mut self, key: &str) -> (DataStructure, DataStructure, DataStructure) {
        let p = self.producers.get_mut(key).expect("producer registered");
        p.sequence += 1;
        let producer_id = DataStructure::ProducerId(Box::new(p.id.clone()));
        let message_id = DataStructure::MessageId(Box::new(MessageId {
            producer_id: Some(Box::new(producer_id.clone())),
            producer_sequence_id: p.sequence,
            ..Default::default()
        }));
        (producer_id, message_id, p.destination.clone())
    }

    /// Send a built message through producer `key`: in the open transaction
    /// (asynchronously, as Java does; the commit confirms it), else by Java's
    /// sync/async rules, within the producer window.
    fn send_built(&mut self, key: &str, mut m: crate::types::Message, label: &str) -> Result<()> {
        let destination = self.producers[key].destination.clone();
        let id_text = m
            .fields
            .message_id
            .as_deref()
            .and_then(consumer::message_id_string)
            .unwrap_or_default();
        let size = producer::message_size(&m);
        let in_tx = self.tx.is_some();
        let is_async = in_tx || self.send_opts.is_async(m.fields.persistent);
        let cmd_id = self.next_id();
        m.fields.command_id = cmd_id;
        m.fields.response_required = !is_async;
        m.fields.transaction_id = self.tx.clone().map(Box::new);
        let ds = DataStructure::Message(Box::new(m));
        self.check_frame_size(&ds, size)?;

        if is_async {
            self.wait_for_window(key)?;
            self.send(&ds)?;
            if !in_tx {
                self.unconfirmed += 1;
            }
            if let Some(p) = self.producers.get_mut(key) {
                if p.window > 0 {
                    p.in_flight += size;
                }
            }
        } else {
            let deadline = self.send_deadline();
            let what = format!("message to {}", destination_text(&destination));
            self.request(ds, cmd_id, deadline, &what)?;
        }
        self.conn.trace().info(format_args!(
            "sent {label} {id_text} to {} ({})",
            destination_text(&destination),
            if in_tx {
                "in transaction"
            } else if is_async {
                "async"
            } else {
                "sync"
            }
        ));
        Ok(())
    }

    /// DestinationInfo ADD or REMOVE, waiting for the broker.
    fn destination_info(&mut self, dest: &Destination, op: u8) -> Result<()> {
        let (destination, _) = producer::parse_destination(&dest.name)?;
        self.destination_info_ds(destination, op)
    }

    fn destination_info_ds(&mut self, destination: DataStructure, op: u8) -> Result<()> {
        let id = self.next_id();
        let what = format!(
            "{} {}",
            if op == DESTINATION_ADD {
                "create"
            } else {
                "delete"
            },
            destination_text(&destination)
        );
        let cmd = DataStructure::DestinationInfo(Box::new(DestinationInfo {
            command_id: id,
            response_required: true,
            connection_id: Some(Box::new(self.connection_id_ds())),
            destination: Some(Box::new(destination)),
            operation_type: op,
            timeout: 0,
            broker_path: None,
        }));
        let deadline = self.conn.reply_deadline();
        self.request(cmd, id, deadline, &what)?;
        self.conn.trace().info(format_args!("{what}: done"));
        Ok(())
    }

    /// `broker-type`, or from the broker's id: ActiveMQ's are `ID:host-…`,
    /// Artemis's are UUIDs.
    pub fn broker_type(&self) -> BrokerType {
        if let Some(t) = self.broker_type {
            return t;
        }
        let id = self
            .broker_info
            .as_ref()
            .and_then(|b| match b.broker_id.as_deref() {
                Some(DataStructure::BrokerId(id)) => id.value.clone(),
                _ => None,
            });
        match id {
            Some(id) if !id.starts_with("ID:") => BrokerType::Artemis,
            _ => BrokerType::ActiveMq,
        }
    }

    /// Ask Artemis's management API: requests to `activemq.management`, all
    /// sent at once, replies matched by correlation id on a temporary queue
    /// (gone with the connection). Returns each reply's body, in order.
    fn artemis_management(&mut self, calls: &[Management]) -> Result<Vec<String>> {
        if calls.is_empty() {
            return Ok(Vec::new());
        }
        self.next_temp += 1;
        let reply_to = DataStructure::TempQueue(Box::new(crate::types::Destination {
            physical_name: Some(format!("{}:{}", self.connection_id, self.next_temp)),
        }));
        self.destination_info_ds(reply_to.clone(), DESTINATION_ADD)?;
        self.next_consumer += 1;
        let consumer_id = DataStructure::ConsumerId(Box::new(ConsumerId {
            connection_id: Some(self.connection_id.clone()),
            session_id: 1,
            value: self.next_consumer,
        }));
        let id = self.next_id();
        let info = DataStructure::ConsumerInfo(Box::new(ConsumerInfo {
            command_id: id,
            response_required: true,
            consumer_id: Some(Box::new(consumer_id.clone())),
            destination: Some(Box::new(reply_to.clone())),
            prefetch_size: consumer::QUEUE_PREFETCH,
            dispatch_async: true,
            ..Default::default()
        }));
        let deadline = self.conn.reply_deadline();
        self.request(info, id, deadline, "management reply consumer")?;

        let result = (|| -> Result<Vec<String>> {
            let management = DataStructure::Queue(Box::new(crate::types::Destination {
                physical_name: Some("activemq.management".into()),
            }));
            let key = self.register_producer(
                "pub:artemis-management".into(),
                management.clone(),
                producer::ProducerOptions {
                    window_size: 0,
                    dispatch_async: false,
                },
            )?;
            for (i, call) in calls.iter().enumerate() {
                let (props, body) = match call {
                    Management::Operation {
                        resource,
                        name,
                        params,
                    } => (
                        vec![
                            (
                                "_AMQ_ResourceName".to_string(),
                                Primitive::String(resource.to_string()),
                            ),
                            (
                                "_AMQ_OperationName".to_string(),
                                Primitive::String(name.to_string()),
                            ),
                        ],
                        *params,
                    ),
                    Management::Attribute { resource, name } => (
                        vec![
                            (
                                "_AMQ_ResourceName".to_string(),
                                Primitive::String(resource.clone()),
                            ),
                            (
                                "_AMQ_Attribute".to_string(),
                                Primitive::String(name.to_string()),
                            ),
                        ],
                        "[]",
                    ),
                };
                let (producer_id, message_id, destination) = self.next_message_ids(&key);
                let m = crate::types::Message {
                    kind: crate::types::kind::TEXT,
                    fields: MessageFields {
                        producer_id: Some(Box::new(producer_id)),
                        destination: Some(Box::new(destination)),
                        message_id: Some(Box::new(message_id)),
                        correlation_id: Some(format!("pub-mgmt-{i}")),
                        reply_to: Some(Box::new(reply_to.clone())),
                        priority: 4,
                        content: Some(crate::primitive::encode_text(body)),
                        marshalled_properties: Some(encode_map(&props)?),
                        ..Default::default()
                    },
                    blob: None,
                };
                self.send_built(&key, m, "management request")?;
            }

            let mut replies: Vec<Option<String>> = vec![None; calls.len()];
            let deadline = self.conn.reply_deadline();
            while replies.iter().any(Option::is_none) {
                let Some(ds) = self.recv_until(deadline)? else {
                    bail_kind!(
                        Timeout,
                        "Artemis management did not answer (the user needs the 'manage' permission on activemq.management)"
                    );
                };
                let DataStructure::MessageDispatch(md) = ds else {
                    continue;
                };
                if md.consumer_id.as_deref() != Some(&consumer_id) {
                    continue;
                }
                let Some(DataStructure::Message(m)) = md.message.as_deref() else {
                    continue;
                };
                let id = self.next_id();
                self.send(&DataStructure::MessageAck(Box::new(MessageAck {
                    command_id: id,
                    destination: md.destination.clone(),
                    consumer_id: Some(Box::new(consumer_id.clone())),
                    ack_type: consumer::ack::STANDARD,
                    first_message_id: m.fields.message_id.clone(),
                    last_message_id: m.fields.message_id.clone(),
                    message_count: 1,
                    ..Default::default()
                })))?;
                let Some(i) = m
                    .fields
                    .correlation_id
                    .as_deref()
                    .and_then(|c| c.strip_prefix("pub-mgmt-"))
                    .and_then(|n| n.parse::<usize>().ok())
                    .filter(|i| *i < calls.len())
                else {
                    continue;
                };
                let body = consumer::to_pub_message(m, 0, self.conn.limits().max_payload)?;
                let text = String::from_utf8_lossy(&body.body).into_owned();
                if body.property("_AMQ_OperationSucceeded") == Some(&pub_core::Value::Bool(false)) {
                    bail_kind!(Protocol, "Artemis management refused: {text}");
                }
                replies[i] = Some(text);
            }
            Ok(replies.into_iter().map(Option::unwrap_or_default).collect())
        })();

        let id = self.next_id();
        let _ = self.send(&DataStructure::RemoveInfo(Box::new(RemoveInfo {
            command_id: id,
            object_id: Some(Box::new(consumer_id)),
            last_delivered_sequence_id: -1,
            ..Default::default()
        })));
        let _ = self.destination_info_ds(reply_to, DESTINATION_REMOVE);
        result
    }

    /// Artemis: anycast queues, and multicast addresses as topics.
    fn artemis_destinations(&mut self, kinds: DestinationKinds) -> Result<Vec<String>> {
        let replies = self.artemis_management(&[
            Management::Operation {
                resource: "broker",
                name: "getQueueNames",
                params: r#"["ANYCAST"]"#,
            },
            Management::Operation {
                resource: "broker",
                name: "getAddressNames",
                params: "[]",
            },
        ])?;
        let keep = |n: &String| kinds.internal || !is_artemis_internal(n);
        let mut out = Vec::new();
        if kinds.queues {
            out.extend(
                management_names(&replies[0])?
                    .into_iter()
                    .filter(keep)
                    .map(|n| format!("queue://{n}")),
            );
        }
        if kinds.topics {
            let addresses: Vec<String> = management_names(&replies[1])?
                .into_iter()
                .filter(keep)
                .collect();
            let calls: Vec<Management> = addresses
                .iter()
                .map(|a| Management::Attribute {
                    resource: format!("address.{a}"),
                    name: "routingTypes",
                })
                .collect();
            let types = self.artemis_management(&calls)?;
            for (a, t) in addresses.iter().zip(types) {
                if management_names(&t)?.iter().any(|r| r == "MULTICAST") {
                    out.push(format!("topic://{a}"));
                }
            }
        }
        out.sort();
        out.dedup();
        Ok(out)
    }

    /// COMMIT_ONE_PHASE or ROLLBACK of the open transaction, waiting for the
    /// broker (`jms.sendTimeout`, else receive-timeout).
    fn end_transaction(&mut self, kind: u8, what: &str) -> Result<()> {
        let Some(tx) = self.tx.take() else {
            bail_kind!(Usage, "no transaction is open to {what}");
        };
        let id = self.next_id();
        let cmd = DataStructure::TransactionInfo(Box::new(TransactionInfo {
            command_id: id,
            response_required: true,
            connection_id: Some(Box::new(self.connection_id_ds())),
            transaction_id: Some(Box::new(tx)),
            kind,
        }));
        let deadline = self.send_deadline();
        self.request(cmd, id, deadline, &format!("transaction {what}"))?;
        self.conn.trace().info(format_args!(
            "transaction {}",
            if kind == tx::ROLLBACK {
                "rolled back"
            } else {
                "committed"
            }
        ));
        Ok(())
    }
}

impl Session for OpenWireSession {
    fn info(&self) -> Vec<(String, String)> {
        let mut info = vec![
            ("version".to_string(), self.negotiated.version.to_string()),
            (
                "encoding".to_string(),
                if self.negotiated.tight {
                    "tight".into()
                } else {
                    "loose".into()
                },
            ),
        ];
        let mut add = |k: &str, v: Option<String>| {
            if let Some(v) = v {
                info.push((k.to_string(), v));
            }
        };
        let b = self.broker_info.as_ref();
        add("broker-name", b.and_then(|b| b.broker_name.clone()));
        add("broker-url", b.and_then(|b| b.broker_url.clone()));
        add(
            "broker-id",
            b.and_then(|b| match b.broker_id.as_deref() {
                Some(DataStructure::BrokerId(id)) => id.value.clone(),
                _ => None,
            }),
        );
        let s = |k: &str| match get(&self.broker_props, k) {
            Some(Primitive::String(v)) => Some(v.clone()),
            _ => None,
        };
        add(
            "provider",
            s("ProviderName").map(|n| format!("{n} {}", s("ProviderVersion").unwrap_or_default())),
        );
        add("connection-id", Some(self.connection_id.clone()));
        add(
            "inactivity",
            Some(
                self.negotiated
                    .max_inactivity
                    .map_or("off".into(), |d| format!("{}ms", d.as_millis())),
            ),
        );
        add(
            "broker-max-frame-size",
            self.negotiated.broker_max_frame.map(|n| {
                if n >= i64::MAX as u64 {
                    "unlimited".to_string()
                } else {
                    n.to_string()
                }
            }),
        );
        info
    }

    fn send(&mut self, dest: &Destination, msg: &Message) -> Result<()> {
        let key = self.producer(dest)?;
        let (producer_id, message_id, destination) = self.next_message_ids(&key);
        let m =
            producer::build_message(msg, &destination, &producer_id, message_id, &self.send_opts)?;
        let label = if msg.body_type == Some(pub_core::BodyType::Text) {
            "TextMessage"
        } else {
            "BytesMessage"
        };
        self.send_built(&key, m, label)
    }

    /// `TransactionContext.begin`: a new LocalTransactionId, BEGIN sent without
    /// waiting.
    fn begin(&mut self) -> Result<()> {
        if self.tx.is_some() {
            bail_kind!(Usage, "a transaction is already open");
        }
        self.next_tx += 1;
        let tx = DataStructure::LocalTransactionId(Box::new(LocalTransactionId {
            value: self.next_tx,
            connection_id: Some(Box::new(self.connection_id_ds())),
        }));
        let id = self.next_id();
        self.send(&DataStructure::TransactionInfo(Box::new(TransactionInfo {
            command_id: id,
            response_required: false,
            connection_id: Some(Box::new(self.connection_id_ds())),
            transaction_id: Some(Box::new(tx.clone())),
            kind: tx::BEGIN,
        })))?;
        self.tx = Some(tx);
        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        self.end_transaction(tx::COMMIT_ONE_PHASE, "commit")
    }

    fn rollback(&mut self) -> Result<()> {
        self.end_transaction(tx::ROLLBACK, "rollback")
    }

    fn receive(
        &mut self,
        dest: &Destination,
        opts: &ReceiveOptions,
        sink: &mut dyn FnMut(Message) -> Result<()>,
    ) -> Result<usize> {
        self.consume(dest, opts, Mode::Receive, sink)
    }

    fn browse(
        &mut self,
        dest: &Destination,
        opts: &ReceiveOptions,
        sink: &mut dyn FnMut(Message) -> Result<()>,
    ) -> Result<usize> {
        self.consume(dest, opts, Mode::Browse, sink)
    }

    /// ActiveMQ's `DestinationSource`: subscribe to the advisory topics, where
    /// the broker replays a DestinationInfo (ADD) for every destination it has;
    /// there is no end marker, so listen until `wait` passes without one.
    fn list_destinations(
        &mut self,
        kinds: DestinationKinds,
        wait: Duration,
    ) -> Result<Vec<String>> {
        if self.broker_type() == BrokerType::Artemis {
            return self.artemis_destinations(kinds);
        }
        let mut topics = Vec::new();
        if kinds.queues {
            topics.push(ADVISORY_QUEUE);
        }
        if kinds.topics {
            topics.push(ADVISORY_TOPIC);
        }
        let mut consumers = Vec::new();
        for t in topics {
            self.next_consumer += 1;
            let consumer_id = DataStructure::ConsumerId(Box::new(ConsumerId {
                connection_id: Some(self.connection_id.clone()),
                session_id: 1,
                value: self.next_consumer,
            }));
            let id = self.next_id();
            let info = DataStructure::ConsumerInfo(Box::new(ConsumerInfo {
                command_id: id,
                response_required: true,
                consumer_id: Some(Box::new(consumer_id.clone())),
                destination: Some(Box::new(DataStructure::Topic(Box::new(
                    crate::types::Destination {
                        physical_name: Some(t.to_string()),
                    },
                )))),
                prefetch_size: consumer::TOPIC_PREFETCH,
                dispatch_async: true,
                ..Default::default()
            }));
            let deadline = self.conn.reply_deadline();
            self.request(info, id, deadline, &format!("advisory consumer for {t}"))?;
            consumers.push(consumer_id);
        }

        let mut found = std::collections::BTreeSet::new();
        let result = (|| -> Result<()> {
            loop {
                let Some(ds) = self.recv_until(Some(Instant::now() + wait))? else {
                    return Ok(());
                };
                let DataStructure::MessageDispatch(md) = ds else {
                    continue;
                };
                let Some(DataStructure::Message(m)) = md.message.as_deref() else {
                    continue;
                };
                if let Some(DataStructure::DestinationInfo(info)) =
                    m.fields.data_structure.as_deref()
                {
                    if let Some(d) = info.destination.as_deref() {
                        let name = destination_text(d);
                        if info.operation_type == DESTINATION_REMOVE {
                            found.remove(&name);
                        } else {
                            found.insert(name);
                        }
                    }
                }
                // Room for the next advisory.
                let id = self.next_id();
                self.send(&DataStructure::MessageAck(Box::new(MessageAck {
                    command_id: id,
                    destination: md.destination.clone(),
                    consumer_id: md.consumer_id.clone(),
                    ack_type: consumer::ack::STANDARD,
                    first_message_id: m.fields.message_id.clone(),
                    last_message_id: m.fields.message_id.clone(),
                    message_count: 1,
                    ..Default::default()
                })))?;
            }
        })();
        for c in consumers {
            let id = self.next_id();
            let _ = self.send(&DataStructure::RemoveInfo(Box::new(RemoveInfo {
                command_id: id,
                object_id: Some(Box::new(c)),
                last_delivered_sequence_id: -1,
                ..Default::default()
            })));
        }
        result?;
        Ok(found
            .into_iter()
            .filter(|n| kinds.internal || !is_internal_destination(n))
            .filter(|n| {
                (kinds.queues && n.starts_with("queue://"))
                    || (kinds.topics && n.starts_with("topic://"))
            })
            .collect())
    }

    fn create_destination(&mut self, dest: &Destination) -> Result<()> {
        self.destination_info(dest, DESTINATION_ADD)
    }

    /// `ActiveMQConnection.destroyDestination`: DestinationInfo REMOVE, synchronously.
    fn delete_destination(&mut self, dest: &Destination) -> Result<()> {
        self.destination_info(dest, DESTINATION_REMOVE)
    }

    fn move_messages(
        &mut self,
        from: &Destination,
        to: &Destination,
        opts: &ReceiveOptions,
        moved: &mut dyn FnMut(Message) -> Result<()>,
    ) -> Result<usize> {
        // Moving onto itself would redeliver each copy forever.
        if crate::producer::parse_destination(&from.name)?.0
            == crate::producer::parse_destination(&to.name)?.0
        {
            bail_kind!(Usage, "moving {} to itself", from.name);
        }
        // Moving is always transactional: a copy and its original's ack commit together.
        let opts = ReceiveOptions {
            transaction: Some(
                opts.transaction
                    .unwrap_or(pub_core::module::DEFAULT_MOVE_BATCH),
            ),
            ..opts.clone()
        };
        self.consume(from, &opts, Mode::Move(to), moved)
    }

    /// `ActiveMQSession.unsubscribe`: RemoveSubscriptionInfo, synchronously.
    fn unsubscribe(&mut self, _dest: &Destination, name: &str) -> Result<()> {
        let client_id = self.durable_client_id()?;
        let id = self.next_id();
        let cmd = DataStructure::RemoveSubscriptionInfo(Box::new(RemoveSubscriptionInfo {
            command_id: id,
            response_required: true,
            connection_id: Some(Box::new(self.connection_id_ds())),
            subscription_name: Some(name.to_string()),
            client_id: Some(client_id.clone()),
        }));
        let deadline = self.conn.reply_deadline();
        let what = format!("unsubscribe of '{name}' (client ID {client_id})");
        self.request(cmd, id, deadline, &what)?;
        self.conn.trace().info(format_args!(
            "removed durable subscription '{name}' of client ID {client_id}"
        ));
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        // An unfinished transaction is rolled back, as closing a JMS session does.
        if self.tx.is_some() {
            let _ = Session::rollback(self);
        }
        // A clean shutdown is a courtesy; don't fail the command over it,
        // unless it is the only confirmation of asynchronous sends.
        let producers: Vec<ProducerId> = self.producers.values().map(|p| p.id.clone()).collect();
        for pid in producers {
            let id = self.next_id();
            let _ = self.send(&DataStructure::RemoveInfo(Box::new(RemoveInfo {
                command_id: id,
                object_id: Some(Box::new(DataStructure::ProducerId(Box::new(pid)))),
                ..Default::default()
            })));
        }
        if let Some(sid) = self.session_id.clone() {
            let id = self.next_id();
            let _ = self.send(&DataStructure::RemoveInfo(Box::new(RemoveInfo {
                command_id: id,
                object_id: Some(Box::new(sid)),
                last_delivered_sequence_id: 0,
                ..Default::default()
            })));
        }
        let id = self.next_id();
        let remove = DataStructure::RemoveInfo(Box::new(RemoveInfo {
            command_id: id,
            response_required: true,
            object_id: Some(Box::new(self.connection_id_ds())),
            last_delivered_sequence_id: 0,
        }));
        let deadline = Instant::now() + self.close_timeout;
        let confirmed = self.request(remove, id, Some(deadline), "connection close");
        let id = self.next_id();
        let _ = self.send(&DataStructure::ShutdownInfo(Box::new(ShutdownInfo {
            command_id: id,
            response_required: false,
        })));
        self.conn.shutdown();
        match (self.unconfirmed, confirmed) {
            (0, _) | (_, Ok(())) => Ok(()),
            (n, Err(e)) => Err(e.context(format!(
                "{n} message(s) sent asynchronously were not confirmed by the broker"
            ))),
        }
    }
}

impl Drop for OpenWireSession {
    fn drop(&mut self) {
        if !self.closed {
            self.conn.shutdown();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts() -> WireOptions {
        WireOptions {
            tight: true,
            stack_traces: false,
            tcp_no_delay: true,
            max_inactivity_ms: 30_000,
            initial_delay_ms: 10_000,
            max_frame: 1 << 26,
        }
    }

    fn broker(version: i32, props: Vec<(String, Primitive)>) -> WireFormatInfo {
        WireFormatInfo {
            magic: MAGIC,
            version,
            marshalled_properties: Some(encode_map(&props).unwrap()),
        }
    }

    #[test]
    fn negotiation_rules() {
        let b = broker(
            12,
            vec![
                ("TightEncodingEnabled".into(), Primitive::Bool(true)),
                ("StackTraceEnabled".into(), Primitive::Bool(true)),
                ("MaxInactivityDuration".into(), Primitive::Long(20_000)),
                ("MaxFrameSize".into(), Primitive::Long(104_857_600)),
            ],
        );
        let (n, _) = negotiate(&opts(), &b).unwrap();
        assert!(n.tight && !n.stack_traces); // AND: we asked for no stack traces
        assert_eq!(n.max_inactivity, Some(Duration::from_secs(20))); // min
        assert_eq!(n.broker_max_frame, Some(104_857_600));

        let loose = broker(
            12,
            vec![("TightEncodingEnabled".into(), Primitive::Bool(false))],
        );
        let (n, _) = negotiate(&opts(), &loose).unwrap();
        assert!(!n.tight);
        assert_eq!(n.max_inactivity, None); // broker advertised none -> 0 -> off

        let off = WireOptions {
            max_inactivity_ms: 0,
            ..opts()
        };
        assert_eq!(negotiate(&off, &b).unwrap().0.max_inactivity, None);

        let old = negotiate(&opts(), &broker(11, vec![])).unwrap_err();
        assert!(old.to_string().contains("version 11"), "{old}");
        let mut bad = broker(12, vec![]);
        bad.magic = *b"NotAMQ!!";
        assert!(negotiate(&opts(), &bad).is_err());
        // A newer broker negotiates down to 12.
        assert_eq!(
            negotiate(&opts(), &broker(13, vec![])).unwrap().0.version,
            12
        );
    }

    #[test]
    fn client_ids_from_a_prefix_look_like_javas() {
        let a = generated_client_id("orders-app");
        let b = generated_client_id("orders-app");
        let (seed_a, seq_a) = a.rsplit_once(':').unwrap();
        let (seed_b, seq_b) = b.rsplit_once(':').unwrap();
        assert_eq!(seed_a, seed_b); // one generator per process
        assert_eq!(
            seq_b.parse::<u64>().unwrap(),
            seq_a.parse::<u64>().unwrap() + 1
        );
        let parts: Vec<&str> = seed_a
            .strip_prefix("orders-app-")
            .unwrap()
            .split('-')
            .collect();
        assert_eq!(parts.len(), 3, "{a}");
        assert_eq!(parts[0], std::process::id().to_string());
        assert!(parts[1].parse::<u128>().is_ok());
        assert_eq!(parts[2], "0");

        // Connection IDs: their own sequence, instance 1 (Java's second generator).
        let c = generated_connection_id("orders-conn");
        assert!(
            c.starts_with(&format!("orders-conn-{}-", std::process::id())),
            "{c}"
        );
        let (seed, seq) = c.rsplit_once(':').unwrap();
        assert!(seed.ends_with("-1"), "{c}");
        assert_eq!(seq, "1");
        assert_eq!(
            seed_a
                .rsplit_once('-')
                .unwrap()
                .0
                .rsplit_once('-')
                .unwrap()
                .1,
            seed.rsplit_once('-').unwrap().0.rsplit_once('-').unwrap().1
        ); // same start time
    }

    #[test]
    fn artemis_management_replies() {
        assert_eq!(management_names(r#"[["a","b"]]"#).unwrap(), ["a", "b"]);
        assert_eq!(management_names(r#"["MULTICAST"]"#).unwrap(), ["MULTICAST"]);
        assert_eq!(management_names("[[]]").unwrap(), Vec::<String>::new());
        assert!(management_names("nope").is_err());
        assert!(
            is_artemis_internal("$sys.mqtt.sessions")
                && is_artemis_internal("activemq.notifications")
        );
        assert!(!is_artemis_internal("DLQ"));
        assert!(is_artemis_internal("ID:pub-60044-1791034208764-1:1"));
        assert!(!is_artemis_internal("ID:orders"));
    }

    #[test]
    fn broker_errors_map_to_exit_codes() {
        let e = |class: &str| BrokerError {
            class: Some(class.into()),
            message: Some("nope".into()),
            ..Default::default()
        };
        let code = |class: &str| pub_core::exit_code(&broker_error("x", Some(&e(class))));
        assert_eq!(code("java.lang.SecurityException"), 67);
        assert_eq!(code("jakarta.jms.JMSSecurityException"), 67);
        assert_eq!(code("javax.jms.InvalidDestinationException"), 2);
        assert_eq!(code("jakarta.jms.InvalidSelectorException"), 2);
        assert_eq!(
            code("org.apache.activemq.artemis.api.core.ActiveMQInvalidFilterExpressionException"),
            2
        );
        assert_eq!(code("javax.jms.InvalidClientIDException"), 8);
        let msg = broker_error(
            "broker refused ConnectionInfo",
            Some(&e("java.lang.SecurityException")),
        )
        .to_string();
        assert_eq!(
            msg,
            "broker refused ConnectionInfo: nope (SecurityException)"
        );
    }
}
