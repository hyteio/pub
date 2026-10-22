//! The AMQP 1.0 conversation: SASL, open, one session, links.
//!
//! 1. protocol header for SASL; pick a mechanism the broker offers (EXTERNAL
//!    with a client certificate, PLAIN with a username, else ANONYMOUS);
//! 2. protocol header for AMQP, `open` (idle timeout: we send empty frames at
//!    half the broker's, and give up when the broker is silent for ours);
//! 3. `begin` on channel 0, with session flow control;
//! 4. send: a sender link per destination; each message waits for credit and
//!    then for the broker's disposition (accepted = stored; rejected,
//!    released or modified are errors);
//! 5. receive: a receiver link with credit following `-n`; each message is
//!    settled (accepted) after it was handed on; selectors, durable
//!    subscriptions and browsing (`copy` distribution, drained) are native;
//! 6. transactions (part 4): a sender link to the coordinator declares and
//!    discharges them; sends and dispositions in between carry a
//!    `transactional-state`. `move` receives from the source and, per
//!    message, sends a copy (`to` set to the target) and accepts the original,
//!    in one transaction per batch;
//! 7. close: detach, end, close.

mod servicebus;

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Context as _;
use pub_core::module::{Destination, ReceiveOptions};
use pub_core::{bail_kind, err, Connection, Context, Endpoint, Message, Result, Session};

use crate::codec::{AmqpCodec, Frame, Performative, SaslFrame, PROTOCOL_AMQP, PROTOCOL_SASL};
use crate::described::*;
use crate::message::{self, AmqpMessage, Body, Prefixes, TextBody};
use crate::types::Value;

/// `apache.org:selector-filter:string`, the JMS selector filter.
pub const SELECTOR_FILTER: u64 = 0x0000_468C_0000_0004;

/// Credit granted when nothing smaller is asked for.
pub const DEFAULT_CREDIT: u32 = 1000;

/// Our session windows (transfer frames); replenished at half.
const WINDOW: u32 = 2048;

/// The coordinator's entry among the senders.
const COORDINATOR: &str = "\0coordinator";

/// Error conditions as pub's exit codes.
pub fn amqp_error(what: &str, e: Option<&Error>) -> anyhow::Error {
    let condition = e.map_or("amqp:internal-error", |e| e.condition.0.as_str());
    let description = e.and_then(|e| e.description.as_deref()).unwrap_or("");
    let text = if description.is_empty() {
        format!("{what}: {condition}")
    } else {
        format!("{what}: {description} ({condition})")
    };
    match condition {
        "amqp:unauthorized-access" => err!(Auth, "{text}"),
        "amqp:not-found"
        | "amqp:invalid-field"
        | "amqp:not-allowed"
        | "amqp:link:message-size-exceeded" => {
            err!(Usage, "{text}")
        }
        // e.g. RabbitMQ's answer to a transaction coordinator.
        "amqp:not-implemented" => err!(Unsupported, "{text}"),
        _ => err!(Protocol, "{text}"),
    }
}

fn container_id() -> String {
    static NEXT: AtomicU32 = AtomicU32::new(1);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!(
        "pub-{}-{}-{}",
        std::process::id(),
        now.as_millis(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

fn duration_setting(ep: &Endpoint, key: &str, default: Duration) -> Result<Duration> {
    match ep.prop(key) {
        None => Ok(default),
        Some(v) => pub_core::endpoint::parse_duration(v)
            .ok_or_else(|| err!(Config, "{key} must be a duration (e.g. 60s), got '{v}'")),
    }
}

/// A sender link's handle and credit.
struct Sender {
    handle: u32,
    credit: u32,
    delivery_count: u32,
}

pub struct AmqpSession {
    conn: Connection<AmqpCodec>,
    pub remote_open: Open,
    container_id: String,
    explicit_container: bool,
    sasl_mechanism: String,
    /// Send an empty frame when we've been quiet this long (half the broker's idle timeout).
    heartbeat_every: Option<Duration>,
    /// Give up when the broker has been silent this long (our idle timeout).
    silence_limit: Option<Duration>,
    last_sent: Instant,
    last_read: Instant,
    /// Largest frame the broker accepts.
    max_frame_out: usize,
    // session flow control
    next_outgoing_id: u32,
    next_incoming_id: u32,
    incoming_window: u32,
    remote_incoming_window: u32,
    next_handle: u32,
    next_delivery_id: u32,
    next_tag: u64,
    senders: HashMap<String, Sender>,
    /// Performatives (with transfer payloads) read while waiting for something else.
    pending: VecDeque<(Performative, Vec<u8>)>,
    text_body: TextBody,
    credit: u32,
    follows_count: bool,
    next_message: u64,
    /// Address prefixes the broker advertised (ActiveMQ Classic).
    prefixes: Prefixes,
    /// The open transaction's id.
    txn: Option<Vec<u8>>,
    /// Azure Service Bus (see [`servicebus`]).
    servicebus: bool,
    closed: bool,
}

impl AmqpSession {
    pub fn connect(ep: &Endpoint, ctx: &Context) -> Result<Self> {
        let mut trace = ctx.trace.clone();
        if let Some(p) = &ep.password {
            trace = trace.redacting(p);
        }
        let mut conn = Connection::open(ep, &ctx.scheme, trace, AmqpCodec::default())?;
        let max_frame = conn.limits().max_frame_size;
        conn.codec.max_frame = max_frame;

        // 1. SASL.
        conn.send(&Frame::Header(PROTOCOL_SASL))?;
        match conn.recv_reply("SASL header").context("waiting for the broker's protocol header")? {
            Frame::Header(PROTOCOL_SASL) => {}
            Frame::Header(other) => bail_kind!(
                Protocol,
                "the broker answered with protocol id {other}, not SASL; pub always authenticates with SASL"
            ),
            other => bail_kind!(Protocol, "expected a protocol header, got {other:?}"),
        }
        let offered = match conn.recv_reply("sasl-mechanisms")? {
            Frame::Sasl(SaslFrame::Mechanisms(m)) => m.sasl_server_mechanisms.0,
            other => bail_kind!(Protocol, "expected sasl-mechanisms, got {other:?}"),
        };
        let mechanism = choose_mechanism(ep, &offered)?;
        let initial_response = match mechanism.as_str() {
            "PLAIN" => {
                let user = ep.username.clone().unwrap_or_default();
                let pass = ep.password.clone().unwrap_or_default();
                Some(Binary(format!("\0{user}\0{pass}").into_bytes()))
            }
            "ANONYMOUS" => Some(Binary(b"pub".to_vec())),
            _ => Some(Binary(Vec::new())),
        };
        let vhost = ep
            .prop("vhost")
            .map(str::to_string)
            .unwrap_or_else(|| ep.host.clone());
        conn.send(&Frame::Sasl(SaslFrame::Init(SaslInit {
            mechanism: Symbol(mechanism.clone()),
            initial_response,
            hostname: Some(vhost.clone()),
        })))?;
        match conn.recv_reply("sasl-outcome")? {
            Frame::Sasl(SaslFrame::Outcome(o)) if o.code == 0 => {}
            Frame::Sasl(SaslFrame::Outcome(o)) => {
                let why = match o.code {
                    1 => "authentication failed",
                    2 => "system error",
                    3 => "permanent system error",
                    4 => "temporary system error",
                    _ => "refused",
                };
                if o.code == 1 {
                    bail_kind!(Auth, "the broker refused {mechanism} login: {why}");
                }
                bail_kind!(Protocol, "SASL {mechanism}: {why} (code {})", o.code);
            }
            Frame::Sasl(SaslFrame::Challenge(_)) => {
                bail_kind!(
                    Unsupported,
                    "the broker sent a SASL challenge; pub supports PLAIN, ANONYMOUS and EXTERNAL"
                )
            }
            other => bail_kind!(Protocol, "expected sasl-outcome, got {other:?}"),
        }

        // 2. AMQP and open.
        conn.codec.expect_header = true;
        conn.send(&Frame::Header(PROTOCOL_AMQP))?;
        match conn.recv_reply("AMQP header")? {
            Frame::Header(PROTOCOL_AMQP) => {}
            other => bail_kind!(Protocol, "expected the AMQP protocol header, got {other:?}"),
        }
        let idle = duration_setting(ep, "idle-timeout", Duration::from_secs(60))?;
        let explicit_container = ep.prop("client-id").is_some();
        let container = ep
            .prop("client-id")
            .map(str::to_string)
            .unwrap_or_else(container_id);
        conn.send(&Frame::amqp(
            0,
            Performative::Open(Open {
                container_id: container.clone(),
                hostname: Some(vhost),
                max_frame_size: Some(max_frame.min(u32::MAX as usize) as u32),
                channel_max: Some(0),
                idle_time_out: (!idle.is_zero())
                    .then(|| idle.as_millis().min(u32::MAX as u128) as u32),
                properties: Some(Map(vec![
                    (Value::symbol("product"), Value::String("pub".into())),
                    (
                        Value::symbol("version"),
                        Value::String(env!("CARGO_PKG_VERSION").into()),
                    ),
                ])),
                ..Default::default()
            }),
        ))?;
        let remote_open = match conn.recv_reply("open")? {
            Frame::Amqp {
                performative: Performative::Open(o),
                ..
            } => o,
            Frame::Amqp {
                performative: Performative::Close(c),
                ..
            } => {
                return Err(amqp_error(
                    "the broker refused the connection",
                    c.error.as_ref(),
                ))
            }
            other => bail_kind!(Protocol, "expected open, got {other:?}"),
        };
        let heartbeat_every = remote_open
            .idle_time_out
            .filter(|t| *t > 0)
            // Half the broker's idle timeout, but never a busy loop.
            .map(|t| Duration::from_millis((t as u64 / 2).max(100)));
        let max_frame_out = remote_open
            .max_frame_size
            .map_or(usize::MAX, |n| n as usize)
            .max(512);
        let advertised = |k: &str| {
            remote_open
                .properties
                .as_ref()
                .and_then(|p| p.get(k))
                .and_then(|v| v.as_str().map(str::to_string))
        };
        let prefixes = Prefixes {
            queue: advertised("queue-prefix"),
            topic: advertised("topic-prefix"),
        };

        let servicebus = servicebus::detect(ep, &remote_open.container_id)?;
        let now = Instant::now();
        let mut session = AmqpSession {
            conn,
            remote_open,
            container_id: container,
            explicit_container,
            sasl_mechanism: mechanism,
            heartbeat_every,
            silence_limit: (!idle.is_zero()).then_some(idle),
            last_sent: now,
            last_read: now,
            max_frame_out,
            next_outgoing_id: 0,
            next_incoming_id: 0,
            incoming_window: WINDOW,
            remote_incoming_window: 0,
            next_handle: 0,
            next_delivery_id: 0,
            next_tag: 0,
            senders: HashMap::new(),
            pending: VecDeque::new(),
            text_body: match ep.prop("text-body").map(str::to_ascii_lowercase).as_deref() {
                Some("data") => TextBody::Data,
                _ => TextBody::Value,
            },
            credit: match ep.prop("credit") {
                Some(v) => v
                    .trim()
                    .parse::<u32>()
                    .ok()
                    .filter(|n| *n > 0)
                    .ok_or_else(|| {
                        err!(Config, "credit must be a whole number above 0, got '{v}'")
                    })?,
                None => DEFAULT_CREDIT,
            },
            follows_count: ep.prop_bool("prefetch-follows-count", true)?,
            next_message: 0,
            prefixes,
            txn: None,
            servicebus,
            closed: false,
        };

        // 3. begin.
        session.send(Performative::Begin(Begin {
            remote_channel: None,
            next_outgoing_id: 0,
            incoming_window: WINDOW,
            outgoing_window: WINDOW,
            handle_max: Some(u32::MAX),
            ..Default::default()
        }))?;
        let deadline = session.conn.reply_deadline();
        match session.recv_until(deadline)?.map(|(p, _)| p) {
            Some(Performative::Begin(b)) => {
                session.next_incoming_id = b.next_outgoing_id;
                session.remote_incoming_window = b.incoming_window;
            }
            Some(other) => bail_kind!(Protocol, "expected begin, got {}", other.name()),
            None => bail_kind!(Timeout, "no begin from the broker"),
        }
        session.conn.trace().info(format_args!(
            "AMQP 1.0 connection open (container {}, SASL {})",
            session.container_id, session.sasl_mechanism
        ));
        Ok(session)
    }

    fn send(&mut self, p: Performative) -> Result<()> {
        self.conn.send(&Frame::amqp(0, p))?;
        self.last_sent = Instant::now();
        Ok(())
    }

    /// The next performative (and a transfer's payload) before `deadline`:
    /// heartbeats both ways, session flow bookkeeping, and the broker closing
    /// things on us.
    fn recv_until(&mut self, deadline: Option<Instant>) -> Result<Option<(Performative, Vec<u8>)>> {
        if let Some(p) = self.pending.pop_front() {
            return Ok(Some(p));
        }
        loop {
            let now = Instant::now();
            if let Some(limit) = self.silence_limit {
                if now.duration_since(self.last_read) >= limit {
                    bail_kind!(
                        Timeout,
                        "the broker sent nothing for {}ms (idle-timeout); connection presumed dead",
                        limit.as_millis()
                    );
                }
            }
            let beat_at = self.heartbeat_every.map(|e| self.last_sent + e);
            let silent_at = self.silence_limit.map(|l| self.last_read + l);
            let wake = [deadline, beat_at, silent_at].into_iter().flatten().min();
            match self.conn.recv_until(wake)? {
                Some(Frame::Empty) => self.last_read = Instant::now(),
                Some(Frame::Amqp {
                    performative,
                    payload,
                    ..
                }) => {
                    self.last_read = Instant::now();
                    match performative {
                        Performative::Close(c) => {
                            let _ = self
                                .conn
                                .send(&Frame::amqp(0, Performative::Close(Close::default())));
                            self.closed = true;
                            return Err(amqp_error(
                                "the broker closed the connection",
                                c.error.as_ref(),
                            ));
                        }
                        Performative::End(e) => {
                            let _ = self
                                .conn
                                .send(&Frame::amqp(0, Performative::End(End::default())));
                            self.closed = true;
                            return Err(amqp_error(
                                "the broker ended the session",
                                e.error.as_ref(),
                            ));
                        }
                        Performative::Flow(f) => {
                            self.on_flow(&f);
                            return Ok(Some((Performative::Flow(f), Vec::new())));
                        }
                        Performative::Transfer(t) => {
                            self.next_incoming_id = self.next_incoming_id.wrapping_add(1);
                            self.incoming_window = self.incoming_window.saturating_sub(1);
                            if self.incoming_window < WINDOW / 2 {
                                self.incoming_window = WINDOW;
                                self.send_session_flow(None)?;
                            }
                            return Ok(Some((Performative::Transfer(t), payload)));
                        }
                        other => return Ok(Some((other, Vec::new()))),
                    }
                }
                Some(other) => bail_kind!(Protocol, "unexpected frame after open: {other:?}"),
                None => {
                    let now = Instant::now();
                    if deadline.is_some_and(|d| now >= d) {
                        return Ok(None);
                    }
                    if beat_at.is_some_and(|t| now >= t) {
                        self.conn.send(&Frame::Empty)?;
                        self.last_sent = Instant::now();
                    }
                }
            }
        }
    }

    /// A broker flow: its session window, and credit for our senders.
    fn on_flow(&mut self, f: &Flow) {
        let their_next_incoming = f.next_incoming_id.unwrap_or(0);
        self.remote_incoming_window = their_next_incoming
            .wrapping_add(f.incoming_window)
            .wrapping_sub(self.next_outgoing_id);
        if let Some(handle) = f.handle {
            if let Some(s) = self.senders.values_mut().find(|s| s.handle == handle) {
                let their_count = f.delivery_count.unwrap_or(s.delivery_count);
                s.credit = their_count
                    .wrapping_add(f.link_credit.unwrap_or(0))
                    .wrapping_sub(s.delivery_count);
            }
        }
    }

    /// Our session state, and optionally a receiver link's credit.
    fn send_session_flow(&mut self, link: Option<(u32, u32, u32, bool)>) -> Result<()> {
        let (handle, delivery_count, link_credit, drain) = match link {
            Some((h, d, c, drain)) => (Some(h), Some(d), Some(c), Some(drain)),
            None => (None, None, None, None),
        };
        self.send(Performative::Flow(Flow {
            next_incoming_id: Some(self.next_incoming_id),
            incoming_window: self.incoming_window,
            next_outgoing_id: self.next_outgoing_id,
            outgoing_window: WINDOW,
            handle,
            delivery_count,
            link_credit,
            drain,
            ..Default::default()
        }))
    }

    /// Wait for one kind of performative; others are kept for later.
    fn wait_for<T>(
        &mut self,
        what: &str,
        deadline: Option<Instant>,
        mut pick: impl FnMut(&Performative) -> Option<Result<T>>,
    ) -> Result<T> {
        let mut held = Vec::new();
        let result = loop {
            match self.recv_until(deadline)? {
                None => break Err(err!(Timeout, "no {what} from the broker")),
                Some((p, payload)) => match pick(&p) {
                    Some(r) => break r,
                    None => {
                        if !matches!(p, Performative::Flow(_)) {
                            pub_core::limits::check_held(held.len() + self.pending.len())?;
                            held.push((p, payload));
                        }
                    }
                },
            }
        };
        self.pending.extend(held);
        result
    }

    /// Attach a link and wait for the broker's attach. A refused link comes
    /// back without a source/target and is followed by a detach with the reason.
    fn attach(&mut self, attach: Attach, what: &str) -> Result<Attach> {
        let name = attach.name.clone();
        let handle = attach.handle;
        let role = attach.role;
        self.send(Performative::Attach(attach))?;
        let deadline = self.conn.reply_deadline();
        let remote = self.wait_for(what, deadline, |p| match p {
            Performative::Attach(a) if a.name == name => Some(Ok(a.clone())),
            Performative::Detach(d) if d.handle == handle => Some(Err(amqp_error(
                &format!("the broker refused {what}"),
                d.error.as_ref(),
            ))),
            _ => None,
        })?;
        let refused = if role == SENDER {
            remote.target.is_none()
        } else {
            remote.source.is_none()
        };
        if refused {
            let deadline = self.conn.reply_deadline();
            let reason = self.wait_for("detach", deadline, |p| match p {
                Performative::Detach(d) => Some(Ok(d.error.clone())),
                _ => None,
            });
            let _ = self.send(Performative::Detach(Detach {
                handle,
                closed: Some(true),
                error: None,
            }));
            return Err(amqp_error(
                &format!("the broker refused {what}"),
                reason.ok().flatten().as_ref(),
            ));
        }
        Ok(remote)
    }

    fn detach(&mut self, handle: u32, closed: bool) -> Result<()> {
        self.send(Performative::Detach(Detach {
            handle,
            closed: Some(closed),
            error: None,
        }))?;
        let deadline = self.conn.reply_deadline();
        let _ = self.wait_for("detach", deadline, |p| match p {
            Performative::Detach(d) if d.handle == handle => Some(Ok(())),
            _ => None,
        });
        Ok(())
    }

    /// The sender link for a destination, attached on first use.
    fn sender(&mut self, dest: &str) -> Result<String> {
        if self.senders.contains_key(dest) {
            return Ok(dest.to_string());
        }
        let (address, capability) = message::address(dest)?;
        let address = self.prefixes.apply(&address, capability);
        let handle = self.next_handle;
        self.next_handle += 1;
        self.attach(
            Attach {
                name: format!("{}-sender-{handle}", self.container_id),
                handle,
                role: SENDER,
                snd_settle_mode: Some(settle::SND_UNSETTLED),
                rcv_settle_mode: Some(settle::RCV_FIRST),
                source: Some(Source {
                    address: Some(self.container_id.clone()),
                    ..Default::default()
                }),
                target: Some(
                    Target {
                        address: Some(address.clone()),
                        capabilities: capability.map(|c| Symbols::of(&[c])),
                        ..Default::default()
                    }
                    .into(),
                ),
                initial_delivery_count: Some(0),
                ..Default::default()
            },
            &format!("a sender for {address}"),
        )?;
        self.senders.insert(
            dest.to_string(),
            Sender {
                handle,
                credit: 0,
                delivery_count: 0,
            },
        );
        Ok(dest.to_string())
    }

    /// Wait until the sender has credit and the session window has room.
    fn wait_for_credit(&mut self, key: &str) -> Result<()> {
        let deadline = self.conn.reply_deadline();
        let mut held = Vec::new();
        let result = loop {
            if self.senders[key].credit > 0 && self.remote_incoming_window > 0 {
                break Ok(());
            }
            match self.recv_until(deadline)? {
                None => {
                    break Err(err!(
                        Timeout,
                        "the broker gave no credit to send to {key} (it may be full or refusing producers)"
                    ))
                }
                Some((Performative::Flow(_), _)) => {}
                Some(other) => {
                    pub_core::limits::check_held(held.len() + self.pending.len())?;
                    held.push(other)
                }
            }
        };
        self.pending.extend(held);
        result
    }

    /// Wait until the broker's session window has room for another frame.
    fn wait_for_session_window(&mut self) -> Result<()> {
        let deadline = self.conn.reply_deadline();
        let mut held = Vec::new();
        let result = loop {
            if self.remote_incoming_window > 0 {
                break Ok(());
            }
            match self.recv_until(deadline)? {
                None => break Err(err!(Timeout, "the broker's session window stayed full")),
                Some((Performative::Flow(_), _)) => {}
                Some(other) => {
                    pub_core::limits::check_held(held.len() + self.pending.len())?;
                    held.push(other)
                }
            }
        };
        self.pending.extend(held);
        result
    }

    /// Send one encoded message as one or more transfer frames.
    fn transfer(
        &mut self,
        handle: u32,
        delivery_id: u32,
        tag: Vec<u8>,
        payload: &[u8],
        settled: bool,
        state: Option<DeliveryState>,
    ) -> Result<()> {
        // Room for the transfer performative and frame header.
        let chunk = self.max_frame_out.saturating_sub(64).max(256);
        let pieces: Vec<&[u8]> = if payload.is_empty() {
            vec![payload]
        } else {
            payload.chunks(chunk).collect()
        };
        let last = pieces.len() - 1;
        for (i, piece) in pieces.into_iter().enumerate() {
            if self.remote_incoming_window == 0 {
                self.wait_for_session_window()?;
            }
            let first = i == 0;
            let t = Transfer {
                handle,
                delivery_id: first.then_some(delivery_id),
                delivery_tag: first.then(|| Binary(tag.clone())),
                message_format: first.then_some(0),
                settled: first.then_some(settled),
                more: Some(i < last),
                state: if first { state.clone() } else { None },
                ..Default::default()
            };
            self.conn.send(&Frame::Amqp {
                channel: 0,
                performative: Performative::Transfer(t),
                payload: piece.to_vec(),
            })?;
            self.last_sent = Instant::now();
            self.next_outgoing_id = self.next_outgoing_id.wrapping_add(1);
            self.remote_incoming_window = self.remote_incoming_window.saturating_sub(1);
        }
        Ok(())
    }

    /// Send an encoded message on a sender link and wait for the broker's
    /// disposition (at least once); returns its state.
    fn deliver(
        &mut self,
        key: &str,
        payload: &[u8],
        state: Option<DeliveryState>,
    ) -> Result<Option<DeliveryState>> {
        self.wait_for_credit(key)?;
        let delivery_id = self.next_delivery_id;
        self.next_delivery_id = self.next_delivery_id.wrapping_add(1);
        self.next_tag += 1;
        let tag = self.next_tag.to_be_bytes().to_vec();
        let handle = self.senders[key].handle;
        self.transfer(handle, delivery_id, tag, payload, false, state)?;
        if let Some(s) = self.senders.get_mut(key) {
            s.credit -= 1;
            s.delivery_count = s.delivery_count.wrapping_add(1);
        }
        let deadline = self.conn.reply_deadline();
        let (state, settled) = self.wait_for("disposition", deadline, |p| match p {
            Performative::Disposition(d)
                if d.role == RECEIVER
                    && d.first <= delivery_id
                    && d.last.unwrap_or(d.first) >= delivery_id =>
            {
                Some(Ok((d.state.clone(), d.settled.unwrap_or(false))))
            }
            _ => None,
        })?;
        if !settled {
            self.send(Performative::Disposition(Disposition {
                role: SENDER,
                first: delivery_id,
                settled: Some(true),
                state: state.clone(),
                ..Default::default()
            }))?;
        }
        Ok(state)
    }

    /// [`deliver`](Self::deliver) a message (in the open transaction, if
    /// any); anything but accepted is an error.
    fn deliver_checked(&mut self, key: &str, payload: &[u8]) -> Result<()> {
        let txn = self.txn.clone().map(|txn_id| DeliveryState::Transactional {
            txn_id,
            outcome: None,
        });
        let state = match self.deliver(key, payload, txn)? {
            Some(DeliveryState::Transactional { outcome, .. }) => outcome.map(|o| *o),
            other => other,
        };
        match state {
            Some(DeliveryState::Accepted) | None => Ok(()),
            Some(DeliveryState::Rejected(e)) => {
                Err(amqp_error("the broker rejected the message", e.as_ref()))
            }
            Some(other) => bail_kind!(
                Protocol,
                "the broker did not take the message ({})",
                other.name()
            ),
        }
    }

    /// The coordinator link, attached on first use.
    fn coordinator(&mut self) -> Result<()> {
        if self.senders.contains_key(COORDINATOR) {
            return Ok(());
        }
        let handle = self.next_handle;
        self.next_handle += 1;
        self.attach(
            Attach {
                name: format!("{}-txn-{handle}", self.container_id),
                handle,
                role: SENDER,
                snd_settle_mode: Some(settle::SND_UNSETTLED),
                rcv_settle_mode: Some(settle::RCV_FIRST),
                source: Some(Source {
                    outcomes: Some(Symbols::of(&["amqp:accepted:list", "amqp:rejected:list"])),
                    ..Default::default()
                }),
                target: Some(Terminus::Coordinator(Coordinator {
                    capabilities: Some(Symbols::of(&["amqp:local-transactions"])),
                })),
                initial_delivery_count: Some(0),
                ..Default::default()
            },
            "a transaction coordinator",
        )?;
        self.senders.insert(
            COORDINATOR.to_string(),
            Sender {
                handle,
                credit: 0,
                delivery_count: 0,
            },
        );
        Ok(())
    }

    /// Send a declare or discharge to the coordinator; returns its answer.
    fn control(&mut self, body: Value) -> Result<Option<DeliveryState>> {
        self.coordinator()?;
        let payload = AmqpMessage {
            body: Body::Value(body),
            ..Default::default()
        }
        .encode()?;
        self.deliver(COORDINATOR, &payload, None)
    }

    fn discharge(&mut self, fail: bool) -> Result<()> {
        let what = if fail { "roll back" } else { "commit" };
        let Some(txn_id) = self.txn.take() else {
            bail_kind!(Usage, "no transaction is open to {what}");
        };
        let state = self.control(
            Discharge {
                txn_id: Binary(txn_id),
                fail: Some(fail),
            }
            .to_value(),
        )?;
        match state {
            Some(DeliveryState::Accepted) | None => {
                self.conn.trace().info(format_args!(
                    "transaction {}",
                    if fail { "rolled back" } else { "committed" }
                ));
                Ok(())
            }
            Some(DeliveryState::Rejected(e)) => Err(amqp_error(
                &format!("the broker failed to {what} the transaction"),
                e.as_ref(),
            )),
            Some(other) => bail_kind!(Protocol, "unexpected answer to discharge: {}", other.name()),
        }
    }

    /// The state that settles a received message: accepted, in the open
    /// transaction if there is one.
    fn accepted(&self) -> DeliveryState {
        match &self.txn {
            Some(txn_id) => DeliveryState::Transactional {
                txn_id: txn_id.clone(),
                outcome: Some(Box::new(DeliveryState::Accepted)),
            },
            None => DeliveryState::Accepted,
        }
    }

    fn durable_container(&self) -> Result<()> {
        if !self.explicit_container {
            bail_kind!(
                Usage,
                "durable subscriptions need a fixed client-id (the AMQP container id), e.g. -O client-id=orders-app"
            );
        }
        Ok(())
    }
}

fn choose_mechanism(ep: &Endpoint, offered: &[String]) -> Result<String> {
    let has = |m: &str| offered.iter().any(|o| o.eq_ignore_ascii_case(m));
    let wanted: Vec<String> = match ep.prop("sasl-mechanisms") {
        Some(list) => list
            .split(',')
            .map(|m| m.trim().to_ascii_uppercase())
            .filter(|m| !m.is_empty())
            .collect(),
        None => {
            let mut v = Vec::new();
            if ep.prop("cert").is_some() {
                v.push("EXTERNAL".to_string());
            }
            if ep.username.is_some() {
                v.push("PLAIN".to_string());
            } else {
                v.push("ANONYMOUS".to_string());
            }
            v
        }
    };
    for m in &wanted {
        if !matches!(m.as_str(), "PLAIN" | "ANONYMOUS" | "EXTERNAL") {
            bail_kind!(
                Config,
                "pub supports the SASL mechanisms PLAIN, ANONYMOUS and EXTERNAL, not {m}"
            );
        }
        if has(m) {
            return Ok(m.clone());
        }
    }
    let why = if ep.username.is_none() && has("PLAIN") {
        " (the broker wants a login: -u user)"
    } else {
        ""
    };
    bail_kind!(
        Auth,
        "no usable SASL mechanism: pub would use {}, the broker offers {}{why}",
        wanted.join(", "),
        offered.join(", ")
    )
}

/// What a receiver link is for.
#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Receive,
    Browse,
}

impl Session for AmqpSession {
    fn info(&self) -> Vec<(String, String)> {
        let o = &self.remote_open;
        let prop = |k: &str| {
            o.properties
                .as_ref()
                .and_then(|p| p.get(k))
                .and_then(|v| v.as_str().map(str::to_string))
        };
        let mut info = vec![("version".to_string(), "1.0".to_string())];
        let mut add = |k: &str, v: Option<String>| {
            if let Some(v) = v {
                info.push((k.to_string(), v));
            }
        };
        add(
            "product",
            prop("product").map(|p| format!("{p} {}", prop("version").unwrap_or_default())),
        );
        add("container-id", Some(o.container_id.clone()));
        add("our-container-id", Some(self.container_id.clone()));
        add("sasl", Some(self.sasl_mechanism.clone()));
        add("max-frame-size", o.max_frame_size.map(|n| n.to_string()));
        add("idle-timeout", o.idle_time_out.map(|t| format!("{t}ms")));
        add(
            "capabilities",
            o.offered_capabilities.as_ref().map(|c| c.0.join(", ")),
        );
        if self.servicebus {
            info.push(("broker".into(), "Azure Service Bus".into()));
        }
        info
    }

    fn send(&mut self, dest: &Destination, msg: &Message) -> Result<()> {
        let key = self.sender(&dest.name)?;
        self.next_message += 1;
        let id = format!("ID:{}:1:1:{}", self.container_id, self.next_message);
        let amqp = message::to_amqp(
            msg,
            &dest.name,
            Some(id.clone()),
            self.text_body,
            &self.prefixes,
        )?;
        let mut amqp = amqp;
        if self.servicebus {
            servicebus::schedule(&mut amqp);
        }
        let payload = amqp.encode()?;
        self.deliver_checked(&key, &payload)?;
        self.conn
            .trace()
            .info(format_args!("sent {id} to {} (accepted)", dest.name));
        Ok(())
    }

    fn receive(
        &mut self,
        dest: &Destination,
        opts: &ReceiveOptions,
        sink: &mut dyn FnMut(Message) -> Result<()>,
    ) -> Result<usize> {
        self.consume(dest, opts, Mode::Receive, None, sink)
    }

    fn browse(
        &mut self,
        dest: &Destination,
        opts: &ReceiveOptions,
        sink: &mut dyn FnMut(Message) -> Result<()>,
    ) -> Result<usize> {
        self.consume(dest, opts, Mode::Browse, None, sink)
    }

    /// Qpid JMS's way: attach to the subscription by its link name with no
    /// source (a lookup), then detach it closed, which removes it.
    fn unsubscribe(&mut self, _dest: &Destination, name: &str) -> Result<()> {
        if self.servicebus {
            return self.servicebus_refuse_durable();
        }
        self.durable_container()?;
        let handle = self.next_handle;
        self.next_handle += 1;
        self.send(Performative::Attach(Attach {
            name: name.to_string(),
            handle,
            role: RECEIVER,
            snd_settle_mode: Some(settle::SND_UNSETTLED),
            rcv_settle_mode: Some(settle::RCV_FIRST),
            source: None,
            target: Some(Target::default().into()),
            ..Default::default()
        }))?;
        let deadline = self.conn.reply_deadline();
        let remote = self.wait_for("attach", deadline, |p| match p {
            Performative::Attach(a) if a.name == name => Some(Ok(a.clone())),
            _ => None,
        })?;
        let exists = remote.source.is_some();
        self.detach(handle, true)?;
        if !exists {
            bail_kind!(
                Usage,
                "no durable subscription '{name}' for client-id {}",
                self.container_id
            );
        }
        self.conn
            .trace()
            .info(format_args!("removed durable subscription '{name}'"));
        Ok(())
    }

    fn begin(&mut self) -> Result<()> {
        if self.txn.is_some() {
            bail_kind!(Usage, "a transaction is already open");
        }
        match self.control(Declare::default().to_value())? {
            Some(DeliveryState::Declared(id)) => {
                self.txn = Some(id);
                Ok(())
            }
            Some(DeliveryState::Rejected(e)) => Err(amqp_error(
                "the broker refused to start a transaction",
                e.as_ref(),
            )),
            other => bail_kind!(
                Protocol,
                "unexpected answer to declare: {}",
                other.as_ref().map_or("none", DeliveryState::name)
            ),
        }
    }

    fn commit(&mut self) -> Result<()> {
        self.discharge(false)
    }

    fn rollback(&mut self) -> Result<()> {
        self.discharge(true)
    }

    /// Move: receive from `from` and, per message, send a copy to `to` and
    /// accept the original, in one transaction per batch.
    fn move_messages(
        &mut self,
        from: &Destination,
        to: &Destination,
        opts: &ReceiveOptions,
        moved: &mut dyn FnMut(Message) -> Result<()>,
    ) -> Result<usize> {
        if from.name == to.name {
            bail_kind!(Usage, "moving {} to itself", from.name);
        }
        if self.servicebus {
            return self.servicebus_refuse_move().map(|_| 0);
        }
        let key = self.sender(&to.name)?;
        let opts = ReceiveOptions {
            transaction: Some(
                opts.transaction
                    .unwrap_or(pub_core::module::DEFAULT_MOVE_BATCH),
            ),
            ack: true,
            ..opts.clone()
        };
        self.consume(from, &opts, Mode::Receive, Some(&key), moved)
    }

    fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        if self.txn.is_some() {
            let _ = self.discharge(true);
        }
        self.closed = true;
        let handles: Vec<u32> = self.senders.values().map(|s| s.handle).collect();
        for h in handles {
            let _ = self.send(Performative::Detach(Detach {
                handle: h,
                closed: Some(true),
                error: None,
            }));
        }
        let _ = self.send(Performative::End(End::default()));
        let _ = self.send(Performative::Close(Close::default()));
        // Wait (briefly) for the broker's close; errors past this point don't matter.
        let deadline =
            Some(Instant::now() + Duration::from_secs(5)).min(self.conn.reply_deadline());
        loop {
            match self.conn.recv_until(deadline) {
                Ok(Some(Frame::Amqp {
                    performative: Performative::Close(_),
                    ..
                }))
                | Ok(None)
                | Err(_) => break,
                Ok(Some(_)) => {}
            }
        }
        self.conn.shutdown();
        Ok(())
    }
}

impl AmqpSession {
    fn consume(
        &mut self,
        dest: &Destination,
        opts: &ReceiveOptions,
        mode: Mode,
        forward: Option<&str>,
        sink: &mut dyn FnMut(Message) -> Result<()>,
    ) -> Result<usize> {
        if !opts.headers.is_empty() {
            bail_kind!(Usage, "AMQP 1.0 subscriptions have no headers");
        }
        let browse = mode == Mode::Browse;
        let has_selector = opts
            .selector
            .as_deref()
            .is_some_and(|s| !s.trim().is_empty());
        if self.servicebus {
            if has_selector {
                return self.servicebus_refuse_selector().map(|_| 0);
            }
            if browse {
                return self.servicebus_peek(dest, opts, sink);
            }
            if opts.subscription.is_some() {
                return self.servicebus_refuse_durable().map(|_| 0);
            }
        }
        let (address, capability) = message::address(&dest.name)?;
        let address = self.prefixes.apply(&address, capability);
        if browse && capability == Some("topic") {
            bail_kind!(
                Usage,
                "only queues can be browsed; {} is a topic",
                dest.name
            );
        }
        let subscription = if browse {
            None
        } else {
            opts.subscription.clone()
        };
        if subscription.is_some() {
            self.durable_container()?;
            if capability == Some("queue") {
                bail_kind!(
                    Usage,
                    "durable subscriptions are for topics; {} is a queue",
                    dest.name
                );
            }
        }
        let filter = opts
            .selector
            .as_deref()
            .filter(|s| !s.trim().is_empty())
            .map(|sel| {
                Map(vec![(
                    Value::symbol("jms-selector"),
                    Value::described(SELECTOR_FILTER, Value::String(sel.to_string())),
                )])
            });
        let handle = self.next_handle;
        self.next_handle += 1;
        let settled_by_broker = browse || !opts.ack;
        let name = subscription
            .clone()
            .unwrap_or_else(|| format!("{}-receiver-{handle}", self.container_id));
        let durable = subscription.is_some();
        let remote = self.attach(
            Attach {
                name,
                handle,
                role: RECEIVER,
                snd_settle_mode: Some(if settled_by_broker {
                    settle::SND_SETTLED
                } else {
                    settle::SND_UNSETTLED
                }),
                rcv_settle_mode: Some(settle::RCV_FIRST),
                source: Some(Source {
                    address: Some(address.clone()),
                    durable: durable.then_some(2),
                    expiry_policy: durable.then(|| Symbol::new(EXPIRY_NEVER)),
                    distribution_mode: browse.then(|| Symbol::new("copy")),
                    filter,
                    capabilities: capability.map(|c| Symbols::of(&[c])),
                    ..Default::default()
                }),
                target: Some(
                    Target {
                        address: Some(self.container_id.clone()),
                        ..Default::default()
                    }
                    .into(),
                ),
                ..Default::default()
            },
            &format!("a receiver for {address}"),
        )?;
        // Browsing must not consume: a broker that ignores copy distribution
        // (RabbitMQ's classic queues) would hand the messages over for good.
        if browse {
            let copies = remote
                .source
                .as_ref()
                .and_then(|s| s.distribution_mode.as_ref())
                .is_some_and(|m| m.0 == "copy");
            if !copies {
                let _ = self.detach(handle, true);
                bail_kind!(
                    Unsupported,
                    "this broker doesn't browse {address}: it didn't agree to copy distribution, so browsing would consume the messages"
                );
            }
        }
        // A filter the broker didn't put in effect isn't in its source
        // (part 3.5.3): receiving anyway would hand over everything.
        if has_selector {
            let applied = remote
                .source
                .as_ref()
                .and_then(|s| s.filter.as_ref())
                .is_some_and(|f| f.get("jms-selector").is_some());
            if !applied {
                let _ = self.detach(handle, true);
                bail_kind!(
                    Unsupported,
                    "this broker didn't apply the selector to {address}, so it would hand over every message"
                );
            }
        }
        let mut delivery_count = remote.initial_delivery_count.unwrap_or(0);
        // Credit follows -n unless set; a browse asks in batches and drains.
        // Credit follows -n when asked to, and always when the broker settles
        // (pre-settled messages pub doesn't want would be lost).
        let credit = match (self.follows_count || settled_by_broker, opts.count) {
            (true, Some(n)) => self.credit.min(n.min(u32::MAX as usize) as u32).max(1),
            _ => self.credit,
        };
        self.conn.trace().info(format_args!(
            "{} {address} (credit {credit}){}",
            if browse { "browsing" } else { "receiving from" },
            subscription
                .as_deref()
                .map(|n| format!(" as durable subscription '{n}'"))
                .unwrap_or_default()
        ));
        let mut outstanding = credit;
        // Where the current browse batch ends: the drain answer for it says so.
        let mut batch_end = delivery_count.wrapping_add(credit);
        self.send_session_flow(Some((handle, delivery_count, credit, browse)))?;

        let max_payload = self.conn.limits().max_payload;
        let deadline = opts.timeout.map(|t| Instant::now() + t);
        let mut idle_until = opts.idle.map(|d| Instant::now() + d);
        let batch = if browse { None } else { opts.transaction };
        // Where copies go when moving: the target's address, for `to`.
        let forward = match forward {
            Some(key) => {
                let (a, cap) = message::address(key)?;
                Some((key, self.prefixes.apply(&a, cap)))
            }
            None => None,
        };
        let mut received = 0usize;
        let mut partial: Option<(u32, bool, Vec<u8>)> = None;
        let result = (|| -> Result<()> {
            if batch.is_some() {
                Session::begin(self)?;
            }
            while opts.count.is_none_or(|c| received < c) {
                let wait = match (deadline, browse) {
                    (None, true) => self.conn.reply_deadline(),
                    (d, _) => [d, idle_until].into_iter().flatten().min(),
                };
                let Some((p, payload)) = self.recv_until(wait)? else {
                    if browse && deadline.is_none() {
                        bail_kind!(
                            Timeout,
                            "the broker stopped sending before the end of the browse"
                        );
                    }
                    break;
                };
                match p {
                    Performative::Transfer(t) if t.handle == handle => {
                        let (id, settled, mut bytes) = match partial.take() {
                            Some(p) => p,
                            None => (
                                t.delivery_id.ok_or_else(|| {
                                    err!(Protocol, "a delivery's first transfer has no delivery-id")
                                })?,
                                t.settled.unwrap_or(false),
                                Vec::new(),
                            ),
                        };
                        if t.aborted == Some(true) {
                            // An aborted delivery still used credit and advanced
                            // the sender's delivery-count (part 2.6.14).
                            delivery_count = delivery_count.wrapping_add(1);
                            outstanding = outstanding.saturating_sub(1);
                            continue;
                        }
                        if bytes.len() + payload.len() > max_payload.saturating_add(1 << 20) {
                            bail_kind!(
                                Protocol,
                                "AMQP message larger than max-payload ({max_payload} bytes)"
                            );
                        }
                        bytes.extend_from_slice(&payload);
                        if t.more == Some(true) {
                            partial = Some((id, settled, bytes));
                            continue;
                        }
                        delivery_count = delivery_count.wrapping_add(1);
                        outstanding = outstanding.saturating_sub(1);
                        let mut amqp = AmqpMessage::decode(&bytes)?;
                        let msg = message::from_amqp(&amqp)?;
                        if let Some((key, to)) = &forward {
                            // A copy of the message, not of this delivery.
                            amqp.delivery_annotations = None;
                            if let Some(h) = amqp.header.as_mut() {
                                h.delivery_count = None;
                                h.first_acquirer = None;
                            }
                            let props = amqp.properties.get_or_insert_with(Default::default);
                            props.to = Some(to.clone());
                            let copy = amqp.encode()?;
                            self.deliver_checked(key, &copy)?;
                        }
                        sink(msg)?;
                        if !settled {
                            let state = self.accepted();
                            self.send(Performative::Disposition(Disposition {
                                role: RECEIVER,
                                first: id,
                                settled: Some(true),
                                state: Some(state),
                                ..Default::default()
                            }))?;
                        }
                        received += 1;
                        idle_until = opts.idle.map(|d| Instant::now() + d);
                        if let Some(n) = batch.filter(|n| *n > 0) {
                            if received.is_multiple_of(n) {
                                Session::commit(self)?;
                                Session::begin(self)?;
                            }
                        }
                        if browse {
                            // A batch used up by messages gets no drain answer: ask again.
                            if outstanding == 0 && opts.count.is_none_or(|c| received < c) {
                                outstanding = credit;
                                batch_end = delivery_count.wrapping_add(credit);
                                self.send_session_flow(Some((
                                    handle,
                                    delivery_count,
                                    credit,
                                    true,
                                )))?;
                            }
                            continue;
                        }
                        // Top the credit up at half, without exceeding what -n still wants.
                        let wanted = opts
                            .count
                            .map_or(credit, |c| (c - received).min(credit as usize) as u32);
                        if !browse && outstanding <= credit / 2 && wanted > outstanding {
                            outstanding = wanted;
                            self.send_session_flow(Some((
                                handle,
                                delivery_count,
                                outstanding,
                                false,
                            )))?;
                        }
                    }
                    Performative::Flow(f) if f.handle == Some(handle) && browse => {
                        // The drain answer: the broker has used our credit up, the
                        // unused part by advancing its delivery count (part 2.6.7).
                        // The answer for the current batch (older ones are stale):
                        // credit left over means the queue has been read.
                        if f.link_credit.unwrap_or(0) == 0
                            && f.delivery_count == Some(batch_end)
                            && outstanding > 0
                        {
                            break;
                        }
                    }
                    Performative::Detach(d) if d.handle == handle => {
                        return Err(amqp_error(
                            "the broker detached the receiver",
                            d.error.as_ref(),
                        ));
                    }
                    _ => {}
                }
            }
            if batch.is_some() {
                Session::commit(self)?;
            }
            Ok(())
        })();
        if result.is_err() && self.txn.is_some() {
            let _ = Session::rollback(self);
        }
        // A durable subscription stays (detach, not close); otherwise the link
        // closes and unsettled deliveries go back to the queue.
        let detached = self.detach(handle, !durable);
        result?;
        detached?;
        Ok(received)
    }
}

impl Drop for AmqpSession {
    fn drop(&mut self) {
        if !self.closed {
            self.conn.shutdown();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errors_map_to_exit_codes() {
        let e = |c: &str| Error {
            condition: Symbol::new(c),
            description: Some("why".into()),
            info: None,
        };
        let code = |c: &str| pub_core::exit_code(&amqp_error("x", Some(&e(c))));
        assert_eq!(code("amqp:unauthorized-access"), 67);
        assert_eq!(code("amqp:not-found"), 2);
        assert_eq!(code("amqp:resource-limit-exceeded"), 8);
        assert_eq!(code("amqp:not-implemented"), 1);
        assert_eq!(
            amqp_error("broker refused", Some(&e("amqp:not-found"))).to_string(),
            "broker refused: why (amqp:not-found)"
        );
    }

    #[test]
    fn mechanism_choice() {
        let ep = |url: &str| {
            let mut e = Endpoint::parse(url).unwrap();
            e.apply_credentials().unwrap();
            e
        };
        let offered = |m: &[&str]| m.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(
            choose_mechanism(&ep("amqp://u:p@h"), &offered(&["ANONYMOUS", "PLAIN"])).unwrap(),
            "PLAIN"
        );
        assert_eq!(
            choose_mechanism(&ep("amqp://h"), &offered(&["PLAIN", "ANONYMOUS"])).unwrap(),
            "ANONYMOUS"
        );
        let e = choose_mechanism(&ep("amqp://h"), &offered(&["PLAIN"])).unwrap_err();
        assert_eq!(pub_core::exit_code(&e), 67);
        assert!(e.to_string().contains("-u user"), "{e}");
        assert_eq!(
            choose_mechanism(
                &ep("amqp://u:p@h?sasl-mechanisms=ANONYMOUS"),
                &offered(&["PLAIN", "ANONYMOUS"])
            )
            .unwrap(),
            "ANONYMOUS"
        );
        assert!(choose_mechanism(
            &ep("amqp://h?sasl-mechanisms=SCRAM-SHA-256"),
            &offered(&["SCRAM-SHA-256"])
        )
        .is_err());
    }
}
