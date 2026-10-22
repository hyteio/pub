//! The AMQP 0-9-1 conversation: one connection, one channel (1).
//!
//! 1. protocol header; `start` → `start-ok` (EXTERNAL with a client
//!    certificate, PLAIN with a user, else ANONYMOUS; RabbitMQ's
//!    `authentication_failure_close` turns a bad login into a clean 403);
//! 2. `tune` → `tune-ok` (frame size, heartbeat), `open` the vhost, open channel 1;
//! 3. send: `basic.publish` + header + body frames; with publisher confirms
//!    (default) each message waits for the broker's ack, and a `mandatory`
//!    message nobody could route comes back as `basic.return` — an error;
//! 4. receive: `basic.qos` (prefetch following `-n`), `basic.consume`,
//!    deliveries assembled from header and body frames, acked after they were
//!    handed on; `--durable NAME` on an exchange binds a named durable queue;
//! 5. transactions (`tx.select/commit/rollback`) and `move`;
//! 6. close: channel, then connection.

use std::collections::VecDeque;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Context as _;
use pub_core::module::{Destination, DestinationKinds, ReceiveOptions};
use pub_core::{bail_kind, err, Connection, Context, Endpoint, Message, Result, Session};

use crate::codec::*;
use crate::message::{self, Target};
use crate::wire::{FieldValue, Table};

const CHANNEL: u16 = 1;
/// The `basic` class, for content headers.
const BASIC: u16 = 60;
/// A broker reply code as pub's exit codes.
pub fn reply_error(what: &str, code: u16, text: &str) -> anyhow::Error {
    let msg = format!("{what}: {text} ({code})");
    match code {
        403 | 530 => err!(Auth, "{msg}"),
        404 | 405 | 406 | 312 | 313 => err!(Usage, "{msg}"),
        _ => err!(Protocol, "{msg}"),
    }
}

fn duration_setting(ep: &Endpoint, key: &str, default: Duration) -> Result<Duration> {
    match ep.prop(key) {
        None => Ok(default),
        Some(v) => pub_core::endpoint::parse_duration(v)
            .ok_or_else(|| err!(Config, "{key} must be a duration (e.g. 60s), got '{v}'")),
    }
}

fn choose_mechanism(ep: &Endpoint, offered: &[&str]) -> Result<String> {
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
                v.push("EXTERNAL".into());
            }
            v.push(
                if ep.username.is_some() {
                    "PLAIN"
                } else {
                    "ANONYMOUS"
                }
                .into(),
            );
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
    let hint = if ep.username.is_none() && has("PLAIN") {
        " (the broker wants a login: -u user)"
    } else {
        ""
    };
    bail_kind!(
        Auth,
        "no usable SASL mechanism: pub would use {}, the broker offers {}{hint}",
        wanted.join(", "),
        offered.join(" ")
    )
}

/// A message on its way in: the method that started it, then content.
struct Incoming {
    exchange: String,
    routing_key: String,
    redelivered: bool,
    delivery_tag: u64,
    properties: Option<BasicProperties>,
    size: u64,
    body: Vec<u8>,
}

pub struct RampqSession {
    conn: Connection<RampqCodec>,
    pub server_properties: Table,
    mechanism: String,
    vhost: String,
    frame_max: usize,
    heartbeat: Option<Duration>,
    last_sent: Instant,
    last_read: Instant,
    /// Publisher confirms are on (the next publish gets the next sequence number).
    confirming: bool,
    publish_seq: u64,
    in_tx: bool,
    tx_selected: bool,
    use_confirms: bool,
    mandatory: Option<bool>,
    prefetch: u16,
    follows_count: bool,
    exchange_type: String,
    queue_type: Option<String>,
    next_message: u64,
    /// Frames read while waiting for something else, in order (a delivery's
    /// method and its content stay together).
    pending: VecDeque<Frame>,
    closed: bool,
}

impl RampqSession {
    pub fn connect(ep: &Endpoint, ctx: &Context) -> Result<Self> {
        let mut trace = ctx.trace.clone();
        if let Some(p) = &ep.password {
            trace = trace.redacting(p);
        }
        let mut conn = Connection::open(ep, &ctx.scheme, trace, RampqCodec::default())?;
        let our_max = conn.limits().max_frame_size;
        conn.codec.max_frame = our_max;
        conn.send(&Frame::ProtocolHeader(PROTOCOL_HEADER))?;

        let start = match conn
            .recv_reply("connection.start")
            .context("waiting for the broker to start")?
        {
            Frame::Method {
                method: Method::ConnectionStart(s),
                ..
            } => s,
            Frame::ProtocolHeader(h) => bail_kind!(
                Protocol,
                "the broker speaks AMQP {}.{}.{}, not 0-9-1 (for AMQP 1.0 use amqp://)",
                h[5],
                h[6],
                h[7]
            ),
            other => bail_kind!(Protocol, "expected connection.start, got {other:?}"),
        };
        let mechanisms = String::from_utf8_lossy(&start.mechanisms.0).into_owned();
        let offered: Vec<&str> = mechanisms.split_whitespace().collect();
        let mechanism = choose_mechanism(ep, &offered)?;
        let response = match mechanism.as_str() {
            "PLAIN" => format!(
                "\0{}\0{}",
                ep.username.clone().unwrap_or_default(),
                ep.password.clone().unwrap_or_default()
            )
            .into_bytes(),
            _ => Vec::new(),
        };
        let capabilities: Table = [
            "publisher_confirms",
            "exchange_exchange_bindings",
            "basic.nack",
            "consumer_cancel_notify",
            "connection.blocked",
            "authentication_failure_close",
        ]
        .iter()
        .map(|c| (c.to_string(), FieldValue::Bool(true)))
        .collect();
        let s = |v: &str| FieldValue::LongStr(v.as_bytes().to_vec());
        let mut client_properties: Table = vec![
            ("product".into(), s("pub")),
            ("version".into(), s(env!("CARGO_PKG_VERSION"))),
            ("platform".into(), s("Rust")),
            ("capabilities".into(), FieldValue::Table(capabilities)),
        ];
        if let Some(name) = ep.prop("connection-name") {
            client_properties.push(("connection_name".into(), s(name)));
        }
        conn.send(&Frame::method(
            0,
            Method::ConnectionStartOk(ConnectionStartOk {
                client_properties,
                mechanism: mechanism.clone(),
                response: LongStr(response),
                locale: "en_US".into(),
            }),
        ))?;

        let tune = match conn.recv_reply("connection.tune") {
            Ok(Frame::Method {
                method: Method::ConnectionTune(t),
                ..
            }) => t,
            Ok(Frame::Method {
                method: Method::ConnectionClose(c),
                ..
            }) => {
                return Err(reply_error(
                    "the broker refused the login",
                    c.reply_code,
                    &c.reply_text,
                ))
            }
            Ok(Frame::Method {
                method: Method::ConnectionSecure(_),
                ..
            }) => {
                bail_kind!(
                    Unsupported,
                    "the broker sent a SASL challenge; pub supports PLAIN, ANONYMOUS and EXTERNAL"
                )
            }
            Ok(other) => bail_kind!(Protocol, "expected connection.tune, got {other:?}"),
            // Brokers without authentication_failure_close just hang up.
            Err(e) => {
                return Err(err!(
                Auth,
                "the broker closed the connection during login (wrong user or password?): {e:#}"
            ))
            }
        };
        // 4096 is the spec's frame-min-size; a broker asking for less is broken.
        let frame_max = match tune.frame_max {
            0 => our_max,
            n if (n as usize) < 4096 => {
                bail_kind!(
                    Protocol,
                    "the broker's frame-max {n} is below AMQP's minimum of 4096"
                )
            }
            n => (n as usize).min(our_max),
        };
        let wanted_hb = duration_setting(ep, "heartbeat", Duration::from_secs(60))?
            .as_secs()
            .min(u16::MAX as u64) as u16;
        // As RabbitMQ's clients do: either side's 0 defers to the other, else the smaller.
        let heartbeat = match (tune.heartbeat, wanted_hb) {
            (0, w) | (w, 0) => w,
            (a, b) => a.min(b),
        };
        conn.send(&Frame::method(
            0,
            Method::ConnectionTuneOk(ConnectionTuneOk {
                channel_max: 1, // pub uses one channel
                frame_max: frame_max.min(u32::MAX as usize) as u32,
                heartbeat,
            }),
        ))?;
        conn.codec.max_frame = frame_max.max(4096);
        let vhost = ep.prop("vhost").unwrap_or("/").to_string();
        conn.send(&Frame::method(
            0,
            Method::ConnectionOpen(ConnectionOpen {
                virtual_host: vhost.clone(),
                ..Default::default()
            }),
        ))?;
        match conn.recv_reply("connection.open-ok")? {
            Frame::Method {
                method: Method::ConnectionOpenOk(_),
                ..
            } => {}
            Frame::Method {
                method: Method::ConnectionClose(c),
                ..
            } => {
                return Err(reply_error(
                    &format!("the broker refused vhost '{vhost}'"),
                    c.reply_code,
                    &c.reply_text,
                ))
            }
            other => bail_kind!(Protocol, "expected connection.open-ok, got {other:?}"),
        }

        let now = Instant::now();
        let mut session = RampqSession {
            conn,
            server_properties: start.server_properties,
            mechanism,
            vhost,
            frame_max,
            heartbeat: (heartbeat > 0).then(|| Duration::from_secs(heartbeat as u64)),
            last_sent: now,
            last_read: now,
            confirming: false,
            publish_seq: 0,
            in_tx: false,
            tx_selected: false,
            use_confirms: ep.prop_bool("confirms", true)?,
            mandatory: ep
                .prop("mandatory")
                .map(|_| ep.prop_bool("mandatory", true))
                .transpose()?,
            prefetch: match ep.prop("prefetch") {
                Some(v) => v
                    .trim()
                    .parse::<u16>()
                    .ok()
                    .filter(|n| *n > 0)
                    .ok_or_else(|| err!(Config, "prefetch must be 1..65535, got '{v}'"))?,
                None => 1000,
            },
            follows_count: ep.prop_bool("prefetch-follows-count", true)?,
            exchange_type: ep.prop("exchange-type").unwrap_or("topic").to_string(),
            queue_type: ep.prop("queue-type").map(str::to_string),
            next_message: 0,
            pending: VecDeque::new(),
            closed: false,
        };
        session.send(Method::ChannelOpen(ChannelOpen::default()))?;
        session.expect("channel.open-ok", |m| matches!(m, Method::ChannelOpenOk(_)))?;
        session.conn.trace().info(format_args!(
            "AMQP 0-9-1 connection open (vhost {}, {}, frame-max {}, heartbeat {}s)",
            session.vhost, session.mechanism, session.frame_max, heartbeat
        ));
        Ok(session)
    }

    fn send(&mut self, m: Method) -> Result<()> {
        self.conn.send(&Frame::method(CHANNEL, m))?;
        self.last_sent = Instant::now();
        Ok(())
    }

    /// The next frame on our channel before `deadline`: heartbeats both ways,
    /// and the broker closing the channel or connection is an error.
    fn recv_frame(&mut self, deadline: Option<Instant>) -> Result<Option<Frame>> {
        loop {
            let now = Instant::now();
            if let Some(hb) = self.heartbeat {
                // Two missed heartbeats: the broker is gone.
                if now.duration_since(self.last_read) >= hb * 2 {
                    bail_kind!(
                        Timeout,
                        "the broker missed two heartbeats ({}s); connection presumed dead",
                        hb.as_secs()
                    );
                }
            }
            let beat_at = self.heartbeat.map(|hb| self.last_sent + hb / 2);
            let dead_at = self.heartbeat.map(|hb| self.last_read + hb * 2);
            let wake = [deadline, beat_at, dead_at].into_iter().flatten().min();
            match self.conn.recv_until(wake)? {
                Some(frame) => {
                    self.last_read = Instant::now();
                    match frame {
                        Frame::Heartbeat => {}
                        Frame::Method {
                            method: Method::ConnectionClose(c),
                            ..
                        } => {
                            let _ = self.conn.send(&Frame::method(
                                0,
                                Method::ConnectionCloseOk(ConnectionCloseOk {}),
                            ));
                            self.closed = true;
                            return Err(reply_error(
                                "the broker closed the connection",
                                c.reply_code,
                                &c.reply_text,
                            ));
                        }
                        Frame::Method {
                            method: Method::ChannelClose(c),
                            ..
                        } => {
                            let _ = self.send(Method::ChannelCloseOk(ChannelCloseOk {}));
                            self.closed = true;
                            let _ = self.conn.send(&Frame::method(
                                0,
                                Method::ConnectionClose(ConnectionClose::default()),
                            ));
                            return Err(reply_error(
                                "the broker refused",
                                c.reply_code,
                                &c.reply_text,
                            ));
                        }
                        Frame::Method {
                            method: Method::ConnectionBlocked(b),
                            ..
                        } => {
                            self.conn
                                .trace()
                                .info(format_args!("the broker blocked publishing: {}", b.reason));
                        }
                        Frame::Method {
                            method: Method::ConnectionUnblocked(_),
                            ..
                        } => {}
                        other => return Ok(Some(other)),
                    }
                }
                None => {
                    let now = Instant::now();
                    if deadline.is_some_and(|d| now >= d) {
                        return Ok(None);
                    }
                    if beat_at.is_some_and(|t| now >= t) {
                        self.conn.send(&Frame::Heartbeat)?;
                        self.last_sent = Instant::now();
                    }
                }
            }
        }
    }

    /// The next frame: set-aside ones first.
    fn next_frame(&mut self, deadline: Option<Instant>) -> Result<Option<Frame>> {
        match self.pending.pop_front() {
            Some(f) => Ok(Some(f)),
            None => self.recv_frame(deadline),
        }
    }

    /// Wait for a method `pick` wants; everything else (deliveries and their
    /// content) is set aside, in order. A `basic.return`'s content is skipped:
    /// on one channel it follows the return directly.
    fn wait_method<T>(
        &mut self,
        what: &str,
        deadline: Option<Instant>,
        mut pick: impl FnMut(&mut Self, Method) -> Option<Result<T>>,
    ) -> Result<T> {
        let mut held = Vec::new();
        let result = loop {
            match self.next_frame(deadline)? {
                None => break Err(err!(Timeout, "no {what} from the broker")),
                Some(Frame::Method { method, .. }) => {
                    let returned = matches!(method, Method::BasicReturn(_));
                    let kept = method.clone();
                    match pick(self, method) {
                        Some(r) => break r,
                        None if returned => self.skip_content(deadline)?,
                        None => {
                            pub_core::limits::check_held(held.len() + self.pending.len())?;
                            held.push(Frame::method(CHANNEL, kept))
                        }
                    }
                }
                Some(other) => {
                    pub_core::limits::check_held(held.len() + self.pending.len())?;
                    held.push(other)
                }
            }
        };
        held.extend(self.pending.drain(..));
        self.pending = held.into();
        result
    }

    /// Skip a returned message's header and body frames.
    fn skip_content(&mut self, deadline: Option<Instant>) -> Result<()> {
        let size = match self.next_frame(deadline)? {
            Some(Frame::Header { body_size, .. }) => body_size,
            _ => bail_kind!(Protocol, "expected the returned message's content header"),
        };
        let mut seen = 0u64;
        while seen < size {
            match self.next_frame(deadline)? {
                Some(Frame::Body { data, .. }) => seen += data.len() as u64,
                _ => bail_kind!(Protocol, "the returned message's body is cut short"),
            }
        }
        Ok(())
    }

    /// Wait for a reply method; everything else is kept for later.
    fn expect(&mut self, what: &str, is: impl Fn(&Method) -> bool) -> Result<Method> {
        let deadline = self.conn.reply_deadline();
        self.wait_method(what, deadline, |_, m| is(&m).then(|| Ok(m)))
    }

    fn publish(
        &mut self,
        target: &Target,
        props: BasicProperties,
        body: &[u8],
        mandatory: bool,
    ) -> Result<()> {
        let (exchange, routing_key) = target.publish_to();
        let publish = Method::BasicPublish(BasicPublish {
            exchange: exchange.to_string(),
            routing_key: routing_key.to_string(),
            mandatory,
            ..Default::default()
        });
        self.conn.send(&Frame::method(CHANNEL, publish))?;
        self.conn.send(&Frame::Header {
            channel: CHANNEL,
            class_id: BASIC,
            body_size: body.len() as u64,
            properties: props,
        })?;
        let chunk = self.frame_max.saturating_sub(8).max(1);
        for piece in body.chunks(chunk) {
            self.conn.send(&Frame::Body {
                channel: CHANNEL,
                data: piece.to_vec(),
            })?;
        }
        self.last_sent = Instant::now();
        Ok(())
    }

    fn enable_confirms(&mut self) -> Result<()> {
        if self.confirming {
            return Ok(());
        }
        self.send(Method::ConfirmSelect(ConfirmSelect { nowait: false }))?;
        self.expect("confirm.select-ok", |m| {
            matches!(m, Method::ConfirmSelectOk(_))
        })?;
        self.confirming = true;
        Ok(())
    }

    /// Publisher confirms: the broker acks (stored, or routed for transient
    /// messages) or nacks each publish; a `basic.return` before the ack means
    /// nothing could route it.
    fn wait_for_confirm(&mut self, seq: u64, target: &Target) -> Result<()> {
        let deadline = self.conn.reply_deadline();
        let mut returned: Option<(u16, String)> = None;
        let display = target.display();
        self.wait_method("publisher confirm", deadline, |_, m| match m {
            Method::BasicReturn(r) => {
                returned = Some((r.reply_code, r.reply_text));
                None
            }
            Method::BasicAck(a)
                if a.delivery_tag == seq || (a.multiple && a.delivery_tag > seq) =>
            {
                Some(match returned.take() {
                    Some((code, text)) => Err(reply_error(
                        &format!("nothing could route the message to {display}"),
                        code,
                        &text,
                    )),
                    None => Ok(()),
                })
            }
            Method::BasicNack(n)
                if n.delivery_tag == seq || (n.multiple && n.delivery_tag > seq) =>
            {
                Some(Err(err!(
                    Protocol,
                    "the broker refused the message (basic.nack)"
                )))
            }
            _ => None,
        })
    }

    fn check_no_selector(&self, opts: &ReceiveOptions) -> Result<()> {
        if opts
            .selector
            .as_deref()
            .is_some_and(|s| !s.trim().is_empty())
        {
            bail_kind!(Usage, "AMQP 0-9-1 has no message selectors (bind with a routing key instead: /exchange/NAME/KEY)");
        }
        if !opts.headers.is_empty() {
            bail_kind!(Usage, "AMQP 0-9-1 subscriptions have no headers");
        }
        Ok(())
    }

    /// The queue to consume for a target: a queue as is; an exchange through a
    /// private queue (or a named durable one for a durable subscription) bound
    /// with the routing key.
    fn consume_queue(&mut self, target: &Target, durable: Option<&str>) -> Result<String> {
        match (target, durable) {
            (Target::Queue(_), Some(_)) => bail_kind!(
                Usage,
                "--durable is for exchanges (/exchange/NAME/KEY or /topic/KEY); a queue already keeps its messages"
            ),
            (Target::Queue(q), None) => Ok(q.clone()),
            (Target::Exchange { name, routing_key }, sub) => {
                self.send(Method::QueueDeclare(QueueDeclare {
                    queue: sub.unwrap_or("").to_string(),
                    durable: sub.is_some(),
                    exclusive: sub.is_none(),
                    auto_delete: sub.is_none(),
                    arguments: self.queue_arguments(),
                    ..Default::default()
                }))?;
                let Method::QueueDeclareOk(ok) = self.expect("queue.declare-ok", |m| matches!(m, Method::QueueDeclareOk(_)))? else {
                    unreachable!()
                };
                self.send(Method::QueueBind(QueueBind {
                    queue: ok.queue.clone(),
                    exchange: name.clone(),
                    routing_key: routing_key.clone(),
                    ..Default::default()
                }))?;
                self.expect("queue.bind-ok", |m| matches!(m, Method::QueueBindOk(_)))?;
                Ok(ok.queue)
            }
        }
    }

    fn queue_arguments(&self) -> Table {
        match &self.queue_type {
            Some(t) => vec![(
                "x-queue-type".into(),
                FieldValue::LongStr(t.as_bytes().to_vec()),
            )],
            None => Vec::new(),
        }
    }

    /// Consume `queue`, handing each delivery to `handle`; with a transaction
    /// batch, acks (and anything `handle` publishes) commit together.
    fn consume(
        &mut self,
        queue: &str,
        opts: &ReceiveOptions,
        mut handle: impl FnMut(&mut Self, Incoming) -> Result<()>,
    ) -> Result<usize> {
        let prefetch = match (self.follows_count, opts.count) {
            (true, Some(n)) => (n.min(u16::MAX as usize) as u16).min(self.prefetch).max(1),
            _ => self.prefetch,
        };
        let batch = opts.transaction;
        if batch.is_some() {
            Session::begin(self)?;
        }
        self.send(Method::BasicQos(BasicQos {
            prefetch_count: prefetch,
            ..Default::default()
        }))?;
        self.expect("basic.qos-ok", |m| matches!(m, Method::BasicQosOk(_)))?;
        // Always acked by pub: with no-ack RabbitMQ ignores basic.qos and pushes
        // everything, which `-n` would then lose. `--no-ack` acks on arrival.
        self.send(Method::BasicConsume(BasicConsume {
            queue: queue.to_string(),
            no_ack: false,
            ..Default::default()
        }))?;
        let Method::BasicConsumeOk(ok) = self.expect("basic.consume-ok", |m| {
            matches!(m, Method::BasicConsumeOk(_))
        })?
        else {
            unreachable!()
        };
        let tag = ok.consumer_tag;
        self.conn
            .trace()
            .info(format_args!("consuming {queue} (prefetch {prefetch})"));

        let idle = opts.idle;
        let deadline = opts.timeout.map(|t| Instant::now() + t);
        let mut idle_until = idle.map(|d| Instant::now() + d);
        let mut received = 0usize;
        let mut current: Option<Incoming> = None;
        let result = (|| -> Result<()> {
            while opts.count.is_none_or(|c| received < c) {
                let wait = [deadline, idle_until].into_iter().flatten().min();
                let Some(frame) = self.next_frame(wait)? else {
                    break;
                };
                match frame {
                    Frame::Method {
                        method: Method::BasicDeliver(d),
                        ..
                    } if d.consumer_tag == tag => {
                        current = Some(Incoming {
                            exchange: d.exchange,
                            routing_key: d.routing_key,
                            redelivered: d.redelivered,
                            delivery_tag: d.delivery_tag,
                            properties: None,
                            size: 0,
                            body: Vec::new(),
                        });
                    }
                    Frame::Method {
                        method: Method::BasicCancel(c),
                        ..
                    } if c.consumer_tag == tag => {
                        bail_kind!(
                            Protocol,
                            "the broker cancelled the consumer (the queue was deleted?)"
                        );
                    }
                    Frame::Header {
                        properties,
                        body_size,
                        ..
                    } => {
                        if let Some(inc) = current.as_mut() {
                            if body_size > self.conn.limits().max_payload as u64 {
                                bail_kind!(
                                    Protocol,
                                    "message of {body_size} bytes exceeds max-payload"
                                );
                            }
                            inc.properties = Some(properties);
                            inc.size = body_size;
                        }
                    }
                    Frame::Body { data, .. } => {
                        let Some(inc) = current.as_mut().filter(|i| i.properties.is_some()) else {
                            bail_kind!(
                                Protocol,
                                "content body frame without a delivery and header"
                            );
                        };
                        if inc.body.len() as u64 + data.len() as u64 > inc.size {
                            bail_kind!(
                                Protocol,
                                "content body longer than its header's {} bytes",
                                inc.size
                            );
                        }
                        inc.body.extend_from_slice(&data);
                    }
                    _ => {}
                }
                let complete = current
                    .as_ref()
                    .is_some_and(|i| i.properties.is_some() && i.body.len() as u64 >= i.size);
                if complete {
                    let inc = current.take().unwrap();
                    let tag = inc.delivery_tag;
                    let ack = Method::BasicAck(BasicAck {
                        delivery_tag: tag,
                        multiple: false,
                    });
                    if !opts.ack {
                        self.send(ack.clone())?;
                    }
                    handle(self, inc)?;
                    if opts.ack {
                        self.send(ack)?;
                    }
                    received += 1;
                    idle_until = idle.map(|d| Instant::now() + d);
                    if let Some(n) = batch.filter(|n| *n > 0) {
                        if received.is_multiple_of(n) {
                            Session::commit(self)?;
                            Session::begin(self)?;
                        }
                    }
                }
            }
            if batch.is_some() {
                Session::commit(self)?;
            }
            Ok(())
        })();
        if result.is_err() && self.in_tx {
            let _ = Session::rollback(self);
        }
        // Stop the consumer; deliveries it had prefetched return to the queue
        // when the channel closes.
        if !self.closed {
            let _ = self.send(Method::BasicCancel(BasicCancel {
                consumer_tag: tag.clone(),
                no_wait: false,
            }));
            let _ = self.expect("basic.cancel-ok", |m| matches!(m, Method::BasicCancelOk(_)));
        }
        result?;
        Ok(received)
    }
}

impl Session for RampqSession {
    fn info(&self) -> Vec<(String, String)> {
        let s = |k: &str| table_str(&self.server_properties, k);
        let mut info = vec![("version".to_string(), "0-9-1".to_string())];
        if let Some(p) = s("product") {
            info.push((
                "product".into(),
                format!("{p} {}", s("version").unwrap_or_default()),
            ));
        }
        if let Some(c) = s("cluster_name") {
            info.push(("cluster".into(), c));
        }
        info.push(("vhost".into(), self.vhost.clone()));
        info.push(("sasl".into(), self.mechanism.clone()));
        info.push(("frame-max".into(), self.frame_max.to_string()));
        info.push((
            "heartbeat".into(),
            self.heartbeat
                .map_or("off".into(), |h| format!("{}s", h.as_secs())),
        ));
        info
    }

    fn send(&mut self, dest: &Destination, msg: &Message) -> Result<()> {
        let target = Target::parse(&dest.name)?;
        if self.use_confirms && !self.in_tx && !self.tx_selected {
            self.enable_confirms()?;
        }
        self.next_message += 1;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let id = format!(
            "ID:pub-{}-{}-{}",
            std::process::id(),
            now.as_millis(),
            self.next_message
        );
        let props = message::to_properties(msg, Some(id.clone()))?;
        let mandatory = self.mandatory.unwrap_or(matches!(target, Target::Queue(_)));
        self.publish(&target, props, &msg.body, mandatory)?;
        if self.confirming {
            self.publish_seq += 1;
            let seq = self.publish_seq;
            self.wait_for_confirm(seq, &target)?;
        }
        self.conn.trace().info(format_args!(
            "sent {id} to {}{}",
            target.display(),
            if self.confirming {
                " (confirmed)"
            } else if self.in_tx {
                " (in transaction)"
            } else {
                ""
            }
        ));
        Ok(())
    }

    fn receive(
        &mut self,
        dest: &Destination,
        opts: &ReceiveOptions,
        sink: &mut dyn FnMut(Message) -> Result<()>,
    ) -> Result<usize> {
        self.check_no_selector(opts)?;
        let target = Target::parse(&dest.name)?;
        let queue = self.consume_queue(&target, opts.subscription.as_deref())?;
        self.consume(&queue, opts, |_, inc| {
            let props = inc.properties.unwrap_or_default();
            sink(message::from_delivery(
                &inc.exchange,
                &inc.routing_key,
                inc.redelivered,
                &props,
                inc.body,
            ))
        })
    }

    /// A durable subscription is a named durable queue: deleting it unsubscribes.
    /// RabbitMQ deletes missing queues happily, so check it exists first.
    fn unsubscribe(&mut self, _dest: &Destination, name: &str) -> Result<()> {
        self.send(Method::QueueDeclare(QueueDeclare {
            queue: name.to_string(),
            passive: true,
            ..Default::default()
        }))?;
        self.expect("queue.declare-ok", |m| {
            matches!(m, Method::QueueDeclareOk(_))
        })
        .map_err(|e| e.context(format!("no durable subscription (queue) '{name}'")))?;
        self.send(Method::QueueDelete(QueueDelete {
            queue: name.to_string(),
            ..Default::default()
        }))?;
        self.expect("queue.delete-ok", |m| matches!(m, Method::QueueDeleteOk(_)))?;
        self.conn
            .trace()
            .info(format_args!("deleted subscription queue {name}"));
        Ok(())
    }

    fn begin(&mut self) -> Result<()> {
        if self.in_tx {
            bail_kind!(Usage, "a transaction is already open");
        }
        if self.confirming {
            bail_kind!(Usage, "this channel already uses publisher confirms; RabbitMQ can't mix them with transactions");
        }
        if !self.tx_selected {
            self.send(Method::TxSelect(TxSelect {}))?;
            self.expect("tx.select-ok", |m| matches!(m, Method::TxSelectOk(_)))?;
            self.tx_selected = true;
        }
        self.in_tx = true;
        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        if !self.in_tx {
            bail_kind!(Usage, "no transaction is open to commit");
        }
        self.in_tx = false;
        self.send(Method::TxCommit(TxCommit {}))?;
        // A message nobody could route comes back before the commit-ok.
        let mut returned = None;
        let deadline = self.conn.reply_deadline();
        self.wait_method("tx.commit-ok", deadline, |_, m| match m {
            Method::TxCommitOk(_) => Some(Ok(())),
            Method::BasicReturn(r) => {
                returned = Some(r);
                None
            }
            _ => None,
        })?;
        if let Some(r) = returned {
            return Err(reply_error(
                &format!(
                    "committed, but nothing could route a message to {}/{}",
                    r.exchange, r.routing_key
                ),
                r.reply_code,
                &r.reply_text,
            ));
        }
        self.conn
            .trace()
            .info(format_args!("transaction committed"));
        Ok(())
    }

    fn rollback(&mut self) -> Result<()> {
        if !self.in_tx {
            bail_kind!(Usage, "no transaction is open to roll back");
        }
        self.in_tx = false;
        self.send(Method::TxRollback(TxRollback {}))?;
        self.expect("tx.rollback-ok", |m| matches!(m, Method::TxRollbackOk(_)))?;
        self.conn
            .trace()
            .info(format_args!("transaction rolled back"));
        Ok(())
    }

    /// Move: consume the source and republish each message (same properties)
    /// to the target as a mandatory publish with a publisher confirm; the
    /// original is acked only after the broker confirmed the copy. RabbitMQ
    /// reports an unroutable publish in a transaction only at commit, after
    /// the ack has gone too, so move doesn't use one.
    fn move_messages(
        &mut self,
        from: &Destination,
        to: &Destination,
        opts: &ReceiveOptions,
        moved: &mut dyn FnMut(Message) -> Result<()>,
    ) -> Result<usize> {
        self.check_no_selector(opts)?;
        let source = Target::parse(&from.name)?;
        let target = Target::parse(&to.name)?;
        if source == target {
            bail_kind!(Usage, "moving {} to itself", from.name);
        }
        if self.tx_selected {
            bail_kind!(
                Usage,
                "move needs publisher confirms, and this channel uses transactions"
            );
        }
        self.enable_confirms()?;
        let queue = self.consume_queue(&source, opts.subscription.as_deref())?;
        let opts = ReceiveOptions {
            transaction: None,
            ack: true,
            ..opts.clone()
        };
        self.consume(&queue, &opts, |s, inc| {
            let props = inc.properties.unwrap_or_default();
            s.publish(&target, props.clone(), &inc.body, true)?;
            s.publish_seq += 1;
            let seq = s.publish_seq;
            s.wait_for_confirm(seq, &target)?;
            moved(message::from_delivery(
                &inc.exchange,
                &inc.routing_key,
                inc.redelivered,
                &props,
                inc.body,
            ))
        })
    }

    fn list_destinations(
        &mut self,
        _kinds: DestinationKinds,
        _wait: Duration,
    ) -> Result<Vec<String>> {
        bail_kind!(
            Unsupported,
            "AMQP 0-9-1 can't list queues and exchanges; RabbitMQ's HTTP API (rabbitmqadmin list queues) can"
        )
    }

    fn create_destination(&mut self, dest: &Destination) -> Result<()> {
        match Target::parse(&dest.name)? {
            Target::Queue(q) => {
                self.send(Method::QueueDeclare(QueueDeclare {
                    queue: q.clone(),
                    durable: true,
                    arguments: self.queue_arguments(),
                    ..Default::default()
                }))?;
                self.expect("queue.declare-ok", |m| {
                    matches!(m, Method::QueueDeclareOk(_))
                })?;
            }
            Target::Exchange { name, .. } => {
                self.send(Method::ExchangeDeclare(ExchangeDeclare {
                    exchange: name,
                    kind: self.exchange_type.clone(),
                    durable: true,
                    ..Default::default()
                }))?;
                self.expect("exchange.declare-ok", |m| {
                    matches!(m, Method::ExchangeDeclareOk(_))
                })?;
            }
        }
        Ok(())
    }

    fn delete_destination(&mut self, dest: &Destination) -> Result<()> {
        match Target::parse(&dest.name)? {
            Target::Queue(q) => {
                self.send(Method::QueueDelete(QueueDelete {
                    queue: q,
                    ..Default::default()
                }))?;
                self.expect("queue.delete-ok", |m| matches!(m, Method::QueueDeleteOk(_)))?;
            }
            Target::Exchange { name, .. } => {
                self.send(Method::ExchangeDelete(ExchangeDelete {
                    exchange: name,
                    ..Default::default()
                }))?;
                self.expect("exchange.delete-ok", |m| {
                    matches!(m, Method::ExchangeDeleteOk(_))
                })?;
            }
        }
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        if self.in_tx {
            let _ = self.send(Method::TxRollback(TxRollback {}));
        }
        let _ = self.send(Method::ChannelClose(ChannelClose {
            reply_code: 200,
            reply_text: "bye".into(),
            ..Default::default()
        }));
        let _ = self.conn.send(&Frame::method(
            0,
            Method::ConnectionClose(ConnectionClose {
                reply_code: 200,
                reply_text: "bye".into(),
                ..Default::default()
            }),
        ));
        let deadline =
            Some(Instant::now() + Duration::from_secs(5)).min(self.conn.reply_deadline());
        loop {
            match self.conn.recv_until(deadline) {
                Ok(Some(Frame::Method {
                    method: Method::ConnectionCloseOk(_),
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

impl Drop for RampqSession {
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
    fn reply_codes_map_to_exit_codes() {
        let code = |c: u16| pub_core::exit_code(&reply_error("x", c, "why"));
        assert_eq!(code(403), 67);
        assert_eq!(code(530), 67);
        assert_eq!(code(404), 2);
        assert_eq!(code(312), 2);
        assert_eq!(code(320), 8);
    }

    #[test]
    fn mechanism_choice() {
        let ep = |url: &str| {
            let mut e = Endpoint::parse(url).unwrap();
            e.apply_credentials().unwrap();
            e
        };
        assert_eq!(
            choose_mechanism(&ep("rampq://u:p@h"), &["AMQPLAIN", "PLAIN"]).unwrap(),
            "PLAIN"
        );
        assert_eq!(
            choose_mechanism(&ep("rampq://h"), &["PLAIN", "ANONYMOUS"]).unwrap(),
            "ANONYMOUS"
        );
        let e = choose_mechanism(&ep("rampq://h"), &["PLAIN", "AMQPLAIN"]).unwrap_err();
        assert_eq!(pub_core::exit_code(&e), 67);
        assert!(e.to_string().contains("-u user"), "{e}");
    }
}
