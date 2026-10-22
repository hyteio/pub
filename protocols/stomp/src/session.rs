//! The STOMP conversation: CONNECT, SEND (+RECEIPT), SUBSCRIBE/ACK, DISCONNECT.
//!
//! Heart-beats (STOMP 1.1+): pub offers `heart-beat` both ways; each side then
//! uses the larger of what one offers and the other wants. While waiting, pub
//! sends a bare EOL when it has been quiet that long, and gives up when the
//! broker is silent for twice its interval.
//!
//! Broker extensions (ActiveMQ / Artemis): browsing (`browser:true`, Classic
//! only, ended by a `browser:end` message) and durable subscriptions
//! (`activemq.subscriptionName` / `durable-subscription-name`, with a
//! `client-id` on CONNECT).
//!
//! Transactions: BEGIN / COMMIT / ABORT (each with a receipt); SENDs and
//! ACKs in between carry the `transaction` header. `move` subscribes to the
//! source and, per message, SENDs a copy and ACKs the original in one
//! transaction per batch. STOMP can't set a message id, so the broker gives
//! each copy a new one; the other headers are copied as they arrived.
//!
//! `receive-timeout` bounds the wait for CONNECTED and each RECEIPT.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use anyhow::Context as _;

use pub_core::{
    bail_kind, BodyType, Connection, Context, Destination, Endpoint, Message, ReceiveOptions,
    Result, Session,
};

use crate::codec::{Frame, StompCodec};
use crate::mapping;

/// DISCONNECT is a courtesy; never hold the user up long for its receipt.
const MAX_DISCONNECT_WAIT: Duration = Duration::from_secs(2);

/// MESSAGE headers that describe this delivery, not the message: not copied
/// by `move`.
const DELIVERY_HEADERS: &[&str] = &[
    "message-id",
    "destination",
    "subscription",
    "ack",
    "redelivered",
    "content-length",
    "receipt",
    "transaction",
];

pub struct StompSession {
    conn: Connection<StompCodec>,
    version: String,
    connected: Frame,
    receipts: bool,
    next_id: u64,
    closed: bool,
    explicit_client_id: bool,
    /// Send a heart-beat after this much quiet.
    send_every: Option<Duration>,
    /// The broker promised a frame or heart-beat at least this often.
    expect_every: Option<Duration>,
    last_sent: Instant,
    last_read: Instant,
    heartbeats_seen: u64,
    /// The open transaction's id.
    tx: Option<String>,
    /// MESSAGEs that arrived while waiting for a RECEIPT.
    pending: VecDeque<Frame>,
}

/// `heart-beat: cx,cy` in milliseconds.
fn parse_heart_beat(v: Option<&str>) -> (u64, u64) {
    let mut it = v
        .unwrap_or("0,0")
        .split(',')
        .map(|n| n.trim().parse::<u64>().unwrap_or(0));
    (it.next().unwrap_or(0), it.next().unwrap_or(0))
}

impl StompSession {
    pub fn connect(ep: &Endpoint, ctx: &Context) -> Result<Self> {
        let mut trace = ctx.trace.clone();
        if let Some(p) = &ep.password {
            trace = trace.redacting(p);
        }
        let receipts = ep.prop_bool("receipts", true)?;
        let heart_beat = match ep.prop("heart-beat") {
            None => Duration::from_secs(10),
            Some(v) => pub_core::endpoint::parse_duration(v).ok_or_else(|| {
                pub_core::err!(
                    Config,
                    "heart-beat must be a duration (e.g. 10s), got '{v}'"
                )
            })?,
        };
        let hb_ms = heart_beat.as_millis().min(u32::MAX as u128) as u64;
        let mut conn = Connection::open(ep, &ctx.scheme, trace, StompCodec::default())?;
        conn.codec.max_frame = conn.limits().max_frame_size;

        let mut connect = Frame::new("CONNECT")
            .header("accept-version", "1.0,1.1,1.2")
            .header("host", ep.prop("vhost").unwrap_or(&ep.host))
            .header("heart-beat", format!("{hb_ms},{hb_ms}"));
        if let Some(u) = &ep.username {
            connect = connect.header("login", u.as_str());
        }
        if let Some(p) = &ep.password {
            connect = connect.header("passcode", p.as_str());
        }
        if let Some(id) = ep.prop("client-id") {
            connect = connect.header("client-id", id);
        }

        let now = Instant::now();
        let mut session = StompSession {
            conn,
            version: "1.0".into(),
            connected: Frame::default(),
            receipts,
            next_id: 0,
            closed: false,
            explicit_client_id: ep.prop("client-id").is_some(),
            send_every: None,
            expect_every: None,
            last_sent: now,
            last_read: now,
            heartbeats_seen: 0,
            tx: None,
            pending: VecDeque::new(),
        };
        session.conn.send(&connect)?;
        let reply = session
            .conn
            .recv_reply("CONNECTED frame")
            .context("waiting for CONNECTED")?;
        match reply.command.as_str() {
            "CONNECTED" => {}
            "ERROR" => bail_kind!(Auth, "connection refused by server: {}", reply.error_text()),
            other => bail_kind!(Protocol, "expected CONNECTED, got {other}"),
        }
        session.version = reply.get("version").unwrap_or("1.0").to_string();
        if !matches!(session.version.as_str(), "1.0" | "1.1" | "1.2") {
            bail_kind!(
                Protocol,
                "the server chose STOMP version {}, which pub doesn't speak",
                session.version
            );
        }
        session.conn.codec.escape = session.version != "1.0";
        if session.version != "1.0" {
            let (sx, sy) = parse_heart_beat(reply.get("heart-beat"));
            let ms = |n: u64| Duration::from_millis(n);
            session.send_every = (hb_ms > 0 && sy > 0).then(|| ms(hb_ms.max(sy)));
            session.expect_every = (sx > 0 && hb_ms > 0).then(|| ms(sx.max(hb_ms)));
        }
        session.conn.trace().info(format_args!(
            "STOMP {} session established",
            session.version
        ));
        session.connected = reply;
        Ok(session)
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    fn send(&mut self, f: &Frame) -> Result<()> {
        self.conn.send(f)?;
        self.last_sent = Instant::now();
        Ok(())
    }

    /// The next frame before `deadline`, with heart-beats both ways.
    fn recv(&mut self, deadline: Option<Instant>) -> Result<Option<Frame>> {
        match self.pending.pop_front() {
            Some(f) => Ok(Some(f)),
            None => self.recv_wire(deadline),
        }
    }

    fn recv_wire(&mut self, deadline: Option<Instant>) -> Result<Option<Frame>> {
        loop {
            let beat_at = self.send_every.map(|e| self.last_sent + e);
            let dead_at = self.expect_every.map(|e| self.last_read + e * 2);
            let wake = [deadline, beat_at, dead_at].into_iter().flatten().min();
            let got = self.conn.recv_until(wake)?;
            let now = Instant::now();
            if self.conn.codec.heartbeats_seen != self.heartbeats_seen {
                self.heartbeats_seen = self.conn.codec.heartbeats_seen;
                self.last_read = now;
            }
            if let Some(f) = got {
                self.last_read = now;
                return Ok(Some(f));
            }
            if deadline.is_some_and(|d| now >= d) {
                return Ok(None);
            }
            if let Some(every) = self.expect_every {
                if now.duration_since(self.last_read) >= every * 2 {
                    bail_kind!(
                        Timeout,
                        "the broker sent nothing for {}ms (two heart-beats); connection presumed dead",
                        (every * 2).as_millis()
                    );
                }
            }
            if beat_at.is_some_and(|t| now >= t) {
                self.send(&Frame::heartbeat())?;
            }
        }
    }

    fn is_classic(&self) -> bool {
        self.connected
            .get("server")
            .is_some_and(|s| s.starts_with("ActiveMQ/"))
    }

    fn need_client_id(&self) -> Result<()> {
        if !self.explicit_client_id {
            bail_kind!(
                Usage,
                "durable subscriptions need a fixed client-id (sent on CONNECT), e.g. -O client-id=orders-app"
            );
        }
        Ok(())
    }

    /// A frame that belongs to the open transaction, if there is one.
    fn in_tx(&self, mut frame: Frame) -> Frame {
        if let Some(tx) = &self.tx {
            frame.headers.push(("transaction".into(), tx.clone()));
        }
        frame
    }

    /// A send with a receipt, waiting for it (or the ERROR that refuses it).
    fn send_with_receipt(&mut self, mut frame: Frame) -> Result<()> {
        let id = self.next("req");
        frame.headers.push(("receipt".into(), id.clone()));
        self.send(&frame)?;
        let deadline = self.conn.reply_deadline();
        self.await_receipt(&id, deadline)
    }

    /// BEGIN, COMMIT or ABORT, confirmed by a receipt unless receipts are off.
    fn tx_frame(&mut self, command: &str, id: &str) -> Result<()> {
        let mut frame = Frame::new(command).header("transaction", id);
        let what = format!("{} of transaction {id}", command.to_ascii_lowercase());
        if !self.receipts {
            return self.send(&frame);
        }
        let receipt = self.next("req");
        frame.headers.push(("receipt".into(), receipt.clone()));
        self.send(&frame)?;
        // An ABORT usually follows an error: don't wait long on a broker that may be gone.
        let mut deadline = self.conn.reply_deadline();
        if command == "ABORT" {
            let cap = Instant::now() + MAX_DISCONNECT_WAIT;
            deadline = Some(deadline.map_or(cap, |d| d.min(cap)));
        }
        self.await_receipt(&receipt, deadline)
            .map_err(|e| e.context(what))
    }

    fn next(&mut self, prefix: &str) -> String {
        self.next_id += 1;
        format!("pub-{prefix}-{}", self.next_id)
    }

    /// Wait for the RECEIPT matching `id`, failing on ERROR.
    fn await_receipt(&mut self, id: &str, deadline: Option<Instant>) -> Result<()> {
        loop {
            let Some(f) = self.recv_wire(deadline)? else {
                bail_kind!(
                    Timeout,
                    "no RECEIPT for {id} from the server (receive-timeout)"
                );
            };
            match f.command.as_str() {
                "RECEIPT" if f.get("receipt-id") == Some(id) => return Ok(()),
                "ERROR" => bail_kind!(Protocol, "server error: {}", f.error_text()),
                // Deliveries keep coming while a COMMIT is confirmed.
                "MESSAGE" => {
                    pub_core::limits::check_held(self.pending.len())?;
                    self.pending.push_back(f)
                }
                _ => {}
            }
        }
    }

    fn ack(
        &mut self,
        message_id: Option<&str>,
        ack_id: Option<&str>,
        subscription: &str,
    ) -> Result<()> {
        let message_id = message_id.unwrap_or_default().to_string();
        if message_id.is_empty() && ack_id.is_none_or(str::is_empty) {
            bail_kind!(
                Protocol,
                "a MESSAGE without a message-id (or ack) header can't be acknowledged"
            );
        }
        let ack = match self.version.as_str() {
            "1.2" => Frame::new("ACK").header("id", ack_id.unwrap_or(&message_id)),
            "1.1" => Frame::new("ACK")
                .header("message-id", &*message_id)
                .header("subscription", subscription),
            _ => Frame::new("ACK").header("message-id", message_id),
        };
        let ack = self.in_tx(ack);
        self.send(&ack)
    }
}

impl StompSession {
    /// SUBSCRIBE and hand messages on: a plain receive, a durable
    /// subscription (`opts.subscription`), or a browse. With
    /// `opts.transaction`, ACKs (and the copies `forward` sends) commit in
    /// batches; an error rolls back the open batch.
    fn subscription(
        &mut self,
        destination: &Destination,
        opts: &ReceiveOptions,
        browse: bool,
        forward: Option<&Destination>,
        sink: &mut dyn FnMut(Message) -> Result<()>,
    ) -> Result<usize> {
        let batch = if browse { None } else { opts.transaction };
        if batch.is_some() {
            Session::begin(self)?;
        }
        let result = self.consume(destination, opts, browse, batch, forward, sink);
        if result.is_err() && self.tx.is_some() {
            let _ = Session::rollback(self);
        }
        result
    }

    fn consume(
        &mut self,
        destination: &Destination,
        opts: &ReceiveOptions,
        browse: bool,
        batch: Option<usize>,
        forward: Option<&Destination>,
        sink: &mut dyn FnMut(Message) -> Result<()>,
    ) -> Result<usize> {
        let durable = if browse {
            None
        } else {
            opts.subscription.clone()
        };
        if durable.is_some() {
            self.need_client_id()?;
            if !destination.name.starts_with("/topic/") {
                bail_kind!(
                    Usage,
                    "durable subscriptions are for topics (/topic/NAME); {} isn't one",
                    destination.name
                );
            }
        }
        let sub_id = self.next("sub");
        // A browse doesn't consume, so there is nothing to acknowledge.
        let ack_mode = match (opts.ack && !browse, self.version.as_str()) {
            (false, _) => "auto",
            (true, "1.0") => "client",
            (true, _) => "client-individual",
        };
        let mut subscribe = Frame::new("SUBSCRIBE")
            .header("id", sub_id.as_str())
            .header("destination", destination.name.as_str())
            .header("ack", ack_mode);
        if let Some(sel) = &opts.selector {
            subscribe.headers.push(("selector".into(), sel.clone()));
        }
        if browse {
            subscribe.headers.push(("browser".into(), "true".into()));
        }
        if let Some(name) = &durable {
            for h in ["activemq.subscriptionName", "durable-subscription-name"] {
                subscribe.headers.push((h.into(), name.clone()));
            }
        }
        subscribe
            .headers
            .extend(destination.options.iter().cloned());
        subscribe.headers.extend(opts.headers.iter().cloned());
        self.send(&subscribe)?;

        let deadline = opts.timeout.map(|t| Instant::now() + t);
        let mut idle_until = opts.idle.map(|d| Instant::now() + d);
        let mut received = 0;
        while opts.count.is_none_or(|c| received < c) {
            // A browse ends when the broker says so; without -m, don't wait
            // forever for that.
            let wait = match (deadline, browse) {
                (None, true) => self.conn.reply_deadline(),
                (d, _) => [d, idle_until].into_iter().flatten().min(),
            };
            let Some(f) = self.recv(wait)? else {
                if browse && deadline.is_none() {
                    bail_kind!(
                        Timeout,
                        "the broker stopped sending before the end of the browse"
                    );
                }
                break;
            };
            match f.command.as_str() {
                // A late delivery for an earlier subscription.
                "MESSAGE" if f.get("subscription").is_some_and(|s| s != sub_id) => {}
                "MESSAGE" if browse && f.get("browser") == Some("end") => break,
                "MESSAGE" => {
                    // Ack only after the message was written out: at-least-once.
                    let ack = (opts.ack && !browse).then(|| {
                        (
                            f.get("message-id").map(str::to_string),
                            f.get("ack").map(str::to_string),
                        )
                    });
                    if let Some(to) = forward {
                        let copy = self.in_tx(copy_of(&f, to));
                        self.send(&copy)?;
                    }
                    sink(mapping::message_from_frame(f))?;
                    if let Some((message_id, ack_id)) = ack {
                        self.ack(message_id.as_deref(), ack_id.as_deref(), &sub_id)?;
                    }
                    received += 1;
                    idle_until = opts.idle.map(|d| Instant::now() + d);
                    if let Some(n) = batch.filter(|n| *n > 0) {
                        if received.is_multiple_of(n) {
                            Session::commit(self)?;
                            Session::begin(self)?;
                        }
                    }
                }
                "ERROR" => bail_kind!(Protocol, "server error: {}", f.error_text()),
                _ => {}
            }
        }

        // A durable subscription stays (UNSUBSCRIBE could remove it); the
        // consumer goes with the connection.
        if durable.is_none() {
            self.send(&Frame::new("UNSUBSCRIBE").header("id", sub_id))?;
        }
        if batch.is_some() {
            Session::commit(self)?;
        }
        Ok(received)
    }
}

/// A SEND to `to` carrying `message`'s headers and body. Without
/// `content-length` the broker made it a TextMessage; so does the copy.
fn copy_of(message: &Frame, to: &Destination) -> Frame {
    let mut copy = Frame::new("SEND").header("destination", to.name.as_str());
    copy.headers.extend(
        message
            .headers
            .iter()
            .filter(|(k, _)| !DELIVERY_HEADERS.contains(&k.as_str()))
            .cloned(),
    );
    copy.headers.extend(to.options.iter().cloned());
    copy.body = message.body.clone();
    copy.omit_content_length =
        message.get("content-length").is_none() && !message.body.contains(&0);
    copy
}

impl Session for StompSession {
    fn info(&self) -> Vec<(String, String)> {
        let mut info = vec![("version".to_string(), self.version.clone())];
        for key in ["server", "session", "heart-beat"] {
            if let Some(v) = self.connected.get(key) {
                info.push((key.to_string(), v.to_string()));
            }
        }
        info
    }

    fn send(&mut self, destination: &Destination, msg: &Message) -> Result<()> {
        let mut frame = Frame::new("SEND").header("destination", destination.name.as_str());
        frame.headers.extend(destination.options.iter().cloned());
        frame.headers.extend(mapping::message_headers(msg));
        frame.body = msg.body.clone();
        // pub's own receipt and transaction headers must be the ones that count.
        frame
            .headers
            .retain(|(k, _)| k != "receipt" && k != "transaction");
        let mut frame = self.in_tx(frame);
        if msg.body_type == Some(BodyType::Text) {
            // No content-length: ActiveMQ and Artemis then create a TextMessage.
            if msg.body.contains(&0) {
                bail_kind!(Usage, "a text message over STOMP cannot contain NUL bytes (use -O message-type=bytes)");
            }
            frame.omit_content_length = true;
        }
        if !self.receipts {
            return self.send(&frame);
        }
        let id = self.next("send");
        frame.headers.push(("receipt".into(), id.clone()));
        self.send(&frame)?;
        let deadline = self.conn.reply_deadline();
        self.await_receipt(&id, deadline)
    }

    fn receive(
        &mut self,
        destination: &Destination,
        opts: &ReceiveOptions,
        sink: &mut dyn FnMut(Message) -> Result<()>,
    ) -> Result<usize> {
        self.subscription(destination, opts, false, None, sink)
    }

    /// ActiveMQ Classic browses (`browser:true`); others would consume the
    /// messages instead, so pub refuses.
    fn browse(
        &mut self,
        destination: &Destination,
        opts: &ReceiveOptions,
        sink: &mut dyn FnMut(Message) -> Result<()>,
    ) -> Result<usize> {
        if !self.is_classic() {
            bail_kind!(
                Unsupported,
                "only ActiveMQ browses over STOMP (this server is {}); browsing elsewhere would consume",
                self.connected.get("server").unwrap_or("unknown")
            );
        }
        if destination.name.starts_with("/topic/") {
            bail_kind!(
                Usage,
                "only queues can be browsed; {} is a topic",
                destination.name
            );
        }
        self.subscription(destination, opts, true, None, sink)
    }

    /// Remove a durable subscription: UNSUBSCRIBE with its name, which both
    /// ActiveMQ Classic and Artemis honour (for the connection's client-id).
    fn unsubscribe(&mut self, _destination: &Destination, name: &str) -> Result<()> {
        self.need_client_id()?;
        let id = self.next("unsub");
        self.send_with_receipt(
            Frame::new("UNSUBSCRIBE")
                .header("id", id)
                .header("activemq.subscriptionName", name)
                .header("durable-subscription-name", name),
        )
        .map_err(|e| e.context(format!("removing durable subscription '{name}'")))
    }

    fn begin(&mut self) -> Result<()> {
        if self.tx.is_some() {
            bail_kind!(Usage, "a transaction is already open");
        }
        let id = self.next("tx");
        self.tx_frame("BEGIN", &id)?;
        self.tx = Some(id);
        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        let Some(id) = self.tx.take() else {
            bail_kind!(Usage, "no transaction is open to commit");
        };
        self.tx_frame("COMMIT", &id)?;
        self.conn
            .trace()
            .info(format_args!("transaction {id} committed"));
        Ok(())
    }

    fn rollback(&mut self) -> Result<()> {
        let Some(id) = self.tx.take() else {
            bail_kind!(Usage, "no transaction is open to roll back");
        };
        self.tx_frame("ABORT", &id)?;
        self.conn
            .trace()
            .info(format_args!("transaction {id} rolled back"));
        Ok(())
    }

    /// Move: subscribe to `from` and, per message, SEND a copy to `to` and
    /// ACK the original, in one transaction per batch.
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
        let opts = ReceiveOptions {
            transaction: Some(
                opts.transaction
                    .unwrap_or(pub_core::module::DEFAULT_MOVE_BATCH),
            ),
            ack: true,
            ..opts.clone()
        };
        self.subscription(from, &opts, false, Some(to), moved)
    }

    fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        if let Some(tx) = self.tx.take() {
            let _ = self.send(&Frame::new("ABORT").header("transaction", tx));
        }
        let id = self.next("disconnect");
        // A clean DISCONNECT is a courtesy; don't fail the command over it.
        if self
            .send(&Frame::new("DISCONNECT").header("receipt", id.as_str()))
            .is_ok()
        {
            let cap = Instant::now() + MAX_DISCONNECT_WAIT;
            let deadline = self.conn.reply_deadline().map_or(cap, |d| d.min(cap));
            let _ = self.await_receipt(&id, Some(deadline));
        }
        self.conn.shutdown();
        Ok(())
    }
}

impl Drop for StompSession {
    fn drop(&mut self) {
        if !self.closed {
            self.conn.shutdown();
        }
    }
}
