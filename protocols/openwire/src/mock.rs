//! A small in-process OpenWire broker for tests (feature `mock`).
//!
//! Like Artemis, it waits for the client's WireFormatInfo before sending its
//! own, so a client that waits for the broker to speak first hangs here too.
//! Knobs simulate refused credentials, an older protocol version, loose-only
//! encoding, keep-alive traffic, a broker that goes silent, a refused
//! producer and a broker that withholds ProducerAcks (a full producer window).
//! Messages it receives are kept for inspection ([`MockBroker::messages`]) and
//! queued per destination for consumers (prefetch, MessagePull, acks, and
//! redelivery of unacknowledged messages). Topics behave like queues here.

use std::collections::{HashMap, VecDeque};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use pub_core::transport::Stream;
use pub_core::{Connection, Trace};

use crate::codec::OpenWireCodec;
use crate::primitive::{decode_map, encode_map, Primitive};
use crate::types::*;
use crate::wire::Encoding;

#[derive(Debug, Clone)]
pub struct MockConfig {
    pub credentials: Option<(String, String)>,
    pub version: i32,
    pub tight: bool,
    pub max_inactivity_ms: i64,
    /// Send a KeepAliveInfo this often while idle.
    pub keep_alive_every: Option<Duration>,
    /// After answering ConnectionInfo, never send anything again (a dead broker).
    pub silent_after_connect: bool,
    /// After answering the first ProducerInfo, never send anything again.
    pub silent_after_producer: bool,
    /// Refuse producers for this destination (e.g. "queue://secret") with a SecurityException.
    pub deny_producer: Option<String>,
    /// Acknowledge asynchronous sends of windowed producers (ProducerAck).
    pub producer_acks: bool,
}

impl Default for MockConfig {
    fn default() -> Self {
        MockConfig {
            credentials: None,
            version: 12,
            tight: true,
            max_inactivity_ms: 30_000,
            keep_alive_every: None,
            silent_after_connect: false,
            silent_after_producer: false,
            deny_producer: None,
            producer_acks: true,
        }
    }
}

#[derive(Default)]
struct State {
    log: Vec<DataStructure>,
    negotiated_tight: Option<bool>,
    /// By destination (`queue://x`); messages not yet dispatched or acknowledged.
    queues: HashMap<String, VecDeque<Message>>,
    next_seq: i64,
    /// Durable subscriptions by (client ID, name).
    subscriptions: std::collections::HashSet<(String, String)>,
    commits: usize,
    rollbacks: usize,
    /// Destinations the broker knows (`queue://x`, `topic://x`), as ActiveMQ
    /// ActiveMQ announces them on its advisory topics.
    destinations: std::collections::BTreeSet<String>,
}

pub struct MockBroker {
    pub addr: SocketAddr,
    state: Arc<Mutex<State>>,
}

impl MockBroker {
    pub fn start(cfg: MockConfig) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(State::default()));
        let st = state.clone();
        thread::spawn(move || {
            for tcp in listener.incoming().flatten() {
                let (st, cfg) = (st.clone(), cfg.clone());
                thread::spawn(move || {
                    let _ = serve(tcp, &st, &cfg);
                });
            }
        });
        MockBroker { addr, state }
    }

    pub fn url(&self, path: &str) -> String {
        format!("amq://{}{path}", self.addr)
    }

    /// Commands received from clients, by name (e.g. "ConnectionInfo").
    pub fn commands(&self, name: &str) -> Vec<DataStructure> {
        self.state
            .lock()
            .unwrap()
            .log
            .iter()
            .filter(|d| d.name() == name)
            .cloned()
            .collect()
    }

    /// Messages received, in order.
    pub fn messages(&self) -> Vec<Message> {
        self.state
            .lock()
            .unwrap()
            .log
            .iter()
            .filter_map(|d| match d {
                DataStructure::Message(m) => Some((**m).clone()),
                _ => None,
            })
            .collect()
    }

    /// Put a message on a destination (`queue://x`), as if a Java client had sent it.
    pub fn enqueue(&self, dest: &str, msg: Message) {
        enqueue(&self.state, dest, msg);
    }

    /// Destinations the broker knows, sorted.
    pub fn destinations(&self) -> Vec<String> {
        self.state
            .lock()
            .unwrap()
            .destinations
            .iter()
            .cloned()
            .collect()
    }

    /// Transactions committed and rolled back, over all connections.
    pub fn transactions(&self) -> (usize, usize) {
        let st = self.state.lock().unwrap();
        (st.commits, st.rollbacks)
    }

    /// Durable subscriptions as (client ID, name), sorted.
    pub fn subscriptions(&self) -> Vec<(String, String)> {
        let mut v: Vec<_> = self
            .state
            .lock()
            .unwrap()
            .subscriptions
            .iter()
            .cloned()
            .collect();
        v.sort();
        v
    }

    /// Messages waiting on a destination (not dispatched, or given back unacknowledged).
    pub fn queued(&self, dest: &str) -> Vec<Message> {
        self.state
            .lock()
            .unwrap()
            .queues
            .get(dest)
            .map(|q| q.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Whether the last connection negotiated tight encoding.
    pub fn negotiated_tight(&self) -> Option<bool> {
        self.state.lock().unwrap().negotiated_tight
    }
}

fn security_error(user: &str) -> BrokerError {
    BrokerError {
        class: Some("java.lang.SecurityException".into()),
        message: Some(format!("User name [{user}] or password is invalid.")),
        ..Default::default()
    }
}

fn serve(tcp: TcpStream, state: &Mutex<State>, cfg: &MockConfig) -> pub_core::Result<()> {
    let mut conn = Connection::new(
        Stream::Plain(tcp),
        OpenWireCodec::default(),
        Trace::default(),
    );

    // The client must speak first.
    let Some(DataStructure::WireFormatInfo(client)) = conn.recv(Some(Duration::from_secs(5)))?
    else {
        return Ok(());
    };
    let client_props = client
        .marshalled_properties
        .as_deref()
        .and_then(|b| decode_map(b).ok().flatten())
        .unwrap_or_default();
    state
        .lock()
        .unwrap()
        .log
        .push(DataStructure::WireFormatInfo(client));
    let props = vec![
        (
            "TightEncodingEnabled".to_string(),
            Primitive::Bool(cfg.tight),
        ),
        ("CacheEnabled".into(), Primitive::Bool(false)),
        ("StackTraceEnabled".into(), Primitive::Bool(false)),
        (
            "MaxInactivityDuration".into(),
            Primitive::Long(cfg.max_inactivity_ms),
        ),
        ("ProviderName".into(), Primitive::String("mock".into())),
        ("ProviderVersion".into(), Primitive::String("1.0".into())),
    ];
    conn.send(&DataStructure::WireFormatInfo(Box::new(WireFormatInfo {
        magic: MAGIC,
        version: cfg.version,
        marshalled_properties: Some(encode_map(&props).unwrap()),
    })))?;
    if cfg.version < 12 {
        return Ok(()); // the client refuses; nothing more to do
    }
    let client_tight = client_props
        .iter()
        .any(|(k, v)| k == "TightEncodingEnabled" && *v == Primitive::Bool(true));
    let tight = cfg.tight && client_tight;
    state.lock().unwrap().negotiated_tight = Some(tight);
    conn.codec.encoding = Encoding {
        tight,
        stack_traces: false,
    };

    let mut sess = MockSession::default();
    let id = sess.id();
    conn.send(&DataStructure::BrokerInfo(Box::new(BrokerInfo {
        command_id: id,
        broker_id: Some(Box::new(DataStructure::BrokerId(Box::new(BrokerId {
            value: Some("ID:mock-broker".into()),
        })))),
        broker_name: Some("mock".into()),
        broker_url: Some("tcp://mock:61616".into()),
        ..Default::default()
    })))?;
    let result = sess.run(&mut conn, state, cfg);
    // A dropped connection returns what it was given, counted as redelivered.
    for c in sess.consumers.drain(..) {
        requeue(state, c, i64::MAX);
    }
    result
}

struct MockConsumer {
    id: DataStructure,
    dest: String,
    destination: DataStructure,
    prefetch: i32,
    inflight: Vec<Message>,
    /// An outstanding MessagePull: `Some(None)` waits forever, `Some(Some(t))` until `t`.
    pull: Option<Option<Instant>>,
    /// A queue browser: copies of the queue taken when it started, the next one
    /// to send, and whether the end-of-browse dispatch went out.
    browse: Option<(Vec<Message>, usize, bool)>,
    /// A durable subscription's (client ID, name).
    durable: Option<(String, String)>,
}

fn broker_seq(m: &Message) -> i64 {
    match m.fields.message_id.as_deref() {
        Some(DataStructure::MessageId(id)) => id.broker_sequence_id,
        _ => 0,
    }
}

/// Put a consumer's unacknowledged messages back at the front of its queue;
/// those up to `last_delivered` were seen by the client and count as redelivered.
fn requeue(state: &Mutex<State>, c: MockConsumer, last_delivered: i64) {
    if c.browse.is_some() {
        return; // a browser's messages are copies; the queue never lost them
    }
    let mut st = state.lock().unwrap();
    let q = st.queues.entry(c.dest).or_default();
    for mut m in c.inflight.into_iter().rev() {
        if broker_seq(&m) <= last_delivered {
            m.fields.redelivery_counter += 1;
        }
        q.push_front(m);
    }
}

/// Work held back until its transaction commits.
enum TxOp {
    Send(String, Box<Message>),
    Ack(Box<MessageAck>),
}

#[derive(Default)]
struct MockSession {
    next_id: i32,
    windows: Vec<(DataStructure, i32)>,
    silent: bool,
    consumers: Vec<MockConsumer>,
    tx_pending: Vec<TxOp>,
    /// Advisory messages to send after the current reply.
    advisories: Vec<MessageDispatch>,
}

impl MockSession {
    /// A standard ack covers everything delivered to the consumer up to its last message.
    fn apply_ack(&mut self, a: &MessageAck) {
        if let Some(c) = self
            .consumers
            .iter_mut()
            .find(|c| Some(&c.id) == a.consumer_id.as_deref())
        {
            let last = a.last_message_id.as_deref();
            if let Some(pos) = c
                .inflight
                .iter()
                .position(|m| m.fields.message_id.as_deref() == last)
            {
                c.inflight.drain(..=pos);
            }
        }
    }

    fn id(&mut self) -> i32 {
        self.next_id += 1;
        self.next_id
    }

    fn response(&mut self, cmd_id: i32) -> DataStructure {
        DataStructure::Response(Box::new(Response {
            command_id: self.id(),
            correlation_id: cmd_id,
            ..Default::default()
        }))
    }

    fn exception(&mut self, cmd_id: i32, class: &str, message: String) -> DataStructure {
        DataStructure::ExceptionResponse(Box::new(ExceptionResponse {
            command_id: self.id(),
            correlation_id: cmd_id,
            exception: Some(BrokerError {
                class: Some(class.into()),
                message: Some(message),
                ..Default::default()
            }),
            ..Default::default()
        }))
    }

    fn run(
        &mut self,
        conn: &mut Connection<OpenWireCodec>,
        state: &Mutex<State>,
        cfg: &MockConfig,
    ) -> pub_core::Result<()> {
        let mut last_sent = Instant::now();
        loop {
            if !self.silent {
                self.dispatch(conn, state)?;
            }
            let cmd = match conn.recv(Some(Duration::from_millis(20))) {
                Err(_) => return Ok(()),
                Ok(c) => c,
            };
            let Some(cmd) = cmd else {
                if let (false, Some(every)) = (self.silent, cfg.keep_alive_every) {
                    if last_sent.elapsed() >= every {
                        let id = self.id();
                        conn.send(&DataStructure::KeepAliveInfo(Box::new(KeepAliveInfo {
                            command_id: id,
                            response_required: false,
                        })))?;
                        last_sent = Instant::now();
                    }
                }
                continue;
            };
            state.lock().unwrap().log.push(cmd.clone());
            let (cmd_id, wants_response) = cmd.command_header().unwrap_or((0, false));
            if self.silent {
                continue;
            }
            let reply = match cmd {
                DataStructure::ConnectionInfo(ci) => {
                    let ok = match &cfg.credentials {
                        None => true,
                        Some((u, p)) => {
                            ci.user_name.as_deref() == Some(u) && ci.password.as_deref() == Some(p)
                        }
                    };
                    self.silent = cfg.silent_after_connect;
                    if ok {
                        Some(self.response(cmd_id))
                    } else {
                        let user = ci.user_name.clone().unwrap_or_default();
                        let e = security_error(&user);
                        Some(DataStructure::ExceptionResponse(Box::new(
                            ExceptionResponse {
                                command_id: self.id(),
                                correlation_id: cmd_id,
                                exception: Some(e),
                                ..Default::default()
                            },
                        )))
                    }
                }
                DataStructure::KeepAliveInfo(_) if wants_response => {
                    Some(DataStructure::KeepAliveInfo(Box::new(KeepAliveInfo {
                        command_id: self.id(),
                        response_required: false,
                    })))
                }
                DataStructure::ShutdownInfo(_) => return Ok(()),
                DataStructure::ProducerInfo(pi) => {
                    let dest = pi
                        .destination
                        .as_deref()
                        .map(crate::producer::destination_text);
                    if dest.is_some() && dest == cfg.deny_producer {
                        let dest = dest.unwrap_or_default();
                        Some(self.exception(
                            cmd_id,
                            "java.lang.SecurityException",
                            format!("User anonymous is not authorized to write to: {dest}"),
                        ))
                    } else {
                        self.silent = cfg.silent_after_producer;
                        if let Some(p) = pi.producer_id.as_deref() {
                            self.windows.push((p.clone(), pi.window_size));
                        }
                        Some(self.response(cmd_id))
                    }
                }
                DataStructure::ConsumerInfo(ci) => {
                    let destination = ci
                        .destination
                        .as_deref()
                        .cloned()
                        .unwrap_or(DataStructure::Queue(Box::default()));
                    let dest = crate::producer::destination_text(&destination);
                    if ci.selector.as_deref().is_some_and(|s| s.contains("((")) {
                        Some(self.exception(
                            cmd_id,
                            "javax.jms.InvalidSelectorException",
                            format!(
                                "{} [unbalanced parentheses]",
                                ci.selector.as_deref().unwrap_or("")
                            ),
                        ))
                    } else {
                        let durable = ci
                            .subscription_name
                            .clone()
                            .map(|n| (ci.client_id.clone().unwrap_or_default(), n));
                        if let Some(key) = &durable {
                            state.lock().unwrap().subscriptions.insert(key.clone());
                        }
                        let browse = ci.browser.then(|| {
                            let snapshot = state
                                .lock()
                                .unwrap()
                                .queues
                                .get(&dest)
                                .map(|q| q.iter().cloned().collect())
                                .unwrap_or_default();
                            (snapshot, 0, false)
                        });
                        // ActiveMQ's advisory topics replay the known destinations.
                        let advisory_kind = match dest.as_str() {
                            "topic://ActiveMQ.Advisory.Queue" => Some("queue://"),
                            "topic://ActiveMQ.Advisory.Topic" => Some("topic://"),
                            _ => None,
                        };
                        if let Some(prefix) = advisory_kind {
                            let known: Vec<String> = state
                                .lock()
                                .unwrap()
                                .destinations
                                .iter()
                                .filter(|d| d.starts_with(prefix))
                                .cloned()
                                .collect();
                            for d in known {
                                let name = d[prefix.len()..].to_string();
                                let target = crate::producer::parse_destination(&d)
                                    .map(|x| x.0)
                                    .unwrap_or_else(|_| {
                                        DataStructure::Queue(Box::new(Destination {
                                            physical_name: Some(name),
                                        }))
                                    });
                                let info = DestinationInfo {
                                    destination: Some(Box::new(target)),
                                    operation_type: 0,
                                    ..Default::default()
                                };
                                let m = Message {
                                    kind: crate::types::kind::MESSAGE,
                                    fields: MessageFields {
                                        destination: Some(Box::new(destination.clone())),
                                        data_structure: Some(Box::new(
                                            DataStructure::DestinationInfo(Box::new(info)),
                                        )),
                                        ..Default::default()
                                    },
                                    blob: None,
                                };
                                self.advisories.push(MessageDispatch {
                                    consumer_id: ci.consumer_id.clone(),
                                    destination: Some(Box::new(destination.clone())),
                                    message: Some(Box::new(DataStructure::Message(Box::new(m)))),
                                    ..Default::default()
                                });
                            }
                        } else if !dest.contains("ActiveMQ.Advisory.") {
                            state.lock().unwrap().destinations.insert(dest.clone());
                        }
                        self.consumers.push(MockConsumer {
                            id: ci
                                .consumer_id
                                .as_deref()
                                .cloned()
                                .unwrap_or(DataStructure::ConsumerId(Box::default())),
                            dest,
                            destination,
                            prefetch: ci.prefetch_size,
                            inflight: Vec::new(),
                            pull: None,
                            browse,
                            durable,
                        });
                        Some(self.response(cmd_id))
                    }
                }
                DataStructure::MessagePull(p) => {
                    if let Some(c) = self
                        .consumers
                        .iter_mut()
                        .find(|c| Some(&c.id) == p.consumer_id.as_deref())
                    {
                        c.pull = Some(match p.timeout {
                            0 => None,
                            t => Some(Instant::now() + Duration::from_millis(t.max(0) as u64)),
                        });
                    }
                    None
                }
                DataStructure::MessageAck(a) => {
                    if a.transaction_id.is_some() {
                        self.tx_pending.push(TxOp::Ack(a));
                    } else {
                        self.apply_ack(&a);
                    }
                    wants_response.then(|| self.response(cmd_id))
                }
                DataStructure::TransactionInfo(t) => {
                    match t.kind {
                        tx::COMMIT_ONE_PHASE => {
                            for op in std::mem::take(&mut self.tx_pending) {
                                match op {
                                    TxOp::Send(dest, m) => enqueue(state, &dest, *m),
                                    TxOp::Ack(a) => self.apply_ack(&a),
                                }
                            }
                            state.lock().unwrap().commits += 1;
                        }
                        tx::ROLLBACK => {
                            self.tx_pending.clear();
                            state.lock().unwrap().rollbacks += 1;
                        }
                        _ => {}
                    }
                    wants_response.then(|| self.response(cmd_id))
                }
                DataStructure::DestinationInfo(d) => {
                    let dest = d
                        .destination
                        .as_deref()
                        .map(crate::producer::destination_text)
                        .unwrap_or_default();
                    let in_use = self.consumers.iter().any(|c| c.dest == dest);
                    let reply = if d.operation_type == 1 && in_use {
                        self.exception(
                            cmd_id,
                            "javax.jms.JMSException",
                            format!("Destination still has an active subscription: {dest}"),
                        )
                    } else {
                        let mut st = state.lock().unwrap();
                        if d.operation_type == 1 {
                            st.destinations.remove(&dest);
                            st.queues.remove(&dest);
                        } else {
                            st.destinations.insert(dest);
                        }
                        drop(st);
                        self.response(cmd_id)
                    };
                    wants_response.then_some(reply)
                }
                DataStructure::RemoveSubscriptionInfo(r) => {
                    let key = (
                        r.client_id.clone().unwrap_or_default(),
                        r.subscription_name.clone().unwrap_or_default(),
                    );
                    let active = self
                        .consumers
                        .iter()
                        .any(|c| c.durable.as_ref() == Some(&key));
                    let existed = !active && state.lock().unwrap().subscriptions.remove(&key);
                    Some(if active {
                        self.exception(
                            cmd_id,
                            "javax.jms.JMSException",
                            "Durable consumer is in use".into(),
                        )
                    } else if existed {
                        self.response(cmd_id)
                    } else {
                        self.exception(
                            cmd_id,
                            "javax.jms.InvalidDestinationException",
                            format!(
                                "No durable subscription exists for clientID: {} and subscriptionName: {}",
                                key.0, key.1
                            ),
                        )
                    })
                }
                DataStructure::RemoveInfo(r) => {
                    if let Some(pos) = self
                        .consumers
                        .iter()
                        .position(|c| Some(&c.id) == r.object_id.as_deref())
                    {
                        let c = self.consumers.remove(pos);
                        requeue(state, c, r.last_delivered_sequence_id);
                    }
                    wants_response.then(|| self.response(cmd_id))
                }
                DataStructure::Message(m) => {
                    let dest = m
                        .fields
                        .destination
                        .as_deref()
                        .map(crate::producer::destination_text)
                        .unwrap_or_default();
                    let pid = m.fields.producer_id.clone();
                    let size = crate::producer::message_size(&m) as i32;
                    if m.fields.transaction_id.is_some() {
                        self.tx_pending.push(TxOp::Send(dest, m));
                    } else {
                        enqueue(state, &dest, *m);
                    }
                    if wants_response {
                        Some(self.response(cmd_id))
                    } else {
                        let windowed = self
                            .windows
                            .iter()
                            .any(|(p, w)| Some(p) == pid.as_deref() && *w > 0);
                        (windowed && cfg.producer_acks).then(|| {
                            DataStructure::ProducerAck(Box::new(ProducerAck {
                                command_id: self.id(),
                                response_required: false,
                                producer_id: pid,
                                size,
                            }))
                        })
                    }
                }
                _ if wants_response => Some(self.response(cmd_id)),
                _ => None,
            };
            if let Some(r) = reply {
                conn.send(&r)?;
                last_sent = Instant::now();
            }
            for mut md in std::mem::take(&mut self.advisories) {
                md.command_id = self.id();
                conn.send(&DataStructure::MessageDispatch(Box::new(md)))?;
            }
        }
    }

    /// Hand queued messages to consumers within their prefetch (or one per pull).
    fn dispatch(
        &mut self,
        conn: &mut Connection<OpenWireCodec>,
        state: &Mutex<State>,
    ) -> pub_core::Result<()> {
        let mut out = Vec::new();
        for c in &mut self.consumers {
            if let Some((snapshot, next, done)) = &mut c.browse {
                while c.inflight.len() < c.prefetch.max(1) as usize && !*done {
                    let md = match snapshot.get(*next) {
                        Some(m) => {
                            *next += 1;
                            c.inflight.push(m.clone());
                            MessageDispatch {
                                consumer_id: Some(Box::new(c.id.clone())),
                                destination: Some(Box::new(c.destination.clone())),
                                message: Some(Box::new(DataStructure::Message(Box::new(
                                    m.clone(),
                                )))),
                                ..Default::default()
                            }
                        }
                        None => {
                            *done = true; // the end of the browse
                            MessageDispatch {
                                consumer_id: Some(Box::new(c.id.clone())),
                                destination: Some(Box::new(c.destination.clone())),
                                ..Default::default()
                            }
                        }
                    };
                    out.push(md);
                }
                continue;
            }
            loop {
                let room =
                    c.pull.is_some() || (c.prefetch > 0 && c.inflight.len() < c.prefetch as usize);
                if !room {
                    break;
                }
                let next = state
                    .lock()
                    .unwrap()
                    .queues
                    .get_mut(&c.dest)
                    .and_then(|q| q.pop_front());
                match next {
                    Some(m) => {
                        out.push(MessageDispatch {
                            consumer_id: Some(Box::new(c.id.clone())),
                            destination: Some(Box::new(c.destination.clone())),
                            message: Some(Box::new(DataStructure::Message(Box::new(m.clone())))),
                            redelivery_counter: m.fields.redelivery_counter,
                            ..Default::default()
                        });
                        c.inflight.push(m);
                        if c.pull.take().is_some() && c.prefetch == 0 {
                            break;
                        }
                    }
                    None => {
                        if let Some(Some(t)) = c.pull {
                            if Instant::now() >= t {
                                c.pull = None;
                                out.push(MessageDispatch {
                                    consumer_id: Some(Box::new(c.id.clone())),
                                    destination: Some(Box::new(c.destination.clone())),
                                    ..Default::default()
                                });
                            }
                        }
                        break;
                    }
                }
            }
        }
        for mut md in out {
            md.command_id = self.id();
            conn.send(&DataStructure::MessageDispatch(Box::new(md)))?;
        }
        Ok(())
    }
}

/// Append to a destination (topics behave like queues in the mock), giving the
/// message a broker sequence id.
fn enqueue(state: &Mutex<State>, dest: &str, mut m: Message) {
    let mut st = state.lock().unwrap();
    st.destinations.insert(dest.to_string());
    st.next_seq += 1;
    let seq = st.next_seq;
    match m.fields.message_id.as_deref_mut() {
        Some(DataStructure::MessageId(id)) => id.broker_sequence_id = seq,
        _ => {
            m.fields.message_id = Some(Box::new(DataStructure::MessageId(Box::new(MessageId {
                text_view: Some(format!("ID:mock-{seq}")),
                broker_sequence_id: seq,
                ..Default::default()
            }))))
        }
    }
    st.queues.entry(dest.to_string()).or_default().push_back(m);
}
