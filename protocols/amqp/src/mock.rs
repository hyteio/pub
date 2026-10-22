//! A small in-process AMQP 1.0 broker for tests (feature `mock`).
//!
//! SASL PLAIN / ANONYMOUS (optionally requiring credentials), open, one session
//! per connection, sender and receiver links with credit, drain, copy
//! distribution (browsing, unless `no_copy` — like RabbitMQ's classic queues),
//! durable subscriptions by (container id, link name) with lookup by name, and
//! ActiveMQ Classic's advertised `queue-prefix` / `topic-prefix` when
//! `prefixes` is set. Queues and topics share one namespace keyed by address;
//! topics behave like queues. Messages are kept for inspection.
//! Transactions: a coordinator link declares and discharges them; sends and
//! accepts in a transaction take effect on commit, and accepted messages go
//! back to their queue on rollback.

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use pub_core::transport::Stream;
use pub_core::{Connection, Trace};

use crate::codec::{AmqpCodec, Frame, Performative, SaslFrame, PROTOCOL_AMQP, PROTOCOL_SASL};
use crate::described::*;
use crate::message::AmqpMessage;
use crate::types::Value;

#[derive(Debug, Clone, Default)]
pub struct MockConfig {
    /// Require PLAIN with these credentials (and refuse ANONYMOUS).
    pub credentials: Option<(String, String)>,
    /// Advertise `queue://` / `topic://` prefixes, as ActiveMQ Classic does.
    pub prefixes: bool,
    /// Ignore copy distribution (RabbitMQ classic queues).
    pub no_copy: bool,
    /// Refuse links to this address with `amqp:not-found`.
    pub missing: Option<String>,
    /// Accept filters without applying them (left out of the attach reply).
    pub ignore_filters: bool,
}

#[derive(Default)]
struct State {
    queues: HashMap<String, VecDeque<Vec<u8>>>,
    received: Vec<AmqpMessage>,
    subscriptions: HashSet<(String, String)>,
    attaches: Vec<Attach>,
    dispositions: Vec<Disposition>,
    /// Transactions committed and rolled back.
    commits: usize,
    rollbacks: usize,
}

/// What a transaction holds back: sends (address, bytes) and accepted deliveries.
#[derive(Default)]
struct Txn {
    sends: Vec<(String, Vec<u8>)>,
    accepted: Vec<(String, Vec<u8>)>,
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
        format!("amqp://{}{path}", self.addr)
    }

    /// Messages received, decoded, in order.
    pub fn messages(&self) -> Vec<AmqpMessage> {
        self.state.lock().unwrap().received.clone()
    }

    /// How many messages wait at an address (as sent: with any prefix).
    pub fn queued(&self, address: &str) -> usize {
        self.state
            .lock()
            .unwrap()
            .queues
            .get(address)
            .map_or(0, |q| q.len())
    }

    /// Put an encoded message at an address, as if another client sent it.
    pub fn enqueue(&self, address: &str, m: &AmqpMessage) {
        let bytes = m.encode().unwrap();
        self.state
            .lock()
            .unwrap()
            .queues
            .entry(address.to_string())
            .or_default()
            .push_back(bytes);
    }

    /// Transactions (committed, rolled back).
    pub fn transactions(&self) -> (usize, usize) {
        let st = self.state.lock().unwrap();
        (st.commits, st.rollbacks)
    }

    pub fn attaches(&self) -> Vec<Attach> {
        self.state.lock().unwrap().attaches.clone()
    }

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
}

/// A link the client attached.
struct Link {
    handle: u32,
    name: String,
    address: String,
    /// The client receives on it.
    outgoing: bool,
    credit: u32,
    delivery_count: u32,
    drain: bool,
    copy: bool,
    browse_next: usize,
    settled: bool,
    /// Deliveries sent and not yet settled by the client: (delivery id, bytes).
    unsettled: Vec<(u32, Vec<u8>)>,
    partial: Vec<u8>,
    /// The first frame of an incoming multi-frame delivery: (delivery id, settled, state).
    partial_first: Option<(u32, bool, Option<DeliveryState>)>,
    /// The transaction coordinator.
    coordinator: bool,
}

/// A declare or discharge sent to the coordinator; returns the answer.
fn coordinate(
    bytes: &[u8],
    state: &Mutex<State>,
    txns: &mut HashMap<Vec<u8>, Txn>,
    next_txn: &mut u32,
) -> DeliveryState {
    let refuse = |condition: &str| {
        DeliveryState::Rejected(Some(Error {
            condition: Symbol::new(condition),
            description: None,
            info: None,
        }))
    };
    let body = match AmqpMessage::decode(bytes).map(|m| m.body) {
        Ok(crate::message::Body::Value(v)) => v,
        _ => return refuse("amqp:decode-error"),
    };
    match body.descriptor_code() {
        Some(Declare::CODE) => {
            *next_txn += 1;
            let id = next_txn.to_be_bytes().to_vec();
            txns.insert(id.clone(), Txn::default());
            DeliveryState::Declared(id)
        }
        Some(Discharge::CODE) => {
            let Ok(d) = Discharge::from_value(body) else {
                return refuse("amqp:decode-error");
            };
            let Some(txn) = txns.remove(&d.txn_id.0) else {
                return refuse("amqp:transaction:unknown-id");
            };
            let mut st = state.lock().unwrap();
            if d.fail == Some(true) {
                for (address, bytes) in txn.accepted.into_iter().rev() {
                    st.queues.entry(address).or_default().push_front(bytes);
                }
                st.rollbacks += 1;
            } else {
                for (address, bytes) in txn.sends {
                    if let Ok(m) = AmqpMessage::decode(&bytes) {
                        st.received.push(m);
                    }
                    st.queues.entry(address).or_default().push_back(bytes);
                }
                st.commits += 1;
            }
            DeliveryState::Accepted
        }
        _ => refuse("amqp:not-implemented"),
    }
}

fn send(conn: &mut Connection<AmqpCodec>, p: Performative) -> pub_core::Result<()> {
    conn.send(&Frame::amqp(0, p))
}

fn serve(tcp: TcpStream, state: &Mutex<State>, cfg: &MockConfig) -> pub_core::Result<()> {
    let mut conn = Connection::new(Stream::Plain(tcp), AmqpCodec::default(), Trace::default());
    let wait = Some(Duration::from_secs(5));
    // SASL
    match conn.recv(wait)? {
        Some(Frame::Header(PROTOCOL_SASL)) => {}
        _ => return Ok(()),
    }
    conn.send(&Frame::Header(PROTOCOL_SASL))?;
    let offered = if cfg.credentials.is_some() {
        vec!["PLAIN"]
    } else {
        vec!["PLAIN", "ANONYMOUS"]
    };
    conn.send(&Frame::Sasl(SaslFrame::Mechanisms(SaslMechanisms {
        sasl_server_mechanisms: Symbols::of(&offered),
    })))?;
    let Some(Frame::Sasl(SaslFrame::Init(init))) = conn.recv(wait)? else {
        return Ok(());
    };
    let ok = match (&cfg.credentials, init.mechanism.0.as_str()) {
        (None, _) => true,
        (Some((u, p)), "PLAIN") => {
            init.initial_response.as_ref().map(|r| r.0.clone())
                == Some(format!("\0{u}\0{p}").into_bytes())
        }
        _ => false,
    };
    conn.send(&Frame::Sasl(SaslFrame::Outcome(SaslOutcome {
        code: if ok { 0 } else { 1 },
        additional_data: None,
    })))?;
    if !ok {
        return Ok(());
    }
    conn.codec.expect_header = true;
    match conn.recv(wait)? {
        Some(Frame::Header(PROTOCOL_AMQP)) => {}
        _ => return Ok(()),
    }
    conn.send(&Frame::Header(PROTOCOL_AMQP))?;
    let Some(Frame::Amqp {
        performative: Performative::Open(open),
        ..
    }) = conn.recv(wait)?
    else {
        return Ok(());
    };
    let container = open.container_id.clone();
    let mut props = vec![
        (Value::symbol("product"), Value::String("mock".into())),
        (Value::symbol("version"), Value::String("1.0".into())),
    ];
    if cfg.prefixes {
        props.push((
            Value::symbol("queue-prefix"),
            Value::String("queue://".into()),
        ));
        props.push((
            Value::symbol("topic-prefix"),
            Value::String("topic://".into()),
        ));
    }
    send(
        &mut conn,
        Performative::Open(Open {
            container_id: "mock-broker".into(),
            max_frame_size: Some(16 * 1024),
            idle_time_out: Some(30_000),
            properties: Some(Map(props)),
            ..Default::default()
        }),
    )?;

    let mut links: Vec<Link> = Vec::new();
    let mut txns: HashMap<Vec<u8>, Txn> = HashMap::new();
    let mut next_txn = 0u32;
    let mut next_delivery = 0u32;
    let mut next_outgoing = 0u32;
    loop {
        // Deliver what credit allows.
        for l in links.iter_mut().filter(|l| l.outgoing) {
            while l.credit > 0 {
                let next = if l.copy {
                    let q = state
                        .lock()
                        .unwrap()
                        .queues
                        .get(&l.address)
                        .cloned()
                        .unwrap_or_default();
                    let m = q.get(l.browse_next).cloned();
                    if m.is_some() {
                        l.browse_next += 1;
                    }
                    m
                } else {
                    state
                        .lock()
                        .unwrap()
                        .queues
                        .get_mut(&l.address)
                        .and_then(|q| q.pop_front())
                };
                let Some(bytes) = next else { break };
                let id = next_delivery;
                next_delivery += 1;
                // Split to show multi-frame transfers work.
                let pieces: Vec<Vec<u8>> = bytes.chunks(8 * 1024).map(|c| c.to_vec()).collect();
                let last = pieces.len().saturating_sub(1);
                for (i, piece) in pieces.into_iter().enumerate() {
                    let first = i == 0;
                    conn.send(&Frame::Amqp {
                        channel: 0,
                        performative: Performative::Transfer(Transfer {
                            handle: l.handle,
                            delivery_id: first.then_some(id),
                            delivery_tag: first.then(|| Binary(id.to_be_bytes().to_vec())),
                            message_format: first.then_some(0),
                            settled: first.then_some(l.settled),
                            more: Some(i < last),
                            ..Default::default()
                        }),
                        payload: piece,
                    })?;
                    next_outgoing += 1;
                }
                if !l.settled && !l.copy {
                    l.unsettled.push((id, bytes));
                }
                l.credit -= 1;
                l.delivery_count += 1;
            }
            if l.drain && l.credit > 0 {
                // Drained: use the rest of the credit up.
                l.delivery_count += l.credit;
                l.credit = 0;
                l.drain = false;
                send(
                    &mut conn,
                    Performative::Flow(Flow {
                        next_incoming_id: Some(0),
                        incoming_window: 2048,
                        next_outgoing_id: next_outgoing,
                        outgoing_window: 2048,
                        handle: Some(l.handle),
                        delivery_count: Some(l.delivery_count),
                        link_credit: Some(0),
                        drain: Some(true),
                        ..Default::default()
                    }),
                )?;
            } else if l.drain && l.credit == 0 {
                l.drain = false;
            }
        }

        let frame = match conn.recv(Some(Duration::from_millis(20))) {
            Err(_) => return Ok(()),
            Ok(None) => continue,
            Ok(Some(f)) => f,
        };
        let (p, payload) = match frame {
            Frame::Amqp {
                performative,
                payload,
                ..
            } => (performative, payload),
            Frame::Empty => continue,
            _ => return Ok(()),
        };
        match p {
            Performative::Begin(_) => send(
                &mut conn,
                Performative::Begin(Begin {
                    remote_channel: Some(0),
                    next_outgoing_id: 0,
                    incoming_window: 2048,
                    outgoing_window: 2048,
                    ..Default::default()
                }),
            )?,
            Performative::Attach(a) => {
                state.lock().unwrap().attaches.push(a.clone());
                let client_receives = a.role == RECEIVER;
                let address = if client_receives {
                    a.source.as_ref().and_then(|s| s.address.clone())
                } else {
                    a.target.as_ref().and_then(|t| t.address())
                };
                // A receiver attach without a source: look the durable subscription up.
                let lookup = client_receives && a.source.is_none();
                let key = (container.clone(), a.name.clone());
                let exists = state.lock().unwrap().subscriptions.contains(&key);
                let missing = cfg
                    .missing
                    .as_deref()
                    .is_some_and(|m| address.as_deref() == Some(m));
                let refuse = missing || (lookup && !exists);
                let mut reply = Attach {
                    name: a.name.clone(),
                    handle: a.handle,
                    role: !a.role,
                    snd_settle_mode: a.snd_settle_mode,
                    rcv_settle_mode: a.rcv_settle_mode,
                    initial_delivery_count: client_receives.then_some(0),
                    ..Default::default()
                };
                let copy = !cfg.no_copy
                    && a.source
                        .as_ref()
                        .and_then(|s| s.distribution_mode.as_ref())
                        .is_some_and(|m| m.0 == "copy");
                if !refuse {
                    if client_receives {
                        let mut src = a.source.clone().unwrap_or_default();
                        if !copy {
                            src.distribution_mode = None;
                        }
                        if cfg.ignore_filters {
                            src.filter = None;
                        }
                        reply.source = Some(src);
                        reply.target = a.target.clone();
                    } else {
                        reply.source = a.source.clone();
                        reply.target = a.target.clone();
                    }
                }
                send(&mut conn, Performative::Attach(reply))?;
                if refuse {
                    let error = Error {
                        condition: Symbol::new("amqp:not-found"),
                        description: Some(if lookup {
                            format!("Unknown subscription link: {}", a.name)
                        } else {
                            format!("no node {}", address.unwrap_or_default())
                        }),
                        info: None,
                    };
                    send(
                        &mut conn,
                        Performative::Detach(Detach {
                            handle: a.handle,
                            closed: Some(true),
                            error: Some(error),
                        }),
                    )?;
                    continue;
                }
                if a.source.as_ref().and_then(|s| s.durable).unwrap_or(0) > 0 {
                    state.lock().unwrap().subscriptions.insert(key);
                }
                if !client_receives {
                    // Credit for the client's sender.
                    send(
                        &mut conn,
                        Performative::Flow(Flow {
                            next_incoming_id: Some(0),
                            incoming_window: 2048,
                            next_outgoing_id: next_outgoing,
                            outgoing_window: 2048,
                            handle: Some(a.handle),
                            delivery_count: Some(0),
                            link_credit: Some(100),
                            ..Default::default()
                        }),
                    )?;
                }
                links.push(Link {
                    handle: a.handle,
                    name: a.name.clone(),
                    address: address.unwrap_or_default(),
                    outgoing: client_receives,
                    credit: 0,
                    delivery_count: 0,
                    drain: false,
                    copy,
                    browse_next: 0,
                    settled: a.snd_settle_mode == Some(settle::SND_SETTLED),
                    unsettled: Vec::new(),
                    partial: Vec::new(),
                    partial_first: None,
                    coordinator: matches!(a.target, Some(Terminus::Coordinator(_))),
                });
            }
            Performative::Flow(f) => {
                if let Some(l) = f
                    .handle
                    .and_then(|h| links.iter_mut().find(|l| l.handle == h))
                {
                    let their = f.delivery_count.unwrap_or(l.delivery_count);
                    l.credit = their
                        .wrapping_add(f.link_credit.unwrap_or(0))
                        .wrapping_sub(l.delivery_count);
                    l.drain = f.drain.unwrap_or(false);
                }
            }
            Performative::Transfer(t) => {
                let Some(l) = links.iter_mut().find(|l| l.handle == t.handle) else {
                    continue;
                };
                if l.partial_first.is_none() {
                    l.partial_first = Some((
                        t.delivery_id.unwrap_or(0),
                        t.settled.unwrap_or(false),
                        t.state.clone(),
                    ));
                }
                l.partial.extend_from_slice(&payload);
                if t.more == Some(true) {
                    continue;
                }
                let bytes = std::mem::take(&mut l.partial);
                let (id, settled, tstate) = l.partial_first.take().unwrap_or((0, false, None));
                if l.coordinator {
                    let outcome = coordinate(&bytes, state, &mut txns, &mut next_txn);
                    send(
                        &mut conn,
                        Performative::Disposition(Disposition {
                            role: RECEIVER,
                            first: id,
                            settled: Some(true),
                            state: Some(outcome),
                            ..Default::default()
                        }),
                    )?;
                    continue;
                }
                if let Some(DeliveryState::Transactional { txn_id, .. }) = &tstate {
                    let reply = match txns.get_mut(txn_id) {
                        Some(txn) => {
                            txn.sends.push((l.address.clone(), bytes));
                            DeliveryState::Transactional {
                                txn_id: txn_id.clone(),
                                outcome: Some(Box::new(DeliveryState::Accepted)),
                            }
                        }
                        None => DeliveryState::Rejected(Some(Error {
                            condition: Symbol::new("amqp:transaction:unknown-id"),
                            description: None,
                            info: None,
                        })),
                    };
                    send(
                        &mut conn,
                        Performative::Disposition(Disposition {
                            role: RECEIVER,
                            first: id,
                            settled: Some(true),
                            state: Some(reply),
                            ..Default::default()
                        }),
                    )?;
                    continue;
                }
                {
                    let mut st = state.lock().unwrap();
                    if let Ok(m) = AmqpMessage::decode(&bytes) {
                        st.received.push(m);
                    }
                    st.queues
                        .entry(l.address.clone())
                        .or_default()
                        .push_back(bytes);
                }
                if !settled {
                    send(
                        &mut conn,
                        Performative::Disposition(Disposition {
                            role: RECEIVER,
                            first: id,
                            settled: Some(true),
                            state: Some(DeliveryState::Accepted),
                            ..Default::default()
                        }),
                    )?;
                }
            }
            Performative::Disposition(d) => {
                state.lock().unwrap().dispositions.push(d.clone());
                let last = d.last.unwrap_or(d.first);
                let txn = match &d.state {
                    Some(DeliveryState::Transactional { txn_id, .. }) => txns.get_mut(txn_id),
                    _ => None,
                };
                let mut held = Vec::new();
                for l in links.iter_mut() {
                    l.unsettled.retain(|(id, bytes)| {
                        let settle = *id >= d.first && *id <= last;
                        if settle {
                            held.push((l.address.clone(), bytes.clone()));
                        }
                        !settle
                    });
                }
                if let Some(txn) = txn {
                    txn.accepted.extend(held);
                }
            }
            Performative::Detach(d) => {
                if let Some(pos) = links.iter().position(|l| l.handle == d.handle) {
                    let l = links.remove(pos);
                    // Unsettled deliveries go back to the front of the queue.
                    let mut st = state.lock().unwrap();
                    let q = st.queues.entry(l.address.clone()).or_default();
                    for (_, bytes) in l.unsettled.into_iter().rev() {
                        q.push_front(bytes);
                    }
                    if d.closed == Some(true) {
                        st.subscriptions
                            .remove(&(container.clone(), l.name.clone()));
                    }
                }
                send(
                    &mut conn,
                    Performative::Detach(Detach {
                        handle: d.handle,
                        closed: d.closed,
                        error: None,
                    }),
                )?;
            }
            Performative::End(_) => send(&mut conn, Performative::End(End::default()))?,
            Performative::Close(_) => {
                send(&mut conn, Performative::Close(Close::default()))?;
                return Ok(());
            }
            Performative::Open(_) => {}
        }
    }
}
