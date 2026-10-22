//! A small in-process AMQP 0-9-1 broker for tests (feature `mock`).
//!
//! PLAIN / ANONYMOUS login (credentials optional), vhost `/` only, 16 KiB
//! frames (so bodies split), one channel; the default exchange, `amq.topic`
//! and declared exchanges (direct, fanout, topic matching with `*` and `#`);
//! queues with declare / bind / delete / passive checks; publisher confirms
//! with `basic.return` for unroutable mandatory messages; transactions;
//! consumers with prefetch and acks; unacked messages return (redelivered)
//! when the channel closes.

use std::collections::{HashMap, VecDeque};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use pub_core::transport::Stream;
use pub_core::{Connection, Trace};

use crate::codec::*;
use crate::wire::FieldValue;

/// A publish being assembled: the method, its header (properties, body size), the body so far.
type Publishing = (BasicPublish, Option<(BasicProperties, u64)>, Vec<u8>);

#[derive(Debug, Clone, Default)]
pub struct MockConfig {
    pub credentials: Option<(String, String)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Stored {
    pub properties: BasicProperties,
    pub body: Vec<u8>,
    pub redelivered: bool,
    pub exchange: String,
    pub routing_key: String,
}

#[derive(Default)]
struct State {
    queues: HashMap<String, VecDeque<Stored>>,
    /// Exchange → (type, bindings: (queue, key)).
    exchanges: HashMap<String, (String, Vec<(String, String)>)>,
    published: Vec<Stored>,
    commits: usize,
    rollbacks: usize,
    next_queue: u32,
}

pub struct MockBroker {
    pub addr: SocketAddr,
    state: Arc<Mutex<State>>,
}

impl MockBroker {
    pub fn start(cfg: MockConfig) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().unwrap();
        let mut st = State::default();
        st.exchanges
            .insert("amq.topic".into(), ("topic".into(), Vec::new()));
        let state = Arc::new(Mutex::new(st));
        let s = state.clone();
        thread::spawn(move || {
            for tcp in listener.incoming().flatten() {
                let (s, cfg) = (s.clone(), cfg.clone());
                thread::spawn(move || {
                    let _ = serve(tcp, &s, &cfg);
                });
            }
        });
        MockBroker { addr, state }
    }

    pub fn url(&self, path: &str) -> String {
        format!("rampq://{}{path}", self.addr)
    }

    pub fn declare_queue(&self, name: &str) {
        self.state
            .lock()
            .unwrap()
            .queues
            .entry(name.into())
            .or_default();
    }

    pub fn queued(&self, name: &str) -> Vec<Stored> {
        self.state
            .lock()
            .unwrap()
            .queues
            .get(name)
            .map(|q| q.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn has_queue(&self, name: &str) -> bool {
        self.state.lock().unwrap().queues.contains_key(name)
    }

    /// Messages published (committed), in order.
    pub fn published(&self) -> Vec<Stored> {
        self.state.lock().unwrap().published.clone()
    }

    pub fn transactions(&self) -> (usize, usize) {
        let s = self.state.lock().unwrap();
        (s.commits, s.rollbacks)
    }
}

/// Topic matching: `*` one word, `#` zero or more.
fn topic_matches(pattern: &str, key: &str) -> bool {
    fn go(p: &[&str], k: &[&str]) -> bool {
        match (p.first(), k.first()) {
            (None, None) => true,
            (Some(&"#"), _) => go(&p[1..], k) || (!k.is_empty() && go(p, &k[1..])),
            (Some(&"*"), Some(_)) => go(&p[1..], &k[1..]),
            (Some(a), Some(b)) if a == b => go(&p[1..], &k[1..]),
            _ => false,
        }
    }
    let p: Vec<&str> = pattern.split('.').collect();
    let k: Vec<&str> = if key.is_empty() {
        vec![]
    } else {
        key.split('.').collect()
    };
    go(&p, &k)
}

/// Queues a publish reaches.
fn route(st: &State, exchange: &str, key: &str) -> Vec<String> {
    if exchange.is_empty() {
        return st
            .queues
            .contains_key(key)
            .then(|| key.to_string())
            .into_iter()
            .collect();
    }
    let Some((kind, bindings)) = st.exchanges.get(exchange) else {
        return Vec::new();
    };
    bindings
        .iter()
        .filter(|(_, k)| match kind.as_str() {
            "fanout" => true,
            "topic" => topic_matches(k, key),
            _ => k == key,
        })
        .map(|(q, _)| q.clone())
        .collect()
}

struct Consumer {
    tag: String,
    queue: String,
    no_ack: bool,
}

fn serve(tcp: TcpStream, state: &Mutex<State>, cfg: &MockConfig) -> pub_core::Result<()> {
    let mut conn = Connection::new(Stream::Plain(tcp), RampqCodec::default(), Trace::default());
    // The client's protocol header comes first.
    let header = match conn.recv(Some(Duration::from_secs(5)))? {
        Some(Frame::ProtocolHeader(h)) => h,
        _ => return Ok(()),
    };
    if header != PROTOCOL_HEADER {
        conn.send(&Frame::ProtocolHeader(PROTOCOL_HEADER))?;
        return Ok(());
    }
    let ch0 = |m: Method| Frame::method(0, m);
    let mechanisms = if cfg.credentials.is_some() {
        "PLAIN"
    } else {
        "PLAIN ANONYMOUS"
    };
    let s = |v: &str| FieldValue::LongStr(v.as_bytes().to_vec());
    conn.send(&ch0(Method::ConnectionStart(ConnectionStart {
        version_major: 0,
        version_minor: 9,
        server_properties: vec![("product".into(), s("mock")), ("version".into(), s("1.0"))],
        mechanisms: LongStr(mechanisms.as_bytes().to_vec()),
        locales: LongStr(b"en_US".to_vec()),
    })))?;
    let wait = Some(Duration::from_secs(5));
    let Some(Frame::Method {
        method: Method::ConnectionStartOk(ok),
        ..
    }) = conn.recv(wait)?
    else {
        return Ok(());
    };
    let good = match &cfg.credentials {
        None => true,
        Some((u, p)) => {
            ok.mechanism == "PLAIN" && ok.response.0 == format!("\0{u}\0{p}").into_bytes()
        }
    };
    if !good {
        conn.send(&ch0(Method::ConnectionClose(ConnectionClose {
            reply_code: 403,
            reply_text: "ACCESS_REFUSED - Login was refused".into(),
            ..Default::default()
        })))?;
        return Ok(());
    }
    conn.send(&ch0(Method::ConnectionTune(ConnectionTune {
        channel_max: 2047,
        frame_max: 16384,
        heartbeat: 60,
    })))?;
    let Some(Frame::Method {
        method: Method::ConnectionTuneOk(_),
        ..
    }) = conn.recv(wait)?
    else {
        return Ok(());
    };
    let Some(Frame::Method {
        method: Method::ConnectionOpen(open),
        ..
    }) = conn.recv(wait)?
    else {
        return Ok(());
    };
    if open.virtual_host != "/" {
        conn.send(&ch0(Method::ConnectionClose(ConnectionClose {
            reply_code: 530,
            reply_text: format!("NOT_ALLOWED - vhost {} not found", open.virtual_host),
            ..Default::default()
        })))?;
        return Ok(());
    }
    conn.send(&ch0(Method::ConnectionOpenOk(ConnectionOpenOk::default())))?;

    let mut consumers: Vec<Consumer> = Vec::new();
    // Delivered, not acked: (delivery tag, queue, message).
    let mut unacked: Vec<(u64, String, Stored)> = Vec::new();
    let mut prefetch = 0u16;
    let mut next_tag = 0u64;
    let mut confirming = false;
    let mut publish_seq = 0u64;
    let mut tx = false;
    // Held in a transaction: publishes (exchange, key, mandatory, message) and acks.
    let mut tx_publishes: Vec<(String, String, bool, Stored)> = Vec::new();
    let mut tx_acks: Vec<u64> = Vec::new();
    let mut publishing: Option<Publishing> = None;

    let reply = |conn: &mut Connection<RampqCodec>, m: Method| conn.send(&Frame::method(1, m));
    let requeue = |unacked: &mut Vec<(u64, String, Stored)>| {
        let mut st = state.lock().unwrap();
        for (_, q, mut m) in unacked.drain(..).rev() {
            m.redelivered = true;
            st.queues.entry(q).or_default().push_front(m);
        }
    };

    loop {
        // Deliver within prefetch.
        for c in &consumers {
            loop {
                if !c.no_ack && prefetch > 0 && unacked.len() >= prefetch as usize {
                    break;
                }
                let next = state
                    .lock()
                    .unwrap()
                    .queues
                    .get_mut(&c.queue)
                    .and_then(|q| q.pop_front());
                let Some(m) = next else { break };
                next_tag += 1;
                reply(
                    &mut conn,
                    Method::BasicDeliver(BasicDeliver {
                        consumer_tag: c.tag.clone(),
                        delivery_tag: next_tag,
                        redelivered: m.redelivered,
                        exchange: m.exchange.clone(),
                        routing_key: m.routing_key.clone(),
                    }),
                )?;
                conn.send(&Frame::Header {
                    channel: 1,
                    class_id: 60,
                    body_size: m.body.len() as u64,
                    properties: m.properties.clone(),
                })?;
                for piece in m.body.chunks(16384 - 8) {
                    conn.send(&Frame::Body {
                        channel: 1,
                        data: piece.to_vec(),
                    })?;
                }
                if !c.no_ack {
                    unacked.push((next_tag, c.queue.clone(), m));
                }
            }
        }

        let frame = match conn.recv(Some(Duration::from_millis(20))) {
            Err(_) => {
                requeue(&mut unacked);
                return Ok(());
            }
            Ok(None) => continue,
            Ok(Some(f)) => f,
        };
        // Content frames belong to the publish in progress; methods are handled below.
        let method = match frame {
            Frame::Method {
                method: Method::BasicPublish(p),
                ..
            } => {
                publishing = Some((p, None, Vec::new()));
                None
            }
            Frame::Method { method, .. } => Some(method),
            Frame::Header {
                properties,
                body_size,
                ..
            } => {
                if let Some(p) = publishing.as_mut() {
                    p.1 = Some((properties, body_size));
                }
                None
            }
            Frame::Body { data, .. } => {
                if let Some(p) = publishing.as_mut() {
                    p.2.extend_from_slice(&data);
                }
                None
            }
            Frame::Heartbeat | Frame::ProtocolHeader(_) => continue,
        };

        // A finished publish?
        let complete = publishing
            .as_ref()
            .is_some_and(|(_, h, body)| h.as_ref().is_some_and(|(_, n)| body.len() as u64 >= *n));
        if complete {
            let (p, h, body) = publishing.take().unwrap();
            let stored = Stored {
                properties: h.unwrap().0,
                body,
                redelivered: false,
                exchange: p.exchange.clone(),
                routing_key: p.routing_key.clone(),
            };
            if tx {
                tx_publishes.push((p.exchange, p.routing_key, p.mandatory, stored));
                continue;
            }
            let targets = {
                let mut st = state.lock().unwrap();
                let targets = route(&st, &p.exchange, &p.routing_key);
                st.published.push(stored.clone());
                for q in &targets {
                    st.queues
                        .entry(q.clone())
                        .or_default()
                        .push_back(stored.clone());
                }
                targets
            };
            if p.mandatory && targets.is_empty() {
                reply(
                    &mut conn,
                    Method::BasicReturn(BasicReturn {
                        reply_code: 312,
                        reply_text: "NO_ROUTE".into(),
                        exchange: p.exchange,
                        routing_key: p.routing_key,
                    }),
                )?;
                conn.send(&Frame::Header {
                    channel: 1,
                    class_id: 60,
                    body_size: stored.body.len() as u64,
                    properties: stored.properties,
                })?;
                if !stored.body.is_empty() {
                    conn.send(&Frame::Body {
                        channel: 1,
                        data: stored.body,
                    })?;
                }
            }
            if confirming {
                publish_seq += 1;
                reply(
                    &mut conn,
                    Method::BasicAck(BasicAck {
                        delivery_tag: publish_seq,
                        multiple: false,
                    }),
                )?;
            }
            continue;
        }
        let Some(method) = method else { continue };

        let channel_error = |conn: &mut Connection<RampqCodec>, code: u16, text: String| {
            conn.send(&Frame::method(
                1,
                Method::ChannelClose(ChannelClose {
                    reply_code: code,
                    reply_text: text,
                    ..Default::default()
                }),
            ))
        };
        match method {
            Method::ChannelOpen(_) => {
                reply(&mut conn, Method::ChannelOpenOk(ChannelOpenOk::default()))?
            }
            Method::ConfirmSelect(_) => {
                confirming = true;
                reply(&mut conn, Method::ConfirmSelectOk(ConfirmSelectOk {}))?;
            }
            Method::TxSelect(_) => {
                tx = true;
                reply(&mut conn, Method::TxSelectOk(TxSelectOk {}))?;
            }
            Method::TxCommit(_) => {
                let mut st = state.lock().unwrap();
                let mut returns = Vec::new();
                for (ex, key, mandatory, m) in tx_publishes.drain(..) {
                    let targets = route(&st, &ex, &key);
                    st.published.push(m.clone());
                    for q in &targets {
                        st.queues.entry(q.clone()).or_default().push_back(m.clone());
                    }
                    if mandatory && targets.is_empty() {
                        returns.push((ex, key));
                    }
                }
                unacked.retain(|(t, ..)| !tx_acks.contains(t));
                tx_acks.clear();
                st.commits += 1;
                drop(st);
                for (ex, key) in returns {
                    reply(
                        &mut conn,
                        Method::BasicReturn(BasicReturn {
                            reply_code: 312,
                            reply_text: "NO_ROUTE".into(),
                            exchange: ex,
                            routing_key: key,
                        }),
                    )?;
                    conn.send(&Frame::Header {
                        channel: 1,
                        class_id: 60,
                        body_size: 0,
                        properties: BasicProperties::default(),
                    })?;
                }
                reply(&mut conn, Method::TxCommitOk(TxCommitOk {}))?;
            }
            Method::TxRollback(_) => {
                tx_publishes.clear();
                tx_acks.clear();
                state.lock().unwrap().rollbacks += 1;
                reply(&mut conn, Method::TxRollbackOk(TxRollbackOk {}))?;
            }
            Method::QueueDeclare(d) => {
                let mut st = state.lock().unwrap();
                let name = if d.queue.is_empty() {
                    st.next_queue += 1;
                    format!("amq.gen-{}", st.next_queue)
                } else {
                    d.queue.clone()
                };
                if d.passive && !st.queues.contains_key(&name) {
                    drop(st);
                    channel_error(
                        &mut conn,
                        404,
                        format!("NOT_FOUND - no queue '{name}' in vhost '/'"),
                    )?;
                    continue;
                }
                let count = st.queues.entry(name.clone()).or_default().len() as u32;
                drop(st);
                reply(
                    &mut conn,
                    Method::QueueDeclareOk(QueueDeclareOk {
                        queue: name,
                        message_count: count,
                        consumer_count: 0,
                    }),
                )?;
            }
            Method::QueueBind(b) => {
                let mut st = state.lock().unwrap();
                match st.exchanges.get_mut(&b.exchange) {
                    Some((_, bindings)) => {
                        bindings.push((b.queue, b.routing_key));
                        drop(st);
                        reply(&mut conn, Method::QueueBindOk(QueueBindOk {}))?;
                    }
                    None => {
                        drop(st);
                        channel_error(
                            &mut conn,
                            404,
                            format!("NOT_FOUND - no exchange '{}' in vhost '/'", b.exchange),
                        )?;
                    }
                }
            }
            Method::QueueDelete(d) => {
                let n = state
                    .lock()
                    .unwrap()
                    .queues
                    .remove(&d.queue)
                    .map_or(0, |q| q.len() as u32);
                reply(
                    &mut conn,
                    Method::QueueDeleteOk(QueueDeleteOk { message_count: n }),
                )?;
            }
            Method::ExchangeDeclare(d) => {
                state
                    .lock()
                    .unwrap()
                    .exchanges
                    .entry(d.exchange)
                    .or_insert((d.kind, Vec::new()));
                reply(&mut conn, Method::ExchangeDeclareOk(ExchangeDeclareOk {}))?;
            }
            Method::ExchangeDelete(d) => {
                state.lock().unwrap().exchanges.remove(&d.exchange);
                reply(&mut conn, Method::ExchangeDeleteOk(ExchangeDeleteOk {}))?;
            }
            Method::BasicQos(q) => {
                prefetch = q.prefetch_count;
                reply(&mut conn, Method::BasicQosOk(BasicQosOk {}))?;
            }
            Method::BasicConsume(c) => {
                if !state.lock().unwrap().queues.contains_key(&c.queue) {
                    channel_error(
                        &mut conn,
                        404,
                        format!("NOT_FOUND - no queue '{}' in vhost '/'", c.queue),
                    )?;
                    continue;
                }
                let tag = format!("ctag-{}", consumers.len() + 1);
                reply(
                    &mut conn,
                    Method::BasicConsumeOk(BasicConsumeOk {
                        consumer_tag: tag.clone(),
                    }),
                )?;
                consumers.push(Consumer {
                    tag,
                    queue: c.queue,
                    no_ack: c.no_ack,
                });
            }
            Method::BasicCancel(c) => {
                consumers.retain(|x| x.tag != c.consumer_tag);
                reply(
                    &mut conn,
                    Method::BasicCancelOk(BasicCancelOk {
                        consumer_tag: c.consumer_tag,
                    }),
                )?;
            }
            Method::BasicAck(a) => {
                if tx {
                    tx_acks.push(a.delivery_tag);
                } else {
                    unacked.retain(|(t, ..)| {
                        *t != a.delivery_tag && !(a.multiple && *t <= a.delivery_tag)
                    });
                }
            }
            Method::ChannelClose(_) => {
                requeue(&mut unacked);
                consumers.clear();
                reply(&mut conn, Method::ChannelCloseOk(ChannelCloseOk {}))?;
            }
            Method::ConnectionClose(_) => {
                requeue(&mut unacked);
                conn.send(&Frame::method(
                    0,
                    Method::ConnectionCloseOk(ConnectionCloseOk {}),
                ))?;
                return Ok(());
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::topic_matches;

    #[test]
    fn topics() {
        assert!(topic_matches("a.*", "a.b"));
        assert!(!topic_matches("a.*", "a.b.c"));
        assert!(topic_matches("a.#", "a.b.c"));
        assert!(topic_matches("a.#", "a"));
        assert!(topic_matches("#", "x.y"));
        assert!(!topic_matches("a.b", "a.c"));
    }
}
