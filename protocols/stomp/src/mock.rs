//! A tiny in-process STOMP broker for tests (feature `mock`).
//!
//! Queues persist across connections, so one connection can SEND and a later one
//! can SUBSCRIBE and receive. Messages are delivered at SUBSCRIBE time, or
//! immediately when sent on a connection that already subscribed.
//!
//! [`MockConfig`] adds an advertised heart-beat (the mock itself never sends
//! one, so it doubles as a silent broker), a `server` name — `ActiveMQ/...`
//! turns on ActiveMQ's browsing (`browser:true`, copies then `browser:end`).

use std::collections::{HashMap, VecDeque};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

use pub_core::transport::Stream;
use pub_core::{Connection, Trace};

use crate::codec::{Frame, StompCodec};

#[derive(Default)]
struct State {
    queues: HashMap<String, VecDeque<Frame>>,
    acks: usize,
    next_message: u64,
    /// Every frame received from clients, in order.
    log: Vec<Frame>,
    /// Heart-beats received, counted when a connection disconnects.
    heartbeats: u64,
    /// Transactions committed and aborted.
    commits: usize,
    aborts: usize,
}

#[derive(Debug, Clone, Default)]
pub struct MockConfig {
    pub credentials: Option<(String, String)>,
    /// `heart-beat` in CONNECTED (ms): what the mock offers and wants.
    pub heart_beat: Option<(u64, u64)>,
    /// `server` in CONNECTED (default `mock/1.0`); `ActiveMQ/...` browses.
    pub server: Option<String>,
}

pub struct MockBroker {
    pub addr: SocketAddr,
    state: Arc<Mutex<State>>,
}

impl MockBroker {
    /// Start on an ephemeral localhost port. `credentials` makes CONNECT require that login/passcode.
    pub fn start(credentials: Option<(&str, &str)>) -> Self {
        Self::start_with(MockConfig {
            credentials: credentials.map(|(u, p)| (u.to_string(), p.to_string())),
            ..Default::default()
        })
    }

    pub fn start_with(cfg: MockConfig) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(State::default()));
        let st = state.clone();
        thread::spawn(move || {
            for tcp in listener.incoming().flatten() {
                let st = st.clone();
                let cfg = cfg.clone();
                thread::spawn(move || {
                    let _ = serve(tcp, st, cfg);
                });
            }
        });
        MockBroker { addr, state }
    }

    /// Transactions (committed, aborted).
    pub fn transactions(&self) -> (usize, usize) {
        let st = self.state.lock().unwrap();
        (st.commits, st.aborts)
    }

    /// Heart-beats received from clients that have disconnected.
    pub fn heartbeats(&self) -> u64 {
        self.state.lock().unwrap().heartbeats
    }

    pub fn url(&self, path: &str) -> String {
        format!("stomp://{}{path}", self.addr)
    }

    pub fn queued(&self, dest: &str) -> Vec<Vec<u8>> {
        let st = self.state.lock().unwrap();
        st.queues
            .get(dest)
            .map(|q| q.iter().map(|f| f.body.clone()).collect())
            .unwrap_or_default()
    }

    pub fn acks(&self) -> usize {
        self.state.lock().unwrap().acks
    }

    /// Frames of one command received so far (e.g. "SUBSCRIBE").
    pub fn frames(&self, command: &str) -> Vec<Frame> {
        self.state
            .lock()
            .unwrap()
            .log
            .iter()
            .filter(|f| f.command == command)
            .cloned()
            .collect()
    }
}

fn serve(tcp: TcpStream, state: Arc<Mutex<State>>, cfg: MockConfig) -> pub_core::Result<()> {
    let creds = cfg.credentials.clone();
    let server = cfg.server.clone().unwrap_or_else(|| "mock/1.0".into());
    let mut conn = Connection::new(Stream::Plain(tcp), StompCodec::default(), Trace::default());
    // subscription id -> destination
    let mut subs: Vec<(String, String)> = Vec::new();
    // open transactions: SENDs held back, and ACKs counted, until COMMIT
    let mut txs: HashMap<String, (Vec<Frame>, usize)> = HashMap::new();

    while let Some(f) = conn.recv(None)? {
        state.lock().unwrap().log.push(f.clone());
        let receipt = f.get("receipt").map(str::to_string);
        match f.command.as_str() {
            "CONNECT" | "STOMP" => {
                if let Some((u, p)) = &creds {
                    if f.get("login") != Some(u) || f.get("passcode") != Some(p) {
                        conn.send(&Frame::new("ERROR").header("message", "Bad credentials"))?;
                        return Ok(());
                    }
                }
                let mut connected = Frame::new("CONNECTED")
                    .header("version", "1.2")
                    .header("server", server.as_str())
                    .header("session", "s1");
                if let Some((sx, sy)) = cfg.heart_beat {
                    connected = connected.header("heart-beat", format!("{sx},{sy}"));
                }
                conn.send(&connected)?;
                conn.codec.escape = true;
            }
            "SEND" => match f.get("transaction").and_then(|t| txs.get_mut(t)) {
                Some(tx) => tx.0.push(f.clone()),
                None if f.get("transaction").is_some() => {
                    conn.send(&Frame::new("ERROR").header("message", "unknown transaction"))?;
                    continue;
                }
                None => store(&mut conn, &state, &subs, &f)?,
            },
            "BEGIN" => {
                txs.insert(
                    f.get("transaction").unwrap_or_default().to_string(),
                    (Vec::new(), 0),
                );
            }
            "COMMIT" | "ABORT" => {
                let Some((sends, acks)) = f.get("transaction").and_then(|t| txs.remove(t)) else {
                    conn.send(&Frame::new("ERROR").header("message", "unknown transaction"))?;
                    continue;
                };
                if f.command == "COMMIT" {
                    for m in &sends {
                        store(&mut conn, &state, &subs, m)?;
                    }
                    let mut st = state.lock().unwrap();
                    st.acks += acks;
                    st.commits += 1;
                } else {
                    state.lock().unwrap().aborts += 1;
                }
            }
            "SUBSCRIBE" => {
                let id = f.get("id").unwrap_or_default().to_string();
                let dest = f.get("destination").unwrap_or_default().to_string();
                if f.get("browser") == Some("true") && server.starts_with("ActiveMQ/") {
                    // ActiveMQ's browse: copies, then the end marker.
                    let copies: Vec<Frame> = state
                        .lock()
                        .unwrap()
                        .queues
                        .get(&dest)
                        .map(|q| q.iter().cloned().collect())
                        .unwrap_or_default();
                    for mut m in copies {
                        m.headers.push(("subscription".into(), id.clone()));
                        conn.send(&m)?;
                    }
                    conn.send(
                        &Frame::new("MESSAGE")
                            .header("subscription", id.as_str())
                            .header("browser", "end")
                            .header("destination", dest.as_str()),
                    )?;
                } else {
                    subs.push((id.clone(), dest.clone()));
                    deliver(&mut conn, &state, &dest, &id)?;
                }
            }
            "UNSUBSCRIBE" => subs.retain(|(id, _)| Some(id.as_str()) != f.get("id")),
            "ACK" => match f.get("transaction").and_then(|t| txs.get_mut(t)) {
                Some(tx) => tx.1 += 1,
                None => state.lock().unwrap().acks += 1,
            },
            "DISCONNECT" => {
                state.lock().unwrap().heartbeats += conn.codec.heartbeats_seen;
                if let Some(r) = receipt {
                    conn.send(&Frame::new("RECEIPT").header("receipt-id", r))?;
                }
                return Ok(());
            }
            _ => {
                conn.send(
                    &Frame::new("ERROR")
                        .header("message", format!("unknown command {}", f.command)),
                )?;
                continue;
            }
        }
        if let Some(r) = receipt {
            conn.send(&Frame::new("RECEIPT").header("receipt-id", r))?;
        }
    }
    Ok(())
}

/// A SEND taking effect: queued, and delivered to a subscriber if there is one.
fn store(
    conn: &mut Connection<StompCodec>,
    state: &Mutex<State>,
    subs: &[(String, String)],
    f: &Frame,
) -> pub_core::Result<()> {
    let dest = f.get("destination").unwrap_or_default().to_string();
    let mut st = state.lock().unwrap();
    st.next_message += 1;
    let mut msg = Frame::new("MESSAGE")
        .header("destination", dest.as_str())
        .header("message-id", format!("m{}", st.next_message));
    msg.headers.extend(
        f.headers
            .iter()
            .filter(|(k, _)| {
                !["destination", "receipt", "content-length", "transaction"].contains(&k.as_str())
            })
            .cloned(),
    );
    msg.body = f.body.clone();
    st.queues.entry(dest.clone()).or_default().push_back(msg);
    drop(st);
    if let Some(sub) = subs
        .iter()
        .find(|(_, d)| *d == dest)
        .map(|(id, _)| id.clone())
    {
        deliver(conn, state, &dest, &sub)?;
    }
    Ok(())
}

fn deliver(
    conn: &mut Connection<StompCodec>,
    state: &Mutex<State>,
    dest: &str,
    sub: &str,
) -> pub_core::Result<()> {
    let pending: Vec<Frame> = state
        .lock()
        .unwrap()
        .queues
        .get_mut(dest)
        .map(|q| q.drain(..).collect())
        .unwrap_or_default();
    for mut m in pending {
        let id = m.get("message-id").unwrap_or_default().to_string();
        m.headers.push(("subscription".into(), sub.to_string()));
        m.headers.push(("ack".into(), id));
        conn.send(&m)?;
    }
    Ok(())
}
