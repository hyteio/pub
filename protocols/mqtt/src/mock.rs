//! A small in-process MQTT 5 broker for tests (feature `mock`).
//!
//! Real publish/subscribe semantics: messages fan out to every matching
//! subscription on any connection, retained messages are stored and replayed on
//! subscribe, QoS 1/2 handshakes are answered, and `+`/`#` filters match. Knobs
//! simulate refused credentials, denied topics, a QoS cap and a dead broker.
//! Will messages are published when a connection ends without a normal
//! DISCONNECT (or with reason 0x04); shared subscriptions (`$share/G/f`) get
//! each message round-robin within the group; MQTT 5 topic aliases work both
//! ways when enabled.

use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use pub_core::transport::Stream;
use pub_core::{Connection, Trace};

use crate::codec::PROTOCOL_LEVEL_5;
use crate::codec::{
    prop, Ack, ConnAck, MqttCodec, Packet, PropValue, Props, Publish, ReasonProps, SubAck,
};
use crate::mapping::topic_matches;
use std::collections::HashMap as Map;

#[derive(Debug, Clone)]
pub struct MockConfig {
    /// Require this username/password.
    pub credentials: Option<(String, String)>,
    /// Advertise and enforce a maximum QoS below 2.
    pub max_qos: u8,
    /// Refuse publishing and subscribing under this prefix (reason 0x87).
    pub deny_prefix: Option<String>,
    /// Send this Server Keep Alive in CONNACK.
    pub server_keep_alive: Option<u16>,
    /// Never answer PINGREQ (a broker that has silently gone away).
    pub ignore_pings: bool,
    /// Send retained messages *before* SUBACK, as a resumed session's queue can arrive.
    pub replay_before_suback: bool,
    /// Advertise this Topic Alias Maximum and accept client aliases (MQTT 5).
    pub topic_alias_maximum: u16,
    /// Send to clients with topic aliases, when they allow them.
    pub send_aliases: bool,
    /// No shared subscriptions (advertised, and `$share/` refused).
    pub no_shared: bool,
}

impl Default for MockConfig {
    fn default() -> Self {
        MockConfig {
            credentials: None,
            max_qos: 2,
            deny_prefix: None,
            server_keep_alive: None,
            ignore_pings: false,
            replay_before_suback: false,
            topic_alias_maximum: 0,
            send_aliases: false,
            no_shared: false,
        }
    }
}

struct Sub {
    conn: u64,
    filter: String,
    qos: u8,
    tx: Sender<Publish>,
}

#[derive(Default)]
struct State {
    retained: HashMap<String, Publish>,
    subs: Vec<Sub>,
    log: Vec<Packet>,
    next_conn: u64,
    /// Each connection's will, until it disconnects normally.
    wills: Map<u64, Publish>,
    /// Round-robin position per shared subscription filter.
    shared_next: Map<String, usize>,
}

impl State {
    /// A message taking effect: retained if asked, then delivered to each
    /// matching subscription (one member per shared group). Returns how many.
    fn fan_out(&mut self, p: &Publish) -> usize {
        if p.retain {
            if p.payload.is_empty() {
                self.retained.remove(&p.topic);
            } else {
                self.retained.insert(p.topic.clone(), p.clone());
            }
        }
        let matching: Vec<usize> = (0..self.subs.len())
            .filter(|&i| topic_matches(&self.subs[i].filter, &p.topic))
            .collect();
        let mut chosen = Vec::new();
        let mut groups: Map<String, Vec<usize>> = Map::new();
        for i in matching {
            let f = &self.subs[i].filter;
            if f.starts_with("$share/") {
                groups.entry(f.clone()).or_default().push(i);
            } else {
                chosen.push(i);
            }
        }
        for (filter, members) in groups {
            let next = self.shared_next.entry(filter).or_default();
            chosen.push(members[*next % members.len()]);
            *next += 1;
        }
        let mut n = 0;
        for i in chosen {
            let sub = &self.subs[i];
            let out = Publish {
                qos: p.qos.min(sub.qos),
                retain: false,
                dup: false,
                packet_id: None,
                ..p.clone()
            };
            if sub.tx.send(out).is_ok() {
                n += 1;
            }
        }
        n
    }
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
                let id = {
                    let mut s = st.lock().unwrap();
                    s.next_conn += 1;
                    s.next_conn
                };
                thread::spawn(move || {
                    let _ = serve(tcp, &st, &cfg, id);
                    let mut s = st.lock().unwrap();
                    s.subs.retain(|s| s.conn != id);
                    // Gone without a normal DISCONNECT: the will goes out.
                    if let Some(will) = s.wills.remove(&id) {
                        s.fan_out(&will);
                    }
                });
            }
        });
        MockBroker { addr, state }
    }

    pub fn url(&self, path: &str) -> String {
        format!("mqtt://{}{path}", self.addr)
    }

    /// Packets received from clients, by name (e.g. "PUBLISH").
    pub fn packets(&self, name: &str) -> Vec<Packet> {
        self.state
            .lock()
            .unwrap()
            .log
            .iter()
            .filter(|p| p.name() == name)
            .cloned()
            .collect()
    }

    pub fn retained(&self, topic: &str) -> Option<Vec<u8>> {
        self.state
            .lock()
            .unwrap()
            .retained
            .get(topic)
            .map(|p| p.payload.clone())
    }
}

fn reason(text: &str) -> Props {
    let mut p = Props::default();
    p.push(prop::REASON_STRING, PropValue::Str(text.into()));
    p
}

fn serve(
    tcp: TcpStream,
    state: &Mutex<State>,
    cfg: &MockConfig,
    conn_id: u64,
) -> pub_core::Result<()> {
    let mut conn = Connection::new(Stream::Plain(tcp), MqttCodec::default(), Trace::default());
    let mut v5 = true;
    // Topic aliases: the client's (alias -> topic), ours toward it (topic -> alias).
    let mut aliases_in: Map<u16, String> = Map::new();
    let mut aliases_out: Map<String, u16> = Map::new();
    let mut client_alias_max = 0u16;
    let (tx, rx) = mpsc::channel::<Publish>();
    let mut next_id: u16 = 0;
    let mut next_packet_id = || {
        next_id = next_id.checked_add(1).unwrap_or(1);
        next_id
    };
    let denied = |topic: &str| {
        cfg.deny_prefix
            .as_deref()
            .is_some_and(|d| topic.starts_with(d))
    };

    loop {
        let packet = match conn.recv(Some(Duration::from_millis(20))) {
            Err(_) => return Ok(()),
            Ok(p) => p,
        };
        if let Some(p) = packet {
            state.lock().unwrap().log.push(p.clone());
            match p {
                Packet::Connect(c) => {
                    // Speak whichever version the client asked for.
                    conn.codec.version = c.protocol_level;
                    v5 = c.protocol_level == PROTOCOL_LEVEL_5;
                    if let Some((u, pw)) = &cfg.credentials {
                        if c.username.as_deref() != Some(u)
                            || c.password.as_deref() != Some(pw.as_bytes())
                        {
                            let nack = if v5 {
                                ConnAck {
                                    session_present: false,
                                    reason: 0x86,
                                    props: reason("bad credentials"),
                                }
                            } else {
                                ConnAck {
                                    session_present: false,
                                    reason: 4,
                                    props: Props::default(),
                                }
                            };
                            conn.send(&Packet::ConnAck(nack))?;
                            return Ok(());
                        }
                    }
                    if let Some(w) = &c.will {
                        state.lock().unwrap().wills.insert(
                            conn_id,
                            Publish {
                                qos: w.qos,
                                retain: w.retain,
                                topic: w.topic.clone(),
                                props: w.props.clone(),
                                payload: w.payload.clone(),
                                ..Default::default()
                            },
                        );
                    }
                    client_alias_max = c.props.num(prop::TOPIC_ALIAS_MAXIMUM).unwrap_or(0) as u16;
                    let mut props = Props::default();
                    if cfg.topic_alias_maximum > 0 {
                        props.push(
                            prop::TOPIC_ALIAS_MAXIMUM,
                            PropValue::U16(cfg.topic_alias_maximum),
                        );
                    }
                    if cfg.no_shared {
                        props.push(prop::SHARED_SUBSCRIPTION_AVAILABLE, PropValue::Byte(0));
                    }
                    if c.client_id.is_empty() {
                        props.push(
                            prop::ASSIGNED_CLIENT_IDENTIFIER,
                            PropValue::Str(format!("mock-{conn_id}")),
                        );
                    }
                    if cfg.max_qos < 2 {
                        props.push(prop::MAXIMUM_QOS, PropValue::Byte(cfg.max_qos));
                    }
                    props.push(prop::RETAIN_AVAILABLE, PropValue::Byte(1));
                    if let Some(k) = cfg.server_keep_alive {
                        props.push(prop::SERVER_KEEP_ALIVE, PropValue::U16(k));
                    }
                    if !v5 {
                        props = Props::default(); // 3.1.1 CONNACK has no properties
                    }
                    conn.send(&Packet::ConnAck(ConnAck {
                        session_present: false,
                        reason: 0,
                        props,
                    }))?;
                }
                Packet::Publish(mut p) => {
                    if let Some(a) = p.props.num(prop::TOPIC_ALIAS) {
                        let a = a as u16;
                        let known = if p.topic.is_empty() {
                            aliases_in.get(&a).cloned()
                        } else {
                            aliases_in.insert(a, p.topic.clone());
                            Some(p.topic.clone())
                        };
                        match known {
                            Some(t) if a > 0 && a <= cfg.topic_alias_maximum => p.topic = t,
                            _ => {
                                conn.send(&Packet::Disconnect(ReasonProps {
                                    reason: 0x94,
                                    props: reason("Topic Alias invalid"),
                                }))?;
                                return Ok(());
                            }
                        }
                        p.props.0.retain(|(id, _)| *id != prop::TOPIC_ALIAS);
                    }
                    let id = p.packet_id.unwrap_or(0);
                    // 3.1.1 cannot refuse a PUBLISH; like real brokers, drop it silently.
                    if denied(&p.topic) && !v5 {
                        match p.qos {
                            1 => conn.send(&Packet::PubAck(Ack::new(id)))?,
                            2 => conn.send(&Packet::PubRec(Ack::new(id)))?,
                            _ => {}
                        }
                        continue;
                    }
                    if denied(&p.topic) {
                        let nack = Ack {
                            packet_id: id,
                            reason: 0x87,
                            props: reason("topic denied"),
                        };
                        match p.qos {
                            1 => conn.send(&Packet::PubAck(nack))?,
                            2 => conn.send(&Packet::PubRec(nack))?,
                            _ => {}
                        }
                        continue;
                    }
                    let delivered = state.lock().unwrap().fan_out(&p);
                    let ack_reason = if delivered == 0 && v5 { 0x10 } else { 0 };
                    match p.qos {
                        1 => conn.send(&Packet::PubAck(Ack {
                            packet_id: id,
                            reason: ack_reason,
                            props: Props::default(),
                        }))?,
                        2 => conn.send(&Packet::PubRec(Ack {
                            packet_id: id,
                            reason: ack_reason,
                            props: Props::default(),
                        }))?,
                        _ => {}
                    }
                }
                Packet::PubRel(a) => conn.send(&Packet::PubComp(Ack::new(a.packet_id)))?,
                Packet::PubRec(a) => conn.send(&Packet::PubRel(Ack::new(a.packet_id)))?,
                Packet::PubAck(_) | Packet::PubComp(_) => {}
                Packet::Subscribe(s) => {
                    let mut reasons = Vec::new();
                    let mut replay = Vec::new();
                    {
                        let mut st = state.lock().unwrap();
                        for (filter, opts) in &s.filters {
                            if denied(filter) {
                                reasons.push(if v5 { 0x87 } else { 0x80 });
                                continue;
                            }
                            if cfg.no_shared && filter.starts_with("$share/") {
                                reasons.push(if v5 { 0x9E } else { 0x80 });
                                continue;
                            }
                            let qos = (opts & 0x03).min(cfg.max_qos);
                            reasons.push(qos);
                            st.subs.push(Sub {
                                conn: conn_id,
                                filter: filter.clone(),
                                qos,
                                tx: tx.clone(),
                            });
                            if (opts >> 4) & 0x03 != 2 {
                                for r in st
                                    .retained
                                    .values()
                                    .filter(|r| topic_matches(filter, &r.topic))
                                {
                                    replay.push(Publish {
                                        qos: r.qos.min(qos),
                                        retain: true,
                                        dup: false,
                                        packet_id: None,
                                        ..r.clone()
                                    });
                                }
                            }
                        }
                    }
                    let suback = Packet::SubAck(SubAck {
                        packet_id: s.packet_id,
                        props: Props::default(),
                        reasons,
                    });
                    if !cfg.replay_before_suback {
                        conn.send(&suback)?;
                    }
                    for mut r in replay {
                        if !v5 {
                            r.props = Props::default();
                        }
                        if r.qos > 0 {
                            r.packet_id = Some(next_packet_id());
                        }
                        conn.send(&Packet::Publish(r))?;
                    }
                    if cfg.replay_before_suback {
                        conn.send(&suback)?;
                    }
                }
                Packet::Unsubscribe(u) => {
                    state
                        .lock()
                        .unwrap()
                        .subs
                        .retain(|s| !(s.conn == conn_id && u.filters.contains(&s.filter)));
                    let reasons = vec![0; u.filters.len()];
                    conn.send(&Packet::UnsubAck(SubAck {
                        packet_id: u.packet_id,
                        props: Props::default(),
                        reasons,
                    }))?;
                }
                Packet::PingReq => {
                    if !cfg.ignore_pings {
                        conn.send(&Packet::PingResp)?;
                    }
                }
                Packet::Disconnect(d) => {
                    // A normal DISCONNECT discards the will; reason 0x04 keeps it.
                    if d.reason != 0x04 {
                        state.lock().unwrap().wills.remove(&conn_id);
                    }
                    return Ok(());
                }
                other => {
                    let r = ReasonProps {
                        reason: 0x82,
                        props: reason(&format!("unexpected {}", other.name())),
                    };
                    conn.send(&Packet::Disconnect(r))?;
                    return Ok(());
                }
            }
        }
        while let Ok(mut p) = rx.try_recv() {
            if !v5 {
                p.props = Props::default(); // a 3.1.1 subscriber gets no properties
            }
            if v5 && cfg.send_aliases && client_alias_max > 0 {
                match aliases_out.get(&p.topic) {
                    Some(&a) => {
                        p.props.push(prop::TOPIC_ALIAS, PropValue::U16(a));
                        p.topic.clear();
                    }
                    None if (aliases_out.len() as u16) < client_alias_max => {
                        let a = aliases_out.len() as u16 + 1;
                        aliases_out.insert(p.topic.clone(), a);
                        p.props.push(prop::TOPIC_ALIAS, PropValue::U16(a));
                    }
                    None => {}
                }
            }
            if p.qos > 0 {
                p.packet_id = Some(next_packet_id());
            }
            conn.send(&Packet::Publish(p))?;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::mapping::topic_matches;

    #[test]
    fn filters() {
        assert!(topic_matches("a/+/c", "a/b/c"));
        assert!(!topic_matches("a/+/c", "a/b/x/c"));
        assert!(topic_matches("a/#", "a"));
        assert!(topic_matches("a/#", "a/b/c"));
        assert!(topic_matches("#", "x/y"));
        assert!(!topic_matches("#", "$SYS/load"));
        assert!(topic_matches("$SYS/#", "$SYS/load"));
        assert!(topic_matches("$share/g/a/+", "a/b"));
        assert!(!topic_matches("a/b", "a/b/c"));
    }
}
