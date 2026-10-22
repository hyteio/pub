//! Shared socket handling: DNS, TCP options, timeouts, optional TLS.

use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream, ToSocketAddrs};

use socket2::{Domain, Protocol, SockRef, Socket, TcpKeepalive, Type};
use std::time::Duration;

use crate::endpoint::Endpoint;
use crate::error::Result;
use crate::limits::MAX_DNS_ADDRESSES;
use crate::module::SchemeInfo;
use crate::tls::{self, TlsOptions, TlsStream};
use crate::trace::Trace;
use crate::{bail_kind, err};

pub enum Stream {
    Plain(TcpStream),
    Tls(Box<TlsStream>),
}

impl Stream {
    pub fn tcp(&self) -> &TcpStream {
        match self {
            Stream::Plain(s) => s,
            Stream::Tls(s) => &s.sock,
        }
    }

    pub fn is_tls(&self) -> bool {
        matches!(self, Stream::Tls(_))
    }

    pub fn set_read_timeout(&self, d: Option<Duration>) -> io::Result<()> {
        self.tcp().set_read_timeout(d)
    }

    /// Best-effort close: TLS close_notify, then shut the socket down.
    pub fn shutdown(&mut self) {
        if let Stream::Tls(s) = self {
            s.conn.send_close_notify();
            let _ = s.conn.complete_io(&mut s.sock);
        }
        let _ = self.tcp().shutdown(Shutdown::Both);
    }
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Stream::Plain(s) => s.read(buf),
            Stream::Tls(s) => s.read(buf),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Stream::Plain(s) => s.write(buf),
            Stream::Tls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Stream::Plain(s) => s.flush(),
            Stream::Tls(s) => s.flush(),
        }
    }
}

/// Socket behaviour shared by every module, read from endpoint settings.
///
/// | setting           | default | meaning                                                |
/// |-------------------|---------|--------------------------------------------------------|
/// | `tcp-nodelay`     | true    | disable Nagle; messaging is small request/reply frames |
/// | `tcp-keepalive`   | true    | SO_KEEPALIVE probes on idle connections                |
/// | `tcp-keepalive-time` | 30s  | idle time before the first probe; `0` = OS default     |
/// | `tcp-keepalive-interval` | 10s | time between unanswered probes; `0` = OS default  |
/// | `tcp-keepalive-count` | 3   | unanswered probes before the connection is dropped; `0` = OS default |
/// | `receive-buffer`  | OS      | SO_RCVBUF in bytes (`256k`, `4m`); disables OS autotuning |
/// | `send-buffer`     | OS      | SO_SNDBUF in bytes; disables OS autotuning             |
/// | `connect-timeout` | 5s      | per address for TCP connect, and for the TLS handshake |
/// | `send-timeout`    | 10s     | a single blocked socket write                          |
/// | `receive-timeout` | 10s     | waiting for an expected server reply (see [`crate::Connection::recv_reply`]) |
///
/// A timeout of `0` means wait forever; a buffer of `0` means use the OS default.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SocketOptions {
    pub nodelay: bool,
    pub keepalive: bool,
    /// Idle time before the first keepalive probe; `None` uses the OS default
    /// (2 hours on Linux and macOS).
    pub keepalive_time: Option<Duration>,
    /// Time between unanswered probes; `None` uses the OS default (75s on Linux and macOS).
    pub keepalive_interval: Option<Duration>,
    /// Unanswered probes before giving up; `None` uses the OS default (9 on Linux, 8 on macOS).
    pub keepalive_count: Option<u32>,
    /// `None` leaves the kernel's default and autotuning in charge.
    pub receive_buffer: Option<usize>,
    pub send_buffer: Option<usize>,
    pub connect_timeout: Option<Duration>,
    pub send_timeout: Option<Duration>,
    pub receive_timeout: Option<Duration>,
}

pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_SEND_TIMEOUT: Duration = Duration::from_secs(10);
pub const DEFAULT_RECEIVE_TIMEOUT: Duration = Duration::from_secs(10);
pub const DEFAULT_KEEPALIVE_TIME: Duration = Duration::from_secs(30);
pub const DEFAULT_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
/// With the defaults above a dead peer is noticed after about 30s + 3 × 10s = 1 minute.
pub const DEFAULT_KEEPALIVE_COUNT: u32 = 3;

impl Default for SocketOptions {
    fn default() -> Self {
        SocketOptions {
            nodelay: true,
            keepalive: true,
            keepalive_time: Some(DEFAULT_KEEPALIVE_TIME),
            keepalive_interval: Some(DEFAULT_KEEPALIVE_INTERVAL),
            keepalive_count: Some(DEFAULT_KEEPALIVE_COUNT),
            receive_buffer: None,
            send_buffer: None,
            connect_timeout: Some(DEFAULT_CONNECT_TIMEOUT),
            send_timeout: Some(DEFAULT_SEND_TIMEOUT),
            receive_timeout: Some(DEFAULT_RECEIVE_TIMEOUT),
        }
    }
}

impl SocketOptions {
    pub fn from_endpoint(ep: &Endpoint) -> Result<Self> {
        let d = SocketOptions::default();
        let timeout = |key: &str, default: Option<Duration>| -> Result<Option<Duration>> {
            Ok(match ep.prop_duration(key)? {
                None => default,
                Some(t) if t.is_zero() => None,
                Some(t) => Some(t),
            })
        };
        let buffer =
            |key: &str| -> Result<Option<usize>> { Ok(ep.prop_size(key)?.filter(|&n| n > 0)) };
        Ok(SocketOptions {
            nodelay: ep.prop_bool("tcp-nodelay", d.nodelay)?,
            keepalive: ep.prop_bool("tcp-keepalive", d.keepalive)?,
            keepalive_time: timeout("tcp-keepalive-time", d.keepalive_time)?,
            keepalive_interval: timeout("tcp-keepalive-interval", d.keepalive_interval)?,
            keepalive_count: match ep.prop("tcp-keepalive-count") {
                None => d.keepalive_count,
                Some(v) => match v.trim().parse::<u32>() {
                    Ok(0) => None,
                    Ok(n) => Some(n),
                    Err(_) => bail_kind!(
                        Config,
                        "'tcp-keepalive-count': expected a whole number, got '{v}'"
                    ),
                },
            },
            receive_buffer: buffer("receive-buffer")?,
            send_buffer: buffer("send-buffer")?,
            connect_timeout: timeout("connect-timeout", d.connect_timeout)?,
            send_timeout: timeout("send-timeout", d.send_timeout)?,
            receive_timeout: timeout("receive-timeout", d.receive_timeout)?,
        })
    }
}

/// Per-socket keepalive timing. Interval and count are only settable on the
/// platforms below (socket2's support); elsewhere they fall back to the OS.
fn keepalive_params(opts: &SocketOptions, trace: &Trace) -> TcpKeepalive {
    let mut ka = TcpKeepalive::new();
    if let Some(t) = opts.keepalive_time {
        ka = ka.with_time(t);
    }
    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "dragonfly",
        target_os = "illumos",
        target_os = "windows",
    ))]
    {
        let _ = trace;
        if let Some(i) = opts.keepalive_interval {
            ka = ka.with_interval(i);
        }
        if let Some(n) = opts.keepalive_count {
            ka = ka.with_retries(n);
        }
    }
    #[cfg(not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "dragonfly",
        target_os = "illumos",
        target_os = "windows",
    )))]
    if opts.keepalive_interval.is_some() || opts.keepalive_count.is_some() {
        trace.info("warning: keepalive interval/count not settable on this OS; using OS defaults");
    }
    ka
}

fn describe_keepalive(opts: &SocketOptions) -> String {
    if !opts.keepalive {
        return "off".into();
    }
    let os = || "OS".to_string();
    format!(
        "on (idle {}, interval {}, count {})",
        opts.keepalive_time.map_or_else(os, |t| format!("{t:?}")),
        opts.keepalive_interval
            .map_or_else(os, |t| format!("{t:?}")),
        opts.keepalive_count.map_or_else(os, |n| n.to_string()),
    )
}

/// Human form of an optional timeout for messages.
pub fn describe_timeout(t: Option<Duration>) -> String {
    t.map(|t| format!("{t:?}")).unwrap_or_else(|| "none".into())
}

/// Build a socket with every option applied *before* connecting: buffer sizes
/// must be in place before the SYN so the TCP window scale is negotiated to match.
fn open_socket(addr: &SocketAddr, opts: &SocketOptions, trace: &Trace) -> io::Result<TcpStream> {
    let socket = Socket::new(
        Domain::for_address(*addr),
        Type::STREAM,
        Some(Protocol::TCP),
    )?;
    if let Some(n) = opts.receive_buffer {
        socket.set_recv_buffer_size(n)?;
    }
    if let Some(n) = opts.send_buffer {
        socket.set_send_buffer_size(n)?;
    }
    if let Err(e) = socket.set_tcp_nodelay(opts.nodelay) {
        trace.info(format_args!(
            "warning: could not set TCP_NODELAY={}: {e}",
            opts.nodelay
        ));
    }
    let keepalive = if opts.keepalive {
        // SO_KEEPALIVE plus per-socket TCP_KEEPIDLE (TCP_KEEPALIVE on macOS),
        // TCP_KEEPINTVL and TCP_KEEPCNT; unset ones keep the OS default.
        socket.set_tcp_keepalive(&keepalive_params(opts, trace))
    } else {
        socket.set_keepalive(false)
    };
    if let Err(e) = keepalive {
        trace.info(format_args!(
            "warning: could not configure TCP keepalive: {e}"
        ));
    }
    match opts.connect_timeout {
        Some(t) => socket.connect_timeout(&(*addr).into(), t)?,
        None => socket.connect(&(*addr).into())?,
    }
    Ok(socket.into())
}

/// One line per buffer: the size the OS granted and the size requested.
/// Linux reports double the requested size (it reserves half for bookkeeping) and
/// caps requests at net.core.rmem_max / wmem_max, so the two can differ.
fn describe_buffers(tcp: &TcpStream, opts: &SocketOptions) -> String {
    let sock = SockRef::from(tcp);
    let one = |actual: io::Result<usize>, requested: Option<usize>| {
        let actual = actual
            .map(|n| n.to_string())
            .unwrap_or_else(|e| format!("? ({e})"));
        match requested {
            Some(r) => format!("{actual} (requested {r})"),
            None => format!("{actual} (OS)"),
        }
    };
    format!(
        "Socket buffers: rcv={} snd={}",
        one(sock.recv_buffer_size(), opts.receive_buffer),
        one(sock.send_buffer_size(), opts.send_buffer)
    )
}

/// Open a stream to the endpoint. TLS is used when the scheme demands it or the
/// endpoint sets `tls = true`.
///
/// Name resolution uses the system resolver, which has its own timeouts;
/// `connect-timeout` starts once addresses are known.
pub fn connect(
    ep: &Endpoint,
    scheme: &SchemeInfo,
    opts: &SocketOptions,
    trace: &Trace,
) -> Result<Stream> {
    let port = ep.port.unwrap_or(scheme.default_port);
    let use_tls = scheme.tls || ep.prop_bool("tls", false)?;

    let addrs: Vec<_> = (ep.host.as_str(), port)
        .to_socket_addrs()
        .map_err(|e| err!(Connect, "could not resolve host {}: {e}", ep.host))?
        .collect();
    if addrs.is_empty() {
        bail_kind!(Connect, "could not resolve host {}", ep.host);
    }
    let addrs: Vec<_> = if addrs.len() > MAX_DNS_ADDRESSES {
        trace.info(format_args!(
            "{} resolved to {} addresses; trying the first {MAX_DNS_ADDRESSES}",
            ep.host,
            addrs.len()
        ));
        addrs.into_iter().take(MAX_DNS_ADDRESSES).collect()
    } else {
        addrs
    };

    let mut last_err = None;
    let mut tcp = None;
    for addr in &addrs {
        trace.info(format_args!("Trying {addr}..."));
        match open_socket(addr, opts, trace) {
            Ok(s) => {
                trace.info(format_args!(
                    "Connected to {} ({}) port {port}",
                    ep.host,
                    addr.ip()
                ));
                tcp = Some(s);
                break;
            }
            Err(e) => {
                trace.info(format_args!("{addr}: {e}"));
                last_err = Some(e);
            }
        }
    }
    let tcp = match (tcp, last_err) {
        (Some(s), _) => s,
        (None, Some(e))
            if matches!(
                e.kind(),
                io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
            ) =>
        {
            bail_kind!(
                Timeout,
                "connection to {}:{port} timed out after {} (connect-timeout)",
                ep.host,
                describe_timeout(opts.connect_timeout)
            )
        }
        (None, e) => bail_kind!(
            Connect,
            "failed to connect to {}:{port}: {}",
            ep.host,
            e.map(|e| e.to_string()).unwrap_or_default()
        ),
    };

    trace.info(format_args!(
        "Socket: TCP_NODELAY={}, keepalive={}, connect-timeout={}, send-timeout={}, receive-timeout={}",
        opts.nodelay,
        describe_keepalive(opts),
        describe_timeout(opts.connect_timeout),
        describe_timeout(opts.send_timeout),
        describe_timeout(opts.receive_timeout),
    ));
    // Always show buffers when they were overridden; otherwise only at -vv.
    if opts.receive_buffer.is_some() || opts.send_buffer.is_some() {
        trace.info(describe_buffers(&tcp, opts));
    } else {
        trace.debug(|| describe_buffers(&tcp, opts));
    }

    let stream = if use_tls {
        let tls_opts = TlsOptions::from_endpoint(ep)?;
        Stream::Tls(Box::new(tls::handshake(
            tcp,
            &ep.host,
            &tls_opts,
            opts.connect_timeout,
            trace,
        )?))
    } else {
        Stream::Plain(tcp)
    };
    stream
        .tcp()
        .set_write_timeout(opts.send_timeout)
        .map_err(|e| err!(Io, "{e}"))?;
    Ok(stream)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    #[test]
    fn defaults_and_overrides() {
        let ep = Endpoint::parse("stomp://h").unwrap();
        assert_eq!(
            SocketOptions::from_endpoint(&ep).unwrap(),
            SocketOptions::default()
        );

        let ep = Endpoint::parse(
            "stomp://h?tcp-nodelay=false&tcp-keepalive=off&tcp-keepalive-time=0&tcp-keepalive-interval=2s&tcp-keepalive-count=0&receive-buffer=256k&send-buffer=0\
             &connect-timeout=250ms&send-timeout=0&receive-timeout=1m",
        )
        .unwrap();
        let o = SocketOptions::from_endpoint(&ep).unwrap();
        assert!(!o.nodelay);
        assert!(!o.keepalive);
        assert_eq!(o.keepalive_time, None);
        assert_eq!(o.keepalive_interval, Some(Duration::from_secs(2)));
        assert_eq!(o.keepalive_count, None);
        assert_eq!(o.receive_buffer, Some(256 * 1024));
        assert_eq!(o.send_buffer, None);
        assert_eq!(o.connect_timeout, Some(Duration::from_millis(250)));
        assert_eq!(o.send_timeout, None);
        assert_eq!(o.receive_timeout, Some(Duration::from_secs(60)));

        for bad in [
            "send-timeout=soon",
            "receive-buffer=lots",
            "tcp-keepalive-count=-1",
        ] {
            let ep = Endpoint::parse(&format!("stomp://h?{bad}")).unwrap();
            assert!(SocketOptions::from_endpoint(&ep).is_err(), "{bad}");
        }
    }

    #[test]
    fn options_are_applied_to_the_socket() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let defaults = SocketOptions::default();
        let tcp = open_socket(&addr, &defaults, &Trace::default()).unwrap();
        let sock = SockRef::from(&tcp);
        assert!(sock.tcp_nodelay().unwrap());
        assert!(sock.keepalive().unwrap());
        assert_eq!(sock.tcp_keepalive_time().unwrap(), DEFAULT_KEEPALIVE_TIME);
        assert_eq!(
            sock.tcp_keepalive_interval().unwrap(),
            DEFAULT_KEEPALIVE_INTERVAL
        );
        assert_eq!(
            sock.tcp_keepalive_retries().unwrap(),
            DEFAULT_KEEPALIVE_COUNT
        );

        // Unset values keep whatever the OS would have used on a fresh socket.
        let fresh = Socket::new(Domain::IPV4, Type::STREAM, None).unwrap();
        let os_timing = SocketOptions {
            keepalive_time: None,
            keepalive_interval: None,
            keepalive_count: None,
            ..defaults
        };
        let tcp = open_socket(&addr, &os_timing, &Trace::default()).unwrap();
        let sock = SockRef::from(&tcp);
        assert!(sock.keepalive().unwrap());
        assert_eq!(
            sock.tcp_keepalive_time().unwrap(),
            fresh.tcp_keepalive_time().unwrap()
        );
        assert_eq!(
            sock.tcp_keepalive_interval().unwrap(),
            fresh.tcp_keepalive_interval().unwrap()
        );
        assert_eq!(
            sock.tcp_keepalive_retries().unwrap(),
            fresh.tcp_keepalive_retries().unwrap()
        );

        let tuned = SocketOptions {
            nodelay: false,
            keepalive: false,
            receive_buffer: Some(64 * 1024),
            ..defaults
        };
        let tcp = open_socket(&addr, &tuned, &Trace::default()).unwrap();
        let sock = SockRef::from(&tcp);
        assert!(!sock.tcp_nodelay().unwrap());
        assert!(!sock.keepalive().unwrap());
        // The OS may round or double the request, but never ignores it entirely.
        assert!(sock.recv_buffer_size().unwrap() >= 32 * 1024);
    }
}
