//! A framed connection: shared socket I/O driven by a module-supplied [`Codec`].

use std::io::{self, Read, Write};
use std::time::{Duration, Instant};

use crate::endpoint::Endpoint;
use crate::error::{kind_of, Error, Result};
use crate::limits::{human, Limits};
use crate::module::SchemeInfo;
use crate::sniff;
use crate::trace::Trace;
use crate::transport::{self, describe_timeout, SocketOptions, Stream};
use crate::{bail_kind, err};

/// Bytes a frame's header may add to `max-frame-size` while it is buffered.
const FRAME_HEADER_ALLOWANCE: usize = 64;

/// How much of the server's first bytes to keep, to recognise the wrong protocol.
const FIRST_BYTES: usize = 16;

/// Result of trying to decode the front of the read buffer.
#[derive(Debug, PartialEq)]
pub enum Decoded<F> {
    /// Not enough bytes yet.
    NeedMore,
    /// Discard this many bytes that carry no frame (keep-alives, padding).
    Skip(usize),
    /// A complete frame occupying this many bytes.
    Frame(F, usize),
}

/// What a protocol module provides: how frames become bytes and back.
pub trait Codec {
    type Frame;

    fn encode(&mut self, frame: &Self::Frame, out: &mut Vec<u8>) -> Result<()>;

    fn decode(&mut self, buf: &[u8]) -> Result<Decoded<Self::Frame>>;
}

/// TLS failures (e.g. the server's `CertificateRequired` alert, which TLS 1.3
/// delivers after the handshake) arrive wrapped in an io::Error.
fn tls_error(e: &io::Error) -> Option<&rustls::Error> {
    e.get_ref()
        .and_then(|inner| inner.downcast_ref::<rustls::Error>())
}

pub struct Connection<C: Codec> {
    stream: Stream,
    pub codec: C,
    trace: Trace,
    opts: SocketOptions,
    limits: Limits,
    rbuf: Vec<u8>,
    wbuf: Vec<u8>,
    /// The start of what the server sent, until the first frame decodes.
    first: Vec<u8>,
    framed: bool,
}

impl<C: Codec> Connection<C> {
    /// Connect to the endpoint (TCP or TLS per scheme/settings) and frame it with `codec`.
    /// Socket settings (`tcp-nodelay`, `connect-/send-/receive-timeout`) come from the endpoint.
    pub fn open(ep: &Endpoint, scheme: &SchemeInfo, trace: Trace, codec: C) -> Result<Self> {
        let opts = SocketOptions::from_endpoint(ep)?;
        let limits = Limits::from_endpoint(ep)?;
        let stream = transport::connect(ep, scheme, &opts, &trace)?;
        Ok(Self::with_options(stream, codec, trace, opts).with_limits(limits))
    }

    /// Wrap an already-connected stream with default socket options.
    pub fn new(stream: Stream, codec: C, trace: Trace) -> Self {
        Self::with_options(stream, codec, trace, SocketOptions::default())
    }

    pub fn with_options(stream: Stream, codec: C, trace: Trace, opts: SocketOptions) -> Self {
        Connection {
            stream,
            codec,
            trace,
            opts,
            limits: Limits::default(),
            rbuf: Vec::with_capacity(16 * 1024),
            wbuf: Vec::new(),
            first: Vec::new(),
            framed: false,
        }
    }

    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }

    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    pub fn options(&self) -> &SocketOptions {
        &self.opts
    }

    /// Deadline for a reply that is expected now (`receive-timeout` from now).
    pub fn reply_deadline(&self) -> Option<Instant> {
        self.opts.receive_timeout.map(|t| Instant::now() + t)
    }

    /// Wait for a frame the server owes us (handshake reply, receipt, ...), failing
    /// with a Timeout error after `receive-timeout`. `what` names it in the error.
    pub fn recv_reply(&mut self, what: &str) -> Result<C::Frame> {
        let deadline = self.reply_deadline();
        self.recv_reply_until(deadline, what)
    }

    /// Like [`recv_reply`](Self::recv_reply) with an explicit deadline, for modules
    /// that skip unrelated frames while waiting for a specific one.
    pub fn recv_reply_until(&mut self, deadline: Option<Instant>, what: &str) -> Result<C::Frame> {
        match self.recv_until(deadline)? {
            Some(f) => Ok(f),
            None => bail_kind!(
                Timeout,
                "no {what} from server within {} (receive-timeout)",
                describe_timeout(self.opts.receive_timeout)
            ),
        }
    }

    pub fn trace(&self) -> &Trace {
        &self.trace
    }

    pub fn stream(&self) -> &Stream {
        &self.stream
    }

    pub fn send(&mut self, frame: &C::Frame) -> Result<()> {
        self.wbuf.clear();
        self.codec.encode(frame, &mut self.wbuf)?;
        self.trace.wire_out(&self.wbuf);
        self.stream
            .write_all(&self.wbuf)
            .and_then(|_| self.stream.flush())
            .map_err(|e| match e.kind() {
                io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut => err!(
                    Timeout,
                    "write blocked for {} (send-timeout); server is not reading",
                    describe_timeout(self.opts.send_timeout)
                ),
                _ if tls_error(&e).is_some() => err!(Tls, "TLS error: {}", tls_error(&e).unwrap()),
                _ => err!(Io, "write failed: {e}"),
            })
    }

    /// Wait up to `timeout` (forever if `None`) for the next frame. `Ok(None)` means timed out.
    pub fn recv(&mut self, timeout: Option<Duration>) -> Result<Option<C::Frame>> {
        self.recv_until(timeout.map(|t| Instant::now() + t))
    }

    /// Wait until `deadline` (forever if `None`) for the next frame. `Ok(None)` means timed out.
    pub fn recv_until(&mut self, deadline: Option<Instant>) -> Result<Option<C::Frame>> {
        let result = self.read_frame(deadline);
        if self.framed {
            return result;
        }
        // Nothing has made sense yet: say so if the server speaks another protocol (an amqp://
        // URL pointed at ActiveMQ's OpenWire port, say).
        result.map_err(|e| match (sniff::hint(&self.first), kind_of(&e)) {
            (hint, Some(kind)) if !hint.is_empty() => Error::new(kind, format!("{e}{hint}")).into(),
            _ => e,
        })
    }

    fn read_frame(&mut self, deadline: Option<Instant>) -> Result<Option<C::Frame>> {
        let mut chunk = [0u8; 16 * 1024];
        loop {
            match self.codec.decode(&self.rbuf)? {
                Decoded::Frame(frame, n) => {
                    self.framed = true;
                    self.trace.wire_in(&self.rbuf[..n]);
                    self.rbuf.drain(..n);
                    return Ok(Some(frame));
                }
                Decoded::Skip(n) => {
                    self.trace.wire_noise('<', &self.rbuf[..n]);
                    self.rbuf.drain(..n);
                    continue;
                }
                Decoded::NeedMore => {}
            }
            // A frame that is still incomplete at this size (plus room for its
            // header) will only get bigger: stop before buffering more of it.
            if self.rbuf.len()
                > self
                    .limits
                    .max_frame_size
                    .saturating_add(FRAME_HEADER_ALLOWANCE)
            {
                bail_kind!(
                    Protocol,
                    "server sent a frame larger than max-frame-size ({}); raise it with -O max-frame-size=...",
                    human(self.limits.max_frame_size)
                );
            }

            // Past the deadline, still look once at what has already arrived
            // (the caller may have been slow), then report the timeout.
            let mut last_look = false;
            let wait = match deadline {
                None => None,
                Some(d) => match d.checked_duration_since(Instant::now()) {
                    Some(left) if !left.is_zero() => Some(left.max(Duration::from_millis(1))),
                    _ => {
                        last_look = true;
                        Some(Duration::from_millis(1))
                    }
                },
            };
            self.stream
                .set_read_timeout(wait)
                .map_err(|e| err!(Io, "{e}"))?;

            match self.stream.read(&mut chunk) {
                Ok(0) => bail_kind!(Io, "connection closed by server"),
                Ok(n) => {
                    if !self.framed && self.first.len() < FIRST_BYTES {
                        let keep = n.min(FIRST_BYTES - self.first.len());
                        self.first.extend_from_slice(&chunk[..keep]);
                    }
                    self.rbuf.extend_from_slice(&chunk[..n]);
                }
                Err(e)
                    if matches!(
                        e.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                    ) =>
                {
                    if last_look {
                        return Ok(None);
                    }
                    continue;
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) if tls_error(&e).is_some() => {
                    bail_kind!(Tls, "TLS error: {}", tls_error(&e).unwrap())
                }
                Err(e) => bail_kind!(Io, "read failed: {e}"),
            }
        }
    }

    pub fn shutdown(&mut self) {
        self.stream.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{TcpListener, TcpStream};

    /// A protocol whose only frame is `OK!!`.
    struct Ok4;

    impl Codec for Ok4 {
        type Frame = ();
        fn encode(&mut self, _: &(), _: &mut Vec<u8>) -> Result<()> {
            Ok(())
        }
        fn decode(&mut self, buf: &[u8]) -> Result<Decoded<()>> {
            match buf {
                [b'O', b'K', b'!', b'!', ..] => Ok(Decoded::Frame((), 4)),
                b if b"OK!!".starts_with(b) => Ok(Decoded::NeedMore),
                _ => bail_kind!(Protocol, "malformed"),
            }
        }
    }

    /// A connection to a server that sends `bytes` and closes.
    fn server_sending(bytes: &'static [u8]) -> Connection<Ok4> {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            s.write_all(bytes).unwrap();
        });
        let stream = Stream::Plain(TcpStream::connect(addr).unwrap());
        Connection::new(stream, Ok4, Trace::default())
    }

    fn recv(c: &mut Connection<Ok4>) -> std::result::Result<(), String> {
        match c.recv(Some(Duration::from_secs(5))) {
            Ok(Some(())) => Ok(()),
            Ok(None) => Err("timed out".into()),
            Err(e) => Err(format!("{e}")),
        }
    }

    #[test]
    fn names_the_protocol_a_confusing_server_speaks() {
        let mut c = server_sending(b"HTTP/1.1 400 Bad Request\r\n");
        assert_eq!(
            recv(&mut c).unwrap_err(),
            "malformed: the server looks like an HTTP server"
        );

        // OpenWire's greeting, then the close: the hint survives an I/O error too.
        let mut c = server_sending(b"\x00\x00\x01\x51\x01ActiveMQ");
        let e = recv(&mut c).unwrap_err();
        assert!(
            e.starts_with("malformed: the server looks like an ActiveMQ OpenWire port"),
            "{e}"
        );
        assert_eq!(
            kind_of(&c.recv(None).unwrap_err()),
            Some(crate::error::ErrorKind::Protocol)
        );
    }

    #[test]
    fn no_hint_for_unknown_bytes_or_once_framed() {
        let mut c = server_sending(b"OK");
        assert_eq!(recv(&mut c).unwrap_err(), "connection closed by server");

        let mut c = server_sending(b"OK!!HTTP/1.1");
        assert_eq!(recv(&mut c), Ok(()));
        assert_eq!(recv(&mut c).unwrap_err(), "malformed");
    }
}
