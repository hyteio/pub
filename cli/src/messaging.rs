//! send / receive / ping: resolve the target, pick the module, run the verb.

use std::collections::BTreeSet;
use std::fs::OpenOptions;
use std::io::{self, IsTerminal, Write};
use std::path::Path;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use pub_core::endpoint::parse_duration;
use pub_core::format::{FormatContext, Record, DEFAULT_FORMAT};
use pub_core::limits::{self, check_len, Limits, MAX_DESTINATION, MAX_HEADER_NAME, MAX_SELECTOR};
use pub_core::message::check_message;
use pub_core::module::{Destination, DestinationKinds};
use pub_core::payload::{open_input, parse_header, parse_kv, read_input};
use pub_core::strategy::{StrategyContext, DEFAULT_STRATEGY};
use pub_core::{
    bail_kind, err, Config, Context, Endpoint, Field, Module, ReceiveOptions, Registry, Result,
    Session, Trace, Verb,
};

use crate::cli::{
    BrowseArgs, ConnArgs, DestArgs, DestCmd, MoveArgs, PingArgs, ReceiveArgs, SendArgs,
    UnsubscribeArgs,
};

/// Resolve the target through ~/.pub and apply command-line overrides.
/// Settings are checked against the schema: bad values and unknown keys given on
/// the command line fail; unknown keys from config files only warn.
/// Also returns `core.config.strict`.
fn resolve(registry: &Registry, conn: &ConnArgs) -> Result<(Endpoint, bool)> {
    let cfg = Config::load()?;
    let strict = cfg.strict()?;
    let mut ep = registry.resolve_endpoint(&conn.target, &cfg)?;
    apply_overrides(&mut ep, conn)?;
    // Module aliases (e.g. Java's connectionTimeout) become pub's names before checking.
    let mut issues = registry.normalize_settings(&mut ep);
    issues.extend(registry.check_endpoint(&ep));
    crate::diag::report(&issues, strict)?;
    // Settings may have changed (-O password-file=..., -u); work credentials out again.
    ep.apply_credentials()?;
    ep.prompt_password(pub_core::secrets::ask_password)?;
    Ok((ep, strict))
}

/// Connect with the module that owns the scheme, after checking it supports `verb`.
fn connect(
    registry: &Registry,
    ep: &Endpoint,
    conn: &ConnArgs,
    verb: Verb,
) -> Result<Box<dyn Session>> {
    let (module, scheme) = registry.for_verb(ep, verb)?;
    let trace = Trace::new(conn.verbose);
    if let Some(alias) = &ep.alias {
        trace.info(format_args!("Alias {alias} -> {}", ep.display_url()));
    }
    for (name, reason) in &ep.ignored {
        trace.info(format_args!("Ignoring {name}: {reason}"));
    }
    trace.info(format_args!(
        "Using module {} for {}://",
        module.name(),
        scheme.scheme
    ));

    module.connect(ep, &Context { trace, scheme })
}

fn apply_overrides(ep: &mut Endpoint, conn: &ConnArgs) -> Result<()> {
    if let Some(u) = &conn.user {
        let (user, pass) = match u.split_once(':') {
            Some((u, p)) => (u, Some(p)),
            None => (u.as_str(), None),
        };
        ep.set_explicit_credentials(user, pass);
    }
    if conn.no_keepalive {
        ep.set_prop("tcp-keepalive", "false");
    }
    if conn.insecure {
        ep.set_prop("insecure", "true");
    }
    if conn.tls12 {
        ep.set_prop("tls-min-version", "1.2");
    }
    if conn.tls13 {
        ep.set_prop("tls-min-version", "1.3");
    }
    for (key, value) in [
        ("tls-max-version", &conn.tls_max),
        ("tls-cipher-suites", &conn.ciphers),
        ("tls-groups", &conn.curves),
    ] {
        if let Some(v) = value {
            ep.set_prop(key, v.as_str());
        }
    }
    for (key, path) in [
        ("ca-file", &conn.cacert),
        ("cert", &conn.cert),
        ("key", &conn.key),
    ] {
        if let Some(p) = path {
            ep.set_prop(key, p.to_string_lossy());
        }
    }
    for (key, value) in [
        ("connect-timeout", &conn.connect_timeout),
        ("send-timeout", &conn.send_timeout),
        ("receive-timeout", &conn.receive_timeout),
        ("tcp-keepalive-time", &conn.keepalive_time),
        ("tcp-keepalive-interval", &conn.keepalive_interval),
        ("tcp-keepalive-count", &conn.keepalive_count),
        ("receive-buffer", &conn.receive_buffer),
        ("send-buffer", &conn.send_buffer),
    ] {
        if let Some(v) = value {
            ep.set_prop(key, v.as_str());
        }
    }
    for opt in &conn.options {
        let (k, v) = parse_kv(opt)?;
        ep.set_prop(&k, v);
    }
    Ok(())
}

fn destination(module: &dyn Module, ep: &Endpoint, flag: &Option<String>) -> Result<String> {
    let dest = flag.clone().or_else(|| module.destination_name(ep)).ok_or_else(|| {
        err!(Usage, "no destination: put it in the URL path (e.g. {}/queue/orders), use --dest, or set 'destination' on the alias", ep.target)
    })?;
    check_len("destination", &dest, MAX_DESTINATION)?;
    Ok(dest)
}

/// Run `f`, then always close the session; the first error wins.
fn with_session<T>(
    mut session: Box<dyn Session>,
    f: impl FnOnce(&mut dyn Session) -> Result<T>,
) -> Result<T> {
    let result = f(session.as_mut());
    let closed = session.close();
    let value = result?;
    closed?;
    Ok(value)
}

/// `-F`, else the `format` setting, else raw.
fn format_name(flag: &Option<String>, ep: &Endpoint) -> String {
    flag.clone()
        .or_else(|| ep.prop("format").map(str::to_string))
        .unwrap_or_else(|| DEFAULT_FORMAT.to_string())
}

/// What `send` reads and checks before connecting, at most.
const MAX_SEND_MESSAGES: usize = 1_000_000;
const MAX_SEND_BYTES: usize = 1 << 30;
/// Bytes counted per message on top of its body, for headers and bookkeeping.
const MESSAGE_OVERHEAD: usize = 512;

pub fn send(registry: &Registry, args: SendArgs) -> Result<()> {
    if args.count == 0 {
        bail_kind!(Usage, "-n must be at least 1 for send");
    }
    let (ep, strict) = resolve(registry, &args.conn)?;
    let (module, _) = registry.for_verb(&ep, Verb::Send)?;
    // With --dest-from-input the URL path / --dest is only the fallback.
    let fallback = match destination(module, &ep, &args.dest) {
        Ok(d) => Some(d),
        Err(_) if args.dest_from_input => None,
        Err(e) => return Err(e),
    };
    let limits = Limits::from_endpoint(&ep)?;
    let format = registry.format(&format_name(&args.format, &ep))?;
    if !format.can_read() {
        bail_kind!(
            Usage,
            "the {} format can only be written; send reads raw or a document format (see `pub formats`)",
            format.name()
        );
    }
    let ctx = FormatContext {
        ep: &ep,
        limits,
        lines: false,
    };

    // Read and check every message before connecting: mistakes don't cost a
    // round trip, and a bad record late in a file doesn't leave a partial send.
    let strategy = registry.strategy(
        args.header_strategy
            .as_deref()
            .or_else(|| ep.prop("header-strategy"))
            .unwrap_or(DEFAULT_STRATEGY),
    )?;
    let provider_properties = registry.provider_properties();
    let strategy_ctx = StrategyContext {
        provider_properties: &provider_properties,
    };
    let input = open_input(args.data.as_deref(), args.file.as_deref())?;
    let mut prepared = Vec::new();
    let mut prepared_bytes = 0usize;
    let mut issues: Vec<pub_core::Issue> = Vec::new();
    let mut dropped = BTreeSet::new();
    let (mut restored, mut unrestorable) = (0usize, 0usize);
    for (i, record) in format.reader(input, &ctx)?.enumerate() {
        let Record {
            message: mut msg,
            destination,
        } = record?;
        let which = || format!("message {}", i + 1);
        // The header strategy decides what a captured message keeps: by
        // default a new id/timestamp and the original ttl from now.
        let report = strategy.apply(&mut msg, &strategy_ctx);
        dropped.extend(report.dropped);
        restored += usize::from(report.ttl_restored.is_some());
        unrestorable += usize::from(report.ttl_lost);
        // The command line wins over the document.
        for h in &args.headers {
            msg.apply_header(h, module.protocol_headers())?;
        }
        for p in &args.properties {
            msg.apply_property(p)?;
        }
        if let Some(ct) = &args.content_type {
            msg.set_field(Field::ContentType, ct)?;
        }
        if msg.body_type.is_none() {
            msg.apply_message_type(&ep)
                .map_err(|e| e.context(which()))?;
        }
        limits::check_message(&msg, &limits).map_err(|e| e.context(which()))?;
        for issue in check_message(module, &ep, &msg) {
            if !issues.contains(&issue) {
                issues.push(issue);
            }
        }
        let name = if args.dest_from_input {
            destination.or_else(|| fallback.clone())
        } else {
            fallback.clone()
        }
        .ok_or_else(|| {
            err!(
                Usage,
                "{}: the document names no destination and none was given (URL path or --dest)",
                which()
            )
        })?;
        check_len("destination", &name, MAX_DESTINATION)?;
        prepared_bytes += msg.body.len() + MESSAGE_OVERHEAD;
        if prepared.len() == MAX_SEND_MESSAGES || prepared_bytes > MAX_SEND_BYTES {
            bail_kind!(
                Usage,
                "the input holds more than pub reads in one send ({MAX_SEND_MESSAGES} messages or {} MiB); split it",
                MAX_SEND_BYTES >> 20
            );
        }
        prepared.push((registry.destination(&ep, module, Verb::Send, &name), msg));
    }
    if prepared.is_empty() {
        bail_kind!(Usage, "the input holds no messages");
    }
    crate::diag::report(&issues, strict)?;
    let trace = Trace::new(args.conn.verbose);
    if !dropped.is_empty() {
        trace.info(format_args!(
            "Header strategy {}: not sending {}",
            strategy.name(),
            dropped.into_iter().collect::<Vec<_>>().join(", ")
        ));
    }
    if restored > 0 {
        trace.info(format_args!(
            "ttl restored from expiration - timestamp for {restored} message(s)"
        ));
    }
    if unrestorable > 0 {
        trace.info(format_args!(
            "{unrestorable} message(s) have an expiration but no timestamp: sent without a ttl (use -H 'ttl: ...')"
        ));
    }

    if args.transaction && !module.transactions() {
        bail_kind!(
            Unsupported,
            "the {} module has no transactions",
            module.name()
        );
    }
    let session = connect(registry, &ep, &args.conn, Verb::Send)?;
    with_session(session, |s| {
        // Sent (or, in transactions, committed) so far, for the error report.
        let mut done = 0usize;
        let mut open = false;
        let sent = (|| -> Result<()> {
            let mut n = 0;
            for _ in 0..args.count {
                for (dest, msg) in &prepared {
                    if args.transaction && !open {
                        s.begin()?;
                        open = true;
                    }
                    s.send(dest, msg)?;
                    n += 1;
                    if !args.transaction {
                        done = n;
                    } else if args.batch.is_some_and(|b| b > 0 && n % b == 0) {
                        s.commit()?;
                        open = false;
                        done = n;
                    }
                }
            }
            if open {
                s.commit()?;
                open = false;
                done = n;
            }
            Ok(())
        })();
        match sent {
            Ok(()) => Ok(()),
            Err(e) if args.transaction => {
                // Undo the open batch; committed batches stay sent.
                if open {
                    let _ = s.rollback();
                }
                Err(e.context(format!(
                    "{done} message(s) committed before this; the open batch was rolled back"
                )))
            }
            Err(e) if done > 0 => Err(e.context(format!("{done} message(s) sent before this"))),
            Err(e) => Err(e),
        }
    })
}

/// `-o FILE`: appended to, and created readable by its owner only, since
/// captured messages may be sensitive.
fn open_output(path: &Path) -> Result<std::fs::File> {
    let mut opts = OpenOptions::new();
    opts.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    opts.open(path)
        .map_err(|e| err!(Io, "cannot open {}: {e}", path.display()))
}

/// A selector kept in a file: UTF-8, `#` comment lines dropped (`#` is not
/// part of JMS selector syntax), surrounding whitespace trimmed. Line breaks
/// inside are kept; selectors treat them as whitespace.
fn selector_from_file(path: &Path) -> Result<String> {
    let bytes = read_input(path, MAX_SELECTOR)?;
    let text = String::from_utf8(bytes)
        .map_err(|_| err!(Usage, "selector file {} is not UTF-8", path.display()))?;
    let selector = text
        .lines()
        .filter(|l| !l.trim_start().starts_with('#'))
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_string();
    if selector.is_empty() {
        bail_kind!(Usage, "selector file {} is empty", path.display());
    }
    Ok(selector)
}

/// What `receive` and `browse` share: where messages go and which ones.
struct Consume<'a> {
    verb: Verb,
    conn: &'a ConnArgs,
    dest: &'a Option<String>,
    count: usize,
    wait: Option<Duration>,
    include: bool,
    format: &'a Option<String>,
    output: &'a Option<PathBuf>,
    lines: bool,
    selector: &'a Option<String>,
    selector_file: &'a Option<PathBuf>,
    headers: Vec<(String, String)>,
    ack: bool,
    durable: Option<String>,
    transaction: Option<usize>,
}

fn parse_wait(wait: &Option<String>) -> Result<Option<Duration>> {
    wait.as_deref()
        .map(|t| parse_duration(t).ok_or_else(|| err!(Usage, "invalid --wait '{t}'")))
        .transpose()
}

pub fn receive(registry: &Registry, args: ReceiveArgs) -> Result<()> {
    let wait = parse_wait(&args.wait)?;
    if args.count == 0 && wait.is_none() {
        bail_kind!(Usage, "-n 0 (unlimited) needs -m/--wait");
    }
    let headers = args
        .headers
        .iter()
        .map(|h| parse_header(h))
        .collect::<Result<Vec<_>>>()?;
    let received = consume(
        registry,
        Consume {
            verb: Verb::Receive,
            conn: &args.conn,
            dest: &args.dest,
            count: args.count,
            wait,
            include: args.include,
            format: &args.format,
            output: &args.output,
            lines: args.lines,
            selector: &args.selector,
            selector_file: &args.selector_file,
            headers,
            ack: !args.no_ack,
            durable: args.durable.clone(),
            transaction: args.transaction.then(|| args.batch.unwrap_or(0)),
        },
    )?;
    if received.0 == 0 {
        bail_kind!(
            Timeout,
            "no message received from {} within {}",
            received.1,
            args.wait.unwrap_or_default()
        );
    }
    Ok(())
}

/// Like receive, but nothing is consumed, and an empty queue is a fine answer.
pub fn browse(registry: &Registry, args: BrowseArgs) -> Result<()> {
    let wait = parse_wait(&args.wait)?;
    consume(
        registry,
        Consume {
            verb: Verb::Browse,
            conn: &args.conn,
            dest: &args.dest,
            count: args.count,
            wait,
            include: args.include,
            format: &args.format,
            output: &args.output,
            lines: args.lines,
            selector: &args.selector,
            selector_file: &args.selector_file,
            headers: Vec::new(),
            ack: false,
            durable: None,
            transaction: None,
        },
    )?;
    Ok(())
}

/// Move messages between two destinations of one broker, a batch per transaction.
pub fn move_messages(registry: &Registry, args: MoveArgs) -> Result<()> {
    let idle =
        parse_duration(&args.wait).ok_or_else(|| err!(Usage, "invalid --wait '{}'", args.wait))?;
    let (ep, _) = resolve(registry, &args.conn)?;
    let (module, _) = registry.for_verb(&ep, Verb::Move)?;
    if args.durable.is_some() && !module.durable_subscriptions() {
        bail_kind!(
            Unsupported,
            "the {} module has no durable subscriptions",
            module.name()
        );
    }
    let from = registry.destination(
        &ep,
        module,
        Verb::Move,
        &destination(module, &ep, &args.dest)?,
    );
    check_len("destination", &args.to, MAX_DESTINATION)?;
    let to = Destination::new(args.to.clone());
    let selector = match (&args.selector, &args.selector_file) {
        (_, Some(path)) => Some(selector_from_file(path)?),
        (sel, None) => sel.clone(),
    };
    if let Some(sel) = &selector {
        check_len("selector", sel, MAX_SELECTOR)?;
    }
    // Moved messages are written only when asked for (-F / -o).
    let limits = Limits::from_endpoint(&ep)?;
    let mut writer = match (&args.format, &args.output) {
        (None, None) => None,
        (format, output) => {
            let out: Box<dyn Write> = match output {
                Some(path) => Box::new(open_output(path)?),
                None => Box::new(io::stdout().lock()),
            };
            let ctx = FormatContext {
                ep: &ep,
                limits,
                lines: output.is_none() && io::stdout().is_terminal(),
            };
            Some(
                registry
                    .format(&format_name(format, &ep))?
                    .writer(out, &ctx)?,
            )
        }
    };
    let opts = ReceiveOptions {
        count: (args.count > 0).then_some(args.count),
        ack: true,
        selector,
        subscription: args.durable.clone(),
        transaction: Some(args.batch),
        idle: Some(idle),
        ..Default::default()
    };
    let session = connect(registry, &ep, &args.conn, Verb::Move)?;
    if let Some(w) = writer.as_mut() {
        w.begin()?;
    }
    let moved = with_session(session, |s| {
        s.move_messages(&from, &to, &opts, &mut |m| match writer.as_mut() {
            Some(w) => w.write(&m),
            None => Ok(()),
        })
    });
    let finished = writer.as_mut().map_or(Ok(()), |w| w.finish());
    let moved = moved?;
    finished?;
    eprintln!(
        "pub: moved {moved} message{} from {from} to {to}",
        if moved == 1 { "" } else { "s" }
    );
    Ok(())
}

/// `pub dest list|create|delete`.
pub fn dest(registry: &Registry, cmd: DestCmd) -> Result<()> {
    match cmd {
        DestCmd::List(args) => {
            let wait = parse_duration(&args.wait)
                .ok_or_else(|| err!(Usage, "invalid --wait '{}'", args.wait))?;
            let (ep, _) = resolve(registry, &args.conn)?;
            registry.for_verb(&ep, Verb::Destinations)?;
            let kinds = DestinationKinds {
                queues: !args.topics,
                topics: !args.queues,
                internal: args.all,
            };
            let session = connect(registry, &ep, &args.conn, Verb::Destinations)?;
            let names = with_session(session, |s| s.list_destinations(kinds, wait))?;
            let mut out = io::stdout().lock();
            for n in names {
                writeln!(out, "{n}").map_err(|e| err!(Io, "write failed: {e}"))?;
            }
            Ok(())
        }
        DestCmd::Create(args) => dest_change(registry, args, true),
        DestCmd::Delete(args) => dest_change(registry, args, false),
    }
}

fn dest_change(registry: &Registry, args: DestArgs, create: bool) -> Result<()> {
    let (ep, _) = resolve(registry, &args.conn)?;
    let (module, _) = registry.for_verb(&ep, Verb::Destinations)?;
    let dest = Destination::new(destination(module, &ep, &args.dest)?);
    let session = connect(registry, &ep, &args.conn, Verb::Destinations)?;
    with_session(session, |s| {
        if create {
            s.create_destination(&dest)
        } else {
            s.delete_destination(&dest)
        }
    })
}

pub fn unsubscribe(registry: &Registry, args: UnsubscribeArgs) -> Result<()> {
    let (mut ep, _) = resolve(registry, &args.conn)?;
    let (module, _) = registry.for_verb(&ep, Verb::Unsubscribe)?;
    module.prepare_durable(&mut ep, &args.durable, true)?;
    // The subscription is named by client ID + name; a topic in the URL is optional.
    let dest = Destination::new(module.destination_name(&ep).unwrap_or_default());
    let session = connect(registry, &ep, &args.conn, Verb::Unsubscribe)?;
    with_session(session, |s| s.unsubscribe(&dest, &args.durable))
}

/// Returns how many messages were handed on, and the destination's name.
fn consume(registry: &Registry, c: Consume) -> Result<(usize, String)> {
    let ensure_newline = c.lines || (c.output.is_none() && io::stdout().is_terminal());

    let (mut ep, _) = resolve(registry, c.conn)?;
    if let Some(name) = &c.durable {
        let (module, _) = registry.for_verb(&ep, c.verb)?;
        if module.durable_subscriptions() {
            module.prepare_durable(&mut ep, name, false)?;
        }
    }
    let limits = Limits::from_endpoint(&ep)?;
    let format_name = match (c.format, c.include) {
        (Some(f), true) if !f.eq_ignore_ascii_case("text") => {
            bail_kind!(Usage, "-i means -F text; use one or the other")
        }
        (_, true) => "text".to_string(),
        (flag, false) => format_name(flag, &ep),
    };
    let format = registry.format(&format_name)?;
    let ctx = FormatContext {
        ep: &ep,
        limits,
        lines: ensure_newline,
    };
    if c.headers.len() > limits.max_headers {
        bail_kind!(
            Usage,
            "{} subscription headers; the limit is max-headers ({})",
            c.headers.len(),
            limits.max_headers
        );
    }
    for (k, v) in &c.headers {
        check_len("header name", k, MAX_HEADER_NAME)?;
        check_len(&format!("value of '{k}'"), v, limits.max_header_value)?;
    }
    let selector = match (c.selector, c.selector_file) {
        (_, Some(path)) => Some(selector_from_file(path)?),
        (sel, None) => sel.clone(),
    };
    if let Some(sel) = &selector {
        check_len("selector", sel, MAX_SELECTOR)?;
    }
    let (module, _) = registry.for_verb(&ep, c.verb)?;
    if c.transaction.is_some() && !module.transactions() {
        bail_kind!(
            Unsupported,
            "the {} module has no transactions",
            module.name()
        );
    }
    if c.durable.is_some() && !module.durable_subscriptions() {
        bail_kind!(
            Unsupported,
            "the {} module has no durable subscriptions",
            module.name()
        );
    }
    let dest = registry.destination(&ep, module, c.verb, &destination(module, &ep, c.dest)?);
    // Opened only now, so a mistake above doesn't leave an empty file behind.
    let out: Box<dyn Write> = match c.output {
        Some(path) => Box::new(open_output(path)?),
        None => Box::new(io::stdout().lock()),
    };
    let mut writer = format.writer(out, &ctx)?;
    let session = connect(registry, &ep, c.conn, c.verb)?;
    let opts = ReceiveOptions {
        count: (c.count > 0).then_some(c.count),
        timeout: c.wait,
        ack: c.ack,
        selector,
        headers: c.headers,
        subscription: c.durable,
        transaction: c.transaction,
        idle: None,
    };

    writer.begin()?;
    let verb = c.verb;
    let received = with_session(session, |s| {
        let mut sink = |m| writer.write(&m);
        match verb {
            Verb::Browse => s.browse(&dest, &opts, &mut sink),
            _ => s.receive(&dest, &opts, &mut sink),
        }
    });
    // Close the document even after an error, so what was written stays valid.
    let finished = writer.finish();
    let received = received?;
    finished?;
    Ok((received, dest.to_string()))
}

pub fn ping(registry: &Registry, args: PingArgs) -> Result<()> {
    let (ep, _) = resolve(registry, &args.conn)?;
    let start = Instant::now();
    let session = connect(registry, &ep, &args.conn, Verb::Ping)?;
    let elapsed = start.elapsed();
    let info = session.info();
    with_session(session, |_| Ok(()))?;

    println!(
        "{} connected in {} ms",
        ep.display_url(),
        elapsed.as_millis()
    );
    let width = info.iter().map(|(k, _)| k.len()).max().unwrap_or(0);
    for (k, v) in info {
        println!("  {k:<width$}  {v}");
    }
    Ok(())
}
