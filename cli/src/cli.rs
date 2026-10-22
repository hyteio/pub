use std::path::PathBuf;

use clap::{ArgAction, Args, Parser, Subcommand};

const AFTER_HELP: &str = "\
Targets are URLs (amq://host:61616/queue/orders) or aliases from ~/.pub
(`pub alias add jms.company.com amq://mq01.company.com:61616`), optionally
followed by a path: jms.company.com/queue/orders

Configuration: ~/.pub/config and ~/.pub/config.d/* (git-config syntax);
set PUB_HOME to use another directory.";

/// Multi-letter short options clap can't express, rewritten to their long
/// forms before parsing. Only after the `recv`/`receive`/`browse` subcommands, only exact
/// tokens (and `-sf=FILE`), so `-sfoo` still means `-s foo`; nothing after `--`
/// is touched.
const SHORT_ALIASES: &[(&str, &str)] = &[("-sf", "--selector-file")];

pub fn expand_short_aliases<I: IntoIterator<Item = std::ffi::OsString>>(
    args: I,
) -> Vec<std::ffi::OsString> {
    let mut out = Vec::new();
    let (mut receive, mut literal) = (false, false);
    for arg in args {
        if !receive {
            receive = matches!(arg.to_str(), Some("recv" | "receive" | "browse" | "move"));
        } else if !literal {
            if arg == "--" {
                literal = true;
            } else if let Some(s) = arg.to_str() {
                if let Some((_, long)) = SHORT_ALIASES.iter().find(|(short, _)| *short == s) {
                    out.push(long.into());
                    continue;
                }
                if let Some((short, long)) = SHORT_ALIASES
                    .iter()
                    .find(|(short, _)| s.starts_with(&format!("{short}=")))
                {
                    out.push(format!("{long}{}", &s[short.len()..]).into());
                    continue;
                }
            }
        }
        out.push(arg);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expand(args: &[&str]) -> Vec<String> {
        expand_short_aliases(args.iter().map(std::ffi::OsString::from))
            .into_iter()
            .map(|a| a.into_string().unwrap())
            .collect()
    }

    #[test]
    fn sf_means_selector_file() {
        assert_eq!(
            expand(&["pub", "recv", "u", "-sf", "f"]),
            ["pub", "recv", "u", "--selector-file", "f"]
        );
        assert_eq!(
            expand(&["pub", "recv", "-sf=f"]),
            ["pub", "recv", "--selector-file=f"]
        );
        // Not -s with a value, not other subcommands, not after --.
        assert_eq!(expand(&["pub", "recv", "-sfoo"]), ["pub", "recv", "-sfoo"]);
        assert_eq!(
            expand(&["pub", "send", "u", "-d", "-sf"]),
            ["pub", "send", "u", "-d", "-sf"]
        );
        assert_eq!(
            expand(&["pub", "recv", "--", "-sf"]),
            ["pub", "recv", "--", "-sf"]
        );
    }
}

#[derive(Parser)]
#[command(name = "pub", version, about = "The curl for messaging", after_help = AFTER_HELP)]
pub struct Cli {
    #[command(subcommand)]
    pub cmd: Cmd,
}

#[derive(Subcommand)]
pub enum Cmd {
    /// Send a message to a destination
    Send(SendArgs),
    /// Receive messages from a destination
    #[command(name = "recv", visible_alias = "receive")]
    Receive(ReceiveArgs),
    /// Show a queue's messages without consuming them
    Browse(BrowseArgs),
    /// Remove a durable subscription
    #[command(name = "unsub", visible_alias = "unsubscribe")]
    Unsubscribe(UnsubscribeArgs),
    /// Move messages to another destination on the same broker, transactionally
    Move(MoveArgs),
    /// List, create and delete destinations
    #[command(subcommand, visible_alias = "destinations")]
    Dest(DestCmd),
    /// Connect, handshake and show server information
    Ping(PingArgs),
    /// Manage host aliases (~/.pub/config.d)
    #[command(subcommand)]
    Alias(AliasCmd),
    /// Inspect or initialise configuration
    #[command(subcommand)]
    Config(ConfigCmd),
    /// List protocol modules, their schemes and supported commands
    #[command(visible_alias = "protocols")]
    Modules,
    /// List message formats (-F) and header strategies (--header-strategy)
    Formats,
}

/// Connection options shared by every messaging command.
#[derive(Args)]
pub struct ConnArgs {
    /// URL (scheme://host[:port]/destination) or alias[/destination]
    pub target: String,

    /// Credentials as user[:password]
    #[arg(short, long, value_name = "USER[:PASS]")]
    pub user: Option<String>,

    /// Skip TLS certificate verification
    #[arg(short = 'k', long)]
    pub insecure: bool,

    /// Extra CA certificates (PEM) to trust
    #[arg(long, value_name = "FILE")]
    pub cacert: Option<PathBuf>,

    /// Accept TLS 1.2 or newer (default minimum is 1.3)
    #[arg(long = "tlsv1.2", conflicts_with = "tls13")]
    pub tls12: bool,

    /// Require TLS 1.3
    #[arg(long = "tlsv1.3", id = "tls13")]
    pub tls13: bool,

    /// Highest TLS version to offer (1.2 or 1.3)
    #[arg(long, value_name = "VERSION")]
    pub tls_max: Option<String>,

    /// Allowed TLS cipher suites, in preference order (comma or colon separated)
    #[arg(long, value_name = "LIST")]
    pub ciphers: Option<String>,

    /// TLS key exchange groups, in preference order, e.g. X25519:P-256
    #[arg(long, value_name = "LIST")]
    pub curves: Option<String>,

    /// Client certificate (PEM) for mutual TLS
    #[arg(long, value_name = "FILE", requires = "key")]
    pub cert: Option<PathBuf>,

    /// Client private key (PEM) for mutual TLS
    #[arg(long, value_name = "FILE", requires = "cert")]
    pub key: Option<PathBuf>,

    /// TCP connect + TLS handshake timeout [default: 5s; 0 = none]
    #[arg(long, value_name = "DURATION")]
    pub connect_timeout: Option<String>,

    /// Timeout for a blocked socket write [default: 10s; 0 = none]
    #[arg(long, value_name = "DURATION")]
    pub send_timeout: Option<String>,

    /// Timeout for an expected server reply, e.g. handshake or receipt [default: 10s; 0 = none]
    #[arg(long, value_name = "DURATION")]
    pub receive_timeout: Option<String>,

    /// Disable TCP keepalive probes (on by default)
    #[arg(long)]
    pub no_keepalive: bool,

    /// Idle time before the first keepalive probe [default: 30s; 0 = OS default]
    #[arg(long, value_name = "DURATION")]
    pub keepalive_time: Option<String>,

    /// Time between unanswered keepalive probes [default: 10s; 0 = OS default]
    #[arg(long, value_name = "DURATION")]
    pub keepalive_interval: Option<String>,

    /// Unanswered keepalive probes before the connection is dropped [default: 3; 0 = OS default]
    #[arg(long, value_name = "N")]
    pub keepalive_count: Option<String>,

    /// SO_RCVBUF size, e.g. 256k, 4m [default: OS autotuning]
    #[arg(long, value_name = "BYTES")]
    pub receive_buffer: Option<String>,

    /// SO_SNDBUF size, e.g. 256k, 4m [default: OS autotuning]
    #[arg(long, value_name = "BYTES")]
    pub send_buffer: Option<String>,

    /// Set a module/connection setting, e.g. -O vhost=prod -O receipts=false
    #[arg(short = 'O', long = "option", value_name = "KEY=VALUE")]
    pub options: Vec<String>,

    /// Show connection details and protocol frames on stderr (-vv: no truncation)
    #[arg(short, long, action = ArgAction::Count)]
    pub verbose: u8,
}

#[derive(Args)]
pub struct SendArgs {
    #[command(flatten)]
    pub conn: ConnArgs,

    /// Destination (overrides the URL path)
    #[arg(long, value_name = "DEST")]
    pub dest: Option<String>,

    /// Message body; @FILE reads a file, @- reads stdin
    #[arg(short, long, value_name = "DATA")]
    pub data: Option<String>,

    /// Read the message body from FILE (- for stdin)
    #[arg(short, long, value_name = "FILE")]
    pub file: Option<PathBuf>,

    /// Header: a standard field (priority, correlation-id, ttl, JMSType, ... see
    /// `pub config headers`), else a String property (repeatable)
    #[arg(short = 'H', long = "header", value_name = "NAME: VALUE")]
    pub headers: Vec<String>,

    /// Application property, optionally typed: -P orderId:long=42 (types: string,
    /// bool, byte, short, int, long, float, double) (repeatable)
    #[arg(short = 'P', long = "property", value_name = "NAME[:TYPE]=VALUE")]
    pub properties: Vec<String>,

    /// Content type header
    #[arg(short = 't', long, value_name = "TYPE")]
    pub content_type: Option<String>,

    /// Send the message N times (with -F: every message in the input, N times)
    #[arg(short = 'n', long, default_value_t = 1, value_name = "N")]
    pub count: usize,

    /// Input format: raw (the input is the body; default) or a document format
    /// holding whole messages, e.g. json (see `pub formats`)
    #[arg(short = 'F', long, value_name = "FORMAT")]
    pub format: Option<String>,

    /// Send each message to the destination its document names (falls back to
    /// the URL path / --dest)
    #[arg(long)]
    pub dest_from_input: bool,

    /// How headers of messages read from a document are treated: default (new
    /// message-id and timestamp, original ttl from now) or administrative (admin;
    /// exact message-id, timestamp and expiration)
    #[arg(long, value_name = "STRATEGY")]
    pub header_strategy: Option<String>,

    /// Send everything in one local transaction: all or nothing
    #[arg(long)]
    pub transaction: bool,

    /// With --transaction: commit every N messages
    #[arg(long, value_name = "N", requires = "transaction")]
    pub batch: Option<usize>,
}

#[derive(Args)]
pub struct ReceiveArgs {
    #[command(flatten)]
    pub conn: ConnArgs,

    /// Destination (overrides the URL path)
    #[arg(long, value_name = "DEST")]
    pub dest: Option<String>,

    /// Stop after N messages (0 = until --wait expires)
    #[arg(short = 'n', long, default_value_t = 1, value_name = "N")]
    pub count: usize,

    /// How long to wait for messages, e.g. 10, 10s, 2m [default: forever]
    #[arg(short = 'm', long, alias = "timeout", value_name = "DURATION")]
    pub wait: Option<String>,

    /// Print message headers before each body (same as -F text)
    #[arg(short = 'i', long)]
    pub include: bool,

    /// Output format: raw (bodies; default), text (as -i), json, ... (see `pub formats`)
    #[arg(short = 'F', long, value_name = "FORMAT")]
    pub format: Option<String>,

    /// Append message bodies to FILE instead of stdout
    #[arg(short, long, value_name = "FILE")]
    pub output: Option<PathBuf>,

    /// Ensure each body ends with a newline (default when stdout is a terminal)
    #[arg(short = 'l', long)]
    pub lines: bool,

    /// Only receive messages matching this selector, e.g. "priority > 4 AND region = 'EU'"
    #[arg(short = 's', long, value_name = "EXPR")]
    pub selector: Option<String>,

    /// Read the selector from FILE (also -sf; '-' = stdin; lines starting with # are comments)
    #[arg(long, value_name = "FILE", conflicts_with = "selector")]
    pub selector_file: Option<PathBuf>,

    /// Raw protocol header for the subscription, passed through verbatim (repeatable)
    #[arg(short = 'H', long = "header", value_name = "NAME: VALUE")]
    pub headers: Vec<String>,

    /// Don't acknowledge messages (auto-ack mode)
    #[arg(long)]
    pub no_ack: bool,

    /// Receive through the durable topic subscription NAME (created if new; needs
    /// a fixed client-id); `pub unsub` removes it
    #[arg(long, value_name = "NAME")]
    pub durable: Option<String>,

    /// Acknowledge in a local transaction committed at the end; a failure
    /// (e.g. writing the output) rolls back and the messages stay on the broker
    #[arg(long)]
    pub transaction: bool,

    /// With --transaction: commit every N messages
    #[arg(long, value_name = "N", requires = "transaction")]
    pub batch: Option<usize>,
}

#[derive(Subcommand)]
pub enum DestCmd {
    /// List the broker's queues and topics, one per line (queue://NAME, topic://NAME)
    List(DestListArgs),
    /// Create a destination named by the URL path or --dest (a queue unless /topic/ or topic://)
    Create(DestArgs),
    /// Delete a destination and its messages (the broker refuses while it is in use)
    Delete(DestArgs),
}

#[derive(Args)]
pub struct DestListArgs {
    #[command(flatten)]
    pub conn: ConnArgs,

    /// Only queues
    #[arg(long, conflicts_with = "topics")]
    pub queues: bool,

    /// Only topics
    #[arg(long)]
    pub topics: bool,

    /// Include the broker's own destinations (e.g. ActiveMQ.Advisory.*)
    #[arg(long)]
    pub all: bool,

    /// How long to listen for the broker's list (it has no end marker)
    #[arg(short = 'm', long, default_value = "1s", value_name = "DURATION")]
    pub wait: String,
}

#[derive(Args)]
pub struct DestArgs {
    #[command(flatten)]
    pub conn: ConnArgs,

    /// Destination (overrides the URL path)
    #[arg(long, value_name = "DEST")]
    pub dest: Option<String>,
}

#[derive(Args)]
pub struct MoveArgs {
    /// Where messages are taken from (URL path, --dest, or the alias's destination)
    #[command(flatten)]
    pub conn: ConnArgs,

    /// Source destination (overrides the URL path)
    #[arg(long, value_name = "DEST")]
    pub dest: Option<String>,

    /// Target destination on the same broker, e.g. /queue/orders
    #[arg(long, value_name = "DEST")]
    pub to: String,

    /// Move at most N messages (0 = until the source is quiet)
    #[arg(short = 'n', long, default_value_t = 0, value_name = "N")]
    pub count: usize,

    /// Stop when no message has arrived for this long
    #[arg(short = 'm', long, default_value = "1s", value_name = "DURATION")]
    pub wait: String,

    /// Commit every N messages (each batch moves completely or not at all)
    #[arg(long, value_name = "N", default_value_t = pub_core::module::DEFAULT_MOVE_BATCH)]
    pub batch: usize,

    /// Only move messages matching this selector
    #[arg(short = 's', long, value_name = "EXPR")]
    pub selector: Option<String>,

    /// Read the selector from FILE (also -sf)
    #[arg(long, value_name = "FILE", conflicts_with = "selector")]
    pub selector_file: Option<PathBuf>,

    /// Move from this durable subscription of the source topic
    #[arg(long, value_name = "NAME")]
    pub durable: Option<String>,

    /// Also write each moved message, in this format (raw, text, json, ...)
    #[arg(short = 'F', long, value_name = "FORMAT")]
    pub format: Option<String>,

    /// Write moved messages to FILE (with -F; default stdout)
    #[arg(short, long, value_name = "FILE")]
    pub output: Option<PathBuf>,
}

#[derive(Args)]
pub struct BrowseArgs {
    #[command(flatten)]
    pub conn: ConnArgs,

    /// Queue (overrides the URL path)
    #[arg(long, value_name = "DEST")]
    pub dest: Option<String>,

    /// Show at most N messages (0 = the whole queue)
    #[arg(short = 'n', long, default_value_t = 0, value_name = "N")]
    pub count: usize,

    /// Stop after this long, e.g. 10s [default: when the queue has been read]
    #[arg(short = 'm', long, alias = "timeout", value_name = "DURATION")]
    pub wait: Option<String>,

    /// Print message headers before each body (same as -F text)
    #[arg(short = 'i', long)]
    pub include: bool,

    /// Output format: raw (bodies; default), text (as -i), json, ... (see `pub formats`)
    #[arg(short = 'F', long, value_name = "FORMAT")]
    pub format: Option<String>,

    /// Append message bodies to FILE instead of stdout
    #[arg(short, long, value_name = "FILE")]
    pub output: Option<PathBuf>,

    /// Ensure each body ends with a newline (default when stdout is a terminal)
    #[arg(short = 'l', long)]
    pub lines: bool,

    /// Only show messages matching this selector
    #[arg(short = 's', long, value_name = "EXPR")]
    pub selector: Option<String>,

    /// Read the selector from FILE (also -sf)
    #[arg(long, value_name = "FILE", conflicts_with = "selector")]
    pub selector_file: Option<PathBuf>,
}

#[derive(Args)]
pub struct UnsubscribeArgs {
    #[command(flatten)]
    pub conn: ConnArgs,

    /// The durable subscription to remove (with the same client-id that created it)
    #[arg(long, value_name = "NAME")]
    pub durable: String,
}

#[derive(Args)]
pub struct PingArgs {
    #[command(flatten)]
    pub conn: ConnArgs,
}

#[derive(Subcommand)]
pub enum AliasCmd {
    /// List aliases
    #[command(visible_alias = "ls")]
    List,
    /// Show an alias's settings and where they come from
    Show { name: String },
    /// Create an alias in ~/.pub/config.d/<name>.conf
    Add {
        name: String,
        url: String,
        /// Extra setting for the alias, e.g. --set user=app (repeatable)
        #[arg(long = "set", value_name = "KEY=VALUE")]
        set: Vec<String>,
        /// Overwrite an existing alias file / allow a scheme no module handles
        #[arg(long)]
        force: bool,
    },
    /// Remove an alias created by `pub alias add`
    #[command(visible_alias = "rm")]
    Remove { name: String },
    /// Show what a target resolves to
    Resolve { target: String },
}

#[derive(Subcommand)]
pub enum ConfigCmd {
    /// List every setting with the file and line it came from
    #[command(visible_alias = "ls")]
    List,
    /// Show the pub home directory and loaded files
    Path,
    /// Create ~/.pub with a commented starter config
    Init,
    /// Validate all config files against the settings schema
    Check,
    /// List the settings pub understands (core, or one module's)
    Settings {
        /// Module name, e.g. openwire; omit for all
        module: Option<String>,
        /// Print Markdown tables (for the documentation)
        #[arg(long)]
        markdown: bool,
    },
    /// List standard message headers and how each module carries them
    Headers {
        /// Module name, e.g. openwire; omit for all
        module: Option<String>,
    },
}
