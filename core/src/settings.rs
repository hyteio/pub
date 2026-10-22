//! The settings schema: every key that may appear in `[defaults]`, `[protocol "x"]`,
//! `[alias "x"]`, a URL query or `-O`, with its type, default and help text.
//!
//! Core declares the settings it reads itself ([`CORE_SETTINGS`]); each module adds
//! its own through [`crate::Module::settings`]. Values are checked with the same
//! parsers the code uses when connecting, so a value that validates will be accepted.

use std::fmt;

use crate::aliases::{find_alias, find_inert, SettingAlias};
use crate::config::{expand_path, parse_bool, Config};
use crate::endpoint::{parse_duration, parse_size, Endpoint, Source};
use crate::module::{Destination, DestinationOption, Module, Registry, Verb};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettingType {
    Text,
    Bool,
    /// `10`, `1.5s`, `500ms`, `2m`, `1h`
    Duration,
    /// `65536`, `256k`, `4m`
    Size,
    /// Non-negative whole number.
    Count,
    /// File that must exist; `~/` is expanded.
    Path,
    /// Text that is never displayed.
    Secret,
    /// Octal permission mode, e.g. 0400.
    Mode,
    /// `spiffe://trust-domain/path` or `spiffe://trust-domain/*`.
    SpiffeId,
    /// `1.2` or `1.3`.
    TlsVersion,
    /// One of a fixed set of values, e.g. MQTT's `qos` (0, 1, 2).
    OneOf(&'static [&'static str]),
    /// Ordered list of cipher suite names this build supports.
    TlsCipherSuites,
    /// Ordered list of key exchange group names this build supports.
    TlsGroups,
}

impl SettingType {
    pub fn name(self) -> &'static str {
        match self {
            SettingType::Text => "string",
            SettingType::Bool => "bool",
            SettingType::Duration => "duration",
            SettingType::Size => "size",
            SettingType::Count => "count",
            SettingType::Path => "path",
            SettingType::Secret => "secret",
            SettingType::Mode => "mode",
            SettingType::SpiffeId => "spiffe-id",
            SettingType::TlsVersion => "tls-version",
            SettingType::OneOf(_) => "choice",
            SettingType::TlsCipherSuites => "suite-list",
            SettingType::TlsGroups => "group-list",
        }
    }

    /// Check a value, returning a human-readable reason when it is not acceptable.
    pub fn check(self, value: &str) -> std::result::Result<(), String> {
        let ok = match self {
            SettingType::Text | SettingType::Secret => true,
            SettingType::Bool => parse_bool(value).is_some(),
            SettingType::Duration => parse_duration(value).is_some(),
            SettingType::Size => parse_size(value).is_some(),
            SettingType::Count => value.trim().parse::<u32>().is_ok(),
            SettingType::Mode => crate::secrets::parse_mode(value).is_some(),
            SettingType::SpiffeId => {
                return crate::tls::SpiffeMatch::parse(value).map(|_| ());
            }
            SettingType::TlsVersion => return crate::tls::TlsVersion::parse(value).map(|_| ()),
            SettingType::OneOf(choices) => {
                return if choices.contains(&value.trim()) {
                    Ok(())
                } else {
                    Err(format!(
                        "expected one of {}, got '{value}'",
                        choices.join(", ")
                    ))
                };
            }
            SettingType::TlsCipherSuites => {
                return crate::tls::parse_cipher_suites(value).map(|_| ())
            }
            SettingType::TlsGroups => return crate::tls::parse_groups(value).map(|_| ()),
            SettingType::Path => {
                let path = expand_path(value);
                return if value.trim().is_empty() {
                    Err("expected a file path".into())
                } else if path.is_file() {
                    Ok(())
                } else {
                    Err(format!("file not found: {}", path.display()))
                };
            }
        };
        if ok {
            Ok(())
        } else {
            Err(format!(
                "expected a {} ({}), got '{value}'",
                self.name(),
                self.example()
            ))
        }
    }

    fn example(self) -> &'static str {
        match self {
            SettingType::Bool => "true/false",
            SettingType::Duration => "e.g. 10s, 500ms, 2m; 0 = none",
            SettingType::Size => "e.g. 65536, 256k, 4m",
            SettingType::Count => "e.g. 3",
            SettingType::Mode => "octal, e.g. 0400",
            _ => "text",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Setting {
    pub key: &'static str,
    pub ty: SettingType,
    /// Shown in listings; `None` means unset (the code or OS decides).
    pub default: Option<&'static str>,
    pub help: &'static str,
}

const fn s(
    key: &'static str,
    ty: SettingType,
    default: Option<&'static str>,
    help: &'static str,
) -> Setting {
    Setting {
        key,
        ty,
        default,
        help,
    }
}

use SettingType::*;

/// Settings read by pub-core for every protocol.
pub const CORE_SETTINGS: &[Setting] = &[
    // identity
    s("user", Text, None, "login name (also taken from the URL)"),
    s("username", Text, None, "same as `user`"),
    s(
        "password",
        Secret,
        None,
        "password (also taken from the URL); prefer password-env",
    ),
    s(
        "password-file",
        Path,
        None,
        "file holding the password; must be yours and mode 0400 (see core.config.secret-file-*)",
    ),
    s(
        "password-env",
        Text,
        None,
        "name of an environment variable holding the password",
    ),
    s(
        "password-prompt",
        Bool,
        Some("false"),
        "ask for the password on the terminal when none is configured (-u user always asks)",
    ),
    s(
        "destination",
        Text,
        None,
        "default destination when the target has no path",
    ),
    // socket
    s(
        "tcp-nodelay",
        Bool,
        Some("true"),
        "disable Nagle's algorithm",
    ),
    s(
        "tcp-keepalive",
        Bool,
        Some("true"),
        "send keepalive probes on idle connections",
    ),
    s(
        "tcp-keepalive-time",
        Duration,
        Some("30s"),
        "idle time before the first probe; 0 = OS default",
    ),
    s(
        "tcp-keepalive-interval",
        Duration,
        Some("10s"),
        "time between unanswered probes; 0 = OS default",
    ),
    s(
        "tcp-keepalive-count",
        Count,
        Some("3"),
        "unanswered probes before the connection is dropped; 0 = OS default",
    ),
    s(
        "receive-buffer",
        Size,
        None,
        "SO_RCVBUF; unset = OS autotuning (setting it disables autotuning)",
    ),
    s(
        "send-buffer",
        Size,
        None,
        "SO_SNDBUF; unset = OS autotuning (setting it disables autotuning)",
    ),
    s(
        "max-frame-size",
        Size,
        Some("64m"),
        "largest frame accepted from the server; larger ones are a protocol error",
    ),
    s("max-payload", Size, Some("64m"), "largest message body read from -d, -f or stdin"),
    s("max-headers", Count, Some("256"), "most headers + properties per message (and subscription headers)"),
    s("max-header-value", Size, Some("64k"), "largest header or property value"),
    s(
        "connect-timeout",
        Duration,
        Some("5s"),
        "TCP connect and TLS handshake; 0 = none",
    ),
    s(
        "send-timeout",
        Duration,
        Some("10s"),
        "a blocked socket write; 0 = none",
    ),
    s(
        "receive-timeout",
        Duration,
        Some("10s"),
        "an expected server reply (handshake, receipt); 0 = none",
    ),
    // tls
    s(
        "tls",
        Bool,
        Some("false"),
        "use TLS even if the scheme does not require it",
    ),
    s(
        "insecure",
        Bool,
        Some("false"),
        "skip TLS certificate verification",
    ),
    s(
        "ca-file",
        Path,
        None,
        "CA certificates (PEM) to trust instead of the system's",
    ),
    s(
        "cert",
        Path,
        None,
        "client certificate (PEM) for mutual TLS",
    ),
    s("key", Path, None, "client private key (PEM) for mutual TLS"),
    s(
        "tls-min-version",
        TlsVersion,
        Some("1.3"),
        "lowest TLS version to accept (1.2 or 1.3); set 1.2 per host for older brokers",
    ),
    s("tls-max-version", TlsVersion, Some("1.3"), "highest TLS version to offer"),
    s(
        "tls-cipher-suites",
        TlsCipherSuites,
        None,
        "allowed cipher suites in preference order; unset = library default (see `pub config settings`)",
    ),
    s(
        "tls-groups",
        TlsGroups,
        None,
        "key exchange groups in preference order, e.g. X25519,secp256r1; unset = library default",
    ),
    s(
        "header-strategy",
        Text,
        Some("default"),
        "how send treats headers of messages read from a document (-F json): default (new id, timestamp, ttl from now) or administrative (exact id, timestamp, expiration)",
    ),
    s(
        "format",
        Text,
        Some("raw"),
        "message format for receive output and send input (`pub formats` lists them; also -F)",
    ),
    s(
        "message-type",
        OneOf(&["text", "bytes"]),
        Some("bytes"),
        "body type to send: text (must be UTF-8) or bytes; set per module with [module \"<name>\"]",
    ),
    s(
        "server-spiffe-id",
        SpiffeId,
        None,
        "verify the server by SPIFFE ID (spiffe://domain/path, or spiffe://domain/* for any workload) against ca-file, instead of by hostname",
    ),
    s(
        "server-name",
        Text,
        None,
        "TLS server name (SNI) if it differs from the host",
    ),
];

/// Keys allowed in the `[core]` section.
pub const CORE_SECTION: &[Setting] = &[s(
    "default-scheme",
    Text,
    None,
    "scheme for targets that are neither URLs nor aliases",
)];

/// Keys allowed in `[core "config"]` (printed as `core.config.<key>`).
pub const CONFIG_SECTION: &[Setting] = &[
    s(
        "strict",
        Bool,
        Some("true"),
        "treat warnings as errors: unknown config keys/sections, message features the module cannot carry",
    ),
    s(
        "secret-file-max-mode",
        Mode,
        Some("0400"),
        "most permissive mode allowed on password-file (e.g. 0440 for group read)",
    ),
    s(
        "secret-file-check-owner",
        Bool,
        Some("true"),
        "require password-file to be owned by you",
    ),
    s(
        "secret-file-follow-symlinks",
        Bool,
        Some("false"),
        "accept password-file being a symbolic link",
    ),
    s(
        "tls-key-max-mode",
        Mode,
        Some("0600"),
        "most permissive mode allowed on the TLS private key file",
    ),
    s(
        "tls-key-check-owner",
        Bool,
        Some("true"),
        "require the TLS private key file to be owned by you",
    ),
    s(
        "tls-follow-symlinks",
        Bool,
        Some("true"),
        "accept cert, key and ca-file being symbolic links (cert-manager, certbot live/ paths)",
    ),
    s(
        "max-depth",
        Count,
        Some("16"),
        "how deep config.d subdirectories are read (0 = flat); only honoured in the main config file",
    ),
    s(
        "check-permissions",
        Bool,
        Some("true"),
        "refuse config.d files/dirs writable by group or others, or not owned by you or root (main config only; PUB_CHECK_PERMISSIONS=false skips all)",
    ),
    s("max-files", Count, Some("1024"), "most config files under config.d (main config only)"),
    s("max-file-size", Size, Some("64k"), "largest config file (main config only)"),
    s("max-total-size", Size, Some("4m"), "most bytes of configuration in total (main config only)"),
];

/// `[core "config"]` keys that govern how config.d itself is read, so only the
/// main config file may set them.
pub const MAIN_CONFIG_ONLY: &[&str] = &[
    "max-depth",
    "check-permissions",
    "max-files",
    "max-file-size",
    "max-total-size",
];

/// In strict mode every warning is an error.
pub fn apply_strict(issues: &mut [Issue], strict: bool) {
    if strict {
        for i in issues.iter_mut() {
            i.severity = Severity::Error;
        }
    }
}

/// `prefix.*` / `x-*` (trailing `*`) or an exact name. Case-sensitive: destination
/// options go to the provider as written.
pub fn matches_pattern(pattern: &str, key: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(prefix) => key.starts_with(prefix) && key.len() > prefix.len(),
        None => pattern == key,
    }
}

pub fn find<'a>(settings: &[&'a Setting], key: &str) -> Option<&'a Setting> {
    settings
        .iter()
        .copied()
        .find(|s| s.key.eq_ignore_ascii_case(key))
}

/// The closest known key, for "did you mean" hints.
pub fn suggest<'a>(key: &str, settings: &[&'a Setting]) -> Option<&'a str> {
    let names: Vec<&'a str> = settings.iter().map(|s| s.key).collect();
    suggest_name(key, &names)
}

/// The closest name (settings and aliases alike), compared case-insensitively.
pub fn suggest_name<'a>(key: &str, names: &[&'a str]) -> Option<&'a str> {
    let key = key.to_ascii_lowercase();
    names
        .iter()
        .map(|n| (*n, edit_distance(&key, &n.to_ascii_lowercase())))
        .filter(|(cand, d)| *d <= 2 || *d <= key.len() / 4 || cand.ends_with(&format!("-{key}")))
        .min_by_key(|(_, d)| *d)
        .map(|(cand, _)| cand)
}

fn edit_distance(a: &str, b: &str) -> usize {
    let b: Vec<char> = b.chars().collect();
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.chars().enumerate() {
        let mut cur = vec![i + 1; b.len() + 1];
        for (j, cb) in b.iter().enumerate() {
            cur[j + 1] = (prev[j] + usize::from(ca != *cb))
                .min(prev[j + 1] + 1)
                .min(cur[j] + 1);
        }
        prev = cur;
    }
    prev[b.len()]
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    Warning,
    Error,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Issue {
    pub severity: Severity,
    /// Where it came from: `file:line`, `URL`, `command line`.
    pub location: String,
    pub message: String,
}

impl fmt::Display for Issue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let level = match self.severity {
            Severity::Warning => "warning",
            Severity::Error => "error",
        };
        write!(f, "{level}: {}: {}", self.location, self.message)
    }
}

fn unknown_message(
    key: &str,
    scope: &str,
    known: &[&Setting],
    aliases: &[&SettingAlias],
) -> String {
    let names: Vec<&str> = known
        .iter()
        .map(|s| s.key)
        .chain(aliases.iter().map(|a| a.alias))
        .collect();
    match suggest_name(key, &names) {
        Some(s) => format!("unknown setting '{key}' for {scope}; did you mean '{s}'?"),
        None => format!("unknown setting '{key}' for {scope} (see `pub config settings`)"),
    }
}

impl Registry {
    /// Core settings plus the module's own.
    pub fn settings_for(&self, module: &dyn Module) -> Vec<&'static Setting> {
        CORE_SETTINGS
            .iter()
            .chain(module.settings())
            .chain(self.format_settings())
            .collect()
    }

    /// Options of every compiled-in format (`format.<name>.*`); they apply to
    /// any protocol.
    pub fn format_settings(&self) -> impl Iterator<Item = &'static Setting> + '_ {
        self.formats().flat_map(|f| f.settings().iter())
    }

    /// Every setting any compiled-in module understands.
    pub fn all_settings(&self) -> Vec<&'static Setting> {
        let mut all: Vec<&'static Setting> =
            CORE_SETTINGS.iter().chain(self.format_settings()).collect();
        for m in self.modules() {
            for s in m.settings() {
                if find(&all, s.key).is_none() {
                    all.push(s);
                }
            }
        }
        all
    }

    /// Settings that apply to a scheme: core + its module, or everything if the
    /// scheme is unknown (so we only flag keys nobody understands).
    fn settings_for_scheme(&self, scheme: &str) -> (Vec<&'static Setting>, String) {
        match self.lookup(scheme) {
            Ok((m, _)) => (self.settings_for(m), format!("the {} module", m.name())),
            Err(_) => (self.all_settings(), "any module".to_string()),
        }
    }

    /// Destination-option patterns for a scheme: its module's, or every module's
    /// when the scheme is unknown (`None` means all modules, for `[defaults]`).
    fn options_for(&self, scheme: Option<&str>) -> Vec<&'static DestinationOption> {
        match scheme.map(|s| self.lookup(s)) {
            Some(Ok((m, _))) => m.destination_options().iter().collect(),
            _ => self
                .modules()
                .flat_map(|m| m.destination_options().iter())
                .collect(),
        }
    }

    fn is_option(&self, scheme: Option<&str>, raw_key: &str) -> bool {
        self.options_for(scheme)
            .iter()
            .any(|o| matches_pattern(o.pattern, raw_key))
    }

    /// The destination for a verb: `name` plus the endpoint's destination options
    /// (keys that are not settings and match the module's patterns for this verb).
    pub fn destination(
        &self,
        ep: &Endpoint,
        module: &dyn Module,
        verb: Verb,
        name: &str,
    ) -> Destination {
        let settings = self.settings_for(module);
        let mut dest = Destination::new(name);
        for (key, value) in &ep.props {
            if find(&settings, key).is_some() {
                continue;
            }
            let raw = ep.names.get(key).map_or(key.as_str(), String::as_str);
            if module
                .destination_options()
                .iter()
                .any(|o| o.scope.applies_to(verb) && matches_pattern(o.pattern, raw))
            {
                dest.options.push((raw.to_string(), value.clone()));
            }
        }
        dest
    }

    /// Check a resolved endpoint's settings.
    ///
    /// Invalid values are errors. Unknown keys typed just now (command line, URL)
    /// are errors; unknown keys from config files are warnings, and keys in
    /// `[defaults]` that another module understands are fine.
    pub fn check_endpoint(&self, ep: &Endpoint) -> Vec<Issue> {
        let (known, scope) = self.settings_for_scheme(&ep.scheme);
        let everything = self.all_settings();
        let mut issues = Vec::new();
        for (key, value) in &ep.props {
            // An explicit password (`-u user:pass`, URL userinfo) leaves the
            // configured ones unused: don't fail over a file that isn't needed.
            if ep.explicit_password.is_some()
                && matches!(key.as_str(), "password" | "password-file" | "password-env")
            {
                continue;
            }
            let source = ep.sources.get(key);
            let location = source.map_or_else(|| "settings".to_string(), Source::to_string);
            match find(&known, key) {
                Some(setting) => {
                    if let Err(why) = setting.ty.check(value) {
                        issues.push(Issue {
                            severity: Severity::Error,
                            location,
                            message: format!("{key}: {why}"),
                        });
                    }
                }
                None => {
                    let raw = ep.names.get(key).map_or(key.as_str(), String::as_str);
                    if self.is_option(Some(&ep.scheme), raw) {
                        continue; // passed through to the provider
                    }
                    let from_defaults = matches!(source, Some(Source::File { section, .. }) if section == "defaults");
                    if from_defaults
                        && (find(&everything, key).is_some() || self.is_option(None, raw))
                    {
                        continue;
                    }
                    let severity = match source {
                        Some(Source::File { .. }) => Severity::Warning,
                        _ => Severity::Error,
                    };
                    issues.push(Issue {
                        severity,
                        location,
                        message: unknown_message(
                            key,
                            &scope,
                            &known,
                            &self.aliases_for_scheme(Some(&ep.scheme)),
                        ),
                    });
                }
            }
        }
        issues
    }

    /// Check every configuration file entry: section names, keys, value types,
    /// alias URLs and schemes.
    pub fn check_config(&self, cfg: &Config) -> Vec<Issue> {
        let everything = self.all_settings();
        let core_section: Vec<&Setting> = CORE_SECTION.iter().collect();
        let config_section: Vec<&Setting> = CONFIG_SECTION.iter().collect();
        // An invalid policy is itself reported below; check files with the default meanwhile.
        let files = crate::secrets::FilePolicies::from_config(cfg).unwrap_or_default();
        let mut issues = Vec::new();
        let mut push = |severity, e: &crate::config::Entry, message: String| {
            issues.push(Issue {
                severity,
                location: format!("{}:{}", e.origin.display(), e.line),
                message,
            });
        };

        for e in &cfg.entries {
            let (known, scope): (Vec<&Setting>, String) =
                match (e.section.as_str(), e.subsection.as_deref()) {
                    ("core", None) => {
                        if e.key == "default-scheme" && self.lookup(&e.value).is_err() {
                            push(
                                Severity::Error,
                                e,
                                format!("default-scheme: no module handles '{}'", e.value),
                            );
                            continue;
                        }
                        (core_section.clone(), "[core]".into())
                    }
                    ("core", Some("config")) => {
                        if MAIN_CONFIG_ONLY.contains(&e.key.as_str())
                            && e.origin != cfg.config_file()
                        {
                            push(
                                Severity::Warning,
                                e,
                                format!(
                                "core.config.{} is only read from {}; this setting has no effect",
                                e.key,
                                cfg.config_file().display()
                            ),
                            );
                            continue;
                        }
                        (config_section.clone(), "[core \"config\"]".into())
                    }
                    ("defaults", None) => (everything.clone(), "[defaults]".into()),
                    ("module", Some(name)) => match self.module_named(name) {
                        Some(m) => (self.settings_for(m), format!("[module \"{name}\"]")),
                        None => {
                            let names: Vec<&str> = self.modules().map(|m| m.name()).collect();
                            push(
                                Severity::Warning,
                                e,
                                format!(
                                    "[module \"{name}\"]: no module named '{name}' (available: {})",
                                    names.join(", ")
                                ),
                            );
                            continue;
                        }
                    },
                    ("protocol", Some(scheme)) => {
                        if self.lookup(scheme).is_err() {
                            push(
                                Severity::Warning,
                                e,
                                format!("[protocol \"{scheme}\"]: no module handles '{scheme}'"),
                            );
                            continue;
                        }
                        self.settings_for_scheme(scheme)
                    }
                    ("alias", Some(name)) => {
                        if e.key == "url" {
                            match Endpoint::parse(&e.value) {
                                Err(err) => {
                                    push(Severity::Error, e, format!("alias '{name}': {err}"))
                                }
                                Ok(ep) => {
                                    if let Err(err) = self.lookup(&ep.scheme) {
                                        push(Severity::Error, e, format!("alias '{name}': {err}"));
                                    }
                                }
                            }
                            continue;
                        }
                        let scheme = cfg
                            .get("alias", Some(name), "url")
                            .and_then(|u| Endpoint::parse(u).ok())
                            .map(|ep| ep.scheme)
                            .unwrap_or_default();
                        let (known, module) = self.settings_for_scheme(&scheme);
                        (known, format!("alias '{name}' ({module})"))
                    }
                    (section, sub) => {
                        let header = crate::config::section_header(section, sub);
                        push(Severity::Warning, e, format!("unknown section {header}"));
                        continue;
                    }
                };
            match find(&known, &e.key) {
                Some(setting) => {
                    if let Err(why) = setting.ty.check(&e.value) {
                        push(Severity::Error, e, format!("{}: {why}", e.key));
                    } else if let Some(policy) = match e.key.as_str() {
                        "password-file" => Some(&files.secret),
                        "key" => Some(&files.tls_key),
                        "cert" | "ca-file" => Some(&files.tls_public),
                        _ => None,
                    } {
                        let path = crate::config::expand_path(&e.value);
                        if let Err(err) = crate::secrets::check_file(&path, &e.key, policy) {
                            push(Severity::Error, e, err.to_string());
                        }
                    }
                }
                None => {
                    // Which module's aliases and option patterns apply depends on the section.
                    let section_scheme: Option<Option<String>> =
                        match (e.section.as_str(), e.subsection.as_deref()) {
                            ("protocol", Some(scheme)) => Some(Some(scheme.to_string())),
                            ("alias", Some(n)) => Some(Some(
                                cfg.get("alias", Some(n), "url")
                                    .and_then(|u| Endpoint::parse(u).ok())
                                    .map(|ep| ep.scheme)
                                    .unwrap_or_default(),
                            )),
                            ("defaults", None) => Some(None),
                            ("module", Some(n)) => Some(
                                self.module_named(n)
                                    .map(|m| m.schemes()[0].scheme.to_string()),
                            ),
                            _ => None,
                        };
                    let aliases = match &section_scheme {
                        Some(scheme) => self.aliases_for_scheme(scheme.as_deref()),
                        None => Vec::new(),
                    };
                    let inert = match &section_scheme {
                        Some(scheme) => self.inert_for_scheme(scheme.as_deref()),
                        None => Vec::new(),
                    };
                    if find_inert(&inert, &e.raw_key).is_some() {
                        continue; // accepted for compatibility, no effect
                    }
                    if let Some(alias) = find_alias(&aliases, &e.key) {
                        // Validate the converted value as its target would be.
                        match alias.value.convert(&e.value) {
                            Err(why) => push(Severity::Error, e, format!("{}: {why}", e.raw_key)),
                            Ok(v) => {
                                for target in alias.targets {
                                    if let Some(Err(why)) =
                                        find(&everything, target).map(|s| s.ty.check(&v))
                                    {
                                        push(
                                            Severity::Error,
                                            e,
                                            format!("{}: {target}: {why}", e.raw_key),
                                        );
                                        break; // one report per alias, not per target
                                    }
                                }
                            }
                        }
                        continue;
                    }
                    // Destination options pass through; which module's patterns apply
                    // depends on the section.
                    let is_option = match (e.section.as_str(), e.subsection.as_deref()) {
                        ("protocol", Some(scheme)) => self.is_option(Some(scheme), &e.raw_key),
                        ("alias", Some(n)) => {
                            let scheme = cfg
                                .get("alias", Some(n), "url")
                                .and_then(|u| Endpoint::parse(u).ok())
                                .map(|ep| ep.scheme)
                                .unwrap_or_default();
                            self.is_option(Some(&scheme), &e.raw_key)
                        }
                        ("defaults", None) => self.is_option(None, &e.raw_key),
                        ("module", Some(n)) => self.module_named(n).is_some_and(|m| {
                            self.is_option(Some(m.schemes()[0].scheme), &e.raw_key)
                        }),
                        _ => false,
                    };
                    if !is_option {
                        push(
                            Severity::Warning,
                            e,
                            unknown_message(&e.key, &scope, &known, &aliases),
                        );
                    }
                }
            }
        }

        for name in cfg.subsections("alias") {
            if cfg.get("alias", Some(&name), "url").is_none() {
                if let Some(e) = cfg.section_entries("alias", Some(&name)).first() {
                    let e = (*e).clone();
                    issues.push(Issue {
                        severity: Severity::Error,
                        location: format!("{}:{}", e.origin.display(), e.line),
                        message: format!("alias '{name}' has no 'url'"),
                    });
                }
            }
        }
        issues
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::SocketOptions;

    fn core() -> Vec<&'static Setting> {
        CORE_SETTINGS.iter().collect()
    }

    #[test]
    fn types() {
        assert!(Bool.check("yes").is_ok());
        assert!(Bool.check("maybe").is_err());
        assert!(Duration.check("250ms").is_ok());
        assert!(Duration
            .check("soon")
            .unwrap_err()
            .contains("expected a duration"));
        assert!(Size.check("4m").is_ok());
        assert!(Count.check("-1").is_err());
        assert!(Path
            .check("/definitely/not/here.pem")
            .unwrap_err()
            .contains("file not found"));
        assert!(Path
            .check(&format!("{}/src/settings.rs", env!("CARGO_MANIFEST_DIR")))
            .is_ok());
    }

    #[test]
    fn suggestions() {
        let k = core();
        assert_eq!(suggest("recieve-timeout", &k), Some("receive-timeout"));
        assert_eq!(suggest("keepalive-time", &k), Some("tcp-keepalive-time"));
        assert_eq!(suggest("CONNECT_TIMEOUT", &k), Some("connect-timeout"));
        assert_eq!(suggest("flux-capacitor", &k), None);
    }

    #[test]
    fn keys_are_unique_and_lowercase() {
        let mut keys: Vec<_> = CORE_SETTINGS.iter().map(|s| s.key).collect();
        assert!(keys.iter().all(|k| *k == k.to_ascii_lowercase()));
        keys.sort();
        keys.dedup();
        assert_eq!(keys.len(), CORE_SETTINGS.len());
    }

    struct Dummy;
    impl Module for Dummy {
        fn name(&self) -> &'static str {
            "dummy"
        }
        fn description(&self) -> &'static str {
            ""
        }
        fn schemes(&self) -> &'static [crate::SchemeInfo] {
            &[crate::SchemeInfo {
                scheme: "dummy",
                default_port: 1,
                tls: false,
            }]
        }
        fn verbs(&self) -> &'static [crate::Verb] {
            &[]
        }
        fn settings(&self) -> &'static [Setting] {
            const S: &[Setting] = &[s("flavour", Text, None, "")];
            S
        }
        fn destination_options(&self) -> &'static [DestinationOption] {
            const O: &[DestinationOption] = &[
                DestinationOption {
                    pattern: "consumer.*",
                    scope: crate::OptionScope::Consume,
                    help: "",
                },
                DestinationOption {
                    pattern: "both",
                    scope: crate::OptionScope::Both,
                    help: "",
                },
            ];
            O
        }
        fn connect(
            &self,
            _: &Endpoint,
            _: &crate::Context,
        ) -> crate::Result<Box<dyn crate::Session>> {
            unreachable!()
        }
    }

    struct Other;
    impl Module for Other {
        fn name(&self) -> &'static str {
            "other"
        }
        fn description(&self) -> &'static str {
            ""
        }
        fn schemes(&self) -> &'static [crate::SchemeInfo] {
            &[crate::SchemeInfo {
                scheme: "other",
                default_port: 2,
                tls: false,
            }]
        }
        fn verbs(&self) -> &'static [crate::Verb] {
            &[]
        }
        fn settings(&self) -> &'static [Setting] {
            const S: &[Setting] = &[s("colour", Text, None, "")];
            S
        }
        fn destination_options(&self) -> &'static [DestinationOption] {
            const O: &[DestinationOption] = &[DestinationOption {
                pattern: "other.*",
                scope: crate::OptionScope::Both,
                help: "",
            }];
            O
        }
        fn connect(
            &self,
            _: &Endpoint,
            _: &crate::Context,
        ) -> crate::Result<Box<dyn crate::Session>> {
            unreachable!()
        }
    }

    fn registry() -> Registry {
        let mut r = Registry::new();
        r.register(Box::new(Dummy));
        r.register(Box::new(Other));
        r
    }

    fn cfg(text: &str) -> Config {
        Config {
            entries: crate::config::parse(text, std::path::Path::new("cfg")).unwrap(),
            ..Default::default()
        }
    }

    fn summary(issues: &[Issue]) -> Vec<(Severity, String)> {
        issues
            .iter()
            .map(|i| (i.severity, format!("{}: {}", i.location, i.message)))
            .collect()
    }

    #[test]
    fn endpoint_checks() {
        let r = registry();
        let c = cfg(r#"
[defaults]
    colour = red
    recieve-timeout = 1s
[alias "a"]
    url = dummy://h?flavor=x
    connect-timeout = soon
    flavour = mint
"#);
        let mut ep = Endpoint::resolve("a", &c).unwrap();
        ep.set_prop("bogus", "1");
        let mut got = summary(&r.check_endpoint(&ep));
        got.sort();
        assert_eq!(
            got,
            vec![
                // `colour` belongs to another module: fine in [defaults].
                (Severity::Warning, "cfg:4: unknown setting 'recieve-timeout' for the dummy module; did you mean 'receive-timeout'?".into()),
                (Severity::Error, "URL: unknown setting 'flavor' for the dummy module; did you mean 'flavour'?".into()),
                (Severity::Error, "cfg:7: connect-timeout: expected a duration (e.g. 10s, 500ms, 2m; 0 = none), got 'soon'".into()),
                (Severity::Error, "command line: unknown setting 'bogus' for the dummy module (see `pub config settings`)".into()),
            ]
        );
    }

    #[test]
    fn config_checks() {
        let r = registry();
        let c = cfg(r#"
[core]
    default-scheme = nope
[protocol "other"]
    flavour = x
[alias "a"]
    url = dummy://h
    colour = blue
[alias "b"]
    url = amq://h
[alias "c"]
    flavour = x
[mystery]
    x = 1
[core "config"]
    strict = maybe
    stirct = true
"#);
        let got = summary(&r.check_config(&c));
        assert_eq!(
            got,
            vec![
                (Severity::Error, "cfg:3: default-scheme: no module handles 'nope'".into()),
                (Severity::Warning, "cfg:5: unknown setting 'flavour' for the other module (see `pub config settings`)".into()),
                (Severity::Warning, "cfg:8: unknown setting 'colour' for alias 'a' (the dummy module) (see `pub config settings`)".into()),
                (Severity::Error, "cfg:10: alias 'b': no module handles 'amq://' (available: dummy, other)".into()),
                (Severity::Warning, "cfg:14: unknown section [mystery]".into()),
                (Severity::Error, "cfg:16: strict: expected a bool (true/false), got 'maybe'".into()),
                (Severity::Warning, "cfg:17: unknown setting 'stirct' for [core \"config\"]; did you mean 'strict'?".into()),
                (Severity::Error, "cfg:12: alias 'c' has no 'url'".into()),
            ]
        );
    }

    #[test]
    fn destination_options_pass_through() {
        let r = registry();
        let dummy = r.lookup("dummy").unwrap().0;
        let c = cfg(r#"
[defaults]
    other.flag = 1
[alias "a"]
    url = dummy://h/queue/FOO?consumer.prefetchSize=10&connect-timeout=2s
    consumer.dispatchAsync = false
    both = yes
"#);
        let mut ep = Endpoint::resolve("a", &c).unwrap();
        assert!(
            r.check_endpoint(&ep).is_empty(),
            "{:?}",
            r.check_endpoint(&ep)
        );
        assert!(r.check_config(&c).is_empty(), "{:?}", r.check_config(&c));

        // Case preserved, settings excluded, filtered by verb scope.
        let recv = r.destination(&ep, dummy, Verb::Receive, "/queue/FOO");
        assert_eq!(
            recv.options,
            vec![
                ("both".to_string(), "yes".to_string()),
                ("consumer.dispatchAsync".to_string(), "false".to_string()),
                ("consumer.prefetchSize".to_string(), "10".to_string()),
            ]
        );
        assert_eq!(
            recv.to_string(),
            "/queue/FOO?both=yes&consumer.dispatchAsync=false&consumer.prefetchSize=10"
        );
        let send = r.destination(&ep, dummy, Verb::Send, "/queue/FOO");
        assert_eq!(send.options, vec![("both".to_string(), "yes".to_string())]);

        // A pattern of another module is not accepted outside [defaults].
        ep.set_prop("other.flag", "1");
        let issues = r.check_endpoint(&ep);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].severity, Severity::Error);
        // A bare prefix does not match `consumer.*`.
        assert!(!matches_pattern("consumer.*", "consumer."));
        assert!(matches_pattern("x-*", "x-max-length"));
        assert!(!matches_pattern("activemq.*", "ActiveMQ.prefetchSize"));
    }

    #[test]
    fn documented_limit_defaults_match_code() {
        let mut ep = Endpoint::parse("stomp://h").unwrap();
        for s in CORE_SETTINGS.iter().filter(|s| s.key.starts_with("max-")) {
            ep.set_prop(s.key, s.default.unwrap());
        }
        assert_eq!(
            crate::limits::Limits::from_endpoint(&ep).unwrap(),
            crate::limits::Limits::default()
        );

        let mut text = String::from("[core \"config\"]\n");
        for s in CONFIG_SECTION.iter().filter(|s| s.key.starts_with("max-")) {
            text.push_str(&format!("{} = {}\n", s.key, s.default.unwrap()));
        }
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("config"), text).unwrap();
        // Loads with the documented defaults; config.d limits are exercised in config tests.
        assert!(Config::load_from(dir.path()).is_ok());
        let d = crate::config::ConfigLimits::default();
        let doc = |k: &str| {
            crate::endpoint::parse_size(
                find(&CONFIG_SECTION.iter().collect::<Vec<_>>(), k)
                    .unwrap()
                    .default
                    .unwrap(),
            )
            .unwrap()
        };
        assert_eq!(
            (
                doc("max-files"),
                doc("max-file-size") as u64,
                doc("max-total-size") as u64
            ),
            (d.max_files, d.max_file_size, d.max_total_size)
        );
        assert_eq!(doc("max-depth"), crate::config::DEFAULT_MAX_DEPTH);
    }

    #[test]
    fn module_layer_sits_between_defaults_and_protocol() {
        let r = registry();
        let c = cfg(r#"
[defaults]
    message-type = bytes
    flavour = plain
[module "dummy"]
    message-type = text
    flavour = mint
    connect-timeout = 2s
[protocol "dummy"]
    connect-timeout = 3s
"#);
        let ep = r.resolve_endpoint("dummy://h", &c).unwrap();
        assert_eq!(ep.prop("message-type"), Some("text")); // module beats defaults
        assert_eq!(ep.prop("flavour"), Some("mint"));
        assert_eq!(ep.prop("connect-timeout"), Some("3s")); // protocol beats module
        assert!(
            matches!(ep.sources.get("message-type"), Some(Source::File { section, .. }) if section == "module")
        );
        // Another module's endpoint doesn't see [module "dummy"].
        assert_eq!(
            r.resolve_endpoint("other://h", &c)
                .unwrap()
                .prop("message-type"),
            Some("bytes")
        );
        // Without the registry there is no module layer.
        assert_eq!(
            Endpoint::resolve("dummy://h", &c)
                .unwrap()
                .prop("message-type"),
            Some("bytes")
        );
    }

    #[test]
    fn module_sections_are_checked() {
        let r = registry();
        let c = cfg(r#"
[module "dummy"]
    message-type = words
    colour = red
    flavour = mint
[module "nosuch"]
    x = 1
"#);
        let got = summary(&r.check_config(&c));
        assert_eq!(
            got,
            vec![
                (Severity::Error, "cfg:3: message-type: expected one of text, bytes, got 'words'".into()),
                (Severity::Warning, "cfg:4: unknown setting 'colour' for [module \"dummy\"] (see `pub config settings`)".into()),
                (Severity::Warning, "cfg:7: [module \"nosuch\"]: no module named 'nosuch' (available: dummy, other)".into()),
            ]
        );
    }

    /// The defaults shown to users must be the defaults the code actually uses.
    #[test]
    fn documented_defaults_match_socket_defaults() {
        let mut ep = Endpoint::parse("stomp://h").unwrap();
        for s in CORE_SETTINGS {
            if let Some(d) = s.default {
                assert!(
                    s.ty.check(d).is_ok(),
                    "{}: default '{d}' does not validate",
                    s.key
                );
                ep.set_prop(s.key, d);
            }
        }
        assert_eq!(
            SocketOptions::from_endpoint(&ep).unwrap(),
            SocketOptions::default()
        );
    }
}
