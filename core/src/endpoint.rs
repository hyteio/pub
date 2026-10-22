//! Turning a command-line target into a concrete [`Endpoint`].
//!
//! A target is either a URL (`stomp://host:61613/queue/orders`) or a git-style
//! alias, optionally followed by a path that is appended to the alias URL:
//!
//! ```text
//! [alias "jms.company.com"]
//!     url = stomp://mq01.company.com:61613
//!
//! pub send jms.company.com/queue/orders -d hi
//!   -> stomp://mq01.company.com:61613/queue/orders
//! ```
//!
//! Settings ("props") are layered, later winning:
//! `[defaults]` < `[protocol "<scheme>"]` < `[alias "<name>"]` < URL query < command line.

use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use percent_encoding::percent_decode_str;
use url::{Host, Url};

use crate::config::{expand_path, parse_bool, Config};
use crate::error::Result;
use crate::secrets::{read_secret_file, FilePolicies};
use crate::{bail_kind, err};

/// Where a setting's value came from, for error messages.
#[derive(Debug, Clone, PartialEq)]
pub enum Source {
    File {
        section: String,
        path: PathBuf,
        line: usize,
    },
    Url,
    CommandLine,
}

impl Source {
    /// How specific the place is; a higher rank overrides a lower one.
    /// `[defaults]` < `[module]` < `[protocol]` < `[alias]` < URL < command line.
    pub fn rank(&self) -> u8 {
        match self {
            Source::File { section, .. } => match section.as_str() {
                "module" => 1,
                "protocol" => 2,
                "alias" => 3,
                _ => 0,
            },
            Source::Url => 4,
            Source::CommandLine => 5,
        }
    }
}

impl fmt::Display for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Source::File { path, line, .. } => write!(f, "{}:{line}", path.display()),
            Source::Url => f.write_str("URL"),
            Source::CommandLine => f.write_str("command line"),
        }
    }
}

#[derive(Clone, PartialEq)]
pub struct Endpoint {
    /// The target as given, with any password in it masked.
    pub target: String,
    /// Alias name, when the target was resolved through one.
    pub alias: Option<String>,
    pub scheme: String,
    pub host: String,
    /// Explicit port; modules fall back to their scheme's default.
    pub port: Option<u16>,
    /// Percent-decoded path, e.g. `/queue/orders`. Empty when absent.
    pub path: String,
    /// Percent-decoded fragment, when the URL had a `#`. Some protocols use `#`
    /// in destinations (MQTT's multi-level wildcard); see `Module::destination_name`.
    pub fragment: Option<String>,
    /// Effective credentials; see [`Endpoint::apply_credentials`].
    pub username: Option<String>,
    pub password: Option<String>,
    /// Credentials given explicitly (URL userinfo or `-u`); they beat settings.
    pub explicit_username: Option<String>,
    pub explicit_password: Option<String>,
    /// `-u user` without a password: ask for it (see [`Endpoint::prompt_password`]).
    pub ask_password: bool,
    /// Checks for password, key and certificate files (from `[core "config"]`).
    pub files: FilePolicies,
    /// Merged settings; keys are lowercase.
    pub props: BTreeMap<String, String>,
    /// Where each entry of `props` came from.
    pub sources: BTreeMap<String, Source>,
    /// Each `props` key as originally written (props keys are lowercased).
    pub names: BTreeMap<String, String>,
    /// Settings that were given under an alias: target -> alias as written.
    pub aliased: BTreeMap<String, String>,
    /// Inert settings the module accepted and set aside: (name as written, reason).
    pub ignored: Vec<(String, &'static str)>,
}

/// Like a derived `Debug`, with passwords masked.
impl std::fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mask = |p: &Option<String>| p.as_ref().map(|_| "****");
        let props: BTreeMap<&str, &str> = self
            .props
            .iter()
            .map(|(k, v)| {
                let v = if k.contains("password") {
                    "****"
                } else {
                    v.as_str()
                };
                (k.as_str(), v)
            })
            .collect();
        f.debug_struct("Endpoint")
            .field("target", &self.target)
            .field("alias", &self.alias)
            .field("scheme", &self.scheme)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("path", &self.path)
            .field("fragment", &self.fragment)
            .field("username", &self.username)
            .field("password", &mask(&self.password))
            .field("explicit_username", &self.explicit_username)
            .field("explicit_password", &mask(&self.explicit_password))
            .field("ask_password", &self.ask_password)
            .field("files", &self.files)
            .field("props", &props)
            .field("sources", &self.sources)
            .field("names", &self.names)
            .field("aliased", &self.aliased)
            .field("ignored", &self.ignored)
            .finish()
    }
}

impl Endpoint {
    /// Resolve a target (URL or alias[/path]) against the configuration.
    pub fn resolve(target: &str, cfg: &Config) -> Result<Self> {
        Self::resolve_with(target, cfg, &|_| None)
    }

    /// Like [`resolve`](Self::resolve), with a `[module "<name>"]` layer between
    /// `[defaults]` and `[protocol]`: `module_for` maps the endpoint's scheme to
    /// the name of the module that handles it (see `Registry::resolve_endpoint`).
    pub fn resolve_with(
        target: &str,
        cfg: &Config,
        module_for: &dyn Fn(&str) -> Option<&'static str>,
    ) -> Result<Self> {
        let target = &*query_before_fragment(target.trim());
        if target.is_empty() {
            bail_kind!(Usage, "empty target");
        }

        let (url, alias) = if target.contains("://") {
            (parse_url(target)?, None)
        } else {
            let (name, rest) = match target.find(['/', '?']) {
                Some(i) => (&target[..i], &target[i..]),
                None => (target, ""),
            };
            if cfg.has_section("alias", Some(name)) {
                let base = cfg
                    .get("alias", Some(name), "url")
                    .ok_or_else(|| err!(Config, "alias '{name}' has no 'url' setting"))?;
                let mut url = parse_url(base).map_err(|e| err!(Config, "alias '{name}': {e}"))?;
                // `alias/path?query#fragment`: append the path; the query's settings
                // are added after the alias URL's own, so they win.
                let (rest, fragment) = match rest.split_once('#') {
                    Some((r, f)) => (r, Some(f)),
                    None => (rest, None),
                };
                let (path, query) = match rest.split_once('?') {
                    Some((p, q)) => (p, Some(q)),
                    None => (rest, None),
                };
                if path.split('/').any(|seg| seg == "." || seg == "..") {
                    bail_kind!(
                        Usage,
                        "'{target}': the path after an alias can't contain . or .. segments"
                    );
                }
                if fragment.is_some() {
                    url.set_fragment(fragment);
                }
                if !path.is_empty() {
                    let joined = format!("{}{}", url.path().trim_end_matches('/'), path);
                    url.set_path(&joined);
                }
                if let Some(q) = query {
                    let pairs: Vec<(String, String)> = url::form_urlencoded::parse(q.as_bytes())
                        .into_owned()
                        .collect();
                    url.query_pairs_mut().extend_pairs(pairs);
                }
                (url, Some(name.to_string()))
            } else if let Some(scheme) = cfg.get("core", None, "default-scheme") {
                (parse_url(&format!("{scheme}://{target}"))?, None)
            } else {
                bail_kind!(
                    Config,
                    "'{name}' is not a URL or a configured alias (add one with: pub alias add {name} <url>)"
                );
            }
        };

        let mut ep = Self::from_url(&url)?;
        ep.files = FilePolicies::from_config(cfg)?;
        ep.target = redact_userinfo(target);
        ep.alias = alias;

        // Layer the settings, remembering where each one came from.
        let mut props = BTreeMap::new();
        let mut sources = BTreeMap::new();
        let mut names = std::mem::take(&mut ep.names);
        let module = module_for(&ep.scheme);
        let mut layers = vec![("defaults", None)];
        if let Some(m) = module {
            layers.push(("module", Some(m)));
        }
        layers.push(("protocol", Some(ep.scheme.as_str())));
        if let Some(name) = &ep.alias {
            layers.push(("alias", Some(name.as_str())));
        }
        for (section, sub) in layers {
            for e in cfg.section_entries(section, sub) {
                if e.section == "alias" && e.key == "url" {
                    continue;
                }
                props.insert(e.key.clone(), e.value.clone());
                names.insert(e.key.clone(), e.raw_key.clone());
                sources.insert(
                    e.key.clone(),
                    Source::File {
                        section: e.section.clone(),
                        path: e.origin.clone(),
                        line: e.line,
                    },
                );
            }
        }
        for (k, v) in std::mem::take(&mut ep.props) {
            sources.insert(k.clone(), Source::Url);
            props.insert(k, v);
        }
        ep.props = props;
        ep.sources = sources;
        ep.names = names;

        // Password files and variables are read by `apply_credentials`, once the
        // caller's overrides are in: a password-file `-u user:pass` replaces is
        // never read.
        ep.username = ep
            .explicit_username
            .clone()
            .or_else(|| ep.prop("user").or(ep.prop("username")).map(str::to_string));
        ep.password = ep
            .explicit_password
            .clone()
            .or_else(|| ep.prop("password").map(str::to_string));
        Ok(ep)
    }

    /// Credentials given on the command line (`-u user[:pass]`); like URL userinfo
    /// they take precedence over settings. Call [`apply_credentials`](Self::apply_credentials) after.
    pub fn set_explicit_credentials(&mut self, user: &str, password: Option<&str>) {
        self.explicit_username = Some(user.to_string());
        if password.is_some() {
            self.explicit_password = password.map(str::to_string);
        }
        // curl's rule: `-u user` means "ask me for the password".
        self.ask_password = password.is_none();
    }

    /// Ask for the password when wanted: `-u user` with no password, or the
    /// `password-prompt` setting with none configured. `ask` shows the prompt
    /// and returns `None` when there's no terminal; then `-u user` falls back to
    /// configured passwords, while `password-prompt` is an error. Call after
    /// [`apply_credentials`](Self::apply_credentials).
    pub fn prompt_password(
        &mut self,
        ask: impl FnOnce(&str) -> Result<Option<String>>,
    ) -> Result<()> {
        let by_flag = self.ask_password && self.explicit_password.is_none();
        let by_setting = self.password.is_none() && self.prop_bool("password-prompt", false)?;
        if !(by_flag || by_setting) {
            return Ok(());
        }
        let who = match &self.username {
            Some(u) => format!("{u}@{}", self.host),
            None => self.host.clone(),
        };
        match ask(&format!("Enter password for {who}: "))? {
            Some(p) => {
                self.explicit_password = Some(p.clone());
                self.password = Some(p);
            }
            None if by_setting => bail_kind!(
                Config,
                "password-prompt is set but there is no terminal to ask on; use password-file or password-env"
            ),
            None => {}
        }
        self.ask_password = false;
        Ok(())
    }

    /// Work out the effective username and password. Precedence:
    /// URL userinfo / `-u` > `password` > `password-file` > `password-env`.
    /// Re-run after changing settings (e.g. `-O password-file=...`).
    pub fn apply_credentials(&mut self) -> Result<()> {
        self.username = self.explicit_username.clone().or_else(|| {
            self.prop("user")
                .or(self.prop("username"))
                .map(str::to_string)
        });
        self.password = if let Some(p) = &self.explicit_password {
            Some(p.clone())
        } else if let Some(p) = self.prop("password") {
            Some(p.to_string())
        } else if let Some(file) = self.prop("password-file") {
            Some(read_secret_file(
                &expand_path(file),
                "password-file",
                &self.files.secret,
            )?)
        } else if let Some(var) = self.prop("password-env") {
            Some(std::env::var(var).map_err(|_| {
                err!(
                    Config,
                    "password-env: environment variable {var} is not set"
                )
            })?)
        } else {
            None
        };
        Ok(())
    }

    /// Build an endpoint from a URL alone (no configuration).
    pub fn parse(url: &str) -> Result<Self> {
        let url = &*query_before_fragment(url);
        let mut ep = Self::from_url(&parse_url(url)?)?;
        ep.target = redact_userinfo(url);
        Ok(ep)
    }

    fn from_url(url: &Url) -> Result<Self> {
        let host = match url.host() {
            Some(Host::Domain(d)) => {
                let host = decode(d);
                // Hosts of non-special schemes are only percent-decoded: keep
                // control characters and spaces out of SNI and protocol headers.
                if host.chars().any(|c| c.is_control() || c.is_whitespace()) {
                    bail_kind!(
                        Config,
                        "URL host '{}' contains control characters or spaces",
                        host.escape_debug()
                    );
                }
                host
            }
            Some(Host::Ipv4(a)) => a.to_string(),
            Some(Host::Ipv6(a)) => a.to_string(),
            None => bail_kind!(
                Config,
                "URL '{}' has no host",
                redact_userinfo(url.as_str())
            ),
        };
        let username = Some(decode(url.username())).filter(|u| !u.is_empty());
        let password = url.password().map(decode);
        let path = decode(url.path());
        Ok(Endpoint {
            target: redact_userinfo(url.as_str()),
            alias: None,
            scheme: url.scheme().to_ascii_lowercase(),
            host,
            port: url.port(),
            path: if path == "/" { String::new() } else { path },
            fragment: url.fragment().map(decode),
            explicit_username: username.clone(),
            explicit_password: password.clone(),
            ask_password: false,
            files: FilePolicies::default(),
            username,
            password,
            sources: url
                .query_pairs()
                .map(|(k, _)| (k.to_ascii_lowercase(), Source::Url))
                .collect(),
            aliased: BTreeMap::new(),
            ignored: Vec::new(),
            names: url
                .query_pairs()
                .map(|(k, _)| (k.to_ascii_lowercase(), k.into_owned()))
                .collect(),
            props: url
                .query_pairs()
                .map(|(k, v)| (k.to_ascii_lowercase(), v.into_owned()))
                .collect(),
        })
    }

    /// A setting by name, case-insensitively (keys are stored lowercased, so
    /// camelCase names like `wireFormat.tightEncodingEnabled` work too).
    pub fn prop(&self, key: &str) -> Option<&str> {
        match self.props.get(key) {
            Some(v) => Some(v.as_str()),
            None => self
                .props
                .get(&key.to_ascii_lowercase())
                .map(String::as_str),
        }
    }

    /// Set a setting from the command line.
    pub fn set_prop(&mut self, key: &str, value: impl Into<String>) {
        let lower = key.to_ascii_lowercase();
        self.names.insert(lower.clone(), key.to_string());
        let key = lower;
        self.sources.insert(key.clone(), Source::CommandLine);
        self.props.insert(key, value.into());
    }

    pub fn prop_bool(&self, key: &str, default: bool) -> Result<bool> {
        match self.prop(key) {
            None => Ok(default),
            Some(v) => parse_bool(v)
                .ok_or_else(|| err!(Config, "'{key}' must be true or false, got '{v}'")),
        }
    }

    pub fn prop_duration(&self, key: &str) -> Result<Option<Duration>> {
        self.prop(key)
            .map(|v| {
                parse_duration(v).ok_or_else(|| err!(Config, "'{key}': invalid duration '{v}'"))
            })
            .transpose()
    }

    /// Byte size: `65536`, `64k`, `4m`, `1g` (binary units).
    pub fn prop_size(&self, key: &str) -> Result<Option<usize>> {
        self.prop(key)
            .map(|v| {
                parse_size(v).ok_or_else(|| {
                    err!(Config, "'{key}': invalid size '{v}' (e.g. 65536, 256k, 4m)")
                })
            })
            .transpose()
    }

    /// Default destination: the URL path, else the `destination` setting.
    pub fn destination(&self) -> Option<String> {
        if !self.path.is_empty() {
            return Some(self.path.clone());
        }
        self.prop("destination").map(str::to_string)
    }

    /// `scheme://user@host:port/path` with the password left out.
    pub fn display_url(&self) -> String {
        let user = self
            .username
            .as_ref()
            .map(|u| format!("{u}@"))
            .unwrap_or_default();
        let host = if self.host.contains(':') {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        };
        let port = self.port.map(|p| format!(":{p}")).unwrap_or_default();
        format!("{}://{user}{host}{port}{}", self.scheme, self.path)
    }
}

/// `mqtt://host/sensors/#?qos=1`: in a URL the `?qos=1` would be part of the fragment (and so of
/// the MQTT topic filter), but a `#` wildcard ends a topic, so a `?` after it can only be meant as
/// the query. Move it in front of the `#`, when there's no query already.
fn query_before_fragment(s: &str) -> std::borrow::Cow<'_, str> {
    match s.split_once('#') {
        Some((head, frag)) if !head.contains('?') => match frag.split_once('?') {
            Some((frag, query)) => format!("{head}?{query}#{frag}").into(),
            None => s.into(),
        },
        _ => s.into(),
    }
}

fn parse_url(s: &str) -> Result<Url> {
    let url =
        Url::parse(s).map_err(|e| err!(Config, "invalid URL '{}': {e}", redact_userinfo(s)))?;
    if url.host().is_none() {
        bail_kind!(
            Config,
            "URL '{}' has no host (expected scheme://host[:port][/path])",
            redact_userinfo(s)
        );
    }
    Ok(url)
}

/// `s` with passwords replaced by `****`: the password of a
/// `scheme://user:password@host` URL and the value of any `...password...=`
/// query setting. Works on text that doesn't parse as a URL (aliases, errors).
pub fn redact_userinfo(s: &str) -> String {
    let mut out = s.to_string();
    if let Some(start) = s.find("://").map(|i| i + 3) {
        let authority_end = s[start..]
            .find(['/', '?', '#'])
            .map_or(s.len(), |i| start + i);
        if let Some(at) = s[start..authority_end].rfind('@').map(|i| start + i) {
            if let Some(colon) = s[start..at].find(':').map(|i| start + i) {
                out = format!("{}:****{}", &s[..colon], &s[at..]);
            }
        }
    }
    let Some(q) = out.find('?') else {
        return out;
    };
    let end = out[q..].find('#').map_or(out.len(), |i| q + i);
    let query: Vec<String> = out[q + 1..end]
        .split('&')
        .map(|pair| match pair.split_once('=') {
            Some((k, _)) if k.to_ascii_lowercase().contains("password") => format!("{k}=****"),
            _ => pair.to_string(),
        })
        .collect();
    format!("{}?{}{}", &out[..q], query.join("&"), &out[end..])
}

fn decode(s: &str) -> String {
    percent_decode_str(s).decode_utf8_lossy().into_owned()
}

/// The longest duration a setting may hold: past MQTT 5's "never" (2^32 - 1
/// seconds, about 136 years), well short of overflowing the current time.
pub const MAX_DURATION: Duration = Duration::from_secs(150 * 365 * 24 * 3600);

/// `10`, `1.5`, `10s`, `500ms`, `2m`, `1h`. A non-zero value shorter than a
/// millisecond is one millisecond (zero means "none" or "forever" in settings).
pub fn parse_duration(s: &str) -> Option<Duration> {
    let s = s.trim();
    let split = s
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let n: f64 = num.parse().ok()?;
    let secs = match unit.trim() {
        "" | "s" | "sec" | "secs" => n,
        "ms" => n / 1000.0,
        "m" | "min" => n * 60.0,
        "h" => n * 3600.0,
        _ => return None,
    };
    let d = Duration::try_from_secs_f64(secs).ok()?;
    if d > MAX_DURATION {
        return None;
    }
    if n > 0.0 && d < Duration::from_millis(1) {
        return Some(Duration::from_millis(1));
    }
    Some(d)
}

/// `65536`, `64k`, `64KB`, `4m`, `1g` — binary multiples.
pub fn parse_size(s: &str) -> Option<usize> {
    let s = s.trim();
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let n: usize = num.parse().ok()?;
    let mult: usize = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" | "ki" | "kib" => 1 << 10,
        "m" | "mb" | "mi" | "mib" => 1 << 20,
        "g" | "gb" | "gi" | "gib" => 1 << 30,
        _ => return None,
    };
    // Anything past a terabyte is a mistake, and leaves room for `size + 1`.
    n.checked_mul(mult).filter(|&b| b <= MAX_SIZE)
}

/// The largest size a setting may hold.
pub const MAX_SIZE: usize = 1 << 40;

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn cfg(text: &str) -> Config {
        Config {
            entries: crate::config::parse(text, Path::new("t")).unwrap(),
            ..Default::default()
        }
    }

    #[test]
    fn a_query_after_the_wildcard_is_the_query() {
        let ep = Endpoint::parse("mqtt://h/sensors/#?version=3.1.1").unwrap();
        assert_eq!(
            (ep.path.as_str(), ep.fragment.as_deref()),
            ("/sensors/", Some(""))
        );
        assert_eq!(ep.prop("version"), Some("3.1.1"));
        // An explicit query keeps a later `?` in the fragment.
        let ep = Endpoint::parse("mqtt://h/a?qos=1#b?c").unwrap();
        assert_eq!(
            (ep.prop("qos"), ep.fragment.as_deref()),
            (Some("1"), Some("b?c"))
        );
        assert_eq!(query_before_fragment("stomp://h/q"), "stomp://h/q");
        assert_eq!(query_before_fragment("prod/s/#?qos=1"), "prod/s/?qos=1#");
    }

    #[test]
    fn plain_url() {
        let ep = Endpoint::resolve(
            "stomp://u:p%40ss@mq:61613/queue/a%20b?receipts=false",
            &Config::default(),
        )
        .unwrap();
        assert_eq!(ep.scheme, "stomp");
        assert_eq!(ep.host, "mq");
        assert_eq!(ep.port, Some(61613));
        assert_eq!(ep.path, "/queue/a b");
        assert_eq!(ep.username.as_deref(), Some("u"));
        assert_eq!(ep.password.as_deref(), Some("p@ss"));
        assert_eq!(ep.prop("receipts"), Some("false"));
        assert_eq!(ep.display_url(), "stomp://u@mq:61613/queue/a b");
    }

    #[test]
    fn alias_maps_to_different_host_scheme_and_port() {
        let c = cfg(r#"
[alias "jms.company.com"]
    url = stomp+ssl://mq01.company.com:61612
    user = app
    password = secret
"#);
        let ep = Endpoint::resolve("jms.company.com", &c).unwrap();
        assert_eq!(ep.alias.as_deref(), Some("jms.company.com"));
        assert_eq!(ep.scheme, "stomp+ssl");
        assert_eq!(ep.host, "mq01.company.com");
        assert_eq!(ep.port, Some(61612));
        assert_eq!(ep.path, "");
        assert_eq!(ep.username.as_deref(), Some("app"));
        assert_eq!(ep.password.as_deref(), Some("secret"));
        assert!(!ep.props.contains_key("url"));
    }

    #[test]
    fn alias_path_is_appended() {
        let c = cfg("[alias \"q\"]\nurl = stomp://h/queue\n[alias \"r\"]\nurl = stomp://h:1/\n");
        assert_eq!(
            Endpoint::resolve("q/orders", &c).unwrap().path,
            "/queue/orders"
        );
        assert_eq!(Endpoint::resolve("r/topic/x", &c).unwrap().path, "/topic/x");
    }

    #[test]
    fn query_after_alias_overrides_alias_settings() {
        let c = cfg("[alias \"jms\"]\nurl = stomp://h:1/queue?vhost=a&receipts=false\nreceive-timeout = 10s\n");
        let ep = Endpoint::resolve("jms/orders?receive-timeout=2s&vhost=b", &c).unwrap();
        assert_eq!(ep.path, "/queue/orders");
        assert_eq!(ep.prop("receive-timeout"), Some("2s"));
        assert_eq!(ep.prop("vhost"), Some("b"));
        assert_eq!(ep.prop("receipts"), Some("false"));
        assert_eq!(ep.sources.get("receive-timeout"), Some(&Source::Url));

        let ep = Endpoint::resolve("jms?receive-timeout=3s", &c).unwrap();
        assert_eq!(
            (ep.path.as_str(), ep.prop("receive-timeout")),
            ("/queue", Some("3s"))
        );
    }

    #[cfg(unix)]
    #[test]
    fn credential_precedence() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("pw");
        std::fs::write(&file, "from-file\n").unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o400)).unwrap();

        let c = cfg(&format!(
            "[alias \"a\"]\nurl = stomp://h\nuser = app\npassword-file = {}\npassword-env = PUB_TEST_UNSET_VAR\n",
            file.display()
        ));
        let mut ep = Endpoint::resolve("a", &c).unwrap();
        ep.apply_credentials().unwrap();
        assert_eq!(ep.password.as_deref(), Some("from-file")); // file beats env

        ep.set_prop("password", "literal");
        ep.apply_credentials().unwrap();
        assert_eq!(ep.password.as_deref(), Some("literal")); // password beats file

        ep.set_explicit_credentials("cli-user", Some("cli-pass"));
        ep.apply_credentials().unwrap();
        assert_eq!(
            (ep.username.as_deref(), ep.password.as_deref()),
            (Some("cli-user"), Some("cli-pass"))
        );

        // URL userinfo is explicit too.
        let mut ep = Endpoint::resolve("stomp://u:p@h", &c).unwrap();
        ep.set_prop("password", "ignored");
        ep.apply_credentials().unwrap();
        assert_eq!(ep.password.as_deref(), Some("p"));
    }

    #[test]
    fn settings_layering() {
        let c = cfg(r#"
[defaults]
    connect-timeout = 1s
    receipts = true
[protocol "stomp"]
    connect-timeout = 2s
    vhost = shared
[alias "a"]
    url = stomp://h?receipts=false
    connect-timeout = 3s
"#);
        let ep = Endpoint::resolve("a", &c).unwrap();
        assert_eq!(ep.prop("connect-timeout"), Some("3s"));
        assert_eq!(ep.prop("vhost"), Some("shared"));
        assert_eq!(ep.prop("receipts"), Some("false"));
        assert_eq!(
            ep.prop_duration("connect-timeout").unwrap(),
            Some(Duration::from_secs(3))
        );
    }

    #[test]
    fn unknown_name_without_default_scheme_errors() {
        let e = Endpoint::resolve("nowhere", &Config::default()).unwrap_err();
        assert_eq!(crate::exit_code(&e), 3);
        let c = cfg("[core]\ndefault-scheme = stomp\n");
        let ep = Endpoint::resolve("mq:1234/queue/x", &c).unwrap();
        assert_eq!(
            (ep.scheme.as_str(), ep.port, ep.path.as_str()),
            ("stomp", Some(1234), "/queue/x")
        );
    }

    #[test]
    fn ipv6_host() {
        let ep = Endpoint::parse("stomp://[::1]:61613").unwrap();
        assert_eq!(ep.host, "::1");
        assert_eq!(ep.display_url(), "stomp://[::1]:61613");
    }

    #[test]
    fn sizes() {
        assert_eq!(parse_size("65536"), Some(65536));
        assert_eq!(parse_size("256k"), Some(256 * 1024));
        assert_eq!(parse_size("4MB"), Some(4 << 20));
        assert_eq!(parse_size("1GiB"), Some(1 << 30));
        assert_eq!(parse_size("lots"), None);
        assert_eq!(parse_size("1t"), None);
    }

    #[test]
    fn props_are_case_insensitive() {
        let mut ep = Endpoint::parse("stomp://h?wireFormat.tightEncodingEnabled=false").unwrap();
        assert_eq!(ep.prop("wireFormat.tightEncodingEnabled"), Some("false"));
        assert_eq!(ep.prop("wireformat.tightencodingenabled"), Some("false"));
        ep.set_prop("jms.closeTimeout", "5000");
        assert_eq!(ep.prop("JMS.CLOSETIMEOUT"), Some("5000"));
        assert_eq!(
            ep.names.get("jms.closetimeout").map(String::as_str),
            Some("jms.closeTimeout")
        );
    }

    #[test]
    fn durations() {
        assert_eq!(parse_duration("10"), Some(Duration::from_secs(10)));
        assert_eq!(parse_duration("250ms"), Some(Duration::from_millis(250)));
        assert_eq!(parse_duration("1.5s"), Some(Duration::from_millis(1500)));
        assert_eq!(parse_duration("2m"), Some(Duration::from_secs(120)));
        assert_eq!(parse_duration("soon"), None);
    }

    #[test]
    fn password_prompt_rules() {
        let typed = |p: &str| -> Result<Option<String>> {
            assert!(p.starts_with("Enter password for app@h"), "{p}");
            Ok(Some("typed".into()))
        };
        let no_terminal = |_: &str| -> Result<Option<String>> { Ok(None) };
        let never = |_: &str| -> Result<Option<String>> { panic!("asked") };

        // -u user: always asks, even with a configured password.
        let mut ep = Endpoint::parse("stomp://h?password=configured").unwrap();
        ep.set_explicit_credentials("app", None);
        ep.apply_credentials().unwrap();
        ep.prompt_password(typed).unwrap();
        assert_eq!(ep.password.as_deref(), Some("typed"));
        // ... and asks only once.
        ep.apply_credentials().unwrap();
        ep.prompt_password(never).unwrap();
        assert_eq!(ep.password.as_deref(), Some("typed"));
        // Without a terminal it falls back to the configured one.
        let mut ep = Endpoint::parse("stomp://h?password=configured").unwrap();
        ep.set_explicit_credentials("app", None);
        ep.apply_credentials().unwrap();
        ep.prompt_password(no_terminal).unwrap();
        assert_eq!(ep.password.as_deref(), Some("configured"));
        // -u user:pass and -u user: never ask.
        let mut ep = Endpoint::parse("stomp://h").unwrap();
        ep.set_explicit_credentials("app", Some(""));
        ep.apply_credentials().unwrap();
        ep.prompt_password(never).unwrap();
        assert_eq!(ep.password.as_deref(), Some(""));

        // password-prompt: only when nothing is configured; no terminal is an error.
        let mut ep = Endpoint::parse("stomp://app@h?password-prompt=true").unwrap();
        ep.apply_credentials().unwrap();
        ep.prompt_password(typed).unwrap();
        assert_eq!(ep.password.as_deref(), Some("typed"));
        let mut ep = Endpoint::parse("stomp://app@h?password-prompt=true&password=x").unwrap();
        ep.apply_credentials().unwrap();
        ep.prompt_password(never).unwrap();
        let mut ep = Endpoint::parse("stomp://app@h?password-prompt=true").unwrap();
        ep.apply_credentials().unwrap();
        let e = ep.prompt_password(no_terminal).unwrap_err();
        assert_eq!(crate::exit_code(&e), 3);
    }

    #[test]
    fn passwords_stay_out_of_errors_targets_and_debug() {
        assert_eq!(
            redact_userinfo("stomp://u:hunter2@h:1/q"),
            "stomp://u:****@h:1/q"
        );
        assert_eq!(redact_userinfo("stomp://u@h/q:x@y"), "stomp://u@h/q:x@y");
        assert_eq!(redact_userinfo("stomp://u:p@w@h"), "stomp://u:****@h");
        assert_eq!(redact_userinfo("not a url"), "not a url");
        assert_eq!(
            redact_userinfo("mq?password=zz&x=1"),
            "mq?password=****&x=1"
        );
        let e = Endpoint::parse("stomp://user:hunter2@host:99999/q").unwrap_err();
        assert!(!format!("{e:#}").contains("hunter2"), "{e:#}");
        let ep = Endpoint::parse("stomp://user:hunter2@host/q?password=pw2").unwrap();
        assert!(!ep.target.contains("hunter2"), "{}", ep.target);
        let dbg = format!("{ep:?}");
        assert!(!dbg.contains("hunter2") && !dbg.contains("pw2"), "{dbg}");
    }

    #[test]
    fn sizes_and_durations_are_strict() {
        assert_eq!(parse_size("64k"), Some(65536));
        assert_eq!(parse_size("4 MiB"), Some(4 << 20));
        assert_eq!(parse_size("1bbbb"), None);
        assert_eq!(parse_size("1kiiib"), None);
        assert_eq!(parse_size("18446744073709551615"), None);
        assert_eq!(
            parse_duration("0.0000000001"),
            Some(Duration::from_millis(1))
        );
        assert_eq!(parse_duration("0"), Some(Duration::ZERO));
        assert_eq!(parse_duration("2000000h"), None);
    }

    #[test]
    fn alias_suffixes_keep_fragments_apart_and_refuse_dot_segments() {
        let c = cfg("[alias \"a\"]\nurl = mqtt://h/base\n");
        let ep = Endpoint::resolve("a?password=zz#frag", &c).unwrap();
        assert_eq!(ep.prop("password"), Some("zz"));
        assert_eq!(ep.fragment.as_deref(), Some("frag"));
        let ep = Endpoint::resolve("a/sensors/#", &c).unwrap();
        assert_eq!(
            (ep.path.as_str(), ep.fragment.as_deref()),
            ("/base/sensors/", Some(""))
        );
        assert!(Endpoint::resolve("a/../../x", &c).is_err());
    }
}
