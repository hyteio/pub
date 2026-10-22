//! Configuration in git-config syntax.
//!
//! Files are read from the pub home (`$PUB_HOME`, default `~/.pub`): first `config`,
//! then every file in `config.d/` in name order. Later entries win, so a file in
//! `config.d/` can override the main config.
//!
//! ```text
//! [defaults]
//!     connect-timeout = 5s
//!
//! [alias "jms.company.com"]
//!     url = stomp://company.com:61613
//!     user = app
//! ```
//!
//! Section names and keys are case-insensitive; subsection names (alias names) are
//! case-sensitive, as in git.

use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::{bail_kind, err};

pub const HOME_ENV: &str = "PUB_HOME";
pub const CONFIG_FILE: &str = "config";
pub const CONFIG_DIR: &str = "config.d";

#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    /// Lowercased section name.
    pub section: String,
    pub subsection: Option<String>,
    /// Lowercased key.
    pub key: String,
    /// The key as written; destination options pass through with this spelling.
    pub raw_key: String,
    pub value: String,
    pub origin: PathBuf,
    pub line: usize,
}

impl Entry {
    /// `section.key` or `section.subsection.key`, as git prints it.
    pub fn name(&self) -> String {
        match &self.subsection {
            Some(sub) => format!("{}.{}.{}", self.section, sub, self.key),
            None => format!("{}.{}", self.section, self.key),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Config {
    pub home: PathBuf,
    pub files: Vec<PathBuf>,
    pub entries: Vec<Entry>,
}

impl Config {
    /// The pub home directory: `$PUB_HOME` or `~/.pub`.
    pub fn home_dir() -> Result<PathBuf> {
        if let Some(h) = std::env::var_os(HOME_ENV).filter(|h| !h.is_empty()) {
            return Ok(PathBuf::from(h));
        }
        let home = std::env::var_os("HOME")
            .or_else(|| std::env::var_os("USERPROFILE"))
            .ok_or_else(|| err!(Config, "cannot determine home directory; set {HOME_ENV}"))?;
        Ok(PathBuf::from(home).join(".pub"))
    }

    pub fn load() -> Result<Self> {
        Self::load_from(Self::home_dir()?)
    }

    /// Load `home/config` and `home/config.d/**`. Missing files are not an error.
    ///
    /// Everything read is checked: ownership and permissions (like ssh), sizes and
    /// counts ([`ConfigLimits`]), and the file format (line, key and value lengths,
    /// no control characters).
    pub fn load_from(home: impl Into<PathBuf>) -> Result<Self> {
        let home = home.into();
        let mut cfg = Config {
            home: home.clone(),
            ..Default::default()
        };
        let check_all = permission_checks_from_env()?;
        if check_all && home.is_dir() {
            check_config_permissions(&home)?;
        }

        let mut budget = Budget::new(ConfigLimits::default());
        let main = home.join(CONFIG_FILE);
        if main.is_file() {
            if check_all {
                check_config_permissions(&main)?;
            }
            cfg.add_file_within(&main, &mut budget)?;
        }

        // Only the main config may govern how config.d is read.
        let max_depth = match cfg.get("core", Some("config"), "max-depth") {
            None => DEFAULT_MAX_DEPTH,
            Some(v) => v.trim().parse::<usize>().ok().filter(|d| *d <= MAX_DEPTH_CEILING).ok_or_else(|| {
                err!(Config, "core.config.max-depth must be a number from 0 to {MAX_DEPTH_CEILING}, got '{v}'")
            })?,
        };
        budget.limits = ConfigLimits::from_main(&cfg)?;
        let check_d = check_all
            && match cfg.get("core", Some("config"), "check-permissions") {
                None => true,
                Some(v) => parse_bool(v).ok_or_else(|| {
                    err!(
                        Config,
                        "core.config.check-permissions must be true or false, got '{v}'"
                    )
                })?,
            };

        let dir = home.join(CONFIG_DIR);
        if dir.is_dir() {
            let mut files = Vec::new();
            let mut visited = HashSet::new();
            let walk = Walk {
                max_depth,
                check_permissions: check_d,
            };
            collect_fragments(&dir, PathBuf::new(), 0, &walk, &mut visited, &mut files)?;
            if files.len() > budget.limits.max_files {
                bail_kind!(
                    Config,
                    "{} holds {} config files, more than core.config.max-files ({}); raise it in {}",
                    dir.display(),
                    files.len(),
                    budget.limits.max_files,
                    CONFIG_FILE
                );
            }
            files.sort_by(|a, b| a.0.cmp(&b.0));
            for (_, f) in files {
                cfg.add_file_within(&f, &mut budget)?;
            }
        }
        cfg.check_entry_counts()?;
        Ok(cfg)
    }

    pub fn add_file(&mut self, path: &Path) -> Result<()> {
        self.add_file_within(path, &mut Budget::new(ConfigLimits::default()))
    }

    fn add_file_within(&mut self, path: &Path, budget: &mut Budget) -> Result<()> {
        let text = read_config_text(path, budget.limits.max_file_size)?;
        budget.total += text.len() as u64;
        if budget.total > budget.limits.max_total_size {
            bail_kind!(
                Config,
                "configuration exceeds core.config.max-total-size ({} bytes) at {}; raise it in {}",
                budget.limits.max_total_size,
                path.display(),
                CONFIG_FILE
            );
        }
        self.entries.extend(parse(&text, path)?);
        self.files.push(path.to_path_buf());
        Ok(())
    }

    fn check_entry_counts(&self) -> Result<()> {
        if self.entries.len() > MAX_ENTRIES {
            bail_kind!(
                Config,
                "configuration has {} settings, more than the limit of {MAX_ENTRIES}",
                self.entries.len()
            );
        }
        let mut per_section: BTreeMap<(&str, Option<&str>), usize> = BTreeMap::new();
        for e in &self.entries {
            let n = per_section
                .entry((e.section.as_str(), e.subsection.as_deref()))
                .or_default();
            *n += 1;
            if *n > MAX_ENTRIES_PER_SECTION {
                bail_kind!(
                    Config,
                    "{}:{}: section {} has more than {MAX_ENTRIES_PER_SECTION} settings",
                    e.origin.display(),
                    e.line,
                    section_header(&e.section, e.subsection.as_deref())
                );
            }
        }
        Ok(())
    }

    /// `core.config.strict` (`[core "config"] strict`), default true: config-file
    /// warnings are errors.
    pub fn strict(&self) -> Result<bool> {
        match self.get("core", Some("config"), "strict") {
            None => Ok(true),
            Some(v) => parse_bool(v).ok_or_else(|| {
                err!(
                    Config,
                    "core.config.strict must be true or false, got '{v}'"
                )
            }),
        }
    }

    pub fn config_file(&self) -> PathBuf {
        self.home.join(CONFIG_FILE)
    }

    pub fn config_dir(&self) -> PathBuf {
        self.home.join(CONFIG_DIR)
    }

    fn matching(&self, section: &str, sub: Option<&str>) -> Vec<&Entry> {
        self.entries
            .iter()
            .filter(|e| e.section.eq_ignore_ascii_case(section) && e.subsection.as_deref() == sub)
            .collect()
    }

    /// Last value for `section[.sub].key`.
    pub fn get(&self, section: &str, sub: Option<&str>, key: &str) -> Option<&str> {
        self.matching(section, sub)
            .into_iter()
            .rfind(|e| e.key.eq_ignore_ascii_case(key))
            .map(|e| e.value.as_str())
    }

    /// All keys of a section merged, later entries winning.
    pub fn section(&self, section: &str, sub: Option<&str>) -> BTreeMap<String, String> {
        self.matching(section, sub)
            .into_iter()
            .map(|e| (e.key.clone(), e.value.clone()))
            .collect()
    }

    /// Entries (with origins) of one section, in file order.
    pub fn section_entries(&self, section: &str, sub: Option<&str>) -> Vec<&Entry> {
        self.matching(section, sub)
    }

    pub fn has_section(&self, section: &str, sub: Option<&str>) -> bool {
        !self.matching(section, sub).is_empty()
    }

    /// Distinct subsection names of a section, in order of first appearance.
    pub fn subsections(&self, section: &str) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for e in &self.entries {
            if let Some(sub) = &e.subsection {
                if e.section.eq_ignore_ascii_case(section) && !out.contains(sub) {
                    out.push(sub.clone());
                }
            }
        }
        out
    }
}

/// Volume limits for reading configuration; the main config may raise them under
/// `[core "config"]` (`max-files`, `max-file-size`, `max-total-size`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigLimits {
    /// Files under config.d.
    pub max_files: usize,
    /// Bytes per file.
    pub max_file_size: u64,
    /// Bytes across all files.
    pub max_total_size: u64,
}

impl Default for ConfigLimits {
    fn default() -> Self {
        ConfigLimits {
            max_files: 1024,
            max_file_size: 64 * 1024,
            max_total_size: 4 * 1024 * 1024,
        }
    }
}

impl ConfigLimits {
    fn from_main(cfg: &Config) -> Result<Self> {
        let d = ConfigLimits::default();
        let get = |key: &str, default: u64| -> Result<u64> {
            match cfg.get("core", Some("config"), key) {
                None => Ok(default),
                Some(v) => crate::endpoint::parse_size(v)
                    .map(|n| n as u64)
                    .ok_or_else(|| err!(Config, "core.config.{key} must be a number or size (e.g. 2048, 256k), got '{v}'")),
            }
        };
        Ok(ConfigLimits {
            max_files: get("max-files", d.max_files as u64)? as usize,
            max_file_size: get("max-file-size", d.max_file_size)?,
            max_total_size: get("max-total-size", d.max_total_size)?,
        })
    }
}

struct Budget {
    limits: ConfigLimits,
    total: u64,
}

impl Budget {
    fn new(limits: ConfigLimits) -> Self {
        Budget { limits, total: 0 }
    }
}

/// Format limits; fixed, since no legitimate config comes near them.
pub const MAX_ENTRIES: usize = 16_384;
pub const MAX_ENTRIES_PER_SECTION: usize = 256;
pub const MAX_KEY_LEN: usize = 128;
pub const MAX_SUBSECTION_LEN: usize = 253;
pub const MAX_VALUE_LEN: usize = 4096;
pub const MAX_LINE_LEN: usize = 8192;

/// Environment override for the permission checks (`PUB_CHECK_PERMISSIONS=false`).
/// The main config cannot turn its own check off: a writable config doing that
/// is exactly the attack being prevented.
pub const CHECK_PERMISSIONS_ENV: &str = "PUB_CHECK_PERMISSIONS";

fn permission_checks_from_env() -> Result<bool> {
    match std::env::var(CHECK_PERMISSIONS_ENV) {
        Err(_) => Ok(true),
        Ok(v) => parse_bool(&v).ok_or_else(|| {
            err!(
                Config,
                "{CHECK_PERMISSIONS_ENV} must be true or false, got '{v}'"
            )
        }),
    }
}

/// Like ssh: configuration must be owned by you or root and not writable by group
/// or others, since it can disable TLS verification or redirect credentials.
/// Symlinks are followed; the target is checked.
pub fn check_config_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let meta =
            fs::metadata(path).map_err(|e| err!(Config, "cannot read {}: {e}", path.display()))?;
        let how = format!(
            "(to skip this check: core.config.check-permissions = false in {CONFIG_FILE} for config.d, or {CHECK_PERMISSIONS_ENV}=false)"
        );
        // SAFETY: geteuid has no preconditions and cannot fail.
        let me = unsafe { libc::geteuid() };
        if meta.uid() != me && meta.uid() != 0 {
            bail_kind!(
                Config,
                "{} is owned by uid {}; configuration must be owned by you (uid {me}) or root {how}",
                path.display(),
                meta.uid()
            );
        }
        let mode = meta.mode() & 0o777;
        if mode & 0o022 != 0 {
            bail_kind!(
                Config,
                "{} is writable by {} (mode {mode:04o}); run `chmod go-w {}` {how}",
                path.display(),
                if mode & 0o002 != 0 {
                    "everyone"
                } else {
                    "its group"
                },
                path.display()
            );
        }
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn read_config_text(path: &Path, max: u64) -> Result<String> {
    use std::io::Read;
    let file =
        fs::File::open(path).map_err(|e| err!(Config, "cannot read {}: {e}", path.display()))?;
    let mut buf = Vec::new();
    file.take(max.saturating_add(1))
        .read_to_end(&mut buf)
        .map_err(|e| err!(Config, "cannot read {}: {e}", path.display()))?;
    if buf.len() as u64 > max {
        bail_kind!(
            Config,
            "{} is larger than core.config.max-file-size ({max} bytes); raise it in {CONFIG_FILE}",
            path.display()
        );
    }
    String::from_utf8(buf).map_err(|_| err!(Config, "{} is not valid UTF-8", path.display()))
}

/// `config.d` is read recursively up to this depth unless `core.config.max-depth`
/// (in the main config) says otherwise; 0 means flat.
pub const DEFAULT_MAX_DEPTH: usize = 16;
const MAX_DEPTH_CEILING: usize = 64;

/// Walk `dir`, collecting (relative path, path) of config fragments.
///
/// - Hidden entries are skipped, files and directories alike. Besides dotfiles
///   this skips Kubernetes ConfigMap internals (`..data`, `..2026_...`), so each
///   key is read once, through its top-level symlink.
/// - Symlinks are followed; a directory reached twice (links, loops) is read once.
/// - Going deeper than `max_depth` is an error rather than a silent skip.
struct Walk {
    max_depth: usize,
    check_permissions: bool,
}

fn collect_fragments(
    dir: &Path,
    rel: PathBuf,
    depth: usize,
    walk: &Walk,
    visited: &mut HashSet<PathBuf>,
    out: &mut Vec<(PathBuf, PathBuf)>,
) -> Result<()> {
    let max_depth = walk.max_depth;
    if walk.check_permissions {
        check_config_permissions(dir)?;
    }
    let real =
        fs::canonicalize(dir).map_err(|e| err!(Config, "cannot read {}: {e}", dir.display()))?;
    if !visited.insert(real) {
        return Ok(());
    }
    let entries =
        fs::read_dir(dir).map_err(|e| err!(Config, "cannot read {}: {e}", dir.display()))?;
    for entry in entries {
        let entry = entry.map_err(|e| err!(Config, "cannot read {}: {e}", dir.display()))?;
        let path = entry.path();
        let name = entry.file_name();
        if name.to_string_lossy().starts_with('.') {
            continue;
        }
        // Follows symlinks; a dangling link is reported rather than ignored.
        let meta =
            fs::metadata(&path).map_err(|e| err!(Config, "cannot read {}: {e}", path.display()))?;
        if meta.is_dir() {
            if depth + 1 > max_depth {
                bail_kind!(
                    Config,
                    "{} is nested deeper than core.config.max-depth ({max_depth}); flatten it or raise max-depth in {}",
                    path.display(),
                    CONFIG_FILE
                );
            }
            collect_fragments(&path, rel.join(&name), depth + 1, walk, visited, out)?;
        } else if meta.is_file() && is_config_fragment(&path) {
            if walk.check_permissions {
                check_config_permissions(&path)?;
            }
            out.push((rel.join(&name), path));
        }
    }
    Ok(())
}

/// Skip hidden files and editor leftovers in `config.d/`.
fn is_config_fragment(p: &Path) -> bool {
    let Some(name) = p.file_name().and_then(|n| n.to_str()) else {
        return false;
    };
    !(name.starts_with('.')
        || name.ends_with('~')
        || name.ends_with(".swp")
        || name.ends_with(".bak"))
}

pub fn parse_file(path: &Path) -> Result<Vec<Entry>> {
    let text = read_config_text(path, ConfigLimits::default().max_file_size)?;
    parse(&text, path)
}

/// Parse git-config text.
pub fn parse(text: &str, origin: &Path) -> Result<Vec<Entry>> {
    let mut out = Vec::new();
    let mut section: Option<(String, Option<String>)> = None;

    for (idx, raw) in text.lines().enumerate() {
        let line_no = idx + 1;
        let at = || format!("{}:{line_no}", origin.display());
        if raw.len() > MAX_LINE_LEN {
            bail_kind!(Config, "{}: line is longer than {MAX_LINE_LEN} bytes", at());
        }
        if let Some(c) = raw.chars().find(|c| c.is_control() && *c != '\t') {
            bail_kind!(
                Config,
                "{}: control character U+{:04X} is not allowed",
                at(),
                c as u32
            );
        }
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }

        if let Some(rest) = line.strip_prefix('[') {
            let header = parse_section_header(rest).map_err(|m| err!(Config, "{}: {m}", at()))?;
            if header
                .1
                .as_ref()
                .is_some_and(|sub| sub.len() > MAX_SUBSECTION_LEN)
            {
                bail_kind!(
                    Config,
                    "{}: section name is longer than {MAX_SUBSECTION_LEN} bytes",
                    at()
                );
            }
            section = Some(header);
            continue;
        }

        let Some((sec, sub)) = &section else {
            bail_kind!(Config, "{}: key outside of any [section]", at());
        };
        let (key, value) = match line.find('=') {
            Some(i) => (
                line[..i].trim(),
                parse_value(&line[i + 1..]).map_err(|m| err!(Config, "{}: {m}", at()))?,
            ),
            // A bare key is boolean true, as in git.
            None => (
                line.split(['#', ';']).next().unwrap_or("").trim(),
                "true".to_string(),
            ),
        };
        if key.len() > MAX_KEY_LEN {
            bail_kind!(Config, "{}: key is longer than {MAX_KEY_LEN} bytes", at());
        }
        if !valid_key(key) {
            bail_kind!(Config, "{}: invalid key '{key}'", at());
        }
        if value.len() > MAX_VALUE_LEN {
            bail_kind!(
                Config,
                "{}: value of '{key}' is longer than {MAX_VALUE_LEN} bytes",
                at()
            );
        }
        out.push(Entry {
            section: sec.clone(),
            subsection: sub.clone(),
            key: key.to_ascii_lowercase(),
            raw_key: key.to_string(),
            value,
            origin: origin.to_path_buf(),
            line: line_no,
        });
    }
    Ok(out)
}

fn parse_section_header(rest: &str) -> std::result::Result<(String, Option<String>), String> {
    let close = rest.rfind(']').ok_or("missing ']' in section header")?;
    let trailing = rest[close + 1..].trim();
    if !(trailing.is_empty() || trailing.starts_with('#') || trailing.starts_with(';')) {
        return Err("unexpected text after section header".into());
    }
    let inner = rest[..close].trim();
    let (name, sub) = match inner.find(|c: char| c.is_whitespace()) {
        None => (inner, None),
        Some(i) => {
            let quoted = inner[i..].trim();
            let body = quoted
                .strip_prefix('"')
                .and_then(|q| q.strip_suffix('"'))
                .ok_or("subsection name must be quoted: [section \"name\"]")?;
            let mut sub = String::new();
            let mut chars = body.chars();
            while let Some(c) = chars.next() {
                if c == '\\' {
                    sub.push(chars.next().ok_or("trailing backslash in subsection")?);
                } else {
                    sub.push(c);
                }
            }
            (&inner[..i], Some(sub))
        }
    };
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '.')
    {
        return Err(format!("invalid section name '{name}'"));
    }
    Ok((name.to_ascii_lowercase(), sub))
}

fn valid_key(key: &str) -> bool {
    let mut chars = key.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '.' || c == '_')
}

/// Value syntax: optional double quotes, `\n \t \\ \"` escapes, `#`/`;` start a
/// comment outside quotes, unquoted surrounding whitespace is trimmed.
fn parse_value(raw: &str) -> std::result::Result<String, String> {
    let mut out = String::new();
    let mut pending_ws = String::new();
    let mut in_quotes = false;
    let mut chars = raw.trim_start().chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => {
                let e = chars.next().ok_or("trailing backslash")?;
                let r = match e {
                    'n' => '\n',
                    't' => '\t',
                    '\\' => '\\',
                    '"' => '"',
                    other => return Err(format!("invalid escape '\\{other}'")),
                };
                out.push_str(&std::mem::take(&mut pending_ws));
                out.push(r);
            }
            '"' => {
                out.push_str(&std::mem::take(&mut pending_ws));
                in_quotes = !in_quotes;
            }
            '#' | ';' if !in_quotes => break,
            c if c.is_whitespace() && !in_quotes => pending_ws.push(c),
            c => {
                out.push_str(&std::mem::take(&mut pending_ws));
                out.push(c);
            }
        }
    }
    if in_quotes {
        return Err("unterminated quote".into());
    }
    Ok(out)
}

/// Render a value so that [`parse_value`] reads it back unchanged.
pub fn quote_value(v: &str) -> String {
    let plain = !v.is_empty()
        && !v.starts_with(char::is_whitespace)
        && !v.ends_with(char::is_whitespace)
        && !v.contains(['#', ';', '"', '\\', '\n', '\t']);
    if plain {
        return v.to_string();
    }
    let mut s = String::from("\"");
    for c in v.chars() {
        match c {
            '"' => s.push_str("\\\""),
            '\\' => s.push_str("\\\\"),
            '\n' => s.push_str("\\n"),
            '\t' => s.push_str("\\t"),
            c => s.push(c),
        }
    }
    s.push('"');
    s
}

/// Render a section header, quoting the subsection.
pub fn section_header(section: &str, sub: Option<&str>) -> String {
    match sub {
        Some(s) => format!(
            "[{section} \"{}\"]",
            s.replace('\\', "\\\\").replace('"', "\\\"")
        ),
        None => format!("[{section}]"),
    }
}

/// Parse git-style booleans.
pub fn parse_bool(s: &str) -> Option<bool> {
    match s.trim().to_ascii_lowercase().as_str() {
        "true" | "yes" | "on" | "1" => Some(true),
        "false" | "no" | "off" | "0" | "" => Some(false),
        _ => None,
    }
}

/// Expand a leading `~/` to the user's home directory.
pub fn expand_path(p: &str) -> PathBuf {
    if let Some(rest) = p.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }
    PathBuf::from(p)
}

/// Keys whose values should never be echoed back to the terminal.
pub fn is_secret_key(key: &str) -> bool {
    let k = key.to_ascii_lowercase();
    // `password-file` / `password-env` name where the secret is, not the secret.
    if k.ends_with("-file") || k.ends_with("-env") || k.ends_with("-mode") || k.ends_with("-owner")
    {
        return false;
    }
    k.contains("password") || k.contains("passcode") || k.contains("secret") || k.ends_with("token")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(text: &str) -> Vec<Entry> {
        parse(text, Path::new("test")).unwrap()
    }

    #[test]
    fn parses_sections_and_values() {
        let e = p(r#"
# comment
[Core]
    Default-Scheme = stomp   ; trailing comment
[alias "jms.company.com"]
    url = stomp://company.com:61613
    password = "p#ss; word "
    verbose
"#);
        assert_eq!(e.len(), 4);
        assert_eq!(e[0].name(), "core.default-scheme");
        assert_eq!(e[0].value, "stomp");
        assert_eq!(e[1].subsection.as_deref(), Some("jms.company.com"));
        assert_eq!(e[2].value, "p#ss; word ");
        assert_eq!(e[3].value, "true");
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse("key = 1", Path::new("t")).is_err());
        assert!(parse("[a\nk=1", Path::new("t")).is_err());
        assert!(parse("[a b]\n", Path::new("t")).is_err());
        assert!(parse("[a]\n k = \"open", Path::new("t")).is_err());
    }

    #[test]
    fn quote_round_trip() {
        for v in [
            "plain",
            " lead",
            "a#b",
            "q\"uote",
            "back\\slash",
            "",
            "multi\nline",
        ] {
            let text = format!("[s]\nk = {}\n", quote_value(v));
            assert_eq!(p(&text)[0].value, v, "value {v:?}");
        }
    }

    #[test]
    fn subsection_escapes_round_trip() {
        let hdr = section_header("alias", Some(r#"we"ird\name"#));
        let e = p(&format!("{hdr}\nurl = x\n"));
        assert_eq!(e[0].subsection.as_deref(), Some(r#"we"ird\name"#));
    }

    #[test]
    fn load_order_config_then_config_d() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("config"),
            "[alias \"a\"]\nurl = stomp://one\n[core]\nx = 1\n",
        )
        .unwrap();
        fs::create_dir(dir.path().join("config.d")).unwrap();
        fs::write(
            dir.path().join("config.d/b.conf"),
            "[alias \"a\"]\nurl = stomp://two\n",
        )
        .unwrap();
        fs::write(
            dir.path().join("config.d/a.conf"),
            "[alias \"b\"]\nurl = stomp://b\n",
        )
        .unwrap();
        fs::write(dir.path().join("config.d/.hidden"), "garbage [[[").unwrap();

        let cfg = Config::load_from(dir.path()).unwrap();
        assert_eq!(cfg.get("alias", Some("a"), "URL"), Some("stomp://two"));
        assert_eq!(cfg.get("CORE", None, "x"), Some("1"));
        assert_eq!(cfg.subsections("alias"), vec!["a", "b"]);
        assert_eq!(cfg.files.len(), 3);
    }

    #[test]
    fn strict_defaults_to_true() {
        let cfg = |t: &str| Config {
            entries: p(t),
            ..Default::default()
        };
        assert!(cfg("").strict().unwrap());
        assert!(!cfg("[core \"config\"]\nstrict = false\n").strict().unwrap());
        assert!(cfg("[core \"config\"]\nstrict = maybe\n").strict().is_err());
    }

    fn alias(name: &str) -> String {
        format!("[alias \"{name}\"]\nurl = stomp://{name}\n")
    }

    #[test]
    fn config_d_is_read_recursively_in_path_order() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path().join("config.d");
        fs::create_dir_all(d.join("team-a/nested")).unwrap();
        fs::create_dir_all(d.join("team-b")).unwrap();
        fs::write(d.join("a.conf"), alias("a") + "[core]\nx = top\n").unwrap();
        fs::write(d.join("team-a/b.conf"), alias("b")).unwrap();
        fs::write(
            d.join("team-a/nested/c.conf"),
            alias("c") + "[core]\nx = nested\n",
        )
        .unwrap();
        fs::write(d.join("team-b/d.conf"), alias("d")).unwrap();
        fs::write(d.join("team-b/.hidden"), "garbage [[[").unwrap();
        fs::create_dir(d.join(".git")).unwrap();
        fs::write(d.join(".git/config"), "garbage [[[").unwrap();

        let cfg = Config::load_from(dir.path()).unwrap();
        assert_eq!(cfg.subsections("alias"), vec!["a", "b", "c", "d"]);
        assert_eq!(cfg.get("core", None, "x"), Some("nested")); // later path wins
        assert_eq!(cfg.files.len(), 4);
    }

    /// The layout kubelet creates for a ConfigMap volume: each key is a symlink
    /// into `..data`, which points at a hidden timestamped directory.
    #[cfg(unix)]
    #[test]
    fn kubernetes_configmap_mount_loads_each_key_once() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let mount = dir.path().join("config.d/brokers");
        let ts = mount.join("..2026_10_02_12_00_00.123456789");
        fs::create_dir_all(&ts).unwrap();
        fs::write(ts.join("jms.conf"), alias("jms")).unwrap();
        fs::write(ts.join("kafka.conf"), alias("kafka")).unwrap();
        symlink("..2026_10_02_12_00_00.123456789", mount.join("..data")).unwrap();
        symlink("..data/jms.conf", mount.join("jms.conf")).unwrap();
        symlink("..data/kafka.conf", mount.join("kafka.conf")).unwrap();

        let cfg = Config::load_from(dir.path()).unwrap();
        assert_eq!(cfg.subsections("alias"), vec!["jms", "kafka"]);
        assert_eq!(cfg.files.len(), 2, "{:?}", cfg.files);
    }

    #[cfg(unix)]
    #[test]
    fn symlink_loops_are_read_once() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path().join("config.d");
        fs::create_dir_all(d.join("x")).unwrap();
        fs::write(d.join("x/a.conf"), alias("a")).unwrap();
        std::os::unix::fs::symlink("..", d.join("x/up")).unwrap(); // x/up -> config.d
        let cfg = Config::load_from(dir.path()).unwrap();
        assert_eq!(cfg.files.len(), 1, "{:?}", cfg.files);
    }

    #[test]
    fn max_depth_comes_from_the_main_config() {
        let dir = tempfile::tempdir().unwrap();
        let deep = dir.path().join("config.d/1/2/3");
        fs::create_dir_all(&deep).unwrap();
        fs::write(deep.join("a.conf"), alias("a")).unwrap();
        assert!(Config::load_from(dir.path()).is_ok()); // depth 3 <= 16

        fs::write(
            dir.path().join("config"),
            "[core \"config\"]\nmax-depth = 2\n",
        )
        .unwrap();
        let e = Config::load_from(dir.path()).unwrap_err().to_string();
        assert!(e.contains("deeper than core.config.max-depth (2)"), "{e}");

        fs::write(
            dir.path().join("config"),
            "[core \"config\"]\nmax-depth = 0\n",
        )
        .unwrap();
        assert!(Config::load_from(dir.path())
            .unwrap_err()
            .to_string()
            .contains("max-depth (0)"));

        fs::write(
            dir.path().join("config"),
            "[core \"config\"]\nmax-depth = lots\n",
        )
        .unwrap();
        assert!(Config::load_from(dir.path()).is_err());
    }

    #[test]
    fn format_limits() {
        let err = |t: &str| parse(t, Path::new("t")).unwrap_err().to_string();
        assert!(err(&format!("[s]\nk = {}\n", "v".repeat(MAX_LINE_LEN))).contains("line is longer"));
        assert!(err("[s]\nk = a\u{1}b\n").contains("control character U+0001"));
        assert!(err("[s]\nk = a\rb\n").contains("control character U+000D"));
        assert!(
            err(&format!("[s]\n{} = 1\n", "k".repeat(MAX_KEY_LEN + 1))).contains("key is longer")
        );
        assert!(
            err(&format!("[s]\nk = {}\n", "v".repeat(MAX_VALUE_LEN + 1)))
                .contains("value of 'k' is longer")
        );
        assert!(err(&format!(
            "[alias \"{}\"]\n",
            "a".repeat(MAX_SUBSECTION_LEN + 1)
        ))
        .contains("section name is longer"));
        assert!(parse("[s]\nk = a\tb\r\n", Path::new("t")).is_ok()); // tab and CRLF are fine
    }

    #[test]
    fn volume_limits() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path().join("config.d");
        fs::create_dir(&d).unwrap();
        for i in 0..3 {
            fs::write(d.join(format!("{i}.conf")), alias(&format!("a{i}"))).unwrap();
        }
        assert!(Config::load_from(dir.path()).is_ok());

        fs::write(
            dir.path().join("config"),
            "[core \"config\"]\nmax-files = 2\n",
        )
        .unwrap();
        let e = Config::load_from(dir.path()).unwrap_err().to_string();
        assert!(
            e.contains("holds 3 config files, more than core.config.max-files (2)"),
            "{e}"
        );

        fs::write(
            dir.path().join("config"),
            "[core \"config\"]\nmax-file-size = 40\n",
        )
        .unwrap();
        fs::write(
            d.join("big.conf"),
            format!("{}# {}\n", alias("big"), "x".repeat(40)),
        )
        .unwrap();
        assert!(Config::load_from(dir.path())
            .unwrap_err()
            .to_string()
            .contains("max-file-size (40 bytes)"));
        fs::remove_file(d.join("big.conf")).unwrap();

        fs::write(
            dir.path().join("config"),
            "[core \"config\"]\nmax-total-size = 100\n",
        )
        .unwrap();
        assert!(Config::load_from(dir.path())
            .unwrap_err()
            .to_string()
            .contains("max-total-size (100 bytes)"));

        // The default 64 KiB file limit also applies to the main config.
        fs::write(
            dir.path().join("config"),
            format!("# {}\n", "x".repeat(70 * 1024)),
        )
        .unwrap();
        assert!(Config::load_from(dir.path())
            .unwrap_err()
            .to_string()
            .contains("max-file-size"));
    }

    #[test]
    fn entries_per_section_limit() {
        let dir = tempfile::tempdir().unwrap();
        let mut text = String::from("[defaults]\n");
        for i in 0..=MAX_ENTRIES_PER_SECTION {
            text.push_str(&format!("k{i} = 1\n"));
        }
        fs::write(dir.path().join("config"), text).unwrap();
        let e = Config::load_from(dir.path()).unwrap_err().to_string();
        assert!(
            e.contains("section [defaults] has more than 256 settings"),
            "{e}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn writable_config_is_refused() {
        use std::os::unix::fs::PermissionsExt;
        let mode =
            |p: &Path, m: u32| fs::set_permissions(p, fs::Permissions::from_mode(m)).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path().join("config.d");
        fs::create_dir(&d).unwrap();
        fs::write(d.join("a.conf"), alias("a")).unwrap();
        fs::write(dir.path().join("config"), "").unwrap();
        mode(dir.path(), 0o755);
        mode(&d, 0o755);
        mode(&d.join("a.conf"), 0o644);
        assert!(Config::load_from(dir.path()).is_ok());

        mode(&d.join("a.conf"), 0o664);
        let e = Config::load_from(dir.path()).unwrap_err().to_string();
        assert!(
            e.contains("writable by its group (mode 0664)") && e.contains("chmod go-w"),
            "{e}"
        );
        mode(&d.join("a.conf"), 0o644);

        mode(&d, 0o777);
        assert!(Config::load_from(dir.path())
            .unwrap_err()
            .to_string()
            .contains("writable by everyone"));

        // The main config may relax the check for config.d...
        fs::write(
            dir.path().join("config"),
            "[core \"config\"]\ncheck-permissions = false\n",
        )
        .unwrap();
        assert!(Config::load_from(dir.path()).is_ok());
        // ...but not for itself or the home directory.
        mode(&dir.path().join("config"), 0o666);
        assert!(Config::load_from(dir.path())
            .unwrap_err()
            .to_string()
            .contains("config is writable by everyone"));
        mode(&dir.path().join("config"), 0o644);
        mode(dir.path(), 0o775);
        assert!(Config::load_from(dir.path()).is_err());
        mode(dir.path(), 0o755);
        mode(&d, 0o755);
    }

    #[test]
    fn missing_home_is_empty() {
        let cfg = Config::load_from("/nonexistent/pub-home").unwrap();
        assert!(cfg.entries.is_empty());
    }
}
