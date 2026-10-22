//! alias / config / modules sub-commands.

use std::fs;
use std::io::Write;
use std::path::Path;

use pub_core::config::{is_secret_key, quote_value, section_header};
use pub_core::message::FIELDS;
use pub_core::payload::parse_kv;
use pub_core::{
    bail_kind, err, Config, Endpoint, Module, Registry, Result, Setting, Severity, Verb,
};

use crate::cli::{AliasCmd, ConfigCmd};

const STARTER_CONFIG: &str = r#"# pub configuration (git-config syntax). Later files win:
#   ~/.pub/config, then ~/.pub/config.d/* in name order.

[core]
	# Scheme used when a target is neither a URL nor an alias:
	#   pub ping mq01:61613   ->   stomp://mq01:61613
	# default-scheme = stomp

[core "config"]
	# Unknown keys/sections in config files are errors (true) or warnings (false).
	# strict = true
	# password-file must be owned by you and allow no more than this mode:
	# secret-file-max-mode = 0400
	# secret-file-check-owner = true
	# secret-file-follow-symlinks = false
	# TLS private key: owner-only, mode 0600 or stricter. cert/key/ca-file may be symlinks.
	# tls-key-max-mode = 0600
	# tls-key-check-owner = true
	# tls-follow-symlinks = true
	# config.d is read recursively (e.g. Kubernetes ConfigMap mounts); hidden
	# entries are skipped. Only honoured here, in the main config:
	# max-depth = 16
	# Volume limits for config.d (main config only):
	# max-files = 1024
	# max-file-size = 64k
	# max-total-size = 4m
	# Refuse config.d entries writable by group/others (PUB_CHECK_PERMISSIONS=false skips all):
	# check-permissions = true

[defaults]
	# Settings applied to every connection (0 disables a timeout).
	# tcp-nodelay = true
	# tcp-keepalive = true     # probe idle connections
	# tcp-keepalive-time = 30s     # idle time before the first probe (0 = OS default)
	# tcp-keepalive-interval = 10s # between unanswered probes (0 = OS default)
	# tcp-keepalive-count = 3      # unanswered probes before giving up (0 = OS default)
	# Socket buffers are left to the OS, which autotunes them. Setting one pins
	# it and turns autotuning off for that socket; only do so for a reason.
	# receive-buffer = 4m
	# send-buffer = 4m
	# connect-timeout = 5s     # TCP connect + TLS handshake
	# send-timeout = 10s       # a blocked socket write
	# receive-timeout = 10s    # an expected server reply (handshake, receipt)
	# max-frame-size = 64m     # largest frame accepted from the server
	# max-payload = 64m        # largest body from -d / -f / stdin

# Per-protocol defaults:
# [protocol "stomp"]
#	user = app
#	receipts = true

# Aliases usually live one per file in ~/.pub/config.d/ (see `pub alias add`):
# [alias "jms.company.com"]
#	url = stomp+ssl://mq01.company.com:61612
#	user = app
#	password-file = ~/.pub/jms.password   # chmod 400
#	password-env = JMS_PASSWORD
#	ca-file = ~/.pub/company-ca.pem
"#;

fn shown(key: &str, value: &str) -> String {
    if is_secret_key(key) {
        "****".into()
    } else if key.eq_ignore_ascii_case("url") {
        pub_core::endpoint::redact_userinfo(value)
    } else {
        value.to_string()
    }
}

fn valid_alias_name(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with('.')
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_'))
}

pub fn alias(registry: &Registry, cmd: AliasCmd) -> Result<()> {
    let cfg = Config::load()?;
    match cmd {
        AliasCmd::List => {
            let names = cfg.subsections("alias");
            let width = names.iter().map(String::len).max().unwrap_or(0);
            for name in names {
                let url = cfg.get("alias", Some(&name), "url").unwrap_or("(no url)");
                println!(
                    "{name:<width$}  {}",
                    pub_core::endpoint::redact_userinfo(url)
                );
            }
        }
        AliasCmd::Show { name } => {
            let entries = cfg.section_entries("alias", Some(&name));
            if entries.is_empty() {
                bail_kind!(Config, "no alias '{name}'");
            }
            for e in entries {
                println!(
                    "{}:{}\t{} = {}",
                    e.origin.display(),
                    e.line,
                    e.key,
                    shown(&e.key, &e.value)
                );
            }
        }
        AliasCmd::Add {
            name,
            url,
            set,
            force,
        } => {
            if !valid_alias_name(&name) {
                bail_kind!(
                    Usage,
                    "alias names may contain letters, digits, '.', '-' and '_' (got '{name}')"
                );
            }
            let mut ep = Endpoint::parse(&url)?;
            let mut pairs = Vec::new();
            for kv in &set {
                let (k, v) = parse_kv(kv)?;
                if k.eq_ignore_ascii_case("url") {
                    bail_kind!(
                        Usage,
                        "give the URL as the second argument, not --set url=..."
                    );
                }
                ep.set_prop(&k, v.as_str());
                pairs.push((k, v));
            }
            if !force {
                registry
                    .lookup(&ep.scheme)
                    .map_err(|e| err!(Unsupported, "{e} (use --force to add anyway)"))?;
                // Be strict when writing config: warnings become errors too.
                let mut issues = registry.normalize_settings(&mut ep);
                issues.extend(registry.check_endpoint(&ep));
                crate::diag::report(&issues, true)
                    .map_err(|e| err!(Config, "{e:#} (use --force to add anyway)"))?;
            }
            let file = cfg.config_dir().join(format!("{name}.conf"));
            if !force && (file.exists() || cfg.has_section("alias", Some(&name))) {
                bail_kind!(
                    Config,
                    "alias '{name}' already exists (use --force to overwrite {})",
                    file.display()
                );
            }

            // Config files hold tabs and line feeds (escaped), not other
            // control characters: refuse them rather than write an unreadable file.
            for (k, v) in std::iter::once(("url", url.as_str()))
                .chain(pairs.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            {
                if v.chars().any(|c| c.is_control() && c != '\n' && c != '\t') {
                    bail_kind!(
                        Usage,
                        "{k}: control characters can't be stored in a config file"
                    );
                }
            }
            let mut text = format!(
                "{}\n\turl = {}\n",
                section_header("alias", Some(&name)),
                quote_value(&url)
            );
            let mut secret = ep.password.is_some();
            for (k, v) in &pairs {
                secret |= is_secret_key(k);
                text.push_str(&format!("\t{k} = {}\n", quote_value(v)));
            }
            write_private(&file, &text, secret)?;
            eprintln!("wrote {}", file.display());
        }
        AliasCmd::Remove { name } => {
            let entries = cfg.section_entries("alias", Some(&name));
            if entries.is_empty() {
                bail_kind!(Config, "no alias '{name}'");
            }
            let own_file = cfg.config_dir().join(format!("{name}.conf"));
            let others: Vec<_> = entries
                .iter()
                .map(|e| &e.origin)
                .filter(|o| **o != own_file)
                .collect();
            if !others.is_empty() {
                let list: Vec<_> = others.iter().map(|o| o.display().to_string()).collect();
                bail_kind!(
                    Config,
                    "alias '{name}' is defined in {}; edit that file to remove it",
                    list.join(", ")
                );
            }
            let other_sections = pub_core::config::parse_file(&own_file)?
                .iter()
                .any(|e| !(e.section == "alias" && e.subsection.as_deref() == Some(name.as_str())));
            if other_sections {
                bail_kind!(
                    Config,
                    "{} holds more than alias '{name}'; edit it by hand",
                    own_file.display()
                );
            }
            fs::remove_file(&own_file)
                .map_err(|e| err!(Io, "cannot remove {}: {e}", own_file.display()))?;
            eprintln!("removed {}", own_file.display());
        }
        AliasCmd::Resolve { target } => {
            let mut ep = registry.resolve_endpoint(&target, &cfg)?;
            let alias_issues = registry.normalize_settings(&mut ep);
            ep.apply_credentials()?;
            let module = registry.lookup(&ep.scheme).ok();
            let port = match (ep.port, module) {
                (Some(p), _) => p.to_string(),
                (None, Some((_, s))) => format!("{} (default)", s.default_port),
                (None, None) => "?".into(),
            };
            let mut rows = vec![
                ("url", ep.display_url()),
                ("alias", ep.alias.clone().unwrap_or_else(|| "-".into())),
                (
                    "module",
                    module
                        .map(|(m, _)| m.name().to_string())
                        .unwrap_or_else(|| format!("none for '{}'", ep.scheme)),
                ),
                ("host", ep.host.clone()),
                ("port", port),
                (
                    "destination",
                    module
                        .and_then(|(m, _)| m.destination_name(&ep))
                        .or_else(|| ep.destination())
                        .unwrap_or_else(|| "-".into()),
                ),
                ("user", ep.username.clone().unwrap_or_else(|| "-".into())),
                (
                    "password",
                    if ep.password.is_some() {
                        "****".into()
                    } else {
                        "-".into()
                    },
                ),
            ];
            if let Some((m, s)) = module {
                let verbs: Vec<_> = m.verbs().iter().map(|v| v.name()).collect();
                rows.push(("commands", verbs.join(", ")));
                rows.push(("tls", (s.tls || ep.prop_bool("tls", false)?).to_string()));
            }
            for (k, v) in rows {
                println!("{k:<16} {v}");
            }
            let options: Vec<(String, String)> = match module {
                Some((m, _)) => {
                    let name = m.destination_name(&ep).unwrap_or_default();
                    let mut all = registry.destination(&ep, m, Verb::Receive, &name).options;
                    for o in registry.destination(&ep, m, Verb::Send, &name).options {
                        if !all.contains(&o) {
                            all.push(o);
                        }
                    }
                    all
                }
                None => Vec::new(),
            };
            for (k, v) in &ep.props {
                let raw = ep.names.get(k).map_or(k.as_str(), String::as_str);
                let is_option = options.iter().any(|(o, _)| o == raw);
                if !is_option
                    && !matches!(
                        k.as_str(),
                        "user" | "username" | "password" | "password-env"
                    )
                {
                    match ep.aliased.get(k) {
                        Some(from) => println!("{k:<16} {}  (from {from})", shown(k, v)),
                        None => println!("{k:<16} {}", shown(k, v)),
                    }
                }
            }
            for (k, v) in &options {
                println!("{:<16} {v}  (destination option)", k);
            }
            for (name, reason) in &ep.ignored {
                println!("{name:<16} ignored: {reason}");
            }
            for issue in alias_issues.iter().chain(&registry.check_endpoint(&ep)) {
                println!("{issue}");
            }
        }
    }
    Ok(())
}

pub fn config(registry: &Registry, cmd: ConfigCmd) -> Result<()> {
    match cmd {
        ConfigCmd::List => {
            let cfg = Config::load()?;
            for e in &cfg.entries {
                println!(
                    "{}:{}\t{}={}",
                    e.origin.display(),
                    e.line,
                    e.name(),
                    shown(&e.key, &e.value)
                );
            }
        }
        ConfigCmd::Path => {
            let cfg = Config::load()?;
            println!("home       {}", cfg.home.display());
            println!(
                "config     {}{}",
                cfg.config_file().display(),
                if cfg.config_file().is_file() {
                    ""
                } else {
                    " (missing)"
                }
            );
            println!(
                "config.d   {}{}",
                cfg.config_dir().display(),
                if cfg.config_dir().is_dir() {
                    ""
                } else {
                    " (missing)"
                }
            );
            for f in &cfg.files {
                println!("loaded     {}", f.display());
            }
        }
        ConfigCmd::Check => {
            let cfg = Config::load()?;
            let mut issues = registry.check_config(&cfg);
            // An unreadable strict flag is itself reported by check_config.
            let strict = cfg.strict().unwrap_or(true);
            pub_core::settings::apply_strict(&mut issues, strict);
            for i in &issues {
                println!("{i}");
            }
            let errors = issues
                .iter()
                .filter(|i| i.severity == Severity::Error)
                .count();
            let summary = format!(
                "{} file(s), {} alias(es): {errors} error(s), {} warning(s)",
                cfg.files.len(),
                cfg.subsections("alias").len(),
                issues.len() - errors,
            );
            if errors > 0 {
                let hint = if strict {
                    format!("\n({})", crate::diag::STRICT_HINT)
                } else {
                    String::new()
                };
                bail_kind!(Config, "{summary}{hint}");
            }
            eprintln!("{summary}");
        }
        ConfigCmd::Settings {
            module,
            markdown: true,
        } => settings_markdown(registry, module.as_deref())?,
        ConfigCmd::Settings { module, .. } => settings(registry, module.as_deref())?,
        ConfigCmd::Headers { module } => headers(registry, module.as_deref())?,
        ConfigCmd::Init => {
            let home = Config::home_dir()?;
            let dir = home.join(pub_core::config::CONFIG_DIR);
            fs::create_dir_all(&dir)
                .map_err(|e| err!(Io, "cannot create {}: {e}", dir.display()))?;
            let file = home.join(pub_core::config::CONFIG_FILE);
            if file.exists() {
                eprintln!("{} already exists; left unchanged", file.display());
            } else {
                write_private(&file, STARTER_CONFIG, false)?;
                eprintln!("wrote {}", file.display());
            }
        }
    }
    Ok(())
}

pub fn modules(registry: &Registry) -> Result<()> {
    let mut rows: Vec<(String, String, String, String)> = registry
        .modules()
        .map(|m| {
            let schemes: Vec<_> = m
                .schemes()
                .iter()
                .map(|s| {
                    format!(
                        "{}:{}{}",
                        s.scheme,
                        s.default_port,
                        if s.tls { " (tls)" } else { "" }
                    )
                })
                .collect();
            let verbs: Vec<_> = m.verbs().iter().map(|v| v.name()).collect();
            (
                m.name().to_string(),
                schemes.join(", "),
                verbs.join(", "),
                m.description().to_string(),
            )
        })
        .collect();
    // By full name (the description starts with it), as the documentation lists them.
    rows.sort_by_key(|r| r.3.to_lowercase());
    let w0 = rows.iter().map(|r| r.0.len()).max().unwrap_or(0).max(6);
    let w1 = rows.iter().map(|r| r.1.len()).max().unwrap_or(0).max(7);
    let w2 = rows.iter().map(|r| r.2.len()).max().unwrap_or(0).max(8);
    println!(
        "{:<w0$}  {:<w1$}  {:<w2$}  DESCRIPTION",
        "MODULE", "SCHEMES", "COMMANDS"
    );
    for (a, b, c, d) in rows {
        println!("{a:<w0$}  {b:<w1$}  {c:<w2$}  {d}");
    }
    Ok(())
}

pub fn formats(registry: &Registry) -> Result<()> {
    let rows: Vec<(&str, &str, &str)> = registry
        .formats()
        .map(|f| {
            let io = if f.can_read() { "read, write" } else { "write" };
            (f.name(), io, f.description())
        })
        .collect();
    let w0 = rows.iter().map(|r| r.0.len()).max().unwrap_or(0).max(6);
    let w1 = rows.iter().map(|r| r.1.len()).max().unwrap_or(0).max(2);
    println!("{:<w0$}  {:<w1$}  DESCRIPTION", "FORMAT", "IO");
    for (a, b, c) in rows {
        println!("{a:<w0$}  {b:<w1$}  {c}");
    }
    let strategies: Vec<(&str, &str)> = registry
        .strategies()
        .map(|s| (s.name(), s.description()))
        .collect();
    let ws = strategies
        .iter()
        .map(|r| r.0.len())
        .max()
        .unwrap_or(0)
        .max(15);
    println!("\n{:<ws$}  DESCRIPTION", "HEADER STRATEGY");
    for (name, desc) in strategies {
        println!("{name:<ws$}  {desc}");
    }
    Ok(())
}

/// `pub config settings [module]`: the schema as tables.
/// A Markdown table cell: help text like `<prefix>` or `a|b` can't become
/// HTML or break the table.
fn cell(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('|', "\\|")
}

/// The settings schema as Markdown tables, as the documentation's reference.
fn settings_markdown(registry: &Registry, module: Option<&str>) -> Result<()> {
    let wanted = |name: &str| module.is_none_or(|want| want.eq_ignore_ascii_case(name));
    if module.is_some() && !registry.modules().any(|m| wanted(m.name())) {
        bail_kind!(Usage, "no module '{}'", module.unwrap_or_default());
    }
    let table = |title: &str, settings: &mut dyn Iterator<Item = &Setting>| {
        println!("## {title}\n");
        println!("| Setting | Type | Default | Description |");
        println!("|---|---|---|---|");
        for s in settings {
            println!(
                "| `{}` | {} | {} | {} |",
                s.key,
                s.ty.name(),
                s.default.map_or("–".to_string(), |d| format!("`{d}`")),
                cell(s.help)
            );
        }
        println!();
    };
    if module.is_none() {
        table(
            "All protocols",
            &mut pub_core::settings::CORE_SETTINGS.iter(),
        );
    }
    for m in registry.modules().filter(|m| wanted(m.name())) {
        table(&format!("{} module", m.name()), &mut m.settings().iter());
        if !m.setting_aliases().is_empty() {
            println!("Other names accepted for pub settings:\n");
            println!("| Name | Sets | Value |");
            println!("|---|---|---|");
            for a in m.setting_aliases() {
                let unit = match a.value {
                    pub_core::AliasValue::Same => "as is".to_string(),
                    other => other.to_string(),
                };
                let targets: Vec<String> = a.targets.iter().map(|t| format!("`{t}`")).collect();
                println!("| `{}` | {} | {unit} |", a.alias, targets.join(", "));
            }
            println!();
        }
        if !m.inert_settings().is_empty() {
            println!("Accepted for compatibility, with no effect in pub:\n");
            println!("| Setting | Why |");
            println!("|---|---|");
            for i in m.inert_settings() {
                println!("| `{}` | {} |", i.pattern, cell(i.reason));
            }
            println!();
        }
        if !m.destination_options().is_empty() {
            println!("Destination options, passed through to the broker:\n");
            println!("| Option | Applies to | Description |");
            println!("|---|---|---|");
            for o in m.destination_options() {
                println!(
                    "| `{}` | {} | {} |",
                    o.pattern,
                    o.scope.name(),
                    cell(o.help)
                );
            }
            println!();
        }
    }
    if module.is_none() {
        table("Message formats", &mut registry.format_settings());
        println!("## Configuration file\n");
        println!("In the main `config` file only.\n");
        println!("| Setting | Default | Description |");
        println!("|---|---|---|");
        for s in pub_core::settings::CORE_SECTION {
            println!("| `[core] {}` | – | {} |", s.key, cell(s.help));
        }
        for s in pub_core::settings::CONFIG_SECTION {
            println!(
                "| `[core \"config\"] {}` | {} | {} |",
                s.key,
                s.default.map_or("–".to_string(), |d| format!("`{d}`")),
                cell(s.help)
            );
        }
        println!();
    }
    Ok(())
}

fn settings(registry: &Registry, module: Option<&str>) -> Result<()> {
    let core: Vec<&Setting> = pub_core::settings::CORE_SETTINGS.iter().collect();
    let mut groups: Vec<(String, Vec<&Setting>)> = vec![("core (all protocols)".into(), core)];
    for m in registry.modules() {
        if module.is_none_or(|want| want.eq_ignore_ascii_case(m.name())) {
            groups.push((
                format!("{} module", m.name()),
                m.settings().iter().collect(),
            ));
        }
    }
    let fmt: Vec<&Setting> = registry.format_settings().collect();
    if module.is_none() && !fmt.is_empty() {
        groups.push(("formats (all protocols)".into(), fmt));
    }
    if groups.len() == 1 && module.is_some() {
        let names: Vec<_> = registry.modules().map(|m| m.name()).collect();
        bail_kind!(
            Usage,
            "no module '{}' (available: {})",
            module.unwrap_or_default(),
            names.join(", ")
        );
    }

    let all: Vec<&Setting> = groups.iter().flat_map(|(_, s)| s.iter().copied()).collect();
    let wk = all.iter().map(|s| s.key.len()).max().unwrap_or(0);
    let wt = all.iter().map(|s| s.ty.name().len()).max().unwrap_or(0);
    let wd = all
        .iter()
        .map(|s| s.default.unwrap_or("-").len())
        .max()
        .unwrap_or(0)
        .max(7);
    for (title, settings) in groups {
        println!("{title}:");
        println!(
            "  {:<wk$}  {:<wt$}  {:<wd$}  DESCRIPTION",
            "KEY", "TYPE", "DEFAULT"
        );
        for s in settings {
            let default = s.default.unwrap_or("-");
            println!(
                "  {:<wk$}  {:<wt$}  {default:<wd$}  {}",
                s.key,
                s.ty.name(),
                s.help
            );
        }
        println!();
    }
    for m in registry.modules() {
        if module.is_some_and(|want| !want.eq_ignore_ascii_case(m.name()))
            || m.setting_aliases().is_empty()
        {
            continue;
        }
        println!(
            "{} module aliases (other names accepted for pub settings):",
            m.name()
        );
        let wa = m
            .setting_aliases()
            .iter()
            .map(|a| a.alias.len())
            .max()
            .unwrap_or(0);
        for a in m.setting_aliases() {
            let unit = match a.value {
                pub_core::AliasValue::Same => String::new(),
                other => format!("  (value in {other})"),
            };
            println!("  {:<wa$}  -> {}{unit}", a.alias, a.targets.join(", "));
        }
        println!();
    }
    for m in registry.modules() {
        if module.is_some_and(|want| !want.eq_ignore_ascii_case(m.name()))
            || m.inert_settings().is_empty()
        {
            continue;
        }
        println!(
            "{} module: accepted for compatibility, no effect in pub:",
            m.name()
        );
        let wi = m
            .inert_settings()
            .iter()
            .map(|i| i.pattern.len())
            .max()
            .unwrap_or(0);
        for i in m.inert_settings() {
            println!("  {:<wi$}  {}", i.pattern, i.reason);
        }
        println!();
    }
    for m in registry.modules() {
        if module.is_some_and(|want| !want.eq_ignore_ascii_case(m.name()))
            || m.destination_options().is_empty()
        {
            continue;
        }
        println!(
            "{} module destination options (passed through to the broker, case preserved):",
            m.name()
        );
        let wp = m
            .destination_options()
            .iter()
            .map(|o| o.pattern.len())
            .max()
            .unwrap_or(0);
        for o in m.destination_options() {
            println!("  {:<wp$}  {:<12}  {}", o.pattern, o.scope.name(), o.help);
        }
        println!();
    }
    for s in pub_core::settings::CORE_SECTION {
        println!("[core] {}: {}", s.key, s.help);
    }
    for s in pub_core::settings::CONFIG_SECTION {
        let default = s.default.unwrap_or("-");
        println!(
            "[core \"config\"] {} (default {default}): {}",
            s.key, s.help
        );
    }
    if module.is_none() {
        println!(
            "TLS cipher suites (tls-cipher-suites): {}",
            pub_core::tls::cipher_suite_names().join(", ")
        );
        println!(
            "TLS key exchange groups (tls-groups): {}",
            pub_core::tls::group_names().join(", ")
        );
        println!();
    }
    println!("Alias sections also take 'url' (required). Durations: 10s, 500ms, 2m (0 = none). Sizes: 65536, 256k, 4m.");
    Ok(())
}

/// Modules selected by an optional name; error if the name matches none.
fn select_modules<'a>(registry: &'a Registry, module: Option<&str>) -> Result<Vec<&'a dyn Module>> {
    let mods: Vec<&dyn Module> = registry
        .modules()
        .filter(|m| module.is_none_or(|want| want.eq_ignore_ascii_case(m.name())))
        .collect();
    if mods.is_empty() {
        let names: Vec<_> = registry.modules().map(|m| m.name()).collect();
        bail_kind!(
            Usage,
            "no module '{}' (available: {})",
            module.unwrap_or_default(),
            names.join(", ")
        );
    }
    Ok(mods)
}

/// `pub config headers [module]`: standard fields, aliases, and per-module support.
fn headers(registry: &Registry, module: Option<&str>) -> Result<()> {
    let mods = select_modules(registry, module)?;
    let wn = FIELDS.iter().map(|f| f.name.len()).max().unwrap_or(0);
    let wa = FIELDS
        .iter()
        .map(|f| f.aliases.join(", ").len())
        .max()
        .unwrap_or(0)
        .max(7);
    let mut header = format!("{:<wn$}  {:<wa$}  ", "FIELD", "ALIASES");
    for m in &mods {
        header.push_str(&format!("{:<8}", m.name().to_uppercase()));
    }
    println!("{header}DESCRIPTION");
    for f in FIELDS {
        let mut line = format!("{:<wn$}  {:<wa$}  ", f.name, f.aliases.join(", "));
        for m in &mods {
            let support = if !f.settable {
                "recv"
            } else if m.message_fields().contains(&f.field) {
                "yes"
            } else {
                "-"
            };
            line.push_str(&format!("{support:<8}"));
        }
        println!("{line}{}", f.help);
    }
    println!();
    println!("-H NAME: VALUE  sets a field above (by name or alias), else a String property.");
    println!("-P NAME[:TYPE]=VALUE  always sets a property; TYPE is string, bool, byte, short, int, long, float or double.");
    for m in &mods {
        if !m.typed_properties() {
            println!("{}: properties are sent as strings.", m.name());
        }
        if !m.reserved_property_names().is_empty() {
            println!(
                "{}: reserved, cannot be property names: {}",
                m.name(),
                m.reserved_property_names().join(", ")
            );
        }
    }
    Ok(())
}

/// Write a config file, creating parent directories; owner-only when it holds secrets.
fn write_private(path: &Path, text: &str, secret: bool) -> Result<()> {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir).map_err(|e| err!(Io, "cannot create {}: {e}", dir.display()))?;
    }
    let mut opts = fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        let mode = if secret { 0o600 } else { 0o644 };
        opts.mode(mode);
        // `mode` only applies on creation; tighten an existing file too.
        if secret && path.exists() {
            fs::set_permissions(path, fs::Permissions::from_mode(mode))
                .map_err(|e| err!(Io, "cannot chmod {}: {e}", path.display()))?;
        }
    }
    #[cfg(not(unix))]
    let _ = secret;
    let mut f = opts
        .open(path)
        .map_err(|e| err!(Io, "cannot write {}: {e}", path.display()))?;
    f.write_all(text.as_bytes())
        .map_err(|e| err!(Io, "cannot write {}: {e}", path.display()))
}
