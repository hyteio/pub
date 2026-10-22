//! Setting aliases and inert settings: other names a module accepts for pub's
//! settings, and settings it accepts but which have no effect in pub.
//!
//! Inert settings exist so configuration written for another client — e.g. a
//! pasted ActiveMQ URI with `jms.copyMessageOnSend=false` — works unchanged.
//! They are removed before validation, are never an error (even in strict
//! mode), and `-v` notes each one with the module's reason.
//!
//! A module can declare that, for its endpoints, a foreign name means one of pub's
//! settings — e.g. the ActiveMQ/Java URI option `connectionTimeout` (milliseconds)
//! is `connect-timeout`. Aliases are rewritten to their targets right after an
//! endpoint is resolved, before validation, so nothing downstream ever sees them.
//!
//! Precedence follows the layers: a value from a more specific place wins
//! (`[defaults]` < `[module]` < `[protocol]` < `[alias]` < URL < command line),
//! whichever name it used. If an alias and its target are set in the *same*
//! place, the target wins and a warning says so.

use std::fmt;

use crate::endpoint::{Endpoint, Source};
use crate::module::Registry;
use crate::settings::{Issue, Severity};

/// How an alias's value becomes the target's value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AliasValue {
    /// Used as-is (booleans, sizes, names).
    Same,
    /// A whole number of milliseconds, as Java APIs use for timeouts.
    Millis,
    /// A whole number of seconds.
    Seconds,
}

impl AliasValue {
    pub fn convert(self, value: &str) -> std::result::Result<String, String> {
        let number = || {
            value
                .trim()
                .parse::<u64>()
                .map_err(|_| format!("expected a whole number of {self}, got '{value}'"))
        };
        Ok(match self {
            AliasValue::Same => value.to_string(),
            AliasValue::Millis => format!("{}ms", number()?),
            AliasValue::Seconds => format!("{}s", number()?),
        })
    }
}

impl fmt::Display for AliasValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            AliasValue::Same => "same value",
            AliasValue::Millis => "milliseconds",
            AliasValue::Seconds => "seconds",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SettingAlias {
    /// The foreign name, matched case-insensitively (`connectionTimeout`).
    pub alias: &'static str,
    /// The pub settings it sets; usually one (`socketBufferSize` sets two).
    pub targets: &'static [&'static str],
    pub value: AliasValue,
}

impl SettingAlias {
    pub fn matches(&self, key: &str) -> bool {
        self.alias.eq_ignore_ascii_case(key)
    }
}

/// A setting a module accepts but ignores, with the reason shown under `-v`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InertSetting {
    /// Name, or a prefix ending in `*` (`jms.redeliveryPolicy.*`); case-insensitive.
    pub pattern: &'static str,
    pub reason: &'static str,
}

impl InertSetting {
    pub fn matches(&self, key: &str) -> bool {
        let key = key.to_ascii_lowercase();
        match self.pattern.strip_suffix('*') {
            Some(prefix) => {
                let prefix = prefix.to_ascii_lowercase();
                key.starts_with(&prefix) && key.len() > prefix.len()
            }
            None => self.pattern.eq_ignore_ascii_case(&key),
        }
    }
}

pub fn find_inert<'a>(inert: &[&'a InertSetting], key: &str) -> Option<&'a InertSetting> {
    inert.iter().copied().find(|i| i.matches(key))
}

pub fn find_alias<'a>(aliases: &[&'a SettingAlias], key: &str) -> Option<&'a SettingAlias> {
    aliases.iter().copied().find(|a| a.matches(key))
}

impl Registry {
    /// Every alias any compiled-in module declares (for `[defaults]`).
    pub fn all_aliases(&self) -> Vec<&'static SettingAlias> {
        self.modules()
            .flat_map(|m| m.setting_aliases().iter())
            .collect()
    }

    /// The aliases that apply to a scheme: its module's, or all when the scheme is unknown.
    pub fn aliases_for_scheme(&self, scheme: Option<&str>) -> Vec<&'static SettingAlias> {
        match scheme.map(|s| self.lookup(s)) {
            Some(Ok((m, _))) => m.setting_aliases().iter().collect(),
            _ => self.all_aliases(),
        }
    }

    /// The inert settings that apply to a scheme: its module's, or all when unknown.
    pub fn inert_for_scheme(&self, scheme: Option<&str>) -> Vec<&'static InertSetting> {
        match scheme.map(|s| self.lookup(s)) {
            Some(Ok((m, _))) => m.inert_settings().iter().collect(),
            _ => self
                .modules()
                .flat_map(|m| m.inert_settings().iter())
                .collect(),
        }
    }

    /// Bring an endpoint's settings into pub's terms: rewrite aliases, then set
    /// aside inert settings (recorded in `ep.ignored`). Call before validating.
    pub fn normalize_settings(&self, ep: &mut Endpoint) -> Vec<Issue> {
        let issues = self.apply_setting_aliases(ep);
        self.remove_inert_settings(ep);
        issues
    }

    /// Remove settings the endpoint's module accepts but ignores.
    pub fn remove_inert_settings(&self, ep: &mut Endpoint) {
        let Ok((module, _)) = self.lookup(&ep.scheme) else {
            return;
        };
        let inert = module.inert_settings();
        let keys: Vec<String> = ep.props.keys().cloned().collect();
        for key in keys {
            let written = ep.names.get(&key).cloned().unwrap_or_else(|| key.clone());
            if let Some(i) = inert.iter().find(|i| i.matches(&written)) {
                ep.props.remove(&key);
                ep.sources.remove(&key);
                ep.names.remove(&key);
                ep.ignored.push((written, i.reason));
            }
        }
    }

    /// Rewrite alias keys in an endpoint's settings to the settings they stand for.
    /// Returns problems: unconvertible values (errors) and an alias set alongside
    /// its target in the same place (warnings).
    pub fn apply_setting_aliases(&self, ep: &mut Endpoint) -> Vec<Issue> {
        let Ok((module, _)) = self.lookup(&ep.scheme) else {
            return Vec::new();
        };
        let aliases = module.setting_aliases();
        let mut issues = Vec::new();
        let keys: Vec<String> = ep.props.keys().cloned().collect();
        for key in keys {
            let Some(alias) = aliases.iter().find(|a| a.matches(&key)) else {
                continue;
            };
            let value = ep.props.remove(&key).unwrap_or_default();
            let source = ep.sources.remove(&key);
            let written = ep.names.remove(&key).unwrap_or_else(|| key.clone());
            let location = source
                .as_ref()
                .map_or_else(|| "settings".to_string(), Source::to_string);
            let converted = match alias.value.convert(&value) {
                Ok(v) => v,
                Err(why) => {
                    issues.push(Issue {
                        severity: Severity::Error,
                        location,
                        message: format!("{written}: {why}"),
                    });
                    continue;
                }
            };
            let rank = source.as_ref().map_or(0, Source::rank);
            for target in alias.targets {
                match ep.sources.get(*target).map(Source::rank) {
                    // Set somewhere more specific under its own name: that wins.
                    Some(existing) if existing > rank => {}
                    Some(existing) if existing == rank && ep.props.contains_key(*target) => {
                        issues.push(Issue {
                            severity: Severity::Warning,
                            location: location.clone(),
                            message: format!(
                                "{written} and {target} are both set here; using {target}"
                            ),
                        });
                    }
                    _ => {
                        ep.props.insert(target.to_string(), converted.clone());
                        ep.names.insert(target.to_string(), target.to_string());
                        if let Some(s) = &source {
                            ep.sources.insert(target.to_string(), s.clone());
                        }
                        ep.aliased.insert(target.to_string(), written.clone());
                    }
                }
            }
        }
        issues
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::module::{Context, Module, SchemeInfo, Session, Verb};
    use crate::settings::Setting;
    use crate::transport::SocketOptions;
    use std::path::Path;
    use std::time::Duration;

    struct Javaish;
    impl Module for Javaish {
        fn name(&self) -> &'static str {
            "javaish"
        }
        fn description(&self) -> &'static str {
            ""
        }
        fn schemes(&self) -> &'static [SchemeInfo] {
            &[SchemeInfo {
                scheme: "jv",
                default_port: 1,
                tls: false,
            }]
        }
        fn verbs(&self) -> &'static [Verb] {
            &[]
        }
        fn settings(&self) -> &'static [Setting] {
            &[]
        }
        fn inert_settings(&self) -> &'static [InertSetting] {
            const I: &[InertSetting] = &[
                InertSetting {
                    pattern: "jms.copyMessageOnSend",
                    reason: "pub sends each message once",
                },
                InertSetting {
                    pattern: "jms.redeliveryPolicy.*",
                    reason: "pub does not redeliver locally",
                },
            ];
            I
        }
        fn setting_aliases(&self) -> &'static [SettingAlias] {
            const A: &[SettingAlias] = &[
                SettingAlias {
                    alias: "connectionTimeout",
                    targets: &["connect-timeout"],
                    value: AliasValue::Millis,
                },
                SettingAlias {
                    alias: "socketBufferSize",
                    targets: &["receive-buffer", "send-buffer"],
                    value: AliasValue::Same,
                },
                SettingAlias {
                    alias: "jms.userName",
                    targets: &["user"],
                    value: AliasValue::Same,
                },
                SettingAlias {
                    alias: "keepAliveInterval",
                    targets: &["tcp-keepalive-time"],
                    value: AliasValue::Seconds,
                },
            ];
            A
        }
        fn connect(&self, _: &Endpoint, _: &Context) -> crate::Result<Box<dyn Session>> {
            unreachable!()
        }
    }

    fn registry() -> Registry {
        let mut r = Registry::new();
        r.register(Box::new(Javaish));
        r
    }

    fn cfg(text: &str) -> Config {
        Config {
            entries: crate::config::parse(text, Path::new("cfg")).unwrap(),
            ..Default::default()
        }
    }

    fn resolved(target: &str, config: &str) -> (Endpoint, Vec<Issue>) {
        let mut ep = Endpoint::resolve(target, &cfg(config)).unwrap();
        let issues = registry().apply_setting_aliases(&mut ep);
        (ep, issues)
    }

    #[test]
    fn aliases_become_their_targets() {
        let (ep, issues) = resolved(
            "jv://h?CONNECTIONTIMEOUT=2500&socketBufferSize=256k&keepAliveInterval=45",
            "",
        );
        assert!(issues.is_empty(), "{issues:?}");
        assert_eq!(ep.prop("connect-timeout"), Some("2500ms"));
        assert_eq!(
            (ep.prop("receive-buffer"), ep.prop("send-buffer")),
            (Some("256k"), Some("256k"))
        );
        assert!(!ep.props.contains_key("connectiontimeout"));
        assert_eq!(
            ep.aliased.get("connect-timeout").map(String::as_str),
            Some("CONNECTIONTIMEOUT")
        );
        assert_eq!(ep.sources.get("connect-timeout"), Some(&Source::Url));

        let opts = SocketOptions::from_endpoint(&ep).unwrap();
        assert_eq!(opts.connect_timeout, Some(Duration::from_millis(2500)));
        assert_eq!(opts.keepalive_time, Some(Duration::from_secs(45)));
        assert_eq!(opts.receive_buffer, Some(256 * 1024));
    }

    #[test]
    fn more_specific_layer_wins_whichever_name_it_used() {
        // URL alias beats the alias section's canonical name.
        let (ep, _) = resolved(
            "a?connectionTimeout=2500",
            "[alias \"a\"]\nurl = jv://h\nconnect-timeout = 9s\n",
        );
        assert_eq!(ep.prop("connect-timeout"), Some("2500ms"));

        // Command line canonical beats URL alias.
        let mut ep =
            Endpoint::resolve("jv://h?connectionTimeout=2500", &Config::default()).unwrap();
        ep.set_prop("connect-timeout", "1s");
        assert!(registry().apply_setting_aliases(&mut ep).is_empty());
        assert_eq!(ep.prop("connect-timeout"), Some("1s"));
        assert!(!ep.aliased.contains_key("connect-timeout"));

        // [defaults] alias loses to [alias] canonical.
        let (ep, _) = resolved("a", "[defaults]\nconnectionTimeout = 2500\n[alias \"a\"]\nurl = jv://h\nconnect-timeout = 9s\n");
        assert_eq!(ep.prop("connect-timeout"), Some("9s"));
    }

    #[test]
    fn same_place_conflicts_and_bad_values_are_reported() {
        let (ep, issues) = resolved("jv://h?connectionTimeout=2500&connect-timeout=3s", "");
        assert_eq!(ep.prop("connect-timeout"), Some("3s"));
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].severity, Severity::Warning);
        assert!(issues[0].message.contains(
            "connectionTimeout and connect-timeout are both set here; using connect-timeout"
        ));

        let (_, issues) = resolved("jv://h?connectionTimeout=soon", "");
        assert_eq!(issues[0].severity, Severity::Error);
        assert!(
            issues[0]
                .message
                .contains("expected a whole number of milliseconds, got 'soon'"),
            "{issues:?}"
        );
    }

    #[test]
    fn aliased_credentials_reach_the_endpoint() {
        let (mut ep, _) = resolved("jv://h?jms.userName=app", "");
        ep.apply_credentials().unwrap();
        assert_eq!(ep.username.as_deref(), Some("app"));
    }

    #[test]
    fn other_modules_and_unknown_schemes_are_untouched() {
        let mut ep = Endpoint::parse("nope://h?connectionTimeout=1").unwrap();
        assert!(registry().apply_setting_aliases(&mut ep).is_empty());
        assert_eq!(ep.prop("connectiontimeout"), Some("1"));
    }

    #[test]
    fn validation_knows_aliases() {
        let r = registry();
        // A valid alias is not an unknown setting; a bad value is an error.
        let c = cfg("[alias \"a\"]\nurl = jv://h\nconnectionTimeout = 2500\nsocketBufferSize = huge\nconectionTimeout = 1\n");
        let msgs: Vec<String> = r
            .check_config(&c)
            .iter()
            .map(|i| format!("{:?}: {}", i.severity, i.message))
            .collect();
        assert_eq!(
            msgs,
            [
                "Error: socketBufferSize: receive-buffer: expected a size (e.g. 65536, 256k, 4m), got 'huge'",
                "Warning: unknown setting 'conectiontimeout' for alias 'a' (the javaish module); did you mean 'connectionTimeout'?",
            ]
        );
        // After rewriting, endpoint checks see only canonical names.
        let (ep, _) = resolved("jv://h?connectionTimeout=2500", "");
        assert!(r.check_endpoint(&ep).is_empty());
    }

    #[test]
    fn inert_settings_are_accepted_and_set_aside() {
        let r = registry();
        let mut ep = Endpoint::resolve(
            "jv://h?jms.copyMessageOnSend=false&JMS.REDELIVERYPOLICY.maximumRedeliveries=6&connectionTimeout=10",
            &Config::default(),
        )
        .unwrap();
        assert!(r.normalize_settings(&mut ep).is_empty());
        assert!(
            r.check_endpoint(&ep).is_empty(),
            "{:?}",
            r.check_endpoint(&ep)
        );
        assert_eq!(ep.prop("connect-timeout"), Some("10ms"));
        let ignored: Vec<&str> = ep.ignored.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(
            ignored,
            [
                "jms.copyMessageOnSend",
                "JMS.REDELIVERYPOLICY.maximumRedeliveries"
            ]
        );
        assert_eq!(ep.ignored[0].1, "pub sends each message once");

        // Known to config checks too; a bare prefix is not a match.
        let c = cfg("[alias \"a\"]\nurl = jv://h\njms.copyMessageOnSend = whatever\njms.redeliveryPolicy. = 1\n");
        let msgs: Vec<String> = r
            .check_config(&c)
            .iter()
            .map(|i| i.message.clone())
            .collect();
        assert_eq!(msgs.len(), 1, "{msgs:?}");
        assert!(
            msgs[0].contains("unknown setting 'jms.redeliverypolicy.'"),
            "{msgs:?}"
        );

        // Only for the module that declares them.
        let mut other = Endpoint::parse("nope://h?jms.copyMessageOnSend=false").unwrap();
        r.remove_inert_settings(&mut other);
        assert!(other.ignored.is_empty());
    }
}
