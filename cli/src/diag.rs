//! Reporting settings issues found by the schema checks.

use pub_core::settings::apply_strict;
use pub_core::{bail_kind, Issue, Result, Severity};

pub const STRICT_HINT: &str =
    "strict mode: warnings are errors; to only warn, set `strict = false` under [core \"config\"]";

/// Print warnings to stderr; fail with every error listed if there are any.
/// With `strict`, warnings count as errors.
pub fn report(issues: &[Issue], strict: bool) -> Result<()> {
    let had_warnings = issues.iter().any(|i| i.severity == Severity::Warning);
    let mut issues = issues.to_vec();
    apply_strict(&mut issues, strict);

    for i in issues.iter().filter(|i| i.severity == Severity::Warning) {
        eprintln!("pub: {i}");
    }
    let mut errors: Vec<String> = issues
        .iter()
        .filter(|i| i.severity == Severity::Error)
        .map(|i| format!("{}: {}", i.location, i.message))
        .collect();
    if errors.is_empty() {
        return Ok(());
    }
    if strict && had_warnings {
        errors.push(format!("({STRICT_HINT})"));
    }
    match errors.len() {
        1 => bail_kind!(Config, "{}", errors[0]),
        _ => bail_kind!(Config, "invalid settings:\n  {}", errors.join("\n  ")),
    }
}
