//! Reading sensitive files with ssh-style safety checks.
//!
//! Each kind of file has a [`FilePolicy`]: whether it must be owned by the current
//! user, the most permissive mode it may have, and whether a symlink is accepted.
//!
//! | file              | owner | max mode | symlinks | `[core "config"]` keys |
//! |-------------------|-------|----------|----------|-------------------------|
//! | `password-file`   | yes   | 0400     | refused  | `secret-file-check-owner`, `secret-file-max-mode`, `secret-file-follow-symlinks` |
//! | TLS `key`         | yes   | 0600     | followed | `tls-key-check-owner`, `tls-key-max-mode`, `tls-follow-symlinks` |
//! | TLS `cert`, `ca-file` | -  | -        | followed | `tls-follow-symlinks` |
//!
//! Files are opened once (with `O_NOFOLLOW` when symlinks are refused) and the
//! checks run on the open handle, so what is checked is exactly what is read.
//! "Symlink" means the file itself; symlinked parent directories are fine.
//! Non-Unix systems have no owner/mode bits; only the symlink rule applies there.

use std::fs::{self, File};
use std::io::Read;
use std::path::Path;

use crate::config::{parse_bool, Config};
use crate::error::Result;
use crate::{bail_kind, err};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileKind {
    Secret,
    TlsKey,
    TlsPublic,
}

impl FileKind {
    /// `[core "config"]` keys controlling (owner, mode, symlinks) for this kind.
    fn keys(self) -> (&'static str, &'static str, &'static str) {
        match self {
            FileKind::Secret => (
                "secret-file-check-owner",
                "secret-file-max-mode",
                "secret-file-follow-symlinks",
            ),
            FileKind::TlsKey => (
                "tls-key-check-owner",
                "tls-key-max-mode",
                "tls-follow-symlinks",
            ),
            FileKind::TlsPublic => ("", "", "tls-follow-symlinks"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FilePolicy {
    pub kind: FileKind,
    /// Require the file to be owned by the effective user.
    pub check_owner: bool,
    /// Permission bits (`0o777` mask) the file may have; `None` = not checked.
    pub max_mode: Option<u32>,
    /// Accept the path being a symlink.
    pub follow_symlinks: bool,
    /// Largest file accepted, in bytes.
    pub max_size: u64,
}

impl FilePolicy {
    pub const SECRET: FilePolicy = FilePolicy {
        kind: FileKind::Secret,
        check_owner: true,
        max_mode: Some(0o400),
        follow_symlinks: false,
        max_size: 4 * 1024,
    };
    pub const TLS_KEY: FilePolicy = FilePolicy {
        kind: FileKind::TlsKey,
        check_owner: true,
        max_mode: Some(0o600),
        follow_symlinks: true,
        max_size: 64 * 1024,
    };
    pub const TLS_PUBLIC: FilePolicy = FilePolicy {
        kind: FileKind::TlsPublic,
        check_owner: false,
        max_mode: None,
        follow_symlinks: true,
        max_size: 4 * 1024 * 1024,
    };

    fn from_config(default: FilePolicy, cfg: &Config) -> Result<Self> {
        let (owner_key, mode_key, follow_key) = default.kind.keys();
        let get_bool = |key: &str, d: bool| -> Result<bool> {
            match (key.is_empty(), cfg.get("core", Some("config"), key)) {
                (false, Some(v)) => parse_bool(v).ok_or_else(|| {
                    err!(Config, "core.config.{key} must be true or false, got '{v}'")
                }),
                _ => Ok(d),
            }
        };
        let max_mode = match (
            mode_key.is_empty(),
            cfg.get("core", Some("config"), mode_key),
        ) {
            (false, Some(v)) => Some(parse_mode(v).ok_or_else(|| {
                err!(
                    Config,
                    "core.config.{mode_key} must be an octal mode like 0400, got '{v}'"
                )
            })?),
            _ => default.max_mode,
        };
        Ok(FilePolicy {
            kind: default.kind,
            check_owner: get_bool(owner_key, default.check_owner)?,
            max_mode,
            follow_symlinks: get_bool(follow_key, default.follow_symlinks)?,
            max_size: default.max_size,
        })
    }
}

/// The policies for every kind of file, as configured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FilePolicies {
    pub secret: FilePolicy,
    pub tls_key: FilePolicy,
    pub tls_public: FilePolicy,
}

impl Default for FilePolicies {
    fn default() -> Self {
        FilePolicies {
            secret: FilePolicy::SECRET,
            tls_key: FilePolicy::TLS_KEY,
            tls_public: FilePolicy::TLS_PUBLIC,
        }
    }
}

impl FilePolicies {
    pub fn from_config(cfg: &Config) -> Result<Self> {
        Ok(FilePolicies {
            secret: FilePolicy::from_config(FilePolicy::SECRET, cfg)?,
            tls_key: FilePolicy::from_config(FilePolicy::TLS_KEY, cfg)?,
            tls_public: FilePolicy::from_config(FilePolicy::TLS_PUBLIC, cfg)?,
        })
    }
}

/// Octal permission mode, `400`, `0400` or `0o400`, at most `0777`.
pub fn parse_mode(s: &str) -> Option<u32> {
    let t = s.trim();
    let t = t.strip_prefix("0o").unwrap_or(t);
    u32::from_str_radix(t, 8).ok().filter(|m| *m <= 0o777)
}

const LOOSEN: &str = "or loosen the check under [core \"config\"]";

fn open(path: &Path, what: &str, policy: &FilePolicy) -> Result<File> {
    let refused = || {
        err!(
            Config,
            "{what} {} is a symbolic link; point at the file itself, {LOOSEN} ({} = true)",
            path.display(),
            policy.kind.keys().2
        )
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let mut opts = fs::OpenOptions::new();
        opts.read(true);
        // O_NONBLOCK: opening a FIFO would otherwise wait for a writer before
        // the regular-file check below could refuse it.
        let mut flags = libc::O_NONBLOCK;
        if !policy.follow_symlinks {
            flags |= libc::O_NOFOLLOW;
        }
        opts.custom_flags(flags);
        opts.open(path).map_err(|e| {
            if !policy.follow_symlinks && e.raw_os_error() == Some(libc::ELOOP) {
                refused()
            } else {
                err!(Config, "cannot open {what} {}: {e}", path.display())
            }
        })
    }
    #[cfg(not(unix))]
    {
        if !policy.follow_symlinks
            && fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink())
        {
            return Err(refused());
        }
        File::open(path).map_err(|e| err!(Config, "cannot open {what} {}: {e}", path.display()))
    }
}

fn check_open(file: &File, path: &Path, what: &str, policy: &FilePolicy) -> Result<()> {
    let meta = file
        .metadata()
        .map_err(|e| err!(Config, "{what} {}: {e}", path.display()))?;
    if !meta.is_file() {
        bail_kind!(Config, "{what} {} is not a regular file", path.display());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let (owner_key, mode_key, _) = policy.kind.keys();
        if policy.check_owner {
            // SAFETY: geteuid has no preconditions and cannot fail.
            let me = unsafe { libc::geteuid() };
            if meta.uid() != me {
                bail_kind!(
                    Config,
                    "{what} {} is owned by uid {}, not you (uid {me}); chown it, {LOOSEN} ({owner_key} = false)",
                    path.display(),
                    meta.uid()
                );
            }
        }
        if let Some(max) = policy.max_mode {
            let mode = meta.mode() & 0o777;
            if mode & !max != 0 {
                bail_kind!(
                    Config,
                    "{what} {} has mode {mode:04o}, which allows more than {max:04o}; run `chmod {:o} {}`, {LOOSEN} ({mode_key})",
                    path.display(),
                    mode & max,
                    path.display()
                );
            }
        }
    }
    #[cfg(not(unix))]
    let _ = (policy, &meta);
    Ok(())
}

/// Open, check against the policy, and read a file.
pub fn read_checked(path: &Path, what: &str, policy: &FilePolicy) -> Result<Vec<u8>> {
    let file = open(path, what, policy)?;
    check_open(&file, path, what, policy)?;
    let mut buf = Vec::new();
    file.take(policy.max_size.saturating_add(1))
        .read_to_end(&mut buf)
        .map_err(|e| err!(Config, "cannot read {what} {}: {e}", path.display()))?;
    if buf.len() as u64 > policy.max_size {
        bail_kind!(
            Config,
            "{what} {} is larger than {}; is it the right file?",
            path.display(),
            crate::limits::human(policy.max_size as usize)
        );
    }
    Ok(buf)
}

/// Check a file without using its contents (for `pub config check`).
pub fn check_file(path: &Path, what: &str, policy: &FilePolicy) -> Result<()> {
    let file = open(path, what, policy)?;
    check_open(&file, path, what, policy)
}

/// The longest password accepted from the terminal (as for password files).
const MAX_TYPED_PASSWORD: u64 = 4 * 1024;

/// One line typed at the prompt, without its line end.
pub fn read_password_line(r: &mut impl std::io::BufRead) -> Result<String> {
    use std::io::BufRead as _;
    let mut line = String::new();
    let n = r
        .take(MAX_TYPED_PASSWORD + 1)
        .read_line(&mut line)
        .map_err(|e| err!(Io, "cannot read the password: {e}"))?;
    if n == 0 {
        // Ctrl-D: no answer at all (Enter alone is an empty password).
        bail_kind!(Usage, "no password entered");
    }
    if line.len() as u64 > MAX_TYPED_PASSWORD {
        bail_kind!(
            Usage,
            "the password is longer than {MAX_TYPED_PASSWORD} bytes"
        );
    }
    if line.ends_with('\n') {
        line.pop();
        if line.ends_with('\r') {
            line.pop();
        }
    }
    Ok(line)
}

/// Ask for a password on the terminal (`/dev/tty`, so a piped stdin payload is
/// untouched), without echo. `None` when there is no terminal to ask on:
/// neither stdin nor stderr is one, or `/dev/tty` can't be opened.
#[cfg(unix)]
pub fn ask_password(prompt: &str) -> Result<Option<String>> {
    use std::io::{BufReader, IsTerminal, Write};
    use std::os::fd::AsRawFd;

    if !(std::io::stdin().is_terminal() || std::io::stderr().is_terminal()) {
        return Ok(None);
    }
    let Ok(tty) = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/tty")
    else {
        return Ok(None);
    };
    let fd = tty.as_raw_fd();
    // SAFETY: termios is plain data; tcgetattr fills it for a valid fd.
    let mut term: libc::termios = unsafe { std::mem::zeroed() };
    if unsafe { libc::tcgetattr(fd, &mut term) } != 0 {
        return Ok(None);
    }
    /// Puts the terminal back however we leave (an error, a panic).
    struct Restore(i32, libc::termios);
    impl Drop for Restore {
        fn drop(&mut self) {
            // SAFETY: restoring settings read from the same fd.
            unsafe { libc::tcsetattr(self.0, libc::TCSANOW, &self.1) };
        }
    }
    let _restore = Restore(fd, term);
    // Ctrl-C (or a kill) would end the process without running `Restore`:
    // have the signal put the terminal back first.
    let _signals = RestoreOnSignal::install(fd, term);
    term.c_lflag &= !libc::ECHO;
    // Still move to the next line on Enter.
    term.c_lflag |= libc::ECHONL;
    // SAFETY: a valid fd and a termios derived from its own settings. FLUSH
    // drops anything typed before the prompt.
    if unsafe { libc::tcsetattr(fd, libc::TCSAFLUSH, &term) } != 0 {
        return Ok(None);
    }
    let mut out = &tty;
    out.write_all(prompt.as_bytes())
        .and_then(|_| out.flush())
        .map_err(|e| err!(Io, "cannot write the password prompt: {e}"))?;
    read_password_line(&mut BufReader::new(&tty)).map(Some)
}

/// The terminal settings to put back if a signal arrives during the prompt.
#[cfg(unix)]
static SAVED_TERMINAL: std::sync::OnceLock<(i32, libc::termios)> = std::sync::OnceLock::new();

#[cfg(unix)]
const PROMPT_SIGNALS: [libc::c_int; 3] = [libc::SIGINT, libc::SIGTERM, libc::SIGHUP];

#[cfg(unix)]
extern "C" fn restore_terminal_and_reraise(sig: libc::c_int) {
    // SAFETY: tcsetattr, signal and raise are async-signal-safe; SAVED_TERMINAL
    // was set before this handler was installed.
    unsafe {
        if let Some((fd, term)) = SAVED_TERMINAL.get() {
            libc::tcsetattr(*fd, libc::TCSANOW, term);
        }
        libc::signal(sig, libc::SIG_DFL);
        libc::raise(sig);
    }
}

/// Signal handlers that restore the terminal, for the length of the prompt.
#[cfg(unix)]
struct RestoreOnSignal(Vec<(libc::c_int, libc::sighandler_t)>);

#[cfg(unix)]
impl RestoreOnSignal {
    fn install(fd: i32, term: libc::termios) -> Self {
        // A second prompt in one process keeps the first terminal's settings,
        // which are the same /dev/tty's.
        let _ = SAVED_TERMINAL.set((fd, term));
        let handler = restore_terminal_and_reraise as extern "C" fn(libc::c_int);
        let previous = PROMPT_SIGNALS
            .iter()
            // SAFETY: installing a handler that only calls async-signal-safe functions.
            .map(|&sig| {
                (sig, unsafe {
                    libc::signal(sig, handler as libc::sighandler_t)
                })
            })
            .collect();
        RestoreOnSignal(previous)
    }
}

#[cfg(unix)]
impl Drop for RestoreOnSignal {
    fn drop(&mut self) {
        for &(sig, previous) in &self.0 {
            // SAFETY: putting back the handler that was there before.
            unsafe { libc::signal(sig, previous) };
        }
    }
}

#[cfg(not(unix))]
pub fn ask_password(_prompt: &str) -> Result<Option<String>> {
    Ok(None)
}

/// Read a secret (e.g. a password). One trailing newline is removed
/// (`echo secret > file` works); other whitespace is kept.
pub fn read_secret_file(path: &Path, what: &str, policy: &FilePolicy) -> Result<String> {
    let bytes = read_checked(path, what, policy)?;
    let text = String::from_utf8(bytes)
        .map_err(|_| err!(Config, "{what} {} is not UTF-8", path.display()))?;
    let secret = text
        .strip_suffix('\n')
        .map(|t| t.strip_suffix('\r').unwrap_or(t))
        .unwrap_or(&text);
    if secret.is_empty() {
        bail_kind!(Config, "{what} {} is empty", path.display());
    }
    Ok(secret.to_string())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;

    fn file(dir: &Path, name: &str, mode: u32, text: &str) -> PathBuf {
        let p = dir.join(name);
        fs::write(&p, text).unwrap();
        fs::set_permissions(&p, fs::Permissions::from_mode(mode)).unwrap();
        p
    }

    fn cfg(t: &str) -> Config {
        Config {
            entries: crate::config::parse(t, Path::new("t")).unwrap(),
            ..Default::default()
        }
    }

    #[test]
    fn modes() {
        assert_eq!(parse_mode("0400"), Some(0o400));
        assert_eq!(parse_mode("0o640"), Some(0o640));
        assert_eq!(parse_mode("600"), Some(0o600));
        assert_eq!(parse_mode("0800"), None);
        assert_eq!(parse_mode("1777"), None);
    }

    #[test]
    fn secret_policy_requires_0400() {
        let dir = tempfile::tempdir().unwrap();
        let p = FilePolicy::SECRET;
        assert_eq!(
            read_secret_file(
                &file(dir.path(), "a", 0o400, "s3cret\n"),
                "password-file",
                &p
            )
            .unwrap(),
            "s3cret"
        );
        let loose = file(dir.path(), "b", 0o640, "x");
        let e = read_secret_file(&loose, "password-file", &p)
            .unwrap_err()
            .to_string();
        assert!(
            e.contains("mode 0640")
                && e.contains("chmod 400")
                && e.contains("secret-file-max-mode"),
            "{e}"
        );
        let e = read_secret_file(&file(dir.path(), "c", 0o600, "x"), "password-file", &p)
            .unwrap_err()
            .to_string();
        assert!(e.contains("mode 0600"), "{e}");
        let group_ok = FilePolicy {
            max_mode: Some(0o640),
            ..p
        };
        assert_eq!(
            read_secret_file(&loose, "password-file", &group_ok).unwrap(),
            "x"
        );
    }

    #[test]
    fn tls_key_policy_allows_0600() {
        let dir = tempfile::tempdir().unwrap();
        let p = FilePolicy::TLS_KEY;
        assert!(read_checked(&file(dir.path(), "k1", 0o600, "k"), "key", &p).is_ok());
        let e = read_checked(&file(dir.path(), "k2", 0o644, "k"), "key", &p)
            .unwrap_err()
            .to_string();
        assert!(
            e.contains("mode 0644") && e.contains("tls-key-max-mode"),
            "{e}"
        );
        // Public TLS files are not mode-checked.
        assert!(read_checked(
            &file(dir.path(), "c", 0o644, "c"),
            "cert",
            &FilePolicy::TLS_PUBLIC
        )
        .is_ok());
    }

    #[test]
    fn size_limits() {
        let dir = tempfile::tempdir().unwrap();
        let big = file(dir.path(), "big", 0o400, &"x".repeat(4097));
        let e = read_secret_file(&big, "password-file", &FilePolicy::SECRET)
            .unwrap_err()
            .to_string();
        assert!(e.contains("larger than 4 KiB"), "{e}");
        assert!(read_checked(&big, "cert", &FilePolicy::TLS_PUBLIC).is_ok());
    }

    #[test]
    fn content_rules() {
        let dir = tempfile::tempdir().unwrap();
        let p = FilePolicy::SECRET;
        assert_eq!(
            read_secret_file(&file(dir.path(), "a", 0o400, " pass word \r\n"), "f", &p).unwrap(),
            " pass word "
        );
        assert!(
            read_secret_file(&file(dir.path(), "b", 0o400, "\n"), "f", &p)
                .unwrap_err()
                .to_string()
                .contains("empty")
        );
        assert!(read_secret_file(&dir.path().join("missing"), "f", &p).is_err());
        assert!(read_secret_file(dir.path(), "f", &p)
            .unwrap_err()
            .to_string()
            .contains("not a regular file"));
    }

    #[test]
    fn symlinks() {
        let dir = tempfile::tempdir().unwrap();
        let target = file(dir.path(), "real", 0o400, "linked");
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        let e = read_secret_file(&link, "password-file", &FilePolicy::SECRET)
            .unwrap_err()
            .to_string();
        assert!(
            e.contains("symbolic link") && e.contains("secret-file-follow-symlinks = true"),
            "{e}"
        );
        let follow = FilePolicy {
            follow_symlinks: true,
            ..FilePolicy::SECRET
        };
        assert_eq!(
            read_secret_file(&link, "password-file", &follow).unwrap(),
            "linked"
        );

        // TLS files follow by default; the target is what gets checked.
        assert!(read_checked(&link, "cert", &FilePolicy::TLS_PUBLIC).is_ok());
        let no_follow = FilePolicy {
            follow_symlinks: false,
            ..FilePolicy::TLS_PUBLIC
        };
        let e = read_checked(&link, "cert", &no_follow)
            .unwrap_err()
            .to_string();
        assert!(e.contains("tls-follow-symlinks = true"), "{e}");
    }

    #[test]
    fn policies_from_config() {
        assert_eq!(
            FilePolicies::from_config(&cfg("")).unwrap(),
            FilePolicies::default()
        );
        let p = FilePolicies::from_config(&cfg(
            "[core \"config\"]\nsecret-file-max-mode = 0640\nsecret-file-check-owner = false\n\
             secret-file-follow-symlinks = true\ntls-key-max-mode = 0640\ntls-follow-symlinks = false\n",
        ))
        .unwrap();
        assert_eq!(
            p.secret,
            FilePolicy {
                check_owner: false,
                max_mode: Some(0o640),
                follow_symlinks: true,
                ..FilePolicy::SECRET
            }
        );
        assert_eq!(p.tls_key.max_mode, Some(0o640));
        assert!(!p.tls_key.follow_symlinks && !p.tls_public.follow_symlinks);
        assert!(
            FilePolicies::from_config(&cfg("[core \"config\"]\nsecret-file-max-mode = 999\n"))
                .is_err()
        );
    }

    #[test]
    fn typed_password_lines() {
        let read = |s: &str| read_password_line(&mut std::io::Cursor::new(s.as_bytes().to_vec()));
        assert_eq!(read("s3cret\n").unwrap(), "s3cret");
        assert_eq!(read("with space \r\n").unwrap(), "with space ");
        assert_eq!(read("no newline").unwrap(), "no newline");
        assert_eq!(read("\n").unwrap(), ""); // Enter: an empty password
        assert!(read("").is_err()); // Ctrl-D: cancelled
        assert!(read(&"x".repeat(5000)).is_err());
    }
}
