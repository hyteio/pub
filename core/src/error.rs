//! Error kinds and curl-style process exit codes.

use std::fmt;
use std::io;

pub type Result<T> = anyhow::Result<T>;

/// Broad error categories. Each maps to an exit code, borrowing curl's numbers
/// where an equivalent exists so scripts can branch on them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// No module for the scheme, or the module does not support the sub-command.
    Unsupported,
    /// Bad command line usage.
    Usage,
    /// Malformed URL, alias or configuration file.
    Config,
    /// Could not resolve or connect to the host.
    Connect,
    /// The server sent something unexpected or reported an error.
    Protocol,
    /// Local or socket I/O failure.
    Io,
    /// An operation did not complete in time.
    Timeout,
    /// TLS handshake or certificate failure.
    Tls,
    /// The server rejected the credentials.
    Auth,
}

impl ErrorKind {
    pub fn exit_code(self) -> i32 {
        match self {
            ErrorKind::Unsupported => 1,
            ErrorKind::Usage => 2,
            ErrorKind::Config => 3,
            ErrorKind::Connect => 7,
            ErrorKind::Protocol => 8,
            ErrorKind::Io => 26,
            ErrorKind::Timeout => 28,
            ErrorKind::Tls => 35,
            ErrorKind::Auth => 67,
        }
    }
}

#[derive(Debug)]
pub struct Error {
    pub kind: ErrorKind,
    pub message: String,
}

impl Error {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Error {
            kind,
            message: message.into(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

/// Build an `anyhow::Error` carrying an [`ErrorKind`]: `err!(Config, "bad {}", x)`.
#[macro_export]
macro_rules! err {
    ($kind:ident, $($arg:tt)*) => {
        $crate::anyhow::Error::from($crate::error::Error::new($crate::error::ErrorKind::$kind, format!($($arg)*)))
    };
}

/// Return early with a kinded error: `bail_kind!(Timeout, "no reply after {:?}", d)`.
#[macro_export]
macro_rules! bail_kind {
    ($kind:ident, $($arg:tt)*) => {
        return Err($crate::err!($kind, $($arg)*))
    };
}

/// The kind of the first kinded error in the cause chain.
pub fn kind_of(err: &anyhow::Error) -> Option<ErrorKind> {
    err.chain()
        .find_map(|cause| cause.downcast_ref::<Error>())
        .map(|e| e.kind)
}

/// Pick the process exit code for an error by looking for the first kinded error
/// (or recognisable I/O error) in its cause chain.
pub fn exit_code(err: &anyhow::Error) -> i32 {
    for cause in err.chain() {
        if let Some(e) = cause.downcast_ref::<Error>() {
            return e.kind.exit_code();
        }
        if let Some(e) = cause.downcast_ref::<io::Error>() {
            return match e.kind() {
                io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock => {
                    ErrorKind::Timeout.exit_code()
                }
                io::ErrorKind::ConnectionRefused => ErrorKind::Connect.exit_code(),
                _ => ErrorKind::Io.exit_code(),
            };
        }
    }
    1
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context as _;

    #[test]
    fn exit_code_survives_context() {
        let e: anyhow::Result<()> = Err(crate::err!(Timeout, "slow"));
        let e = e.context("while receiving").unwrap_err();
        assert_eq!(exit_code(&e), 28);
    }

    #[test]
    fn plain_errors_exit_1() {
        assert_eq!(exit_code(&anyhow::anyhow!("boom")), 1);
    }
}
