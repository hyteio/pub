//! Process-wide setup for the `pub` binary.

/// Exit quietly when the reader of stdout goes away (`pub modules | head`), as
/// `cat` does. Rust ignores SIGPIPE by default, which turns that into a write
/// error, and into a panic for `println!`.
pub fn default_sigpipe() {
    #[cfg(unix)]
    // SAFETY: restoring the default disposition of a signal, before any
    // threads are started.
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
    }
}
