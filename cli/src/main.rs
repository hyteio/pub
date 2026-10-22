mod admin;
mod cli;
mod diag;
mod messaging;

use clap::Parser;
use pub_core::{exit_code, Registry};

use cli::{Cli, Cmd};

/// All compiled-in protocol modules and message formats. Adding one = add a
/// crate, a cargo feature, and one line here.
fn registry() -> Registry {
    #[allow(unused_mut)]
    let mut r = Registry::new();
    #[cfg(feature = "stomp")]
    r.register(pub_stomp::module());
    #[cfg(feature = "mqtt")]
    r.register(pub_mqtt::module());
    #[cfg(feature = "openwire")]
    r.register(pub_openwire::module());
    #[cfg(feature = "amqp")]
    r.register(pub_amqp::module());
    #[cfg(feature = "ramqp")]
    r.register(pub_ramqp::module());
    #[cfg(feature = "json")]
    r.register_format(pub_format_json::format());
    #[cfg(feature = "xml")]
    r.register_format(pub_format_xml::format());
    r
}

fn main() {
    let cli = Cli::parse_from(cli::expand_short_aliases(std::env::args_os()));
    // Commands that write messages handle a closed stdout themselves: an I/O
    // error (exit 26), the open transaction rolled back, a clean disconnect.
    // The others just print, and exit quietly like `cat` when the reader goes
    // away (`pub config settings | head`).
    if !matches!(cli.cmd, Cmd::Receive(_) | Cmd::Browse(_) | Cmd::Move(_)) {
        pub_core::process::default_sigpipe();
    }
    let registry = registry();
    let result = match cli.cmd {
        Cmd::Send(args) => messaging::send(&registry, args),
        Cmd::Receive(args) => messaging::receive(&registry, args),
        Cmd::Browse(args) => messaging::browse(&registry, args),
        Cmd::Unsubscribe(args) => messaging::unsubscribe(&registry, args),
        Cmd::Move(args) => messaging::move_messages(&registry, args),
        Cmd::Dest(cmd) => messaging::dest(&registry, cmd),
        Cmd::Ping(args) => messaging::ping(&registry, args),
        Cmd::Alias(cmd) => admin::alias(&registry, cmd),
        Cmd::Config(cmd) => admin::config(&registry, cmd),
        Cmd::Modules => admin::modules(&registry),
        Cmd::Formats => admin::formats(&registry),
    };
    if let Err(e) = result {
        eprintln!("pub: {e:#}");
        std::process::exit(exit_code(&e));
    }
}
