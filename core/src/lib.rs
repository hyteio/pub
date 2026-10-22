//! Shared plumbing for `pub`, the curl for messaging.
//!
//! Protocol modules provide a [`module::Module`], a [`connection::Codec`] (bytes <-> frames)
//! and a [`module::Session`] (the protocol conversation); configuration, aliases, sockets,
//! TLS, tracing and payloads live here.

pub mod aliases;
pub mod config;
pub mod connection;
pub mod endpoint;
pub mod error;
pub mod format;
pub mod limits;
pub mod message;
pub mod module;
pub mod payload;
pub mod process;
pub mod secrets;
pub mod settings;
pub mod strategy;
pub mod tls;
pub mod trace;
pub mod transport;

pub use aliases::{AliasValue, InertSetting, SettingAlias};
/// For the [`err!`] and [`bail_kind!`] macros, so crates using them needn't depend on it.
#[doc(hidden)]
pub use anyhow;
pub use config::Config;
pub use connection::{Codec, Connection, Decoded};
pub use endpoint::Endpoint;
pub use error::{exit_code, Error, ErrorKind, Result};
pub use format::{Format, FormatContext, MessageWriter, Record};
pub use message::{now_millis, BodyType, Field, Message, Value};
pub use module::{
    Context, Destination, DestinationOption, MessageCapabilities, Module, OptionScope,
    ReceiveOptions, Registry, SchemeInfo, Session, Verb,
};
pub use settings::{Issue, Setting, SettingType, Severity};
pub use trace::Trace;
