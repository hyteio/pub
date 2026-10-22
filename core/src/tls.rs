//! TLS client setup shared by every module (rustls + ring).
//!
//! Endpoint settings: `insecure`, `ca-file`, `cert`, `key`, `server-name`,
//! `server-spiffe-id`. Trust roots come from `ca-file` when it is set (as with
//! curl's `--cacert`, it replaces the defaults), otherwise from the OS store,
//! falling back to the bundled Mozilla roots. With `server-spiffe-id` the
//! server is verified against `ca-file` only (the SPIFFE trust bundle) and
//! identified by its SPIFFE ID instead of its hostname.
//!
//! Certificate and key files are read through [`crate::secrets`], so the key's
//! owner/mode and every file's symlink policy are enforced (see `[core "config"]`).

use std::fmt;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::verify_server_cert_signed_by_trust_anchor;
use rustls::crypto::SupportedKxGroup;
use rustls::crypto::{self, CryptoProvider};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::ParsedCertificate;
use rustls::{
    CertificateError, ClientConfig, ClientConnection, DigitallySignedStruct, OtherError,
    RootCertStore, SignatureScheme, StreamOwned, SupportedCipherSuite, SupportedProtocolVersion,
};

use crate::config::expand_path;
use crate::endpoint::Endpoint;
use crate::error::Result;
use crate::secrets::{read_checked, FilePolicies, FilePolicy};
use crate::trace::Trace;
use crate::{bail_kind, err};

pub type TlsStream = StreamOwned<ClientConnection, TcpStream>;

#[derive(Debug, Clone, Default)]
pub struct TlsOptions {
    pub insecure: bool,
    pub ca_file: Option<PathBuf>,
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
    pub server_name: Option<String>,
    pub server_spiffe_id: Option<SpiffeMatch>,
    pub files: FilePolicies,
    pub min_version: TlsVersion,
    pub max_version: TlsVersion,
    /// Allowed cipher suites in preference order; `None` = the library default.
    pub cipher_suites: Option<Vec<SupportedCipherSuite>>,
    /// Key exchange groups in preference order; `None` = the library default.
    pub groups: Option<Vec<&'static dyn SupportedKxGroup>>,
}

impl TlsOptions {
    pub fn from_endpoint(ep: &Endpoint) -> Result<Self> {
        let server_spiffe_id = ep
            .prop("server-spiffe-id")
            .map(|v| SpiffeMatch::parse(v).map_err(|why| err!(Config, "server-spiffe-id: {why}")))
            .transpose()?;
        let version = |key: &str| -> Result<TlsVersion> {
            ep.prop(key)
                .map(|v| TlsVersion::parse(v).map_err(|why| err!(Config, "{key}: {why}")))
                .transpose()
                .map(Option::unwrap_or_default)
        };
        let max_version = version("tls-max-version")?;
        // Asking only for an older maximum lowers the default minimum with it.
        let min_version = match ep.prop("tls-min-version") {
            Some(_) => version("tls-min-version")?,
            None => TlsVersion::default().min(max_version),
        };
        if min_version > max_version {
            bail_kind!(
                Config,
                "tls-min-version {min_version} is above tls-max-version {max_version}"
            );
        }
        Ok(TlsOptions {
            insecure: ep.prop_bool("insecure", false)?,
            ca_file: ep.prop("ca-file").map(expand_path),
            cert: ep.prop("cert").map(expand_path),
            key: ep.prop("key").map(expand_path),
            server_name: ep.prop("server-name").map(str::to_string),
            server_spiffe_id,
            files: ep.files,
            min_version,
            max_version,
            cipher_suites: ep
                .prop("tls-cipher-suites")
                .map(|v| {
                    parse_cipher_suites(v).map_err(|why| err!(Config, "tls-cipher-suites: {why}"))
                })
                .transpose()?,
            groups: ep
                .prop("tls-groups")
                .map(|v| parse_groups(v).map_err(|why| err!(Config, "tls-groups: {why}")))
                .transpose()?,
        })
    }
}

/// Which SPIFFE IDs are acceptable for the server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpiffeMatch {
    /// Exactly this ID.
    Exact(String),
    /// Any workload in this trust domain (`spiffe://example.org/*`); holds the `spiffe://example.org/` prefix.
    TrustDomain(String),
}

impl SpiffeMatch {
    pub fn parse(s: &str) -> std::result::Result<Self, String> {
        let rest = s
            .strip_prefix("spiffe://")
            .ok_or_else(|| format!("expected spiffe://trust-domain/path, got '{s}'"))?;
        let (domain, path) = rest.split_once('/').unwrap_or((rest, ""));
        let domain_ok = !domain.is_empty()
            && domain.chars().all(|c| {
                c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '.' | '-' | '_')
            });
        if !domain_ok {
            return Err(format!(
                "'{s}': trust domain must be lowercase letters, digits, '.', '-', '_'"
            ));
        }
        if s.contains(['?', '#']) {
            return Err(format!("'{s}': a SPIFFE ID has no query or fragment"));
        }
        match path {
            "*" => Ok(SpiffeMatch::TrustDomain(format!("spiffe://{domain}/"))),
            "" => Err(format!(
                "'{s}': add a workload path, or /* to accept any workload in {domain}"
            )),
            p if p
                .split('/')
                .any(|seg| seg.is_empty() || seg == "." || seg == "..") =>
            {
                Err(format!("'{s}': invalid path segment"))
            }
            _ => Ok(SpiffeMatch::Exact(s.to_string())),
        }
    }

    pub fn matches(&self, id: &str) -> bool {
        match self {
            SpiffeMatch::Exact(want) => id == want,
            SpiffeMatch::TrustDomain(prefix) => {
                id.starts_with(prefix.as_str()) && id.len() > prefix.len()
            }
        }
    }
}

impl fmt::Display for SpiffeMatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SpiffeMatch::Exact(id) => f.write_str(id),
            SpiffeMatch::TrustDomain(prefix) => write!(f, "{prefix}*"),
        }
    }
}

/// TLS protocol versions rustls supports (it has no SSLv3, TLS 1.0 or 1.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum TlsVersion {
    V1_2,
    #[default]
    V1_3,
}

impl TlsVersion {
    pub fn parse(s: &str) -> std::result::Result<Self, String> {
        let t = s.trim().to_ascii_lowercase();
        let t = t.trim_start_matches("tlsv").trim_start_matches("tls");
        match t {
            "1.2" | "12" => Ok(TlsVersion::V1_2),
            "1.3" | "13" => Ok(TlsVersion::V1_3),
            "1.0" | "1.1" | "1" | "10" | "11" => Err(format!(
                "TLS {t} is not supported (insecure); use 1.2 or 1.3"
            )),
            _ => Err(format!("expected 1.2 or 1.3, got '{s}'")),
        }
    }

    fn rustls(self) -> &'static SupportedProtocolVersion {
        match self {
            TlsVersion::V1_2 => &rustls::version::TLS12,
            TlsVersion::V1_3 => &rustls::version::TLS13,
        }
    }
}

impl fmt::Display for TlsVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            TlsVersion::V1_2 => "1.2",
            TlsVersion::V1_3 => "1.3",
        })
    }
}

fn split_list(list: &str) -> impl Iterator<Item = &str> {
    list.split([',', ':', ' ', '\t']).filter(|s| !s.is_empty())
}

pub fn cipher_suite_name(cs: &SupportedCipherSuite) -> String {
    format!("{:?}", cs.suite())
}

pub fn group_name(g: &dyn SupportedKxGroup) -> String {
    format!("{:?}", g.name())
}

/// Every cipher suite this build supports, in default preference order.
pub fn cipher_suite_names() -> Vec<String> {
    crypto::ring::ALL_CIPHER_SUITES
        .iter()
        .map(cipher_suite_name)
        .collect()
}

/// Every key exchange group this build supports, in default preference order.
pub fn group_names() -> Vec<String> {
    crypto::ring::ALL_KX_GROUPS
        .iter()
        .map(|g| group_name(*g))
        .collect()
}

/// Ordered cipher suite list (`,` `:` or space separated), e.g.
/// `TLS13_AES_256_GCM_SHA384,TLS13_CHACHA20_POLY1305_SHA256`.
pub fn parse_cipher_suites(list: &str) -> std::result::Result<Vec<SupportedCipherSuite>, String> {
    let mut out: Vec<SupportedCipherSuite> = Vec::new();
    for name in split_list(list) {
        let cs = crypto::ring::ALL_CIPHER_SUITES
            .iter()
            .find(|cs| cipher_suite_name(cs).eq_ignore_ascii_case(name))
            .ok_or_else(|| {
                format!(
                    "unknown cipher suite '{name}' (supported: {})",
                    cipher_suite_names().join(", ")
                )
            })?;
        if !out.iter().any(|o| o.suite() == cs.suite()) {
            out.push(*cs);
        }
    }
    if out.is_empty() {
        return Err("empty cipher suite list".into());
    }
    Ok(out)
}

/// Ordered key exchange group list, e.g. `X25519,secp256r1`. `P-256`/`prime256v1`
/// and `P-384` are accepted as aliases.
pub fn parse_groups(list: &str) -> std::result::Result<Vec<&'static dyn SupportedKxGroup>, String> {
    let mut out: Vec<&'static dyn SupportedKxGroup> = Vec::new();
    for raw in split_list(list) {
        let name = match raw.to_ascii_lowercase().as_str() {
            "p-256" | "p256" | "prime256v1" => "secp256r1".to_string(),
            "p-384" | "p384" => "secp384r1".to_string(),
            _ => raw.to_string(),
        };
        let g = crypto::ring::ALL_KX_GROUPS
            .iter()
            .copied()
            .find(|g| group_name(*g).eq_ignore_ascii_case(&name))
            .ok_or_else(|| {
                format!(
                    "unknown key exchange group '{raw}' (supported: {})",
                    group_names().join(", ")
                )
            })?;
        if !out.iter().any(|o| o.name() == g.name()) {
            out.push(g);
        }
    }
    if out.is_empty() {
        return Err("empty key exchange group list".into());
    }
    Ok(out)
}

/// The ring provider, narrowed to the configured cipher suites and groups.
fn provider_for(opts: &TlsOptions) -> Arc<CryptoProvider> {
    let mut p = crypto::ring::default_provider();
    if let Some(suites) = &opts.cipher_suites {
        p.cipher_suites = suites.clone();
    }
    if let Some(groups) = &opts.groups {
        p.kx_groups = groups.clone();
    }
    Arc::new(p)
}

pub fn client_config(opts: &TlsOptions) -> Result<Arc<ClientConfig>> {
    let provider = provider_for(opts);
    let versions: Vec<&'static SupportedProtocolVersion> = [TlsVersion::V1_2, TlsVersion::V1_3]
        .into_iter()
        .filter(|v| (opts.min_version..=opts.max_version).contains(v))
        .map(TlsVersion::rustls)
        .collect();
    let usable = provider
        .cipher_suites
        .iter()
        .any(|cs| versions.iter().any(|v| cs.version() == *v));
    if !usable {
        bail_kind!(
            Config,
            "none of the configured tls-cipher-suites can be used with TLS {}..{}",
            opts.min_version,
            opts.max_version
        );
    }
    let builder = ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&versions)
        .map_err(|e| err!(Config, "TLS configuration: {e}"))?;

    let builder = match (&opts.server_spiffe_id, opts.insecure) {
        (Some(_), true) => bail_kind!(
            Usage,
            "server-spiffe-id and insecure cannot be used together"
        ),
        (Some(id), false) => {
            let ca = opts.ca_file.as_ref().ok_or_else(|| {
                err!(Usage, "server-spiffe-id needs ca-file: the SPIFFE trust bundle (e.g. spiffe-helper's bundle.pem)")
            })?;
            let mut roots = RootCertStore::empty();
            add_ca_file(&mut roots, ca, &opts.files.tls_public)?;
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(SpiffeVerifier {
                    roots: Arc::new(roots),
                    expected: id.clone(),
                    provider,
                }))
        }
        (None, true) => builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerify(provider))),
        (None, false) => builder.with_root_certificates(root_store(opts)?),
    };

    let config = match (&opts.cert, &opts.key) {
        (Some(cert), Some(key)) => {
            let certs = load_certs(cert, "cert", &opts.files.tls_public)?;
            let pem = read_checked(key, "key", &opts.files.tls_key)?;
            let key = PrivateKeyDer::from_pem_slice(&pem)
                .map_err(|e| err!(Tls, "cannot load private key {}: {e}", key.display()))?;
            builder
                .with_client_auth_cert(certs, key)
                .map_err(|e| err!(Tls, "client certificate: {e}"))?
        }
        (None, None) => builder.with_no_client_auth(),
        _ => bail_kind!(Usage, "client TLS auth needs both 'cert' and 'key'"),
    };
    Ok(Arc::new(config))
}

/// `ca-file` alone when set, so a private CA isn't undercut by every public
/// one; otherwise the OS store, or the bundled Mozilla roots without one.
fn root_store(opts: &TlsOptions) -> Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    if let Some(ca) = &opts.ca_file {
        add_ca_file(&mut roots, ca, &opts.files.tls_public)?;
        return Ok(roots);
    }
    let native = rustls_native_certs::load_native_certs();
    roots.add_parsable_certificates(native.certs);
    if roots.is_empty() {
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }
    Ok(roots)
}

fn add_ca_file(roots: &mut RootCertStore, ca: &Path, policy: &FilePolicy) -> Result<()> {
    for cert in load_certs(ca, "ca-file", policy)? {
        roots
            .add(cert)
            .map_err(|e| err!(Tls, "bad CA certificate in {}: {e}", ca.display()))?;
    }
    Ok(())
}

fn load_certs(
    path: &Path,
    what: &str,
    policy: &FilePolicy,
) -> Result<Vec<CertificateDer<'static>>> {
    let pem = read_checked(path, what, policy)?;
    let certs = CertificateDer::pem_slice_iter(&pem)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| err!(Tls, "cannot load certificates from {}: {e}", path.display()))?;
    if certs.is_empty() {
        bail_kind!(Tls, "no certificates found in {}", path.display());
    }
    Ok(certs)
}

/// Wrap a connected socket in TLS and complete the handshake, so certificate
/// problems surface at connect time rather than on first read.
pub fn handshake(
    mut tcp: TcpStream,
    host: &str,
    opts: &TlsOptions,
    timeout: Option<Duration>,
    trace: &Trace,
) -> Result<TlsStream> {
    let config = client_config(opts)?;
    let sni = opts.server_name.as_deref().unwrap_or(host).to_string();
    let name = ServerName::try_from(sni.clone())
        .map_err(|e| err!(Tls, "invalid TLS server name '{sni}': {e}"))?;
    let mut conn = ClientConnection::new(config, name).map_err(|e| err!(Tls, "{e}"))?;

    let verification = match (&opts.server_spiffe_id, opts.insecure) {
        (Some(id), _) => format!(" (verifying SPIFFE ID {id})"),
        (None, true) => " (verification disabled)".to_string(),
        (None, false) => String::new(),
    };
    trace.info(format_args!(
        "TLS handshake, server name {sni}{verification}, versions {}..{}",
        opts.min_version, opts.max_version
    ));
    tcp.set_read_timeout(timeout)
        .and_then(|_| tcp.set_write_timeout(timeout))
        .map_err(|e| err!(Io, "{e}"))?;
    while conn.is_handshaking() {
        if let Err(e) = conn.complete_io(&mut tcp) {
            match e.kind() {
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut => {
                    bail_kind!(Timeout, "TLS handshake with {host} timed out")
                }
                _ => {
                    // TLS 1.2 also uses the group list to say which curves the client can
                    // verify ECDSA certificates on; a narrow list can rule out the server's cert.
                    let hint = if opts.groups.is_some() && opts.min_version == TlsVersion::V1_2 {
                        " (with TLS 1.2, tls-groups must include the curve of the server's ECDSA certificate, usually secp256r1)"
                    } else {
                        ""
                    };
                    bail_kind!(
                        Tls,
                        "TLS handshake with {host} failed: {}{hint}",
                        describe_io_error(&e)
                    )
                }
            }
        }
    }
    tcp.set_read_timeout(None).map_err(|e| err!(Io, "{e}"))?;

    if let (Some(v), Some(cs)) = (conn.protocol_version(), conn.negotiated_cipher_suite()) {
        let group = conn
            .negotiated_key_exchange_group()
            .map(group_name)
            .unwrap_or_else(|| "?".into());
        trace.info(format_args!(
            "TLS connection using {v:?} / {:?} / {group}",
            cs.suite()
        ));
    }
    Ok(StreamOwned::new(conn, tcp))
}

/// Display a TLS failure. rustls shows custom certificate errors with `Debug`
/// (`Other(OtherError(..))`); show our own message instead.
pub fn describe_io_error(e: &std::io::Error) -> String {
    match e.get_ref().and_then(|i| i.downcast_ref::<rustls::Error>()) {
        Some(rustls::Error::InvalidCertificate(CertificateError::Other(OtherError(inner)))) => {
            format!("invalid peer certificate: {inner}")
        }
        _ => e.to_string(),
    }
}

/// The SPIFFE ID of an X.509-SVID: its single `spiffe://` URI SAN.
pub fn spiffe_id_of(cert: &CertificateDer<'_>) -> std::result::Result<String, String> {
    use x509_parser::prelude::{FromDer, GeneralName, X509Certificate};
    let (_, parsed) = X509Certificate::from_der(cert.as_ref())
        .map_err(|e| format!("cannot parse certificate: {e}"))?;
    let san = parsed
        .subject_alternative_name()
        .map_err(|e| format!("bad subjectAltName: {e}"))?
        .ok_or("certificate has no subjectAltName, so no SPIFFE ID")?;
    let uris: Vec<&str> = san
        .value
        .general_names
        .iter()
        .filter_map(|n| match n {
            GeneralName::URI(u) => Some(*u),
            _ => None,
        })
        .collect();
    match uris.as_slice() {
        [id] if id.starts_with("spiffe://") => Ok(id.to_string()),
        [] => Err("certificate has no URI SAN, so no SPIFFE ID".into()),
        [other] => Err(format!("certificate URI SAN '{other}' is not a SPIFFE ID")),
        many => Err(format!(
            "an X.509-SVID has exactly one URI SAN; found {}",
            many.len()
        )),
    }
}

#[derive(Debug)]
struct SpiffeError(String);

impl fmt::Display for SpiffeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SpiffeError {}

fn spiffe_error(msg: String) -> rustls::Error {
    rustls::Error::InvalidCertificate(CertificateError::Other(OtherError(Arc::new(SpiffeError(
        msg,
    )))))
}

/// Verifies the server's chain against the SPIFFE trust bundle (normal WebPKI path
/// validation, server-auth usage, expiry), then checks its SPIFFE ID instead of
/// a DNS name.
#[derive(Debug)]
struct SpiffeVerifier {
    roots: Arc<RootCertStore>,
    expected: SpiffeMatch,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for SpiffeVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        let parsed = ParsedCertificate::try_from(end_entity)?;
        verify_server_cert_signed_by_trust_anchor(
            &parsed,
            &self.roots,
            intermediates,
            now,
            self.provider.signature_verification_algorithms.all,
        )?;
        let id = spiffe_id_of(end_entity).map_err(spiffe_error)?;
        if !self.expected.matches(&id) {
            return Err(spiffe_error(format!(
                "server SPIFFE ID {id} does not match {}",
                self.expected
            )));
        }
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Accepts any server certificate (`-k`/`--insecure`). Signatures are still checked
/// so the handshake itself is sound.
#[derive(Debug)]
struct NoVerify(Arc<CryptoProvider>);

impl ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rcgen::{
        BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, SanType,
    };

    fn ca() -> Ca {
        ca_named("test trust domain CA")
    }

    struct Ca {
        cert: rcgen::Certificate,
        key: KeyPair,
    }

    fn ca_named(name: &str) -> Ca {
        let mut p = CertificateParams::new(vec![]).unwrap();
        p.distinguished_name.push(rcgen::DnType::CommonName, name);
        p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let key = KeyPair::generate().unwrap();
        Ca {
            cert: p.self_signed(&key).unwrap(),
            key,
        }
    }

    fn leaf(ca: &Ca, sans: Vec<SanType>) -> CertificateDer<'static> {
        let mut p = CertificateParams::new(vec![]).unwrap();
        p.subject_alt_names = sans;
        p.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let key = KeyPair::generate().unwrap();
        p.signed_by(&key, &ca.cert, &ca.key).unwrap().der().clone()
    }

    fn uri(s: &str) -> SanType {
        SanType::URI(s.try_into().unwrap())
    }

    fn verifier(ca: &Ca, expected: &str) -> SpiffeVerifier {
        let mut roots = RootCertStore::empty();
        roots.add(ca.cert.der().clone()).unwrap();
        SpiffeVerifier {
            roots: Arc::new(roots),
            expected: SpiffeMatch::parse(expected).unwrap(),
            provider: provider_for(&TlsOptions::default()),
        }
    }

    fn check(v: &SpiffeVerifier, cert: &CertificateDer<'_>) -> std::result::Result<(), String> {
        let name = ServerName::try_from("ignored.example").unwrap();
        v.verify_server_cert(cert, &[], &name, &[], UnixTime::now())
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    #[test]
    fn ca_file_replaces_the_default_roots() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ca.pem");
        std::fs::write(&path, ca().cert.pem()).unwrap();
        let only_ca = TlsOptions {
            ca_file: Some(path),
            ..Default::default()
        };
        assert_eq!(root_store(&only_ca).unwrap().len(), 1);
        // Without one: the OS store or the bundled roots, many of them.
        assert!(root_store(&TlsOptions::default()).unwrap().len() > 1);
    }

    #[test]
    fn spiffe_match_parsing() {
        assert_eq!(
            SpiffeMatch::parse("spiffe://example.org/ns/prod/sa/broker"),
            Ok(SpiffeMatch::Exact(
                "spiffe://example.org/ns/prod/sa/broker".into()
            ))
        );
        assert_eq!(
            SpiffeMatch::parse("spiffe://example.org/*"),
            Ok(SpiffeMatch::TrustDomain("spiffe://example.org/".into()))
        );
        for bad in [
            "https://example.org/x",
            "spiffe://Example.org/x",
            "spiffe://example.org",
            "spiffe://e.org/a//b",
            "spiffe://e.org/x?y",
        ] {
            assert!(SpiffeMatch::parse(bad).is_err(), "{bad}");
        }
        let td = SpiffeMatch::parse("spiffe://example.org/*").unwrap();
        assert!(td.matches("spiffe://example.org/broker"));
        assert!(!td.matches("spiffe://example.org.evil/broker"));
        assert!(!td.matches("spiffe://example.org/"));
    }

    #[test]
    fn verifies_chain_and_spiffe_id_not_hostname() {
        let ca = ca();
        let broker = leaf(&ca, vec![uri("spiffe://example.org/broker")]);

        assert_eq!(
            check(&verifier(&ca, "spiffe://example.org/broker"), &broker),
            Ok(())
        );
        assert_eq!(
            check(&verifier(&ca, "spiffe://example.org/*"), &broker),
            Ok(())
        );

        let e = check(&verifier(&ca, "spiffe://example.org/other"), &broker).unwrap_err();
        assert!(
            e.contains("does not match spiffe://example.org/other"),
            "{e}"
        );
        let e = check(&verifier(&ca, "spiffe://other.org/*"), &broker).unwrap_err();
        assert!(e.contains("does not match"), "{e}");

        // Right ID, wrong trust bundle.
        let other_ca = ca_named("some other CA");
        let e = check(&verifier(&other_ca, "spiffe://example.org/broker"), &broker).unwrap_err();
        assert!(e.contains("UnknownIssuer"), "{e}");

        // Not an SVID.
        let dns_only = leaf(
            &ca,
            vec![SanType::DnsName("broker.example.org".try_into().unwrap())],
        );
        assert!(check(&verifier(&ca, "spiffe://example.org/*"), &dns_only)
            .unwrap_err()
            .contains("no URI SAN"));
        let two = leaf(
            &ca,
            vec![uri("spiffe://example.org/a"), uri("spiffe://example.org/b")],
        );
        assert!(check(&verifier(&ca, "spiffe://example.org/*"), &two)
            .unwrap_err()
            .contains("exactly one URI SAN"));
    }

    #[test]
    fn spiffe_requires_bundle_and_excludes_insecure() {
        let id = Some(SpiffeMatch::parse("spiffe://example.org/x").unwrap());
        let e = client_config(&TlsOptions {
            server_spiffe_id: id.clone(),
            ..Default::default()
        })
        .unwrap_err();
        assert!(e.to_string().contains("needs ca-file"), "{e}");
        let e = client_config(&TlsOptions {
            server_spiffe_id: id,
            insecure: true,
            ..Default::default()
        })
        .unwrap_err();
        assert!(e.to_string().contains("cannot be used together"), "{e}");
    }

    #[test]
    fn tls_setting_parsers() {
        assert_eq!(TlsVersion::parse("TLSv1.2"), Ok(TlsVersion::V1_2));
        assert_eq!(TlsVersion::parse("1.3"), Ok(TlsVersion::V1_3));
        assert!(TlsVersion::parse("1.1")
            .unwrap_err()
            .contains("not supported"));
        assert_eq!(TlsVersion::default(), TlsVersion::V1_3);

        let suites =
            parse_cipher_suites("tls13_chacha20_poly1305_sha256:TLS13_AES_256_GCM_SHA384").unwrap();
        assert_eq!(
            cipher_suite_name(&suites[0]),
            "TLS13_CHACHA20_POLY1305_SHA256"
        );
        assert!(parse_cipher_suites("RC4-MD5")
            .unwrap_err()
            .contains("supported: TLS13_"));

        let groups = parse_groups("P-256, x25519").unwrap();
        assert_eq!(
            groups.iter().map(|g| group_name(*g)).collect::<Vec<_>>(),
            ["secp256r1", "X25519"]
        );
        assert!(parse_groups("X25519MLKEM768")
            .unwrap_err()
            .contains("unknown key exchange group"));
    }

    /// A one-connection rustls server; returns its port and a CA file that trusts it.
    fn tls_server(
        dir: &Path,
        versions: &[&'static SupportedProtocolVersion],
        groups: Option<Vec<&'static dyn SupportedKxGroup>>,
    ) -> (u16, PathBuf) {
        let ca = ca();
        let mut p = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        p.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let key = KeyPair::generate().unwrap();
        let cert = p.signed_by(&key, &ca.cert, &ca.key).unwrap();
        let ca_path = dir.join("ca.pem");
        std::fs::write(&ca_path, ca.cert.pem()).unwrap();

        let mut provider = crypto::ring::default_provider();
        if let Some(g) = groups {
            provider.kx_groups = g;
        }
        let cfg = rustls::ServerConfig::builder_with_provider(Arc::new(provider))
            .with_protocol_versions(versions)
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert.der().clone()],
                PrivateKeyDer::Pkcs8(key.serialize_der().into()),
            )
            .unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            if let Ok((mut tcp, _)) = listener.accept() {
                let mut conn = rustls::ServerConnection::new(Arc::new(cfg)).unwrap();
                while conn.is_handshaking() {
                    if conn.complete_io(&mut tcp).is_err() {
                        return;
                    }
                }
                std::thread::sleep(Duration::from_millis(200));
            }
        });
        (port, ca_path)
    }

    fn connect(port: u16, opts: &TlsOptions) -> std::result::Result<TlsStream, String> {
        let tcp = TcpStream::connect(("127.0.0.1", port)).unwrap();
        handshake(
            tcp,
            "localhost",
            opts,
            Some(Duration::from_secs(5)),
            &Trace::default(),
        )
        .map_err(|e| e.to_string())
    }

    #[test]
    fn min_version_defaults_to_1_3_and_can_be_lowered() {
        let dir = tempfile::tempdir().unwrap();
        let tls12_only = [&rustls::version::TLS12];

        let (port, ca) = tls_server(dir.path(), &tls12_only, None);
        let opts = TlsOptions {
            ca_file: Some(ca),
            ..Default::default()
        };
        assert!(
            connect(port, &opts).unwrap_err().contains("TLS handshake"),
            "default must refuse TLS 1.2"
        );

        let (port, ca) = tls_server(dir.path(), &tls12_only, None);
        let opts = TlsOptions {
            ca_file: Some(ca),
            min_version: TlsVersion::V1_2,
            ..Default::default()
        };
        let stream = connect(port, &opts).unwrap();
        assert_eq!(
            stream.conn.protocol_version(),
            Some(rustls::ProtocolVersion::TLSv1_2)
        );
    }

    #[test]
    fn groups_and_suites_are_honoured() {
        let dir = tempfile::tempdir().unwrap();
        let both = [&rustls::version::TLS13, &rustls::version::TLS12];

        // Our preference order picks the group.
        let (port, ca) = tls_server(dir.path(), &both, None);
        let opts = TlsOptions {
            ca_file: Some(ca),
            groups: Some(parse_groups("secp384r1,X25519").unwrap()),
            cipher_suites: Some(parse_cipher_suites("TLS13_CHACHA20_POLY1305_SHA256").unwrap()),
            ..Default::default()
        };
        let stream = connect(port, &opts).unwrap();
        assert_eq!(
            stream
                .conn
                .negotiated_key_exchange_group()
                .map(group_name)
                .as_deref(),
            Some("secp384r1")
        );
        assert_eq!(
            stream
                .conn
                .negotiated_cipher_suite()
                .map(|c| cipher_suite_name(&c))
                .as_deref(),
            Some("TLS13_CHACHA20_POLY1305_SHA256")
        );

        // No common group: refused.
        let (port, ca) = tls_server(dir.path(), &both, Some(parse_groups("X25519").unwrap()));
        let opts = TlsOptions {
            ca_file: Some(ca),
            groups: Some(parse_groups("secp256r1").unwrap()),
            ..Default::default()
        };
        assert!(connect(port, &opts).is_err());

        // Suites that cannot work with the allowed versions are a config error up front.
        let opts = TlsOptions {
            cipher_suites: Some(
                parse_cipher_suites("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256").unwrap(),
            ),
            ..Default::default()
        };
        let e = client_config(&opts).unwrap_err().to_string();
        assert!(
            e.contains("none of the configured tls-cipher-suites can be used with TLS 1.3..1.3"),
            "{e}"
        );
    }
}
