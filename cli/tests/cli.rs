//! End-to-end tests: run the `pub` binary against the in-process mock STOMP broker.

use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Output, Stdio};

use pub_stomp::mock::MockBroker;

fn run(home: &Path, args: &[&str], stdin: Option<&[u8]>) -> Output {
    run_env(home, args, stdin, &[])
}

fn run_env(home: &Path, args: &[&str], stdin: Option<&[u8]>, env: &[(&str, &str)]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_pub"))
        .args(args)
        .envs(env.iter().copied())
        .env("PUB_HOME", home)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut pipe = child.stdin.take().unwrap();
    if let Some(data) = stdin {
        pipe.write_all(data).unwrap();
    }
    drop(pipe);
    child.wait_with_output().unwrap()
}

fn ok(out: &Output) -> String {
    assert!(
        out.status.success(),
        "status {:?}\nstderr: {}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

#[test]
fn send_and_receive_through_an_alias() {
    let home = tempfile::tempdir().unwrap();
    let broker = MockBroker::start(Some(("app", "s3cret")));
    let url = format!("stomp://{}", broker.addr);
    ok(&run(
        home.path(),
        &[
            "alias",
            "add",
            "jms.company.com",
            &url,
            "--set",
            "user=app",
            "--set",
            "password=s3cret",
        ],
        None,
    ));

    ok(&run(
        home.path(),
        &[
            "send",
            "jms.company.com/queue/orders",
            "-d",
            "first",
            "-H",
            "x-trace: 1",
        ],
        None,
    ));
    ok(&run(
        home.path(),
        &["send", "jms.company.com/queue/orders"],
        Some(b"from stdin"),
    ));
    let file = home.path().join("payload.bin");
    fs::write(&file, b"from file").unwrap();
    ok(&run(
        home.path(),
        &[
            "send",
            "jms.company.com",
            "--dest",
            "/queue/orders",
            "-f",
            file.to_str().unwrap(),
        ],
        None,
    ));
    assert_eq!(broker.queued("/queue/orders").len(), 3);

    let out = ok(&run(
        home.path(),
        &["receive", "jms.company.com/queue/orders", "-n", "3", "-l"],
        None,
    ));
    assert_eq!(out, "first\nfrom stdin\nfrom file\n");
    assert_eq!(broker.acks(), 3);
}

#[test]
fn receive_with_headers_and_timeout() {
    let home = tempfile::tempdir().unwrap();
    let broker = MockBroker::start(None);
    let url = broker.url("/queue/a");

    ok(&run(
        home.path(),
        &["send", &url, "-d", "hi", "-t", "text/plain"],
        None,
    ));
    let out = ok(&run(home.path(), &["recv", &url, "-i"], None));
    assert!(out.contains("content-type: text/plain\n"), "{out}");
    assert!(out.ends_with("\n\nhi"), "{out:?}");

    let out = run(home.path(), &["receive", &url, "-m", "200ms"], None);
    assert_eq!(out.status.code(), Some(28), "{}", stderr(&out));
}

#[test]
fn verbose_trace_hides_password() {
    let home = tempfile::tempdir().unwrap();
    let broker = MockBroker::start(Some(("app", "hunter2")));
    let out = run(
        home.path(),
        &["ping", &broker.url(""), "-u", "app:hunter2", "-v"],
        None,
    );
    let text = ok(&out);
    assert!(
        text.contains("server") && text.contains("mock/1.0"),
        "{text}"
    );
    let err = stderr(&out);
    assert!(
        err.contains("> CONNECT") && err.contains("< CONNECTED"),
        "{err}"
    );
    assert!(err.contains("passcode:****"), "{err}");
    assert!(
        err.contains(
            "Socket: TCP_NODELAY=true, keepalive=on (idle 30s, interval 10s, count 3), connect-timeout=5s, send-timeout=10s, receive-timeout=10s"
        ),
        "{err}"
    );
    assert!(!err.contains("hunter2"), "{err}");
}

#[test]
fn socket_buffers_are_reported() {
    let home = tempfile::tempdir().unwrap();
    let broker = MockBroker::start(None);

    // Defaults: buffers only shown at -vv, and attributed to the OS.
    let out = run(home.path(), &["ping", &broker.url(""), "-v"], None);
    ok(&out);
    assert!(!stderr(&out).contains("Socket buffers"), "{}", stderr(&out));
    let out = run(home.path(), &["ping", &broker.url(""), "-vv"], None);
    ok(&out);
    assert!(stderr(&out).contains(" (OS) snd="), "{}", stderr(&out));

    // Overrides: shown at -v with the requested size next to the applied one.
    let out = run(
        home.path(),
        &[
            "ping",
            &broker.url(""),
            "-v",
            "--receive-buffer",
            "128k",
            "--no-keepalive",
        ],
        None,
    );
    ok(&out);
    let err = stderr(&out);
    assert!(err.contains("(requested 131072)"), "{err}");
    assert!(err.contains("keepalive=off"), "{err}");
}

#[test]
fn exit_codes() {
    let home = tempfile::tempdir().unwrap();
    let code = |args: &[&str]| run(home.path(), args, None).status.code();

    // unsupported scheme / unknown alias / usage
    assert_eq!(code(&["recv", "kafka://company.com/a"]), Some(1));
    assert_eq!(code(&["send", "jms.company.com", "-d", "x"]), Some(3));
    assert_eq!(code(&["send", "stomp://h", "-d", "x"]), Some(2));

    // connection refused
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    assert_eq!(
        code(&["ping", &format!("stomp://127.0.0.1:{port}")]),
        Some(7)
    );

    // bad credentials
    let broker = MockBroker::start(Some(("a", "b")));
    assert_eq!(code(&["ping", &broker.url(""), "-u", "a:wrong"]), Some(67));

    // TLS against a plaintext server fails the handshake
    let url = format!("stomps://{}", broker.addr);
    let out = run(
        home.path(),
        &["ping", &url, "--connect-timeout", "2s"],
        None,
    );
    assert!(
        matches!(out.status.code(), Some(35) | Some(28)),
        "{:?} {}",
        out.status,
        stderr(&out)
    );
}

#[test]
fn alias_lifecycle() {
    let home = tempfile::tempdir().unwrap();
    ok(&run(
        home.path(),
        &["alias", "add", "mq", "stomps://mq01:61612/queue/x"],
        None,
    ));
    let dup = run(home.path(), &["alias", "add", "mq", "stomp://other"], None);
    assert!(!dup.status.success());

    let out = ok(&run(home.path(), &["alias", "resolve", "mq/y"], None));
    assert!(out.contains("stomps://mq01:61612/queue/x/y"), "{out}");
    assert!(out.contains("tls              true"), "{out}");

    // An alias in the main config can't be removed by pub.
    fs::write(
        home.path().join("config"),
        "[alias \"hand\"]\n\turl = stomp://h\n",
    )
    .unwrap();
    assert!(!run(home.path(), &["alias", "rm", "hand"], None)
        .status
        .success());

    ok(&run(home.path(), &["alias", "rm", "mq"], None));
    assert!(!home.path().join("config.d/mq.conf").exists());
    assert_eq!(
        ok(&run(home.path(), &["alias", "list"], None)).trim(),
        "hand  stomp://h"
    );
}

#[test]
fn settings_are_validated() {
    let home = tempfile::tempdir().unwrap();
    let broker = MockBroker::start(None);
    let code = |args: &[&str]| run(home.path(), args, None).status.code();

    // Typos and bad values typed on the command line fail before connecting.
    let out = run(
        home.path(),
        &["ping", &broker.url(""), "-O", "recieve-timeout=1s"],
        None,
    );
    assert_eq!(out.status.code(), Some(3));
    assert!(
        stderr(&out).contains("did you mean 'receive-timeout'"),
        "{}",
        stderr(&out)
    );
    assert_eq!(
        code(&["ping", &broker.url(""), "--keepalive-count", "lots"]),
        Some(3)
    );
    assert_eq!(
        code(&["ping", &format!("{}?vhots=x", broker.url(""))]),
        Some(3)
    );

    // alias add refuses unknown keys unless --force.
    let url = broker.url("/queue/a");
    assert_eq!(
        code(&["alias", "add", "mq", &url, "--set", "vhots=prod"]),
        Some(3)
    );
    ok(&run(
        home.path(),
        &["alias", "add", "mq", &url, "--set", "vhost=prod"],
        None,
    ));
    ok(&run(
        home.path(),
        &[
            "alias",
            "add",
            "mq2",
            &url,
            "--set",
            "vhots=prod",
            "--force",
        ],
        None,
    ));

    // Strict by default: a typo in a config file is an error, with a hint.
    let out = run(home.path(), &["send", "mq2", "-d", "x"], None);
    assert_eq!(out.status.code(), Some(3));
    let err = stderr(&out);
    assert!(
        err.contains("'vhost'") && err.contains("strict = false"),
        "{err}"
    );
    let out = run(home.path(), &["config", "check"], None);
    assert_eq!(out.status.code(), Some(3));
    assert!(
        stderr(&out).contains("1 error(s), 0 warning(s)"),
        "{}",
        stderr(&out)
    );

    // core.config.strict = false: the typo only warns, so the alias works.
    fs::write(
        home.path().join("config"),
        "[core \"config\"]\n\tstrict = false\n",
    )
    .unwrap();
    let out = run(home.path(), &["send", "mq2", "-d", "x"], None);
    ok(&out);
    assert!(
        stderr(&out).contains("warning:") && stderr(&out).contains("'vhost'"),
        "{}",
        stderr(&out)
    );
    let out = run(home.path(), &["config", "check"], None);
    ok(&out);
    assert!(
        stderr(&out).contains("0 error(s), 1 warning(s)"),
        "{}",
        stderr(&out)
    );
    assert!(ok(&run(home.path(), &["config", "list"], None)).contains("core.config.strict=false"));

    // Bad values are errors in either mode.
    fs::write(
        home.path().join("config"),
        "[core \"config\"]\n\tstrict = false\n[defaults]\nconnect-timeout = soon\n",
    )
    .unwrap();
    let out = run(home.path(), &["config", "check"], None);
    assert_eq!(out.status.code(), Some(3));
    assert!(String::from_utf8_lossy(&out.stdout).contains("connect-timeout: expected a duration"));

    // The schema is listable per module.
    let out = ok(&run(home.path(), &["config", "settings", "stomp"], None));
    assert!(
        out.contains("receive-timeout") && out.contains("vhost"),
        "{out}"
    );
    assert_eq!(code(&["config", "settings", "nosuch"]), Some(2));
}

#[test]
fn headers_properties_and_destination_options() {
    let home = tempfile::tempdir().unwrap();
    let broker = MockBroker::start(None);
    let code = |args: &[&str]| run(home.path(), args, None).status.code();

    // Destination options in alias config and in a URL query after the alias; case preserved.
    let url = broker.url("/queue/orders");
    ok(&run(
        home.path(),
        &[
            "alias",
            "add",
            "jms",
            &url,
            "--set",
            "activemq.prefetchSize=10",
        ],
        None,
    ));
    ok(&run(
        home.path(),
        &[
            "send",
            "jms?x-max-length=5",
            "-d",
            "hi",
            "-H",
            "JMSCorrelationID: c-1",
            "-H",
            "priority: 7",
            "-H",
            "ttl: 1m",
            "-H",
            "region: EU",
        ],
        None,
    ));
    let send = &broker.frames("SEND")[0];
    let get = |k: &str| {
        send.headers
            .iter()
            .find(|(n, _)| n == k)
            .map(|(_, v)| v.clone())
    };
    assert_eq!(get("correlation-id").as_deref(), Some("c-1"));
    assert_eq!(get("priority").as_deref(), Some("7"));
    assert!(get("expires").is_some() && get("ttl").is_none());
    assert_eq!(get("region").as_deref(), Some("EU"));
    assert_eq!(get("x-max-length").as_deref(), Some("5")); // x-* applies to send and receive
    assert_eq!(get("activemq.prefetchSize"), None); // consume-only option stays off SEND

    let out = ok(&run(
        home.path(),
        &["receive", "jms", "-i", "-s", "region = 'EU'"],
        None,
    ));
    assert!(
        out.contains("# standard\n") && out.contains("correlation-id: c-1\n"),
        "{out}"
    );
    assert!(
        out.contains("# properties\n") && out.contains("region: EU\n"),
        "{out}"
    );
    assert!(
        out.contains("# protocol\n") && out.contains("subscription: "),
        "{out}"
    );
    let sub = &broker.frames("SUBSCRIBE")[0];
    assert_eq!(sub.get("activemq.prefetchSize"), Some("10"));
    assert_eq!(sub.get("selector"), Some("region = 'EU'"));

    // Unknown query keys are still caught.
    assert_eq!(
        code(&["send", "jms?activemq_prefetch=1", "-d", "x"]),
        Some(3)
    );
    // Broker-set fields can't be sent; bad values are rejected.
    assert_eq!(
        code(&["send", "jms", "-d", "x", "-H", "message-id: 1"]),
        Some(2)
    );
    assert_eq!(
        code(&["send", "jms", "-d", "x", "-H", "priority: 12"]),
        Some(2)
    );
}

#[test]
fn module_limits_follow_strict_mode() {
    let home = tempfile::tempdir().unwrap();
    let broker = MockBroker::start(None);
    let url = broker.url("/queue/a");
    let send = |extra: &[&str]| {
        let mut args = vec!["send", url.as_str(), "-d", "x"];
        args.extend_from_slice(extra);
        run(home.path(), &args, None)
    };

    // A property named like a STOMP header would change meaning: always an error.
    let out = send(&["-P", "priority=1"]);
    assert_eq!(out.status.code(), Some(3));
    assert!(
        stderr(&out).contains("reserved by the protocol"),
        "{}",
        stderr(&out)
    );

    // Typed property / unsupported field: errors in strict mode (default)...
    let out = send(&["-P", "n:int=5"]);
    assert_eq!(out.status.code(), Some(3));
    assert!(
        stderr(&out).contains("will arrive as a String"),
        "{}",
        stderr(&out)
    );
    assert_eq!(send(&["-H", "delivery-delay: 10s"]).status.code(), Some(3));

    // ...warnings otherwise, and the message is sent without the unsupported field.
    fs::write(
        home.path().join("config"),
        "[core \"config\"]\n\tstrict = false\n",
    )
    .unwrap();
    let out = send(&["-P", "n:int=5", "-H", "delivery-delay: 10s"]);
    ok(&out);
    assert!(stderr(&out).contains("warning:"), "{}", stderr(&out));
    let frame = broker.frames("SEND").pop().unwrap();
    assert_eq!(frame.get("n"), Some("5"));
    assert!(frame
        .headers
        .iter()
        .all(|(k, _)| !k.to_lowercase().contains("delay")));
}

#[cfg(unix)]
#[test]
fn password_file_is_checked() {
    use std::os::unix::fs::PermissionsExt;
    let home = tempfile::tempdir().unwrap();
    let broker = MockBroker::start(Some(("app", "s3cret")));
    let pw = home.path().join("app.password");
    fs::write(&pw, "s3cret\n").unwrap();
    let pw_s = pw.to_str().unwrap().to_string();
    let chmod = |m: u32| fs::set_permissions(&pw, fs::Permissions::from_mode(m)).unwrap();
    let url = broker.url("");
    let ping = |extra: &[&str]| {
        let mut args = vec!["ping", url.as_str(), "-u", "app"];
        args.extend_from_slice(extra);
        run(home.path(), &args, None)
    };
    let opt = format!("password-file={pw_s}");

    // Default policy: 0644 is refused with a fix-it hint; 0400 works.
    chmod(0o644);
    let out = ping(&["-O", &opt]);
    assert_eq!(out.status.code(), Some(3));
    assert!(
        stderr(&out).contains("mode 0644") && stderr(&out).contains("chmod 400"),
        "{}",
        stderr(&out)
    );
    chmod(0o400);
    ok(&ping(&["-O", &opt]));

    // Through an alias, and visible (not masked) in config listings.
    ok(&run(
        home.path(),
        &[
            "alias", "add", "mq", &url, "--set", "user=app", "--set", &opt,
        ],
        None,
    ));
    ok(&run(home.path(), &["ping", "mq"], None));
    assert!(
        ok(&run(home.path(), &["config", "list"], None)).contains(&format!("password-file={pw_s}"))
    );

    // Loosened under [core "config"]; config check honours the same policy.
    chmod(0o640);
    assert_eq!(
        run(home.path(), &["ping", "mq"], None).status.code(),
        Some(3)
    );
    let out = run(home.path(), &["config", "check"], None);
    assert_eq!(out.status.code(), Some(3));
    assert!(String::from_utf8_lossy(&out.stdout).contains("mode 0640"));
    fs::write(
        home.path().join("config"),
        "[core \"config\"]\n\tsecret-file-max-mode = 0640\n",
    )
    .unwrap();
    ok(&run(home.path(), &["ping", "mq"], None));
    ok(&run(home.path(), &["config", "check"], None));
    chmod(0o400);

    // Symlinked password files are refused unless allowed.
    let link = home.path().join("pw-link");
    std::os::unix::fs::symlink(&pw, &link).unwrap();
    let link_opt = format!("password-file={}", link.display());
    let out = ping(&["-O", &link_opt]);
    assert_eq!(out.status.code(), Some(3));
    assert!(stderr(&out).contains("symbolic link"), "{}", stderr(&out));
    fs::write(
        home.path().join("config"),
        "[core \"config\"]\n\tsecret-file-follow-symlinks = true\n",
    )
    .unwrap();
    ok(&ping(&["-O", &link_opt]));
}

#[test]
fn input_limits() {
    let home = tempfile::tempdir().unwrap();
    let broker = MockBroker::start(None);
    let url = broker.url("/queue/a");
    let out = |args: &[&str], stdin: Option<&[u8]>| run(home.path(), args, stdin);

    // Payload: -d, -f and stdin are all bounded by max-payload.
    let o = out(&["send", &url, "-O", "max-payload=4", "-d", "hello"], None);
    assert_eq!(o.status.code(), Some(2));
    assert!(
        stderr(&o).contains("larger than max-payload (4 bytes)"),
        "{}",
        stderr(&o)
    );
    assert_eq!(
        out(&["send", &url, "-O", "max-payload=4"], Some(b"hello"))
            .status
            .code(),
        Some(2)
    );
    ok(&out(
        &["send", &url, "-O", "max-payload=5", "-d", "hello"],
        None,
    ));

    // Headers: count, name and value sizes.
    let o = out(
        &[
            "send",
            &url,
            "-d",
            "x",
            "-O",
            "max-headers=1",
            "-H",
            "a: 1",
            "-H",
            "b: 2",
        ],
        None,
    );
    assert!(
        stderr(&o).contains("2 headers and properties; the limit is max-headers (1)"),
        "{}",
        stderr(&o)
    );
    let o = out(
        &[
            "send",
            &url,
            "-d",
            "x",
            "-O",
            "max-header-value=3",
            "-H",
            "a: 1234",
        ],
        None,
    );
    assert!(
        stderr(&o).contains("value of 'a' is 4 bytes"),
        "{}",
        stderr(&o)
    );
    let long_name = format!("{}: 1", "n".repeat(300));
    assert_eq!(
        out(&["send", &url, "-d", "x", "-H", &long_name], None)
            .status
            .code(),
        Some(2)
    );

    // Selector and destination lengths.
    let sel = "a".repeat(5000);
    assert_eq!(
        out(&["receive", &url, "-s", &sel], None).status.code(),
        Some(2)
    );
    let dest = format!("/queue/{}", "d".repeat(1100));
    assert_eq!(
        out(&["send", &broker.url(""), "--dest", &dest, "-d", "x"], None)
            .status
            .code(),
        Some(2)
    );
}

#[cfg(unix)]
#[test]
fn writable_config_is_refused_unless_overridden() {
    use std::os::unix::fs::PermissionsExt;
    let home = tempfile::tempdir().unwrap();
    let broker = MockBroker::start(None);
    let cfg = home.path().join("config");
    fs::write(&cfg, "[defaults]\n\treceipts = true\n").unwrap();
    fs::set_permissions(&cfg, fs::Permissions::from_mode(0o666)).unwrap();

    let o = run(home.path(), &["ping", &broker.url("")], None);
    assert_eq!(o.status.code(), Some(3));
    assert!(
        stderr(&o).contains("writable by everyone")
            && stderr(&o).contains("PUB_CHECK_PERMISSIONS=false"),
        "{}",
        stderr(&o)
    );

    let o = run_env(
        home.path(),
        &["ping", &broker.url("")],
        None,
        &[("PUB_CHECK_PERMISSIONS", "false")],
    );
    ok(&o);
    fs::set_permissions(&cfg, fs::Permissions::from_mode(0o644)).unwrap();
    ok(&run(home.path(), &["ping", &broker.url("")], None));
}

#[test]
fn mqtt_send_and_receive_through_an_alias() {
    use pub_mqtt::mock::{MockBroker as MqttBroker, MockConfig};
    let home = tempfile::tempdir().unwrap();
    let broker = MqttBroker::start(MockConfig::default());
    ok(&run(
        home.path(),
        &["alias", "add", "iot", &broker.url(""), "--set", "qos=1"],
        None,
    ));

    // Subscribe with a '#' wildcard in the URL (a fragment, as far as URLs go).
    let sub = Command::new(env!("CARGO_BIN_EXE_pub"))
        .args(["receive", "iot/sensors/#", "-i", "-m", "5s"])
        .env("PUB_HOME", home.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let start = std::time::Instant::now();
    while broker.packets("SUBSCRIBE").is_empty() {
        assert!(
            start.elapsed() < std::time::Duration::from_secs(5),
            "no SUBSCRIBE"
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    std::thread::sleep(std::time::Duration::from_millis(100));
    ok(&run(
        home.path(),
        &[
            "send",
            "iot/sensors/kitchen",
            "-d",
            "21.5",
            "-t",
            "text/plain",
            "-H",
            "correlation-id: c-1",
            "-H",
            "unit: C",
        ],
        None,
    ));
    let out = sub.wait_with_output().unwrap();
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        text.contains("destination: sensors/kitchen\n") && text.contains("correlation-id: c-1\n"),
        "{text}"
    );
    assert!(
        text.contains("# properties\nunit: C\n") && text.ends_with("21.5"),
        "{text}"
    );
    match &broker.packets("SUBSCRIBE")[0] {
        pub_mqtt::Packet::Subscribe(s) => assert_eq!(s.filters[0].0, "sensors/#"),
        other => panic!("{other:?}"),
    }

    // Module settings are validated like any other.
    let o = run(
        home.path(),
        &["send", "iot/a", "-d", "x", "-O", "qos=3"],
        None,
    );
    assert_eq!(o.status.code(), Some(3));
    assert!(
        stderr(&o).contains("expected one of 0, 1, 2"),
        "{}",
        stderr(&o)
    );
    let o = run(
        home.path(),
        &["send", "iot/a", "-d", "x", "-H", "priority: 9"],
        None,
    );
    assert!(
        stderr(&o).contains("the mqtt module cannot carry 'priority'"),
        "{}",
        stderr(&o)
    );

    // MQTT 3.1.1 via the version setting: no properties at all.
    ok(&run(
        home.path(),
        &["send", "iot/a", "-d", "x", "-O", "version=3.1.1"],
        None,
    ));
    let o = run(
        home.path(),
        &[
            "send",
            "iot/a",
            "-d",
            "x",
            "-O",
            "version=3.1.1",
            "-H",
            "unit: C",
        ],
        None,
    );
    assert_eq!(o.status.code(), Some(3));
    assert!(
        stderr(&o).contains("the mqtt module (MQTT 3.1.1) cannot carry properties"),
        "{}",
        stderr(&o)
    );
    let o = run(
        home.path(),
        &["send", "iot/a", "-d", "x", "-O", "version=4"],
        None,
    );
    assert!(
        stderr(&o).contains("expected one of 5, 3.1.1"),
        "{}",
        stderr(&o)
    );
}

#[test]
fn message_type_layers_and_protocol_mapping() {
    use pub_mqtt::mock::{MockBroker as MqttBroker, MockConfig};
    let home = tempfile::tempdir().unwrap();
    let stomp = MockBroker::start(None);
    let mqtt = MqttBroker::start(MockConfig::default());
    let has_length = |f: &pub_stomp::Frame| f.headers.iter().any(|(k, _)| k == "content-length");

    // Global default: bytes -> STOMP sends content-length (a BytesMessage on ActiveMQ/Artemis).
    ok(&run(
        home.path(),
        &["send", &stomp.url("/queue/a"), "-d", "one"],
        None,
    ));
    assert!(has_length(&stomp.frames("SEND")[0]));

    // [module "stomp"] message-type = text -> no content-length (a TextMessage).
    fs::write(
        home.path().join("config"),
        "[module \"stomp\"]\n\tmessage-type = text\n",
    )
    .unwrap();
    ok(&run(
        home.path(),
        &["send", &stomp.url("/queue/a"), "-d", "two"],
        None,
    ));
    assert!(!has_length(&stomp.frames("SEND")[1]));
    // The STOMP module default doesn't touch MQTT.
    ok(&run(
        home.path(),
        &["send", &mqtt.url("/t"), "-d", "x"],
        None,
    ));
    match mqtt.packets("PUBLISH").last().unwrap() {
        pub_mqtt::Packet::Publish(p) => assert!(p
            .props
            .num(pub_mqtt::codec::prop::PAYLOAD_FORMAT_INDICATOR)
            .is_none()),
        other => panic!("{other:?}"),
    }

    // [protocol "stomp"] is more specific than [module "stomp"]; -O beats both.
    fs::write(
        home.path().join("config"),
        "[module \"stomp\"]\n\tmessage-type = text\n[protocol \"stomp\"]\n\tmessage-type = bytes\n",
    )
    .unwrap();
    ok(&run(
        home.path(),
        &["send", &stomp.url("/queue/a"), "-d", "three"],
        None,
    ));
    assert!(has_length(&stomp.frames("SEND")[2]));
    ok(&run(
        home.path(),
        &[
            "send",
            &stomp.url("/queue/a"),
            "-d",
            "four",
            "-O",
            "message-type=text",
        ],
        None,
    ));
    assert!(!has_length(&stomp.frames("SEND")[3]));

    // A text body must be UTF-8 (and, over STOMP, free of NUL bytes).
    let o = run(
        home.path(),
        &["send", &stomp.url("/queue/a"), "-O", "message-type=text"],
        Some(&[0xFF, 0x00]),
    );
    assert_eq!(o.status.code(), Some(2), "{}", stderr(&o));
    let o = run(
        home.path(),
        &["send", &stomp.url("/queue/a"), "-O", "message-type=text"],
        Some(b"a\0b"),
    );
    assert!(
        stderr(&o).contains("cannot contain NUL bytes"),
        "{}",
        stderr(&o)
    );

    // MQTT 5 carries "text" as the Payload Format Indicator; 3.1.1 can't (strict: error).
    ok(&run(
        home.path(),
        &[
            "send",
            &mqtt.url("/t"),
            "-d",
            "x",
            "-O",
            "message-type=text",
        ],
        None,
    ));
    match mqtt.packets("PUBLISH").last().unwrap() {
        pub_mqtt::Packet::Publish(p) => assert_eq!(
            p.props.num(pub_mqtt::codec::prop::PAYLOAD_FORMAT_INDICATOR),
            Some(1)
        ),
        other => panic!("{other:?}"),
    }
    let o = run(
        home.path(),
        &[
            "send",
            &mqtt.url("/t"),
            "-d",
            "x",
            "-O",
            "message-type=text",
            "-O",
            "version=3.1.1",
        ],
        None,
    );
    assert_eq!(o.status.code(), Some(3));
    assert!(
        stderr(&o).contains("(MQTT 3.1.1) cannot mark a body as text"),
        "{}",
        stderr(&o)
    );
}

#[test]
fn openwire_send() {
    use pub_openwire::mock::{MockBroker as AmqBroker, MockConfig};
    use pub_openwire::primitive::{decode_map, decode_text, Primitive};
    use pub_openwire::producer::destination_text;
    use pub_openwire::types::kind;
    let home = tempfile::tempdir().unwrap();
    let broker = AmqBroker::start(MockConfig::default());
    let url = broker.url("/queue/orders");

    // Text message with JMS headers, typed properties and delivery delay; sent twice.
    ok(&run(
        home.path(),
        &[
            "send",
            &url,
            "-d",
            "hello",
            "-n",
            "2",
            "-O",
            "message-type=text",
            "-H",
            "JMSCorrelationID: c-1",
            "-H",
            "priority: 7",
            "-H",
            "ttl: 1m",
            "-H",
            "delivery-delay: 5s",
            "-P",
            "n:int=5",
            "-P",
            "region=EU",
        ],
        None,
    ));
    // Bytes by default; non-persistent goes asynchronously; a destination option.
    let o = run(
        home.path(),
        &[
            "send",
            &format!("{url}?producer.windowSize=65536"),
            "-d",
            "raw",
            "-H",
            "persistent: false",
            "-v",
        ],
        None,
    );
    ok(&o);
    assert!(
        stderr(&o).contains("BytesMessage") && stderr(&o).contains("(async)"),
        "{}",
        stderr(&o)
    );

    let msgs = broker.messages();
    assert_eq!(msgs.len(), 3);
    let f = &msgs[0].fields;
    assert_eq!(msgs[0].kind, kind::TEXT);
    assert_eq!(
        destination_text(f.destination.as_deref().unwrap()),
        "queue://orders"
    );
    assert_eq!(
        decode_text(f.content.as_deref().unwrap())
            .unwrap()
            .as_deref(),
        Some("hello")
    );
    assert_eq!(
        (f.correlation_id.as_deref(), f.priority, f.persistent),
        (Some("c-1"), 7, true)
    );
    assert_eq!(f.expiration - f.timestamp, 60_000);
    let props = decode_map(f.marshalled_properties.as_deref().unwrap())
        .unwrap()
        .unwrap();
    assert!(
        props.contains(&("n".to_string(), Primitive::Int(5))),
        "{props:?}"
    );
    assert!(
        props.contains(&("region".to_string(), Primitive::String("EU".into()))),
        "{props:?}"
    );
    assert!(
        props.contains(&("AMQ_SCHEDULED_DELAY".to_string(), Primitive::Long(5000))),
        "{props:?}"
    );
    assert_eq!(msgs[2].kind, kind::BYTES);
    assert!(!msgs[2].fields.response_required && !msgs[2].fields.persistent);

    // content-type has no OpenWire home (strict mode makes that an error).
    let o = run(
        home.path(),
        &["send", &url, "-d", "x", "-t", "text/plain"],
        None,
    );
    assert_eq!(o.status.code(), Some(3), "{}", stderr(&o));
    assert!(
        stderr(&o).contains("cannot carry 'content-type'"),
        "{}",
        stderr(&o)
    );
    // A JMS header name as a property would quietly set the header: refused.
    let o = run(
        home.path(),
        &["send", &url, "-d", "x", "-P", "JMSPriority=1"],
        None,
    );
    assert_eq!(o.status.code(), Some(3), "{}", stderr(&o));
    assert!(stderr(&o).contains("reserved"), "{}", stderr(&o));
    assert_eq!(broker.messages().len(), 3);
    // Unknown producer option: usage error, as in the Java client.
    let o = run(
        home.path(),
        &["send", &format!("{url}?producer.windowSiz=1"), "-d", "x"],
        None,
    );
    assert_eq!(o.status.code(), Some(2), "{}", stderr(&o));
}

#[test]
fn openwire_receive() {
    use pub_openwire::mock::{MockBroker as AmqBroker, MockConfig};
    let home = tempfile::tempdir().unwrap();
    let broker = AmqBroker::start(MockConfig::default());
    let url = broker.url("/queue/orders");
    ok(&run(
        home.path(),
        &[
            "send",
            &url,
            "-d",
            "hello",
            "-O",
            "message-type=text",
            "-H",
            "JMSCorrelationID: c-1",
            "-P",
            "n:int=5",
            "-P",
            "region=EU",
        ],
        None,
    ));
    ok(&run(home.path(), &["send", &url, "-d", "second"], None));
    ok(&run(home.path(), &["send", &url, "-d", "third"], None));

    // One message, with headers; prefetch follows -n (shown under -v).
    let o = run(home.path(), &["recv", &url, "-i", "-v"], None);
    let out = ok(&o);
    assert!(out.contains("correlation-id: c-1\n"), "{out}");
    assert!(out.contains("destination: queue://orders\n"), "{out}");
    assert!(out.contains("message-type: text\n"), "{out}");
    assert!(
        out.contains("n (int): 5\n") && out.contains("region: EU\n"),
        "{out}"
    );
    assert!(out.ends_with("\n\nhello"), "{out:?}");
    assert!(stderr(&o).contains("(prefetch 1)"), "{}", stderr(&o));

    // The rest, until the queue is drained; a consumer option from the URL.
    let o = run(
        home.path(),
        &[
            "recv",
            &format!("{url}?consumer.prefetchSize=5"),
            "-n",
            "0",
            "-m",
            "500ms",
            "--lines",
            "-v",
        ],
        None,
    );
    assert_eq!(ok(&o), "second\nthird\n");
    assert!(stderr(&o).contains("(prefetch 5)"), "{}", stderr(&o));

    // A selector from a file (-sf), with a comment line; it reaches the consumer.
    let sel = home.path().join("eu.sel");
    fs::write(&sel, "# EU only\nregion = 'EU'\n  AND n > 1\n").unwrap();
    ok(&run(home.path(), &["send", &url, "-d", "x"], None));
    ok(&run(
        home.path(),
        &["recv", &url, "-sf", sel.to_str().unwrap(), "-m", "500ms"],
        None,
    ));
    let infos = broker.commands("ConsumerInfo");
    match infos.last().unwrap() {
        pub_openwire::DataStructure::ConsumerInfo(ci) => {
            assert_eq!(ci.selector.as_deref(), Some("region = 'EU'\n  AND n > 1"))
        }
        other => panic!("{other:?}"),
    }
    let empty = home.path().join("empty.sel");
    fs::write(&empty, "# nothing\n\n").unwrap();
    let o = run(
        home.path(),
        &["recv", &url, "-sf", empty.to_str().unwrap()],
        None,
    );
    assert_eq!(o.status.code(), Some(2), "{}", stderr(&o));
    let o = run(
        home.path(),
        &[
            "recv",
            &url,
            "-s",
            "a = 1",
            "--selector-file",
            sel.to_str().unwrap(),
        ],
        None,
    );
    assert_eq!(o.status.code(), Some(2), "{}", stderr(&o));

    // Nothing left: timeout (28). A bad consumer option: usage (2).
    let o = run(home.path(), &["recv", &url, "-m", "200ms"], None);
    assert_eq!(o.status.code(), Some(28), "{}", stderr(&o));
    let o = run(
        home.path(),
        &[
            "recv",
            &format!("{url}?consumer.prefetchSiz=5"),
            "-m",
            "200ms",
        ],
        None,
    );
    assert_eq!(o.status.code(), Some(2), "{}", stderr(&o));
}

#[test]
fn json_capture_and_replay() {
    use pub_openwire::mock::{MockBroker as AmqBroker, MockConfig};
    use pub_openwire::primitive::{decode_map, Primitive};
    use pub_openwire::producer::destination_text;
    let home = tempfile::tempdir().unwrap();
    let a = AmqBroker::start(MockConfig::default());
    let b = AmqBroker::start(MockConfig::default());
    let src = a.url("/queue/orders");
    ok(&run(
        home.path(),
        &[
            "send",
            &src,
            "-d",
            "héllo",
            "-O",
            "message-type=text",
            "-H",
            "JMSCorrelationID: c-1",
            "-H",
            "priority: 7",
            "-H",
            "ttl: 90s",
            "-P",
            "__AMQ_CID=from-artemis",
            "-P",
            "n:int=5",
            "-P",
            "region=EU",
        ],
        None,
    ));
    ok(&run(
        home.path(),
        &["send", &src, "-d", "@-"],
        Some(&[0u8, 1, 255][..]),
    ));

    // Capture as JSON Lines.
    let capture = ok(&run(
        home.path(),
        &["recv", &src, "-n", "2", "-F", "json"],
        None,
    ));
    let lines: Vec<&str> = capture.lines().collect();
    assert_eq!(lines.len(), 2, "{capture}");
    assert!(
        lines[0].starts_with(r#"{"pub":1,"destination":"queue://orders","fields":{"message-id":"#),
        "{}",
        lines[0]
    );
    assert!(
        lines[0].contains(r#""correlation-id":"c-1""#) && lines[0].contains(r#""priority":7"#),
        "{}",
        lines[0]
    );
    assert!(
        lines[0]
            .contains(r#""properties":{"__AMQ_CID":"from-artemis","n":{"int":5},"region":"EU"}"#),
        "{}",
        lines[0]
    );
    assert!(
        lines[0].ends_with(r#""body":{"type":"text","data":"héllo"}}"#),
        "{}",
        lines[0]
    );
    assert!(
        lines[1].ends_with(r#""body":{"type":"bytes","base64":"AAH/"}}"#),
        "{}",
        lines[1]
    );

    // Replay into another broker; a command-line header overrides the document.
    let file = home.path().join("capture.jsonl");
    fs::write(&file, &capture).unwrap();
    let o = run(
        home.path(),
        &[
            "send",
            &b.url("/queue/copy"),
            "-F",
            "json",
            "-f",
            file.to_str().unwrap(),
            "-H",
            "priority: 2",
            "-v",
        ],
        None,
    );
    ok(&o);
    assert!(stderr(&o).contains("Header strategy default: not sending __AMQ_CID, delivery-count, expiration, message-id, redelivered, timestamp"), "{}", stderr(&o));
    assert!(
        stderr(&o).contains("ttl restored from expiration - timestamp for 1 message(s)"),
        "{}",
        stderr(&o)
    );
    let got = b.messages();
    assert_eq!(got.len(), 2);
    let f = &got[0].fields;
    assert_eq!(
        destination_text(f.destination.as_deref().unwrap()),
        "queue://copy"
    );
    assert_eq!((f.correlation_id.as_deref(), f.priority), (Some("c-1"), 2));
    // The replayed message expires as long after its new send as the original did.
    assert_eq!(f.expiration - f.timestamp, 90_000);
    assert_eq!(got[1].fields.expiration, 0);
    assert_eq!(got[0].kind, pub_openwire::types::kind::TEXT);
    let props = decode_map(f.marshalled_properties.as_deref().unwrap())
        .unwrap()
        .unwrap();
    assert!(
        props.contains(&("n".to_string(), Primitive::Int(5))),
        "{props:?}"
    );
    assert_eq!(got[1].fields.content.as_deref(), Some(&[0u8, 1, 255][..]));
    assert!(!props.iter().any(|(k, _)| k == "__AMQ_CID"), "{props:?}"); // provider-only
                                                                        // A new identity: not the captured message-id or timestamp.
    let id = |m: &pub_openwire::types::Message| {
        pub_openwire::consumer::message_id_string(m.fields.message_id.as_deref().unwrap()).unwrap()
    };
    let original = a.messages();
    assert_ne!(id(&got[0]), id(&original[0]));

    // Administrative: the exact message-id, timestamp and expiration.
    let admin = AmqBroker::start(MockConfig::default());
    ok(&run(
        home.path(),
        &[
            "send",
            &admin.url("/queue/copy"),
            "-F",
            "json",
            "-f",
            file.to_str().unwrap(),
            "--header-strategy",
            "admin",
        ],
        None,
    ));
    let exact = admin.messages();
    assert_eq!(exact.len(), 2);
    for (copy, orig) in exact.iter().zip(&original) {
        assert_eq!(id(copy), id(orig));
        assert_eq!(copy.fields.timestamp, orig.fields.timestamp);
        assert_eq!(copy.fields.expiration, orig.fields.expiration);
    }
    assert!(exact[0].fields.expiration > 0);
    let props = decode_map(exact[0].fields.marshalled_properties.as_deref().unwrap())
        .unwrap()
        .unwrap();
    assert!(!props.iter().any(|(k, _)| k == "__AMQ_CID"), "{props:?}");
    // The setting works too; a protocol that can't set the id says so (an error in strict mode).
    let stomp = MockBroker::start(None);
    let o = run(
        home.path(),
        &[
            "send",
            &stomp.url("/queue/copy"),
            "-F",
            "json",
            "-f",
            file.to_str().unwrap(),
            "-O",
            "header-strategy=administrative",
        ],
        None,
    );
    assert_eq!(o.status.code(), Some(3), "{}", stderr(&o));
    assert!(
        stderr(&o).contains("cannot carry 'message-id'"),
        "{}",
        stderr(&o)
    );
    let o = run(
        home.path(),
        &[
            "send",
            &b.url("/queue/x"),
            "-F",
            "json",
            "--header-strategy",
            "exakt",
        ],
        Some(b"{}"),
    );
    assert_eq!(o.status.code(), Some(2));
    assert!(
        stderr(&o).contains("unknown header strategy"),
        "{}",
        stderr(&o)
    );

    // --dest-from-input sends each message where its document says.
    let docs = r#"{"destination":"queue://one","body":{"data":"1"}}
{"body":{"data":"2"}}
[{"destination":"topic://three","body":{"data":"3"}}]"#;
    ok(&run(
        home.path(),
        &[
            "send",
            &b.url("/queue/fallback"),
            "-F",
            "json",
            "--dest-from-input",
            "-n",
            "2",
        ],
        Some(docs.as_bytes()),
    ));
    let dests: Vec<String> = b.messages()[2..]
        .iter()
        .map(|m| destination_text(m.fields.destination.as_deref().unwrap()))
        .collect();
    assert_eq!(
        dests,
        [
            "queue://one",
            "queue://fallback",
            "topic://three",
            "queue://one",
            "queue://fallback",
            "topic://three"
        ]
    );
    // Without a fallback, a document with no destination is an error, before anything is sent.
    let before = b.messages().len();
    let o = run(
        home.path(),
        &["send", &b.url(""), "-F", "json", "--dest-from-input"],
        Some(docs.as_bytes()),
    );
    assert_eq!(o.status.code(), Some(2), "{}", stderr(&o));
    assert!(
        stderr(&o).contains("message 2: the document names no destination"),
        "{}",
        stderr(&o)
    );
    // A bad record late in the input: nothing is sent.
    let o = run(
        home.path(),
        &["send", &b.url("/queue/x"), "-F", "json"],
        Some(b"{\"body\":{\"data\":\"ok\"}}\n{\"bdy\":1}\n"),
    );
    assert_eq!(o.status.code(), Some(2), "{}", stderr(&o));
    assert!(
        stderr(&o).contains("message 2: unknown key 'bdy'"),
        "{}",
        stderr(&o)
    );
    assert_eq!(b.messages().len(), before);
}

#[test]
fn format_selection() {
    use pub_openwire::mock::{MockBroker as AmqBroker, MockConfig};
    let home = tempfile::tempdir().unwrap();
    let broker = AmqBroker::start(MockConfig::default());
    let url = broker.url("/queue/f");
    for body in ["a", "b", "c"] {
        ok(&run(
            home.path(),
            &["send", &url, "-d", body, "-H", "priority: 5"],
            None,
        ));
    }
    // -F text is -i; the `format` setting picks a default; -F wins over it.
    let text = ok(&run(home.path(), &["recv", &url, "-F", "text"], None));
    assert!(
        text.starts_with("# standard\n") && text.contains("priority: 5\n"),
        "{text}"
    );
    let json = ok(&run(
        home.path(),
        &["recv", &url, "-O", "format=json"],
        None,
    ));
    assert!(json.starts_with(r#"{"pub":1"#), "{json}");
    let raw = ok(&run(
        home.path(),
        &["recv", &url, "-O", "format=json", "-F", "raw"],
        None,
    ));
    assert_eq!(raw, "c");

    let code = |args: &[&str]| run(home.path(), args, None).status.code();
    assert_eq!(
        code(&["recv", &url, "-i", "-F", "json", "-m", "100ms"]),
        Some(2)
    );
    let o = run(
        home.path(),
        &["recv", &url, "-F", "jsn", "-m", "100ms"],
        None,
    );
    assert_eq!(o.status.code(), Some(2));
    assert!(stderr(&o).contains("did you mean 'json'"), "{}", stderr(&o));
    let o = run(home.path(), &["send", &url, "-F", "text", "-d", "x"], None);
    assert_eq!(o.status.code(), Some(2));
    assert!(stderr(&o).contains("can only be written"), "{}", stderr(&o));

    let formats = ok(&run(home.path(), &["formats"], None));
    assert!(
        formats.contains("raw") && formats.contains("text") && formats.contains("json"),
        "{formats}"
    );
    assert!(formats.contains("read, write"), "{formats}");
    let settings = ok(&run(home.path(), &["config", "settings"], None));
    assert!(
        settings.contains("format.json.pretty") && settings.contains("format.json.body"),
        "{settings}"
    );
    // Format options are validated like any setting.
    assert_eq!(
        code(&[
            "recv",
            &url,
            "-F",
            "json",
            "-O",
            "format.json.body=yes",
            "-m",
            "100ms"
        ]),
        Some(3)
    );
}

#[test]
fn openwire_browse_and_durable() {
    use pub_openwire::mock::{MockBroker as AmqBroker, MockConfig};
    let home = tempfile::tempdir().unwrap();
    let broker = AmqBroker::start(MockConfig::default());
    let q = broker.url("/queue/b");
    for body in ["one", "two"] {
        ok(&run(
            home.path(),
            &["send", &q, "-d", body, "-O", "message-type=text"],
            None,
        ));
    }
    // Browse shows everything and consumes nothing; JSON works as for recv.
    assert_eq!(
        ok(&run(home.path(), &["browse", &q, "--lines"], None)),
        "one\ntwo\n"
    );
    let json = ok(&run(
        home.path(),
        &["browse", &q, "-n", "1", "-F", "json"],
        None,
    ));
    assert!(
        json.contains(r#""body":{"type":"text","data":"one"}"#),
        "{json}"
    );
    assert_eq!(
        ok(&run(home.path(), &["recv", &q, "-n", "2", "--lines"], None)),
        "one\ntwo\n"
    );
    // An empty queue: nothing to show, still success.
    assert_eq!(ok(&run(home.path(), &["browse", &q], None)), "");
    let o = run(home.path(), &["browse", &broker.url("/topic/t")], None);
    assert_eq!(o.status.code(), Some(2), "{}", stderr(&o));

    // Durable subscriptions: need a client ID; unsubscribe removes them.
    let t = broker.url("/topic/news");
    let o = run(
        home.path(),
        &["recv", &t, "--durable", "audit", "-m", "200ms"],
        None,
    );
    assert_eq!(o.status.code(), Some(2), "{}", stderr(&o));
    assert!(stderr(&o).contains("client-id"), "{}", stderr(&o));
    let o = run(
        home.path(),
        &[
            "recv",
            &t,
            "--durable",
            "audit",
            "-O",
            "jms.clientID=app",
            "-m",
            "200ms",
            "-v",
        ],
        None,
    );
    assert_eq!(o.status.code(), Some(28), "{}", stderr(&o)); // nothing published yet
    assert!(
        stderr(&o).contains("as durable subscription 'audit'"),
        "{}",
        stderr(&o)
    );
    assert_eq!(
        broker.subscriptions(),
        [("app".to_string(), "audit".to_string())]
    );
    ok(&run(
        home.path(),
        &[
            "unsubscribe",
            &t,
            "--durable",
            "audit",
            "-O",
            "client-id=app",
        ],
        None,
    ));
    assert!(broker.subscriptions().is_empty());
    let o = run(
        home.path(),
        &[
            "unsubscribe",
            &broker.url(""),
            "--durable",
            "audit",
            "-O",
            "client-id=app",
        ],
        None,
    );
    assert_eq!(o.status.code(), Some(2), "{}", stderr(&o));

    // STOMP has transactions and move too.
    let stomp = MockBroker::start(None);
    let q = stomp.url("/queue/q");
    ok(&run(
        home.path(),
        &[
            "send",
            &q,
            "-d",
            "x",
            "-n",
            "3",
            "--transaction",
            "--batch",
            "2",
        ],
        None,
    ));
    assert_eq!(stomp.frames("COMMIT").len(), 2);
    let o = run(
        home.path(),
        &["move", &q, "--to", "/queue/b", "-m", "200ms"],
        None,
    );
    ok(&o);
    assert!(stderr(&o).contains("moved 3 messages"), "{}", stderr(&o));
    assert_eq!(stomp.queued("/queue/b").len(), 3);

    // MQTT says it can't.
    let mqtt = pub_mqtt::mock::MockBroker::start(pub_mqtt::mock::MockConfig::default());
    let o = run(
        home.path(),
        &["send", &mqtt.url("/t"), "-d", "x", "--transaction"],
        None,
    );
    assert_eq!(o.status.code(), Some(1), "{}", stderr(&o));
    assert!(stderr(&o).contains("no transactions"), "{}", stderr(&o));
    let o = run(home.path(), &["move", &mqtt.url("/a"), "--to", "b"], None);
    assert_eq!(o.status.code(), Some(1), "{}", stderr(&o));
}

#[test]
fn openwire_destination_management() {
    use pub_openwire::mock::{MockBroker as AmqBroker, MockConfig};
    let home = tempfile::tempdir().unwrap();
    let broker = AmqBroker::start(MockConfig::default());
    ok(&run(
        home.path(),
        &["dest", "create", &broker.url("/queue/orders")],
        None,
    ));
    ok(&run(
        home.path(),
        &["destinations", "create", &broker.url("/topic/prices")],
        None,
    ));
    let list = |args: &[&str]| {
        let url = broker.url("");
        let mut a = vec!["dest", "list", url.as_str(), "-m", "300ms"];
        a.extend_from_slice(args);
        ok(&run(home.path(), &a, None))
    };
    assert_eq!(list(&[]), "queue://orders\ntopic://prices\n");
    assert_eq!(list(&["--queues"]), "queue://orders\n");
    assert_eq!(list(&["--topics"]), "topic://prices\n");
    ok(&run(
        home.path(),
        &[
            "dest",
            "delete",
            &broker.url(""),
            "--dest",
            "topic://prices",
        ],
        None,
    ));
    assert_eq!(list(&[]), "queue://orders\n");
    let o = run(home.path(), &["dest", "create", &broker.url("")], None);
    assert_eq!(
        o.status.code(),
        Some(2),
        "no destination named: {}",
        stderr(&o)
    );
    let stomp = MockBroker::start(None);
    let o = run(home.path(), &["dest", "list", &stomp.url("")], None);
    assert_eq!(o.status.code(), Some(1), "{}", stderr(&o));
}

#[test]
fn xml_capture_and_replay() {
    use pub_openwire::mock::{MockBroker as AmqBroker, MockConfig};
    let home = tempfile::tempdir().unwrap();
    let a = AmqBroker::start(MockConfig::default());
    let b = AmqBroker::start(MockConfig::default());
    ok(&run(
        home.path(),
        &[
            "send",
            &a.url("/queue/x"),
            "-d",
            "line\r\n<one> & \"two\"",
            "-O",
            "message-type=text",
            "-P",
            "n:int=5",
            "-H",
            "ttl: 90s",
        ],
        None,
    ));
    ok(&run(
        home.path(),
        &["send", &a.url("/queue/x"), "-d", "@-"],
        Some(&[0u8, 1, 255][..]),
    ));
    let xml = ok(&run(
        home.path(),
        &["recv", &a.url("/queue/x"), "-n", "2", "-F", "xml"],
        None,
    ));
    assert!(
        xml.starts_with("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<messages version=\"1\">\n"),
        "{xml}"
    );
    assert!(xml.ends_with("</messages>\n"), "{xml}");
    assert!(
        xml.contains(r#"<property name="n" type="int">5</property>"#),
        "{xml}"
    );
    assert!(
        xml.contains("line&#13;\n&lt;one&gt; &amp; &quot;two&quot;</body>"),
        "{xml}"
    );
    // Replayed through XML: the same body bytes, types and ttl.
    ok(&run(
        home.path(),
        &["send", &b.url("/queue/y"), "-F", "xml"],
        Some(xml.as_bytes()),
    ));
    let json = ok(&run(
        home.path(),
        &["recv", &b.url("/queue/y"), "-n", "2", "-F", "json"],
        None,
    ));
    let lines: Vec<&str> = json.lines().collect();
    assert!(
        lines[0].contains(r#""data":"line\r\n<one> & \"two\"""#),
        "{json}"
    );
    assert!(lines[0].contains(r#""n":{"int":5}"#), "{json}");
    assert!(lines[1].contains(r#""base64":"AAH/""#), "{json}");
    let got = b.messages();
    assert_eq!(got[0].fields.expiration - got[0].fields.timestamp, 90_000);
    assert!(ok(&run(home.path(), &["formats"], None)).contains("xml"));
}

#[test]
fn password_prompt_without_a_terminal() {
    // Tests have no terminal: -u user falls back to configured passwords (no
    // hang), and password-prompt says why it can't ask.
    use pub_openwire::mock::{MockBroker as AmqBroker, MockConfig};
    let home = tempfile::tempdir().unwrap();
    let broker = AmqBroker::start(MockConfig {
        credentials: Some(("app".into(), "s3cret".into())),
        ..Default::default()
    });
    ok(&run(
        home.path(),
        &[
            "ping",
            &broker.url(""),
            "-u",
            "app",
            "-O",
            "password=s3cret",
        ],
        None,
    ));
    let o = run(
        home.path(),
        &[
            "ping",
            &broker.url("").replace("amq://", "amq://app@"),
            "-O",
            "password-prompt=true",
        ],
        None,
    );
    assert_eq!(o.status.code(), Some(3), "{}", stderr(&o));
    assert!(
        stderr(&o).contains("no terminal to ask on"),
        "{}",
        stderr(&o)
    );
}

#[test]
fn amqp_send_receive_browse() {
    use pub_amqp::mock::{MockBroker as AmqpBroker, MockConfig};
    let home = tempfile::tempdir().unwrap();
    let broker = AmqpBroker::start(MockConfig::default());
    let q = broker.url("/queue/orders");
    ok(&run(
        home.path(),
        &[
            "send",
            &q,
            "-d",
            "héllo",
            "-O",
            "message-type=text",
            "-H",
            "JMSCorrelationID: c-1",
            "-P",
            "n:int=5",
        ],
        None,
    ));
    ok(&run(
        home.path(),
        &["send", &q, "-d", "@-"],
        Some(&[0u8, 255][..]),
    ));
    assert_eq!(
        ok(&run(home.path(), &["browse", &q, "-F", "json"], None))
            .lines()
            .count(),
        2
    );
    let json = ok(&run(
        home.path(),
        &["recv", &q, "-n", "2", "-F", "json"],
        None,
    ));
    let lines: Vec<&str> = json.lines().collect();
    assert!(
        lines[0].contains(r#""correlation-id":"c-1""#) && lines[0].contains(r#""n":{"int":5}"#),
        "{json}"
    );
    assert!(
        lines[0].ends_with(r#""body":{"type":"text","data":"héllo"}}"#),
        "{json}"
    );
    assert!(
        lines[1].ends_with(r#""body":{"type":"bytes","base64":"AP8="}}"#),
        "{json}"
    );
    let ping = ok(&run(home.path(), &["ping", &broker.url("")], None));
    assert!(
        ping.contains("mock 1.0") && ping.contains("ANONYMOUS"),
        "{ping}"
    );
    let modules = ok(&run(home.path(), &["modules"], None));
    assert!(
        modules.contains("amqp:5672") && modules.contains("amqps:5671 (tls)"),
        "{modules}"
    );
    // Durable subscriptions want a fixed client id; content-type is carried.
    let o = run(
        home.path(),
        &[
            "recv",
            &broker.url("/topic/t"),
            "--durable",
            "s",
            "-m",
            "100ms",
        ],
        None,
    );
    assert_eq!(o.status.code(), Some(2), "{}", stderr(&o));
    ok(&run(
        home.path(),
        &["send", &q, "-d", "{}", "-t", "application/json"],
        None,
    ));
}

#[test]
fn ramqp_send_receive_move() {
    use pub_ramqp::mock::{MockBroker as RabbitBroker, MockConfig};
    let home = tempfile::tempdir().unwrap();
    let broker = RabbitBroker::start(MockConfig::default());
    let base = broker.url("");
    ok(&run(
        home.path(),
        &["dest", "create", &format!("{base}/queue/orders")],
        None,
    ));
    let q = format!("{base}/queue/orders");
    ok(&run(
        home.path(),
        &[
            "send",
            &q,
            "-d",
            "héllo",
            "-O",
            "message-type=text",
            "-P",
            "n:int=5",
            "-H",
            "ttl: 1m",
        ],
        None,
    ));
    let json = ok(&run(home.path(), &["recv", &q, "-F", "json"], None));
    assert!(
        json.contains(r#""n":{"int":5}"#)
            && json.contains(r#""body":{"type":"text","data":"héllo"}"#),
        "{json}"
    );
    assert!(json.contains(r#""routing-key":"orders""#), "{json}");
    // Unroutable: no such queue (exit 2); a topic without listeners is fine.
    let o = run(
        home.path(),
        &["send", &format!("{base}/queue/nope"), "-d", "x"],
        None,
    );
    assert_eq!(o.status.code(), Some(2), "{}", stderr(&o));
    ok(&run(
        home.path(),
        &["send", &format!("{base}/topic/nobody.home"), "-d", "x"],
        None,
    ));
    // A '#' wildcard survives the URL (it's a fragment there).
    let o = run(
        home.path(),
        &[
            "recv",
            &format!("{base}/topic/audit.#"),
            "--durable",
            "audit-q",
            "-m",
            "200ms",
        ],
        None,
    );
    assert_eq!(o.status.code(), Some(28), "{}", stderr(&o));
    ok(&run(
        home.path(),
        &["send", &format!("{base}/topic/audit.x.y"), "-d", "kept"],
        None,
    ));
    assert_eq!(
        ok(&run(
            home.path(),
            &[
                "recv",
                &format!("{base}/topic/audit.#"),
                "--durable",
                "audit-q",
                "-m",
                "2s"
            ],
            None
        )),
        "kept"
    );
    ok(&run(
        home.path(),
        &["unsubscribe", &base, "--durable", "audit-q"],
        None,
    ));
    // Transactions and move.
    ok(&run(
        home.path(),
        &["dest", "create", &format!("{base}/queue/back")],
        None,
    ));
    ok(&run(
        home.path(),
        &["send", &q, "-d", "m", "-n", "3", "--transaction"],
        None,
    ));
    let o = run(
        home.path(),
        &["move", &q, "--to", "/queue/back", "-m", "300ms"],
        None,
    );
    assert!(stderr(&o).contains("moved 3 messages"), "{}", stderr(&o));
    let ping = ok(&run(home.path(), &["ping", &base], None));
    assert!(
        ping.contains("0-9-1") && ping.contains("mock 1.0"),
        "{ping}"
    );
    let modules = ok(&run(home.path(), &["modules"], None));
    assert!(
        modules.contains("ramqp:5672") && modules.contains("ramqps:5671 (tls)"),
        "{modules}"
    );
}

#[test]
fn openwire_ping_with_java_style_settings() {
    use pub_openwire::mock::{MockBroker as AmqBroker, MockConfig};
    let home = tempfile::tempdir().unwrap();
    let broker = AmqBroker::start(MockConfig {
        credentials: Some(("app".into(), "s3cret".into())),
        ..Default::default()
    });
    let url = broker.url("");

    // Java connection-factory names: credentials via aliases, an inert option noted under -v.
    let o = run(
        home.path(),
        &[
            "ping",
            &url,
            "-O",
            "jms.userName=app",
            "-O",
            "jms.password=s3cret",
            "-O",
            "jms.copyMessageOnSend=false",
            "-v",
        ],
        None,
    );
    let out = ok(&o);
    assert!(
        out.contains("version") && out.contains("12") && out.contains("tight"),
        "{out}"
    );
    assert!(out.contains("broker-name") && out.contains("mock"), "{out}");
    let err = stderr(&o);
    assert!(
        err.contains("Ignoring jms.copyMessageOnSend: pub sends each message once"),
        "{err}"
    );
    assert!(
        err.contains("OpenWire 12 negotiated: tight encoding"),
        "{err}"
    );
    assert!(!err.contains("s3cret"), "{err}");

    // A pasted ActiveMQ URI query with Java units (ms) and a transport option alias.
    ok(&run(home.path(), &["ping", &format!("{url}?jms.userName=app&jms.password=s3cret&connectionTimeout=3000&jms.closeTimeout=2000")], None));

    // Wrong password: the broker's SecurityException becomes exit 67.
    let o = run(home.path(), &["ping", &url, "-u", "app:nope"], None);
    assert_eq!(o.status.code(), Some(67), "{}", stderr(&o));
    assert!(
        stderr(&o).contains("or password is invalid"),
        "{}",
        stderr(&o)
    );

    // Settings are validated: Java timeouts are milliseconds, unknown keys get suggestions.
    let o = run(
        home.path(),
        &["ping", &url, "-O", "jms.closeTimeout=5s"],
        None,
    );
    assert_eq!(o.status.code(), Some(3), "{}", stderr(&o));
    let o = run(
        home.path(),
        &["ping", &url, "-O", "jms.closeTimout=5000"],
        None,
    );
    assert!(
        stderr(&o).contains("did you mean 'jms.closeTimeout'"),
        "{}",
        stderr(&o)
    );

    let modules = ok(&run(home.path(), &["modules"], None));
    assert!(
        modules.contains("openwire")
            && modules.contains("amq:61616")
            && modules.contains("amqs:61617 (tls)"),
        "{modules}"
    );
}

#[test]
fn mqtt_durable_is_a_persistent_session() {
    use pub_mqtt::codec::prop::SESSION_EXPIRY_INTERVAL;
    use pub_mqtt::mock::{MockBroker as MqttBroker, MockConfig};
    let home = tempfile::tempdir().unwrap();
    let broker = MqttBroker::start(MockConfig::default());
    let t = broker.url("/orders");
    let connect = |i: usize| match &broker.packets("CONNECT")[i] {
        pub_mqtt::Packet::Connect(c) => (
            c.client_id.clone(),
            c.clean_start,
            c.props.num(SESSION_EXPIRY_INTERVAL),
        ),
        other => panic!("{other:?}"),
    };
    let o = run(
        home.path(),
        &["recv", &t, "--durable", "w1", "-m", "200ms"],
        None,
    );
    assert_eq!(o.status.code(), Some(28), "{}", stderr(&o));
    assert_eq!(connect(0), ("w1".into(), false, Some(u32::MAX)));
    assert!(
        broker.packets("UNSUBSCRIBE").is_empty(),
        "the session keeps its subscription"
    );
    let o = run(
        home.path(),
        &[
            "recv",
            &t,
            "--durable",
            "w1",
            "-m",
            "200ms",
            "-O",
            "session-expiry=1h",
        ],
        None,
    );
    assert_eq!(o.status.code(), Some(28), "{}", stderr(&o));
    assert_eq!(connect(1).2, Some(3600));
    // 3.1.1: no expiry, the session lasts until a clean connect.
    let o = run(
        home.path(),
        &[
            "recv",
            &t,
            "--durable",
            "w1",
            "-m",
            "200ms",
            "-O",
            "version=3.1.1",
        ],
        None,
    );
    assert_eq!(o.status.code(), Some(28), "{}", stderr(&o));
    assert_eq!(connect(2), ("w1".into(), false, None));
    // Unsubscribing is a clean connect.
    ok(&run(
        home.path(),
        &["unsubscribe", &t, "--durable", "w1"],
        None,
    ));
    assert_eq!(connect(3), ("w1".into(), true, None));
    // Contradictions are usage errors.
    for extra in ["client-id=other", "clean-start=true", "session-expiry=0"] {
        let o = run(
            home.path(),
            &["recv", &t, "--durable", "w1", "-O", extra],
            None,
        );
        assert_eq!(o.status.code(), Some(2), "{extra}: {}", stderr(&o));
    }
}
