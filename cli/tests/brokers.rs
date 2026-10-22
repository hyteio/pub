//! Real-broker tests, ignored by default: `interop/run.sh` starts ActiveMQ
//! ActiveMQ and Artemis (plain, TLS and mutual-TLS OpenWire, ActiveMQ's STOMP),
//! compiles the Java helpers in `interop/java`, sets the `PUB_IT_*` variables
//! and runs these with `--ignored`.
//!
//! Every test uses its own destinations, so they can run in parallel and
//! against brokers that already hold messages.

use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::Value as Json;

fn env(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("{name} is not set: run these tests with interop/run.sh"))
}

struct Broker {
    name: &'static str,
    url: String,
    tls: String,
    java: String,
    java_creds: &'static [&'static str],
}

fn brokers() -> Vec<Broker> {
    vec![
        Broker {
            name: "activemq",
            url: env("PUB_IT_ACTIVEMQ"),
            tls: env("PUB_IT_ACTIVEMQ_TLS"),
            java: env("PUB_IT_ACTIVEMQ_JAVA"),
            java_creds: &[],
        },
        Broker {
            name: "artemis",
            url: env("PUB_IT_ARTEMIS"),
            tls: env("PUB_IT_ARTEMIS_TLS"),
            java: env("PUB_IT_ARTEMIS_JAVA"),
            java_creds: &["artemis", "artemis"],
        },
    ]
}

/// A destination name no other test (or earlier run) uses.
fn unique(name: &str) -> String {
    static N: AtomicU32 = AtomicU32::new(0);
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!(
        "pub.it.{name}.{}.{t}.{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    )
}

/// Run pub with a fresh, empty home.
fn pub_cmd(args: &[&str], stdin: Option<&[u8]>) -> Output {
    let home = tempfile::tempdir().unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_pub"))
        .args(args)
        .env("PUB_HOME", home.path())
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

fn stderr(o: &Output) -> String {
    String::from_utf8_lossy(&o.stderr).into_owned()
}

fn ok(args: &[&str]) -> String {
    let o = pub_cmd(args, None);
    assert!(
        o.status.success(),
        "pub {args:?}: {:?}\n{}",
        o.status,
        stderr(&o)
    );
    String::from_utf8_lossy(&o.stdout).into_owned()
}

fn code(args: &[&str]) -> Option<i32> {
    pub_cmd(args, None).status.code()
}

/// Run an interop helper (`JP`/`JC`) with ActiveMQ's own Java client.
fn java(class: &str, args: &[&str]) -> String {
    let o = Command::new("java")
        .arg("-cp")
        .arg(env("PUB_IT_JAVA_CP"))
        .arg(class)
        .args(args)
        .output()
        .expect("java");
    assert!(o.status.success(), "{class} {args:?}: {}", stderr(&o));
    String::from_utf8_lossy(&o.stdout)
        .lines()
        .filter(|l| !l.starts_with("SLF4J"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn java_args<'a>(b: &'a Broker, url: &'a str, queue: &'a str) -> Vec<&'a str> {
    let mut a = vec![url, queue];
    a.extend_from_slice(b.java_creds);
    a
}

fn json_lines(out: &str) -> Vec<Json> {
    out.lines()
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("{e}: {l}")))
        .collect()
}

fn ca() -> String {
    format!("ca-file={}", env("PUB_IT_CA"))
}

// ---- TLS

#[test]
#[ignore]
fn tls_server_auth_versions_and_trust() {
    let ca = ca();
    for b in brokers() {
        let out = ok(&["ping", &b.tls, "-O", &ca]);
        assert!(out.contains("tight"), "{}: {out}", b.name);
        // TLS 1.2 when asked (the brokers offer both).
        ok(&[
            "ping",
            &b.tls,
            "-O",
            &ca,
            "-O",
            "tls-min-version=1.2",
            "-O",
            "tls-max-version=1.2",
        ]);
        // Our CA is not a public one: untrusted without it.
        assert_eq!(code(&["ping", &b.tls]), Some(35), "{}", b.name);
        // A send/receive round trip over TLS.
        let q = format!("{}/queue/{}", b.tls, unique("tls"));
        ok(&["send", &q, "-O", &ca, "-d", "over tls"]);
        assert_eq!(ok(&["recv", &q, "-O", &ca, "-m", "5s"]), "over tls");
    }
}

#[test]
#[ignore]
fn tls_mutual_auth_on_activemq() {
    let (ca, url) = (ca(), env("PUB_IT_ACTIVEMQ_MTLS"));
    let cert = format!("cert={}", env("PUB_IT_CLIENT_CERT"));
    let key = format!("key={}", env("PUB_IT_CLIENT_KEY"));
    // The listener requires a client certificate.
    let o = pub_cmd(&["ping", &url, "-O", &ca], None);
    assert_eq!(o.status.code(), Some(35), "{}", stderr(&o));
    let out = ok(&["ping", &url, "-O", &ca, "-O", &cert, "-O", &key]);
    assert!(out.contains("tight"), "{out}");
}

// ---- interop with the Java client

#[test]
#[ignore]
fn java_producer_to_pub() {
    for b in brokers() {
        for compressed in [false, true] {
            let q = unique("j2p");
            let url = if compressed {
                format!("{}?jms.useCompression=true", b.java)
            } else {
                b.java.clone()
            };
            java("JP", &java_args(&b, &url, &q));
            let out = ok(&[
                "recv",
                &format!("{}/queue/{q}", b.url),
                "-n",
                "3",
                "-F",
                "json",
                "-m",
                "10s",
            ]);
            let m = json_lines(&out);
            let what = format!("{} compressed={compressed}", b.name);
            assert_eq!(m.len(), 3, "{what}: {out}");
            let (f, p) = (&m[0]["fields"], &m[0]["properties"]);
            assert_eq!(m[0]["body"]["data"], "from java ünïcödé 😀", "{what}");
            assert_eq!(m[0]["body"]["type"], "text", "{what}");
            assert_eq!(f["correlation-id"], "jc-1", "{what}");
            assert_eq!(f["type"], "jtype", "{what}");
            assert_eq!(f["reply-to"], "topic://jreplies", "{what}");
            assert_eq!(f["priority"], 8, "{what}");
            assert_eq!(
                (&f["group-id"], &f["group-seq"]),
                (&Json::from("grp"), &Json::from(3)),
                "{what}"
            );
            assert!(
                f["expiration"].as_i64().unwrap() > f["timestamp"].as_i64().unwrap(),
                "{what}"
            );
            assert_eq!(p["i"], serde_json::json!({"int": 42}), "{what}");
            assert_eq!(p["l"], serde_json::json!({"long": 1u64 << 40}), "{what}");
            assert_eq!(p["s"], serde_json::json!({"short": 7}), "{what}");
            assert_eq!(p["y"], serde_json::json!({"byte": -1}), "{what}");
            assert_eq!(p["f"], serde_json::json!({"float": 1.25}), "{what}");
            assert_eq!(
                (&p["b"], &p["d"], &p["str"]),
                (&Json::from(true), &Json::from(2.5), &Json::from("x")),
                "{what}"
            );
            assert_eq!(
                m[1]["body"],
                serde_json::json!({"type": "bytes", "base64": "AAEC/w=="}),
                "{what}"
            );
            assert_eq!(m[2]["protocol"]["message-class"], "MapMessage", "{what}");
        }
    }
}

#[test]
#[ignore]
fn pub_to_java_consumer() {
    for b in brokers() {
        let q = unique("p2j");
        let url = format!("{}/queue/{q}", b.url);
        ok(&[
            "send",
            &url,
            "-d",
            "from pub ünï 😀",
            "-O",
            "message-type=text",
            "-H",
            "priority: 9",
            "-H",
            "JMSCorrelationID: pc",
            "-H",
            "reply-to: /queue/back",
            "-H",
            "ttl: 5m",
            "-P",
            "n:long=7",
            "-P",
            "f:float=0.5",
            "-P",
            "s=str",
        ]);
        ok(&[
            "send",
            &url,
            "-d",
            "zipped",
            "-O",
            "message-type=text",
            "-O",
            "jms.useCompression=true",
        ]);
        ok(&["send", &url, "-d", "raw", "-H", "persistent: false"]);
        let out = java("JC", &java_args(&b, &b.java, &q));
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 3, "{}: {out}", b.name);
        assert!(
            lines[0].starts_with("text:from pub ünï 😀 | prio=9 corr=pc"),
            "{}: {out}",
            b.name
        );
        assert!(
            lines[0].contains("replyTo=queue://back") && lines[0].contains("exp=true"),
            "{out}"
        );
        assert!(
            lines[0].contains("n=7(Long)")
                && lines[0].contains("f=0.5(Float)")
                && lines[0].contains("s=str(String)"),
            "{out}"
        );
        assert!(lines[1].starts_with("text:zipped"), "{out}");
        assert!(
            lines[2].starts_with("bytes:3") && lines[2].contains("persistent=false"),
            "{out}"
        );
    }
}

// ---- consumer behaviour

#[test]
#[ignore]
fn prefetch_leftovers_are_not_redelivered() {
    for b in brokers() {
        let url = format!("{}/queue/{}", b.url, unique("prefetch"));
        for i in 1..=5 {
            ok(&[
                "send",
                &url,
                "-d",
                &format!("m{i}"),
                "-O",
                "message-type=text",
            ]);
        }
        let o = pub_cmd(
            &[
                "recv",
                &format!("{url}?jms.prefetchPolicy.queuePrefetch=3"),
                "-v",
            ],
            None,
        );
        assert!(stderr(&o).contains("(prefetch 3)"), "{}", stderr(&o));
        assert_eq!(String::from_utf8_lossy(&o.stdout), "m1", "{}", b.name);
        let next = &json_lines(&ok(&["recv", &url, "-F", "json", "-m", "5s"]))[0];
        assert_eq!(next["body"]["data"], "m2", "{}", b.name);
        assert_eq!(next["fields"]["redelivered"], false, "{}", b.name);
        // The default prefetch follows -n.
        let o = pub_cmd(&["recv", &url, "-n", "2", "-v"], None);
        assert!(stderr(&o).contains("(prefetch 2)"), "{}", stderr(&o));
    }
}

#[test]
#[ignore]
fn selectors_and_zero_prefetch_pulls() {
    for b in brokers() {
        let url = format!("{}/queue/{}", b.url, unique("select"));
        for (body, region) in [("eu1", "EU"), ("us", "US"), ("eu2", "EU")] {
            ok(&["send", &url, "-d", body, "-P", &format!("region={region}")]);
        }
        assert_eq!(
            ok(&["recv", &url, "-s", "region = 'US'", "-m", "5s"]),
            "us",
            "{}",
            b.name
        );
        assert_eq!(
            code(&["recv", &url, "-s", "region = ((", "-m", "2s"]),
            Some(2),
            "{}",
            b.name
        );
        let out = ok(&[
            "recv",
            &format!("{url}?jms.prefetchPolicy.all=0"),
            "-n",
            "0",
            "-m",
            "2s",
            "--lines",
        ]);
        assert_eq!(out, "eu1\neu2\n", "{}", b.name);
        assert_eq!(code(&["recv", &url, "-m", "500ms"]), Some(28), "{}", b.name);
    }
}

#[test]
#[ignore]
fn topics_deliver_to_current_subscribers() {
    for b in brokers() {
        let url = format!("{}/topic/{}", b.url, unique("topic"));
        let listener = {
            let url = url.clone();
            thread::spawn(move || ok(&["recv", &url, "-n", "2", "-m", "15s", "--lines"]))
        };
        thread::sleep(Duration::from_secs(2));
        ok(&["send", &url, "-d", "t1"]);
        ok(&["send", &url, "-d", "t2"]);
        assert_eq!(listener.join().unwrap(), "t1\nt2\n", "{}", b.name);
    }
}

#[test]
#[ignore]
fn scheduled_delivery() {
    for b in brokers() {
        let url = format!("{}/queue/{}", b.url, unique("delay"));
        ok(&["send", &url, "-d", "later", "-H", "delivery-delay: 3s"]);
        assert_eq!(
            code(&["recv", &url, "-m", "1s"]),
            Some(28),
            "{}: delivered early",
            b.name
        );
        assert_eq!(ok(&["recv", &url, "-m", "8s"]), "later", "{}", b.name);
    }
}

// ---- replay

#[test]
#[ignore]
fn replay_header_strategies() {
    for b in brokers() {
        let src = format!("{}/queue/{}", b.url, unique("src"));
        ok(&[
            "send",
            &src,
            "-d",
            "one",
            "-O",
            "message-type=text",
            "-H",
            "ttl: 1h",
            "-P",
            "n:int=1",
        ]);
        let capture = ok(&["recv", &src, "-F", "json", "-m", "5s"]);
        let original = &json_lines(&capture)[0]["fields"];

        let fresh = format!("{}/queue/{}", b.url, unique("fresh"));
        ok(&["send", &fresh, "-F", "json", "-d", &capture]);
        let got = &json_lines(&ok(&["recv", &fresh, "-F", "json", "-m", "5s"]))[0]["fields"];
        assert_ne!(got["message-id"], original["message-id"], "{}", b.name);
        let ttl = |f: &Json| f["expiration"].as_i64().unwrap() - f["timestamp"].as_i64().unwrap();
        assert_eq!(ttl(got), 3_600_000, "{}", b.name);

        let exact = format!("{}/queue/{}", b.url, unique("exact"));
        ok(&[
            "send",
            &exact,
            "-F",
            "json",
            "-d",
            &capture,
            "--header-strategy",
            "admin",
        ]);
        ok(&[
            "send",
            &exact,
            "-F",
            "json",
            "-d",
            &capture,
            "--header-strategy",
            "admin",
        ]);
        let copies = json_lines(&ok(&["recv", &exact, "-F", "json", "-n", "0", "-m", "3s"]));
        for c in &copies {
            for f in ["message-id", "timestamp", "expiration"] {
                assert_eq!(c["fields"][f], original[f], "{}: {f}", b.name);
            }
        }
        // ActiveMQ's duplicate detection drops a repeated id; Artemis keeps it.
        let expected = if b.name == "activemq" { 1 } else { 2 };
        assert_eq!(copies.len(), expected, "{}", b.name);
    }
}

// ---- across protocols

#[test]
#[ignore]
fn stomp_and_openwire_on_activemq() {
    let (amq, stomp) = (env("PUB_IT_ACTIVEMQ"), env("PUB_IT_ACTIVEMQ_STOMP"));
    let q = unique("cross");
    ok(&[
        "send",
        &format!("{stomp}/queue/{q}"),
        "-d",
        "via stomp",
        "-H",
        "JMSCorrelationID: sc",
        "-H",
        "priority: 6",
        "-P",
        "region=EU",
    ]);
    let m = &json_lines(&ok(&[
        "recv",
        &format!("{amq}/queue/{q}"),
        "-F",
        "json",
        "-m",
        "5s",
    ]))[0];
    assert_eq!(m["body"]["base64"], "dmlhIHN0b21w"); // bytes: pub's STOMP default
    assert_eq!(
        (&m["fields"]["correlation-id"], &m["fields"]["priority"]),
        (&Json::from("sc"), &Json::from(6))
    );
    assert_eq!(m["properties"]["region"], "EU");

    ok(&[
        "send",
        &format!("{amq}/queue/{q}"),
        "-d",
        "via openwire",
        "-O",
        "message-type=text",
        "-H",
        "JMSCorrelationID: oc",
        "-P",
        "n:int=4",
    ]);
    let m = &json_lines(&ok(&[
        "recv",
        &format!("{stomp}/queue/{q}"),
        "-F",
        "json",
        "-m",
        "5s",
    ]))[0];
    assert_eq!(m["body"]["data"], "via openwire");
    assert_eq!(m["fields"]["correlation-id"], "oc");
    assert_eq!(m["properties"]["n"], "4"); // STOMP carries strings
}

// ---- browse and durable subscriptions

#[test]
#[ignore]
fn browse_leaves_messages_on_the_queue() {
    for b in brokers() {
        let url = format!("{}/queue/{}", b.url, unique("browse"));
        for (body, region) in [("a", "EU"), ("b", "US"), ("c", "EU")] {
            ok(&["send", &url, "-d", body, "-P", &format!("region={region}")]);
        }
        assert_eq!(ok(&["browse", &url, "--lines"]), "a\nb\nc\n", "{}", b.name);
        assert_eq!(
            ok(&["browse", &url, "-n", "2", "--lines"]),
            "a\nb\n",
            "{}",
            b.name
        );
        assert_eq!(
            ok(&["browse", &url, "-s", "region = 'EU'", "--lines"]),
            "a\nc\n",
            "{}",
            b.name
        );
        let first = &json_lines(&ok(&["browse", &url, "-n", "1", "-F", "json"]))[0];
        assert_eq!(first["fields"]["redelivered"], false, "{}", b.name);
        // Still all there, and not marked redelivered by the browsing.
        let all = json_lines(&ok(&["recv", &url, "-n", "3", "-F", "json", "-m", "5s"]));
        assert_eq!(all.len(), 3, "{}", b.name);
        assert!(
            all.iter().all(|m| m["fields"]["redelivered"] == false),
            "{}: {all:?}",
            b.name
        );
        // Empty now: an empty browse, still success.
        assert_eq!(ok(&["browse", &url]), "", "{}", b.name);
    }
}

#[test]
#[ignore]
fn durable_subscriptions_keep_messages_while_offline() {
    for b in brokers() {
        let topic = format!("{}/topic/{}", b.url, unique("durable"));
        let client = format!("client-id={}", unique("client"));
        let sub = ["--durable", "audit", "-O", &client];
        // Subscribe (nothing yet), go away, miss two messages, come back.
        let mut args = vec!["recv", &topic, "-m", "1s"];
        args.extend_from_slice(&sub);
        assert_eq!(code(&args), Some(28), "{}", b.name);
        ok(&["send", &topic, "-d", "m1"]);
        ok(&["send", &topic, "-d", "m2"]);
        let mut args = vec!["recv", &topic, "-n", "2", "-m", "5s", "--lines"];
        args.extend_from_slice(&sub);
        assert_eq!(ok(&args), "m1\nm2\n", "{}", b.name);
        // Unsubscribed: gone, and removing it again is an error.
        ok(&["unsubscribe", &topic, "--durable", "audit", "-O", &client]);
        let o = pub_cmd(
            &["unsubscribe", &topic, "--durable", "audit", "-O", &client],
            None,
        );
        // Both brokers: InvalidDestinationException, a usage error.
        assert_eq!(o.status.code(), Some(2), "{}: {}", b.name, stderr(&o));
    }
}

// ---- transactions and move

#[test]
#[ignore]
fn transactions_commit_and_roll_back() {
    for b in brokers() {
        let url = format!("{}/queue/{}", b.url, unique("tx"));
        ok(&[
            "send",
            &url,
            "-d",
            "t",
            "-n",
            "4",
            "--transaction",
            "--batch",
            "3",
        ]);
        // A transacted receive whose output fails (stdout closed): rolled back,
        // so the messages are still there, now marked redelivered.
        let home = tempfile::tempdir().unwrap();
        let mut child = Command::new(env!("CARGO_BIN_EXE_pub"))
            .args(["recv", &url, "-n", "4", "--transaction", "-m", "5s"])
            .env("PUB_HOME", home.path())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        drop(child.stdout.take());
        let o = child.wait_with_output().unwrap();
        assert_eq!(o.status.code(), Some(26), "{}: {}", b.name, stderr(&o));
        let all = json_lines(&ok(&[
            "recv",
            &url,
            "-n",
            "4",
            "-F",
            "json",
            "--transaction",
            "-m",
            "5s",
        ]));
        assert_eq!(all.len(), 4, "{}", b.name);
        assert_eq!(all[0]["fields"]["redelivered"], true, "{}", b.name);
        assert_eq!(
            code(&["recv", &url, "-m", "500ms"]),
            Some(28),
            "{}: committed receive left messages",
            b.name
        );
    }
}

#[test]
#[ignore]
fn move_redrives_exact_copies_with_a_selector() {
    for b in brokers() {
        let (dlq, target) = (unique("DLQ"), unique("redriven"));
        java("JP", &java_args(&b, &b.java, &dlq)); // text (typed properties), bytes, map
        ok(&[
            "send",
            &format!("{}/queue/{dlq}", b.url),
            "-d",
            "keep me",
            "-P",
            "region=US",
        ]);
        let before = json_lines(&ok(&[
            "browse",
            &format!("{}/queue/{dlq}", b.url),
            "-F",
            "json",
        ]));
        assert_eq!(before.len(), 4, "{}", b.name);

        let o = pub_cmd(
            &[
                "move",
                &format!("{}/queue/{dlq}", b.url),
                "--to",
                &format!("/queue/{target}"),
                "-s",
                "region IS NULL",
                "--batch",
                "2",
                "-m",
                "2s",
            ],
            None,
        );
        assert!(o.status.success(), "{}: {}", b.name, stderr(&o));
        assert!(
            stderr(&o).contains("moved 3 messages"),
            "{}: {}",
            b.name,
            stderr(&o)
        );
        // The one that didn't match stays.
        assert_eq!(
            ok(&["recv", &format!("{}/queue/{dlq}", b.url), "-m", "5s"]),
            "keep me",
            "{}",
            b.name
        );

        // Exact copies: same ids, and the Java client sees the same types.
        let moved = json_lines(&ok(&[
            "browse",
            &format!("{}/queue/{target}", b.url),
            "-F",
            "json",
        ]));
        for (m, orig) in moved.iter().zip(&before) {
            assert_eq!(
                m["fields"]["message-id"], orig["fields"]["message-id"],
                "{}",
                b.name
            );
            assert_eq!(
                m["fields"]["timestamp"], orig["fields"]["timestamp"],
                "{}",
                b.name
            );
        }
        let out = java("JC", &java_args(&b, &b.java, &target));
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 3, "{}: {out}", b.name);
        assert!(
            lines[0].starts_with("text:from java") && lines[0].contains("i=42(Integer)"),
            "{}: {out}",
            b.name
        );
        assert!(lines[1].starts_with("bytes:4"), "{}: {out}", b.name);
        assert!(
            lines[2].starts_with("ActiveMQMapMessage"),
            "{}: {out}",
            b.name
        );
    }
}

// ---- destination management

#[test]
#[ignore]
fn destination_lifecycle() {
    for b in brokers() {
        let (q, t) = (unique("made"), unique("feed"));
        ok(&["dest", "create", &format!("{}/queue/{q}", b.url)]);
        ok(&["send", &format!("{}/topic/{t}", b.url), "-d", "x"]);
        let all = ok(&["dest", "list", &b.url]);
        assert!(
            all.lines().any(|l| l == format!("queue://{q}")),
            "{}: {all}",
            b.name
        );
        assert!(
            all.lines().any(|l| l == format!("topic://{t}")),
            "{}: {all}",
            b.name
        );
        assert!(
            !all.contains("ActiveMQ.Advisory") && !all.contains("$sys") && !all.contains("ID:pub-"),
            "{}: {all}",
            b.name
        );
        let queues = ok(&["dest", "list", &b.url, "--queues"]);
        assert!(
            queues.lines().all(|l| l.starts_with("queue://")),
            "{}: {queues}",
            b.name
        );
        let topics = ok(&["dest", "list", &b.url, "--topics"]);
        assert!(
            topics.lines().all(|l| l.starts_with("topic://")),
            "{}: {topics}",
            b.name
        );

        // In use: refused while a consumer is attached.
        let busy = format!("{}/queue/{q}", b.url);
        let listener = {
            let busy = busy.clone();
            thread::spawn(move || pub_cmd(&["recv", &busy, "-m", "4s"], None))
        };
        thread::sleep(Duration::from_millis(1500));
        let o = pub_cmd(&["dest", "delete", &busy], None);
        // ActiveMQ: JMSException "still has an active subscription"; Artemis: "it has consumers".
        assert_eq!(o.status.code(), Some(8), "{}: {}", b.name, stderr(&o));
        assert!(
            stderr(&o).contains("refused delete"),
            "{}: {}",
            b.name,
            stderr(&o)
        );
        listener.join().unwrap();

        ok(&["dest", "delete", &busy]);
        let after = ok(&["dest", "list", &b.url, "--queues"]);
        assert!(
            !after.lines().any(|l| l == format!("queue://{q}")),
            "{}: still listed: {after}",
            b.name
        );
    }
}

// ---- XML

#[test]
#[ignore]
fn xml_capture_replays_to_the_java_client() {
    for b in brokers() {
        let (src, dst) = (unique("xsrc"), unique("xdst"));
        java("JP", &java_args(&b, &b.java, &src)); // text with typed properties, bytes, map
        let xml = ok(&[
            "recv",
            &format!("{}/queue/{src}", b.url),
            "-n",
            "3",
            "-F",
            "xml",
            "-m",
            "5s",
        ]);
        assert!(
            xml.contains(r#"<property name="i" type="int">42</property>"#),
            "{}: {xml}",
            b.name
        );
        let o = pub_cmd(
            &["send", &format!("{}/queue/{dst}", b.url), "-F", "xml"],
            Some(xml.as_bytes()),
        );
        assert!(o.status.success(), "{}: {}", b.name, stderr(&o));
        let out = java("JC", &java_args(&b, &b.java, &dst));
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 3, "{}: {out}", b.name);
        assert!(
            lines[0].starts_with("text:from java ünïcödé 😀 | prio=8 corr=jc-1 type=jtype"),
            "{}: {out}",
            b.name
        );
        assert!(
            lines[0].contains("i=42(Integer)") && lines[0].contains("l=1099511627776(Long)"),
            "{}: {out}",
            b.name
        );
        assert!(lines[1].starts_with("bytes:4"), "{}: {out}", b.name);
        // A MapMessage's body travels as bytes in pub's model; it arrives as a BytesMessage.
        assert!(lines[2].starts_with("bytes:"), "{}: {out}", b.name);
    }
}

// ---- STOMP

/// STOMP endpoints: (name, URL, OpenWire URL of the same broker for Java, Java creds).
fn stomp_brokers() -> Vec<(&'static str, String, String, &'static [&'static str])> {
    vec![
        (
            "activemq",
            env("PUB_IT_ACTIVEMQ_STOMP"),
            env("PUB_IT_ACTIVEMQ_JAVA"),
            &[],
        ),
        (
            "artemis",
            env("PUB_IT_ARTEMIS_STOMP"),
            env("PUB_IT_ARTEMIS_JAVA"),
            &["artemis", "artemis"],
        ),
    ]
}

/// What `pub -v` logs once a subscription is in place, per protocol.
const SUBSCRIBED: &[&str] = &[
    "* subscribed to ",  // STOMP (after the RECEIPT), MQTT (after SUBACK)
    "* receiving from ", // AMQP 1.0 (after the broker's attach)
    "* consuming ",      // OpenWire (after the ConsumerInfo response), AMQP 0-9-1
];

/// Start `pub recv` in the background: a subscriber that must exist before
/// anything is sent. Returns once its `-v` trace shows the subscription is in
/// place (or after 15s, leaving the test to fail on what it gets).
fn listen(args: Vec<String>) -> thread::JoinHandle<Output> {
    let home = tempfile::tempdir().unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_pub"))
        .args(&args)
        .arg("-v")
        .env("PUB_HOME", home.path())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let (ready, subscribed) = std::sync::mpsc::channel();
    let err = child.stderr.take().unwrap();
    let stderr = thread::spawn(move || {
        let mut all = Vec::new();
        for line in BufReader::new(err).split(b'\n') {
            let line = line.unwrap_or_default();
            if SUBSCRIBED
                .iter()
                .any(|m| line.windows(m.len()).any(|w| w == m.as_bytes()))
            {
                let _ = ready.send(());
            }
            all.extend_from_slice(&line);
            all.push(b'\n');
        }
        all
    });
    let h = thread::spawn(move || {
        let _home = home;
        let mut stdout = Vec::new();
        child
            .stdout
            .take()
            .unwrap()
            .read_to_end(&mut stdout)
            .unwrap();
        let status = child.wait().unwrap();
        Output {
            status,
            stdout,
            stderr: stderr.join().unwrap(),
        }
    });
    let _ = subscribed.recv_timeout(Duration::from_secs(15));
    h
}

#[test]
#[ignore]
fn stomp_round_trip() {
    for (name, url, ..) in stomp_brokers() {
        let q = format!("{url}/queue/{}", unique("stomp"));
        ok(&[
            "send",
            &q,
            "-d",
            "héllo",
            "-O",
            "message-type=text",
            "-H",
            "JMSCorrelationID: c-1",
            "-H",
            "priority: 7",
            "-P",
            "region=EU",
        ]);
        let o = pub_cmd(&["send", &q], Some(&[0u8, 1, 0, 255][..]));
        assert!(o.status.success(), "{name}: {}", stderr(&o));
        let m = json_lines(&ok(&["recv", &q, "-n", "2", "-F", "json", "-m", "5s"]));
        assert_eq!(m.len(), 2, "{name}");
        assert_eq!(m[0]["body"]["data"], "héllo", "{name}");
        assert_eq!(m[0]["fields"]["correlation-id"], "c-1", "{name}");
        assert_eq!(m[0]["fields"]["priority"], 7, "{name}");
        assert_eq!(m[0]["properties"]["region"], "EU", "{name}");
        assert_eq!(
            m[1]["body"]["base64"], "AAEA/w==",
            "{name}: NUL bytes survive (content-length)"
        );
        // Selectors.
        ok(&["send", &q, "-d", "eu", "-P", "region=EU"]);
        ok(&["send", &q, "-d", "us", "-P", "region=US"]);
        assert_eq!(
            ok(&["recv", &q, "-s", "region = 'US'", "-m", "5s"]),
            "us",
            "{name}"
        );
        assert_eq!(ok(&["recv", &q, "-m", "5s"]), "eu", "{name}");
        // Topics.
        let t = format!("{url}/topic/{}", unique("stomp-topic"));
        let l = listen(vec!["recv".into(), t.clone(), "-m".into(), "10s".into()]);
        ok(&["send", &t, "-d", "news"]);
        let o = l.join().unwrap();
        assert_eq!(
            String::from_utf8_lossy(&o.stdout),
            "news",
            "{name}: {}",
            stderr(&o)
        );
    }
}

#[test]
#[ignore]
fn stomp_heart_beats_browse_and_durable() {
    for (name, url, ..) in stomp_brokers() {
        // Heart-beats both ways every 500ms: a 3s wait isn't cut short by a
        // missed beat, and ends as a plain timeout.
        let q = format!("{url}/queue/{}", unique("stomp-hb"));
        let o = pub_cmd(
            &["recv", &q, "-m", "3s", "-O", "heart-beat=500ms", "-v"],
            None,
        );
        assert_eq!(o.status.code(), Some(28), "{name}: {}", stderr(&o));
        assert!(stderr(&o).contains("heart-beat"), "{name}: {}", stderr(&o));

        // Durable subscriptions keep messages while the subscriber is away.
        let topic = format!("{url}/topic/{}", unique("stomp-durable"));
        let client = format!("client-id={}", unique("client"));
        let sub = ["--durable", "audit", "-O", &client];
        let mut args = vec!["recv", &topic, "-m", "1s"];
        args.extend_from_slice(&sub);
        assert_eq!(code(&args), Some(28), "{name}");
        ok(&["send", &topic, "-d", "m1"]);
        ok(&["send", &topic, "-d", "m2"]);
        let mut args = vec!["recv", &topic, "-n", "2", "-m", "5s", "--lines"];
        args.extend_from_slice(&sub);
        assert_eq!(ok(&args), "m1\nm2\n", "{name}");
        ok(&["unsubscribe", &topic, "--durable", "audit", "-O", &client]);
        ok(&["send", &topic, "-d", "m3"]);
        let mut args = vec!["recv", &topic, "-m", "1s"];
        args.extend_from_slice(&sub);
        assert_eq!(
            code(&args),
            Some(28),
            "{name}: m3 was sent while unsubscribed"
        );
        ok(&["unsubscribe", &topic, "--durable", "audit", "-O", &client]);

        // Browsing: ActiveMQ only, and the queue is untouched.
        let q = format!("{url}/queue/{}", unique("stomp-browse"));
        ok(&["send", &q, "-d", "b1"]);
        ok(&["send", &q, "-d", "b2"]);
        if name == "activemq" {
            assert_eq!(ok(&["browse", &q, "--lines"]), "b1\nb2\n");
            assert_eq!(ok(&["browse", &q, "-n", "1"]), "b1");
        } else {
            let o = pub_cmd(&["browse", &q], None);
            assert_eq!(o.status.code(), Some(1), "{name}: {}", stderr(&o));
        }
        assert_eq!(
            ok(&["recv", &q, "-n", "2", "-m", "5s", "--lines"]),
            "b1\nb2\n",
            "{name}"
        );
    }
}

#[test]
#[ignore]
fn stomp_transactions_and_move() {
    for (name, url, ..) in stomp_brokers() {
        let q = format!("{url}/queue/{}", unique("stomp-tx"));
        // Sending in transactions of two.
        ok(&[
            "send",
            &q,
            "-d",
            "t",
            "-n",
            "4",
            "--transaction",
            "--batch",
            "2",
        ]);
        assert_eq!(
            ok(&["recv", &q, "-n", "0", "-m", "2s", "--lines"]),
            "t\nt\nt\nt\n",
            "{name}"
        );

        // A transactional receive, one message per transaction.
        ok(&["send", &q, "-d", "a"]);
        ok(&["send", &q, "-d", "b"]);
        assert_eq!(
            ok(&[
                "recv",
                &q,
                "-n",
                "2",
                "-m",
                "5s",
                "--lines",
                "--transaction",
                "--batch",
                "1"
            ]),
            "a\nb\n",
            "{name}"
        );

        ok(&[
            "send",
            &q,
            "-d",
            "keep",
            "-H",
            "JMSCorrelationID: c-9",
            "-P",
            "region=EU",
        ]);
        ok(&["send", &q, "-d", "bin"]);

        // Move with a selector: headers and properties travel with the copy.
        let dlq = format!("{url}/queue/{}", unique("stomp-dlq"));
        let to = dlq
            .rsplit_once("/queue/")
            .map(|(_, n)| format!("/queue/{n}"))
            .unwrap();
        let o = pub_cmd(
            &["move", &q, "--to", &to, "-s", "region = 'EU'", "-m", "2s"],
            None,
        );
        assert!(o.status.success(), "{name}: {}", stderr(&o));
        assert!(
            stderr(&o).contains("moved 1 message"),
            "{name}: {}",
            stderr(&o)
        );
        let m = json_lines(&ok(&["recv", &dlq, "-n", "0", "-m", "2s", "-F", "json"]));
        assert_eq!(m.len(), 1, "{name}");
        assert_eq!(m[0]["body"]["data"], "keep", "{name}");
        assert_eq!(m[0]["fields"]["correlation-id"], "c-9", "{name}");
        assert_eq!(m[0]["properties"]["region"], "EU", "{name}");
        // The rest stays.
        assert_eq!(ok(&["recv", &q, "-n", "0", "-m", "2s"]), "bin", "{name}");
    }
}

#[test]
#[ignore]
fn stomp_to_the_java_client() {
    for (name, url, java_url, creds) in stomp_brokers() {
        let q = unique("stomp-java");
        let dest = format!("{url}/queue/{q}");
        ok(&[
            "send",
            &dest,
            "-d",
            "as text",
            "-O",
            "message-type=text",
            "-H",
            "priority: 6",
            "-P",
            "region=EU",
        ]);
        ok(&["send", &dest, "-d", "as bytes"]);
        let mut args = vec![java_url.as_str(), q.as_str()];
        args.extend_from_slice(creds);
        let out = java("JC", &args);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 2, "{name}: {out}");
        // No content-length: a TextMessage; content-length: a BytesMessage.
        assert!(
            lines[0].starts_with("text:as text | prio=6"),
            "{name}: {out}"
        );
        assert!(lines[0].contains("region=EU(String)"), "{name}: {out}");
        assert!(lines[1].starts_with("bytes:8"), "{name}: {out}");
    }
}

#[test]
#[ignore]
fn stomp_tls_and_auth() {
    let ca = ca();
    let tls = format!(
        "{}/queue/{}",
        env("PUB_IT_ACTIVEMQ_STOMP_TLS"),
        unique("stomp-tls")
    );
    ok(&["send", &tls, "-O", &ca, "-d", "over tls"]);
    assert_eq!(ok(&["recv", &tls, "-O", &ca, "-m", "5s"]), "over tls");
    assert_eq!(code(&["ping", &tls]), Some(35));
    let wrong = env("PUB_IT_ARTEMIS_STOMP").replace("artemis:artemis@", "artemis:nope@");
    let o = pub_cmd(&["ping", &wrong], None);
    assert_eq!(o.status.code(), Some(67), "{}", stderr(&o));
}

// ---- MQTT

/// MQTT endpoints and the versions each speaks (ActiveMQ: 3.1.1 only).
fn mqtt_brokers() -> Vec<(&'static str, String, &'static [&'static str])> {
    vec![
        ("mosquitto", env("PUB_IT_MOSQUITTO"), &["5", "3.1.1"]),
        ("artemis", env("PUB_IT_ARTEMIS_MQTT"), &["5", "3.1.1"]),
        ("activemq", env("PUB_IT_ACTIVEMQ_MQTT"), &["3.1.1"]),
    ]
}

#[test]
#[ignore]
fn mqtt_publish_subscribe() {
    for (name, url, versions) in mqtt_brokers() {
        for v in versions.iter() {
            let what = format!("{name} v{v}");
            let ver = format!("version={v}");
            let topic = format!("{url}/{}/sensors/t1", unique("mqtt").replace('.', "-"));
            let l = listen(vec![
                "recv".into(),
                topic.clone(),
                "-O".into(),
                ver.clone(),
                "-F".into(),
                "json".into(),
                "-m".into(),
                "10s".into(),
            ]);
            let mut send = vec!["send", &topic, "-O", &ver, "-d", "21.5"];
            if *v == "5" {
                send.extend_from_slice(&[
                    "-H",
                    "correlation-id: c-9",
                    "-P",
                    "unit=C",
                    "-t",
                    "text/plain",
                ]);
            }
            ok(&send);
            let o = l.join().unwrap();
            assert!(o.status.success(), "{what}: {}", stderr(&o));
            let m = &json_lines(&String::from_utf8_lossy(&o.stdout))[0];
            assert_eq!(m["body"]["data"], "21.5", "{what}: {m}");
            if *v == "5" {
                assert_eq!(m["fields"]["correlation-id"], "c-9", "{what}: {m}");
                assert_eq!(m["fields"]["content-type"], "text/plain", "{what}: {m}");
                assert_eq!(m["properties"]["unit"], "C", "{what}: {m}");
            }

            // A retained message reaches a later subscriber.
            ok(&[
                "send",
                &topic,
                "-O",
                &ver,
                "-O",
                "retain=true",
                "-d",
                "last",
            ]);
            assert_eq!(
                ok(&["recv", &topic, "-O", &ver, "-m", "5s"]),
                "last",
                "{what}"
            );
            ok(&["send", &topic, "-O", &ver, "-O", "retain=true", "-d", ""]); // clear it
        }
    }
}

#[test]
#[ignore]
fn mqtt_persistent_sessions_queue_while_offline() {
    for (name, url, versions) in mqtt_brokers() {
        for v in versions.iter() {
            let what = format!("{name} v{v}");
            let topic = format!("{url}/{}/orders", unique("mqtt-session").replace('.', "-"));
            let client = format!("client-id={}", unique("c").replace('.', "-"));
            let mut session = vec![
                "-O".to_string(),
                format!("version={v}"),
                "-O".into(),
                client.clone(),
                "-O".into(),
                "clean-start=false".into(),
            ];
            if *v == "5" {
                session.extend(["-O".to_string(), "session-expiry=1h".into()]);
            }
            let s: Vec<&str> = session.iter().map(String::as_str).collect();
            let mut first = vec!["recv", topic.as_str(), "-m", "1s"];
            first.extend_from_slice(&s);
            assert_eq!(code(&first), Some(28), "{what}: subscribe");
            let ver = format!("version={v}");
            ok(&["send", &topic, "-O", &ver, "-d", "m1"]);
            ok(&["send", &topic, "-O", &ver, "-d", "m2"]);
            let mut again = vec!["recv", topic.as_str(), "-n", "2", "-m", "5s", "--lines"];
            again.extend_from_slice(&s);
            assert_eq!(ok(&again), "m1\nm2\n", "{what}");
        }
    }
}

#[test]
#[ignore]
fn mqtt_durable_and_unsubscribe() {
    for (name, url, versions) in mqtt_brokers() {
        for v in versions.iter() {
            let what = format!("{name} v{v}");
            let topic = format!("{url}/{}/audit", unique("mqtt-durable").replace('.', "-"));
            let session = unique("d").replace('.', "-");
            let ver = format!("version={v}");
            let durable = ["--durable", session.as_str(), "-O", ver.as_str()];
            let recv = |extra: &[&str]| {
                let mut a = vec!["recv", topic.as_str()];
                a.extend_from_slice(extra);
                a.extend_from_slice(&durable);
                pub_cmd(&a, None)
            };
            assert_eq!(
                recv(&["-m", "1s"]).status.code(),
                Some(28),
                "{what}: subscribe"
            );
            ok(&["send", &topic, "-O", &ver, "-d", "m1"]);
            ok(&["send", &topic, "-O", &ver, "-d", "m2"]);
            let o = recv(&["-n", "2", "-m", "5s", "--lines"]);
            assert_eq!(
                String::from_utf8_lossy(&o.stdout),
                "m1\nm2\n",
                "{what}: {}",
                stderr(&o)
            );
            // Unsubscribed: what is sent next isn't kept for the name.
            ok(&["unsubscribe", &topic, "--durable", &session, "-O", &ver]);
            ok(&["send", &topic, "-O", &ver, "-d", "m3"]);
            assert_eq!(
                recv(&["-m", "1s"]).status.code(),
                Some(28),
                "{what}: m3 kept"
            );
            ok(&["unsubscribe", &topic, "--durable", &session, "-O", &ver]);
        }
    }
}

#[test]
#[ignore]
fn mqtt_wills_shared_subscriptions_and_topic_aliases() {
    for (name, url, versions) in mqtt_brokers() {
        for v in versions.iter() {
            let what = format!("{name} v{v}");
            let ver = format!("version={v}");
            let base = format!("{url}/{}", unique("mqtt-x").replace('.', "-"));
            let status = format!("{base}/status");
            let recv_status = || {
                listen(vec![
                    "recv".into(),
                    status.clone(),
                    "-O".into(),
                    ver.clone(),
                    "-m".into(),
                    "10s".into(),
                ])
            };

            // A will: published when pub's connection dies.
            let l = recv_status();
            let will_topic = format!(
                "will-topic={}",
                status.split_once(&url).unwrap().1.trim_start_matches('/')
            );
            let home = tempfile::tempdir().unwrap();
            let mut victim = Command::new(env!("CARGO_BIN_EXE_pub"))
                .args([
                    "recv",
                    &format!("{base}/idle"),
                    "-m",
                    "30s",
                    "-O",
                    &ver,
                    "-O",
                    &will_topic,
                    "-O",
                    "will-message=gone",
                ])
                .env("PUB_HOME", home.path())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            thread::sleep(Duration::from_secs(2));
            victim.kill().unwrap();
            let _ = victim.wait();
            let o = l.join().unwrap();
            assert_eq!(
                String::from_utf8_lossy(&o.stdout),
                "gone",
                "{what}: {}",
                stderr(&o)
            );

            if *v == "5" {
                // MQTT 5: the will even on a normal disconnect, when asked.
                // Artemis ignores DISCONNECT's reason code (0x04 included).
                if name != "artemis" {
                    let l = recv_status();
                    ok(&[
                        "send",
                        &format!("{base}/x"),
                        "-d",
                        "x",
                        "-O",
                        &ver,
                        "-O",
                        &will_topic,
                        "-O",
                        "will-message=bye",
                        "-O",
                        "will-on-disconnect=true",
                    ]);
                    let o = l.join().unwrap();
                    assert_eq!(
                        String::from_utf8_lossy(&o.stdout),
                        "bye",
                        "{what}: {}",
                        stderr(&o)
                    );
                }

                // Topic aliases: repeated sends to one topic arrive under its name.
                let t = format!("{base}/aliased");
                let l = listen(vec![
                    "recv".into(),
                    t.clone(),
                    "-O".into(),
                    ver.clone(),
                    "-n".into(),
                    "3".into(),
                    "-m".into(),
                    "10s".into(),
                    "-F".into(),
                    "json".into(),
                ]);
                let o = pub_cmd(&["send", &t, "-d", "a", "-n", "3", "-O", &ver, "-v"], None);
                assert!(o.status.success(), "{what}: {}", stderr(&o));
                let o = l.join().unwrap();
                let m = json_lines(&String::from_utf8_lossy(&o.stdout));
                assert_eq!(m.len(), 3, "{what}: {}", stderr(&o));
                let topic = t.split_once(&url).unwrap().1.trim_start_matches('/');
                assert!(m.iter().all(|m| m["destination"] == topic), "{what}: {m:?}");
            }

            // Shared subscriptions: each message to one member of the group.
            if name == "activemq" {
                continue; // no $share in ActiveMQ's MQTT
            }
            let jobs = format!("{base}/jobs");
            let member = || {
                listen(vec![
                    "recv".into(),
                    jobs.clone(),
                    "-O".into(),
                    ver.clone(),
                    "-O".into(),
                    "share-group=workers".into(),
                    "-n".into(),
                    "0".into(),
                    "-m".into(),
                    "6s".into(),
                    "--lines".into(),
                ])
            };
            let (a, b) = (member(), member());
            for i in 0..6 {
                ok(&["send", &jobs, "-O", &ver, "-d", &format!("j{i}")]);
            }
            let (a, b) = (a.join().unwrap(), b.join().unwrap());
            let mut all: Vec<String> = [&a, &b]
                .iter()
                .flat_map(|o| {
                    String::from_utf8_lossy(&o.stdout)
                        .lines()
                        .map(str::to_string)
                        .collect::<Vec<_>>()
                })
                .collect();
            all.sort();
            assert_eq!(
                all,
                (0..6).map(|i| format!("j{i}")).collect::<Vec<_>>(),
                "{what}: {} / {}",
                stderr(&a),
                stderr(&b)
            );
            assert!(
                !a.stdout.is_empty() && !b.stdout.is_empty(),
                "{what}: both members get some"
            );
        }
    }
}

#[test]
#[ignore]
fn mqtt_durable_session_with_two_filters_warns() {
    let url = env("PUB_IT_MOSQUITTO");
    let base = format!("{url}/{}", unique("mqtt-two").replace('.', "-"));
    let (orders, alerts) = (format!("{base}/orders"), format!("{base}/alerts"));
    let session = unique("s").replace('.', "-");
    let durable = ["--durable", session.as_str()];
    let mut first = vec!["recv", orders.as_str(), "-m", "1s"];
    first.extend_from_slice(&durable);
    assert_eq!(code(&first), Some(28));
    ok(&["send", &orders, "-d", "an order"]);
    // Same session, another topic: the queued order still arrives, with a warning.
    let mut second = vec!["recv", alerts.as_str(), "-n", "1", "-m", "5s"];
    second.extend_from_slice(&durable);
    let o = pub_cmd(&second, None);
    assert!(o.status.success(), "{}", stderr(&o));
    assert_eq!(String::from_utf8_lossy(&o.stdout), "an order");
    assert!(
        stderr(&o).contains("another subscription in session"),
        "{}",
        stderr(&o)
    );
    ok(&["unsubscribe", &url, "--durable", &session]);
}

#[test]
#[ignore]
fn mqtt_tls_and_auth_on_mosquitto() {
    let ca = ca();
    let tls = format!(
        "{}/{}/t",
        env("PUB_IT_MOSQUITTO_TLS"),
        unique("mqtts").replace('.', "-")
    );
    ok(&["ping", &tls, "-O", &ca]);
    assert_eq!(code(&["ping", &tls]), Some(35));
    let l = listen(vec![
        "recv".into(),
        tls.clone(),
        "-O".into(),
        ca.clone(),
        "-m".into(),
        "10s".into(),
    ]);
    ok(&["send", &tls, "-O", &ca, "-d", "secure"]);
    assert_eq!(String::from_utf8_lossy(&l.join().unwrap().stdout), "secure");

    let auth = env("PUB_IT_MOSQUITTO_AUTH");
    ok(&["ping", &auth, "-u", "app:s3cret"]);
    let o = pub_cmd(&["ping", &auth, "-u", "app:wrong"], None);
    assert_eq!(o.status.code(), Some(67), "{}", stderr(&o));
}

#[test]
#[ignore]
fn mqtt_and_openwire_meet_on_the_same_topic() {
    // ActiveMQ maps MQTT's '/' to JMS's '.'.
    for (name, mqtt, amq) in [
        (
            "activemq",
            env("PUB_IT_ACTIVEMQ_MQTT"),
            env("PUB_IT_ACTIVEMQ"),
        ),
        ("artemis", env("PUB_IT_ARTEMIS_MQTT"), env("PUB_IT_ARTEMIS")),
    ] {
        let base = unique("cross").replace('.', "-");
        let l = listen(vec![
            "recv".into(),
            format!("{amq}/topic/{base}.a"),
            "-m".into(),
            "10s".into(),
        ]);
        ok(&[
            "send",
            &format!("{mqtt}/{base}/a"),
            "-O",
            "version=3.1.1",
            "-d",
            "from mqtt",
        ]);
        let o = l.join().unwrap();
        assert_eq!(
            String::from_utf8_lossy(&o.stdout),
            "from mqtt",
            "{name}: {}",
            stderr(&o)
        );

        let l = listen(vec![
            "recv".into(),
            format!("{mqtt}/{base}/b"),
            "-O".into(),
            "version=3.1.1".into(),
            "-m".into(),
            "10s".into(),
        ]);
        ok(&[
            "send",
            &format!("{amq}/topic/{base}.b"),
            "-d",
            "from openwire",
        ]);
        let o = l.join().unwrap();
        assert_eq!(
            String::from_utf8_lossy(&o.stdout),
            "from openwire",
            "{name}: {}",
            stderr(&o)
        );
    }
}

/// ActiveMQ speaks MQTT 3.1.1 only: options in the URL after a `#` wildcard still apply, and
/// pub's MQTT 5 default gets a hint instead of a bare "connection closed".
#[test]
#[ignore]
fn mqtt_activemq_wildcard_url_and_version_hint() {
    let mqtt = env("PUB_IT_ACTIVEMQ_MQTT");
    let base = unique("wild").replace('.', "-");
    let l = listen(vec![
        "recv".into(),
        format!("{mqtt}/{base}/#?version=3.1.1"),
        "-m".into(),
        "10s".into(),
    ]);
    ok(&[
        "send",
        &format!("{mqtt}/{base}/kitchen?version=3.1.1"),
        "-d",
        "21.5",
    ]);
    let o = l.join().unwrap();
    assert_eq!(String::from_utf8_lossy(&o.stdout), "21.5", "{}", stderr(&o));

    let o = pub_cmd(&["ping", &mqtt], None);
    assert!(!o.status.success());
    assert!(
        stderr(&o).contains("try -O version=3.1.1"),
        "{}",
        stderr(&o)
    );
}

/// An amqp:// URL pointed at ActiveMQ's OpenWire port says so.
#[test]
#[ignore]
fn wrong_port_names_the_protocol() {
    let openwire = env("PUB_IT_ACTIVEMQ").replacen("amq://", "amqp://", 1);
    let o = pub_cmd(&["ping", &openwire], None);
    assert!(!o.status.success());
    assert!(
        stderr(&o).contains("the server looks like an ActiveMQ OpenWire port"),
        "{}",
        stderr(&o)
    );
}

// ---- AMQP 1.0

/// The same broker over OpenWire, for the Java client: (URL, credentials).
type JavaSide = Option<(String, &'static [&'static str])>;

/// AMQP endpoints: (name, URL, the Java side if the broker has one).
fn amqp_brokers() -> Vec<(&'static str, String, JavaSide)> {
    vec![
        (
            "activemq",
            env("PUB_IT_ACTIVEMQ_AMQP"),
            Some((env("PUB_IT_ACTIVEMQ_JAVA"), &[][..])),
        ),
        (
            "artemis",
            env("PUB_IT_ARTEMIS_AMQP"),
            Some((env("PUB_IT_ARTEMIS_JAVA"), &["artemis", "artemis"][..])),
        ),
        ("rabbitmq", env("PUB_IT_RABBITMQ_AMQP"), None),
    ]
}

/// A queue for a test: RabbitMQ's must be declared first (it never auto-creates
/// over AMQP 1.0) and is addressed as /queues/NAME.
fn amqp_queue(name: &str, url: &str) -> String {
    let q = unique(name);
    if name.is_empty() || !url.contains(&std::env::var("PUB_IT_RABBITMQ_AMQP").unwrap_or_default())
    {
        return format!("{url}/queue/{q}");
    }
    let o = Command::new("docker")
        .args([
            "exec",
            &env("PUB_IT_RABBITMQ_CONTAINER"),
            "rabbitmqadmin",
            "queues",
            "declare",
            "--name",
            &q,
            "--durable",
            "true",
        ])
        .output()
        .expect("docker");
    assert!(o.status.success(), "declaring {q}: {}", stderr(&o));
    format!("{url}/queues/{q}")
}

#[test]
#[ignore]
fn amqp_round_trip() {
    for (name, url, _) in amqp_brokers() {
        let q = amqp_queue("amqp", &url);
        ok(&[
            "send",
            &q,
            "-d",
            "héllo",
            "-O",
            "message-type=text",
            "-H",
            "JMSCorrelationID: c-1",
            "-H",
            "priority: 7",
            "-H",
            "ttl: 5m",
            "-P",
            "n:int=5",
            "-P",
            "region=EU",
        ]);
        let o = pub_cmd(&["send", &q], Some(&[0u8, 1, 0, 255][..]));
        assert!(o.status.success(), "{name}: {}", stderr(&o));
        // Bigger than the brokers' frames (128 KiB): several transfer frames each way.
        let big = vec![b'x'; 300_000];
        let o = pub_cmd(&["send", &q], Some(&big));
        assert!(o.status.success(), "{name}: {}", stderr(&o));
        let m = json_lines(&ok(&["recv", &q, "-n", "3", "-F", "json", "-m", "10s"]));
        assert_eq!(m.len(), 3, "{name}");
        assert_eq!(
            m[0]["body"],
            serde_json::json!({"type": "text", "data": "héllo"}),
            "{name}"
        );
        assert_eq!(m[0]["fields"]["correlation-id"], "c-1", "{name}");
        assert_eq!(m[0]["fields"]["priority"], 7, "{name}");
        assert!(
            m[0]["fields"]["expiration"].as_i64().unwrap()
                > m[0]["fields"]["timestamp"].as_i64().unwrap(),
            "{name}"
        );
        assert_eq!(
            m[0]["properties"]["n"],
            serde_json::json!({"int": 5}),
            "{name}"
        );
        assert_eq!(m[1]["body"]["base64"], "AAEA/w==", "{name}");
        assert_eq!(
            m[2]["body"]["base64"].as_str().unwrap().len(),
            400_000,
            "{name}"
        );
        // Selectors are JMS-only (RabbitMQ has none for classic queues).
        if name != "rabbitmq" {
            ok(&["send", &q, "-d", "eu", "-P", "region=EU"]);
            ok(&["send", &q, "-d", "us", "-P", "region=US"]);
            assert_eq!(
                ok(&["recv", &q, "-s", "region = 'US'", "-m", "5s"]),
                "us",
                "{name}"
            );
            assert_eq!(ok(&["recv", &q, "-m", "5s"]), "eu", "{name}");
        }
    }
}

#[test]
#[ignore]
fn amqp_transactions_and_move() {
    for (name, url, _) in amqp_brokers() {
        let q = amqp_queue("amqp-tx", &url);
        if name == "rabbitmq" {
            // RabbitMQ has no AMQP 1.0 transactions: a clear refusal.
            let o = pub_cmd(&["send", &q, "-d", "x", "--transaction"], None);
            assert_eq!(o.status.code(), Some(1), "{name}: {}", stderr(&o));
            assert!(stderr(&o).contains("transaction"), "{name}: {}", stderr(&o));
            continue;
        }
        ok(&[
            "send",
            &q,
            "-d",
            "t",
            "-n",
            "4",
            "--transaction",
            "--batch",
            "2",
        ]);
        assert_eq!(
            ok(&["recv", &q, "-n", "0", "-m", "2s", "--lines"]),
            "t\nt\nt\nt\n",
            "{name}"
        );
        ok(&["send", &q, "-d", "a"]);
        ok(&["send", &q, "-d", "b"]);
        assert_eq!(
            ok(&[
                "recv",
                &q,
                "-n",
                "2",
                "-m",
                "5s",
                "--lines",
                "--transaction",
                "--batch",
                "1"
            ]),
            "a\nb\n",
            "{name}"
        );

        ok(&[
            "send",
            &q,
            "-d",
            "keep",
            "-O",
            "message-type=text",
            "-H",
            "JMSCorrelationID: c-9",
            "-P",
            "region=EU",
            "-P",
            "n:int=5",
        ]);
        ok(&["send", &q, "-d", "bin"]);
        let dlq_name = unique("amqp-dlq");
        let to = format!("/queue/{dlq_name}");
        let o = pub_cmd(
            &["move", &q, "--to", &to, "-s", "region = 'EU'", "-m", "2s"],
            None,
        );
        assert!(o.status.success(), "{name}: {}", stderr(&o));
        assert!(
            stderr(&o).contains("moved 1 message"),
            "{name}: {}",
            stderr(&o)
        );
        let m = json_lines(&ok(&[
            "recv",
            &format!("{url}{to}"),
            "-n",
            "0",
            "-m",
            "2s",
            "-F",
            "json",
        ]));
        assert_eq!(m.len(), 1, "{name}");
        assert_eq!(m[0]["body"]["data"], "keep", "{name}");
        assert_eq!(m[0]["fields"]["correlation-id"], "c-9", "{name}");
        assert_eq!(m[0]["properties"]["region"], "EU", "{name}");
        assert_eq!(m[0]["properties"]["n"]["int"], 5, "{name}");
        assert_eq!(ok(&["recv", &q, "-n", "0", "-m", "2s"]), "bin", "{name}");
    }
}

#[test]
#[ignore]
fn amqp_and_the_java_client() {
    for (name, url, java_url) in amqp_brokers() {
        let Some((java_url, creds)) = java_url else {
            continue;
        };
        // pub over AMQP → Java over OpenWire, through ActiveMQ's transformer.
        let q = unique("amqp-java");
        let dest = format!("{url}/queue/{q}");
        ok(&[
            "send",
            &dest,
            "-d",
            "as text",
            "-O",
            "message-type=text",
            "-H",
            "priority: 6",
            "-P",
            "n:long=7",
            "-P",
            "region=EU",
        ]);
        ok(&[
            "send",
            &dest,
            "-d",
            "as data text",
            "-O",
            "message-type=text",
            "-O",
            "text-body=data",
        ]);
        ok(&["send", &dest, "-d", "as bytes"]);
        let mut args = vec![java_url.as_str(), q.as_str()];
        args.extend_from_slice(creds);
        let out = java("JC", &args);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 3, "{name}: {out}");
        assert!(
            lines[0].starts_with("text:as text | prio=6"),
            "{name}: {out}"
        );
        assert!(
            lines[0].contains("n=7(Long)") && lines[0].contains("region=EU(String)"),
            "{name}: {out}"
        );
        assert!(
            lines[1].starts_with("text:as data text"),
            "{name}: a data section with text/plain is a TextMessage too: {out}"
        );
        assert!(lines[2].starts_with("bytes:8"), "{name}: {out}");

        // Java → pub over AMQP: every property type survives.
        let q2 = unique("java-amqp");
        let mut jp = vec![java_url.as_str(), q2.as_str()];
        jp.extend_from_slice(creds);
        java("JP", &jp);
        let m = json_lines(&ok(&[
            "recv",
            &format!("{url}/queue/{q2}"),
            "-n",
            "3",
            "-F",
            "json",
            "-m",
            "10s",
        ]));
        assert_eq!(
            m[0]["body"]["data"], "from java ünïcödé 😀",
            "{name}: {}",
            m[0]
        );
        assert_eq!(m[0]["fields"]["correlation-id"], "jc-1", "{name}");
        assert_eq!(
            m[0]["properties"]["i"],
            serde_json::json!({"int": 42}),
            "{name}: {}",
            m[0]
        );
        assert_eq!(
            m[0]["properties"]["l"],
            serde_json::json!({"long": 1u64 << 40}),
            "{name}"
        );
        assert_eq!(m[1]["body"]["type"], "bytes", "{name}: {}", m[1]);
    }
}

#[test]
#[ignore]
fn amqp_topics_durable_browse_and_delay() {
    for (name, url, _) in amqp_brokers()
        .into_iter()
        .filter(|(n, ..)| *n != "rabbitmq")
    {
        // A topic and a queue of the same name are different things.
        let base = unique("amqp-t");
        let t = format!("{url}/topic/{base}");
        let l = listen(vec!["recv".into(), t.clone(), "-m".into(), "10s".into()]);
        ok(&["send", &t, "-d", "news"]);
        assert_eq!(
            String::from_utf8_lossy(&l.join().unwrap().stdout),
            "news",
            "{name}"
        );

        // Durable: subscribe, miss two, come back, unsubscribe.
        let client = format!("client-id={}", unique("amqp-c"));
        let sub = ["--durable", "audit", "-O", client.as_str()];
        let mut first = vec!["recv", t.as_str(), "-m", "1s"];
        first.extend_from_slice(&sub);
        assert_eq!(code(&first), Some(28), "{name}");
        ok(&["send", &t, "-d", "d1"]);
        ok(&["send", &t, "-d", "d2"]);
        let mut again = vec!["recv", t.as_str(), "-n", "2", "-m", "5s", "--lines"];
        again.extend_from_slice(&sub);
        assert_eq!(ok(&again), "d1\nd2\n", "{name}");
        ok(&["unsubscribe", &url, "--durable", "audit", "-O", &client]);
        assert_eq!(
            code(&["unsubscribe", &url, "--durable", "audit", "-O", &client]),
            Some(2),
            "{name}"
        );

        // Browse in small batches: everything, nothing consumed.
        let q = format!("{url}/queue/{}", unique("amqp-b"));
        for i in 1..=5 {
            ok(&["send", &q, "-d", &format!("m{i}")]);
        }
        assert_eq!(
            ok(&["browse", &format!("{q}?credit=2"), "--lines"]),
            "m1\nm2\nm3\nm4\nm5\n",
            "{name}"
        );
        assert_eq!(
            ok(&["recv", &q, "-n", "5", "-m", "5s", "--lines"]),
            "m1\nm2\nm3\nm4\nm5\n",
            "{name}"
        );

        // Scheduled delivery (x-opt-delivery-delay).
        let d = format!("{url}/queue/{}", unique("amqp-d"));
        ok(&["send", &d, "-d", "later", "-H", "delivery-delay: 3s"]);
        assert_eq!(
            code(&["recv", &d, "-m", "1s"]),
            Some(28),
            "{name}: delivered early"
        );
        assert_eq!(ok(&["recv", &d, "-m", "8s"]), "later", "{name}");
    }
}

#[test]
#[ignore]
fn amqp_rabbitmq_specifics_and_tls() {
    let rabbit = env("PUB_IT_RABBITMQ_AMQP");
    // RabbitMQ never creates queues over AMQP 1.0: a clear not-found.
    let o = pub_cmd(
        &[
            "send",
            &format!("{rabbit}/queues/{}", unique("none")),
            "-d",
            "x",
        ],
        None,
    );
    assert_eq!(o.status.code(), Some(2), "{}", stderr(&o));
    assert!(stderr(&o).contains("amqp:not-found"), "{}", stderr(&o));
    // Its classic queues can't browse (no copy distribution): refused, nothing consumed.
    let q = amqp_queue("rabbit-b", &rabbit);
    ok(&["send", &q, "-d", "keep"]);
    let o = pub_cmd(&["browse", &q], None);
    assert_eq!(o.status.code(), Some(1), "{}", stderr(&o));
    assert_eq!(ok(&["recv", &q, "-m", "5s"]), "keep");
    let o = pub_cmd(
        &["ping", &rabbit.replace("guest:guest@", "guest:nope@")],
        None,
    );
    assert_eq!(o.status.code(), Some(67), "{}", stderr(&o));

    // AMQP over TLS on ActiveMQ.
    let ca = ca();
    let tls = format!("{}/queue/{}", env("PUB_IT_ACTIVEMQ_AMQPS"), unique("amqps"));
    ok(&["send", &tls, "-O", &ca, "-d", "over tls"]);
    assert_eq!(ok(&["recv", &tls, "-O", &ca, "-m", "5s"]), "over tls");
    assert_eq!(code(&["ping", &tls]), Some(35));
}

// ---- AMQP 0-9-1 (ramqp) on RabbitMQ

#[test]
#[ignore]
fn ramqp_on_rabbitmq() {
    let url = env("PUB_IT_RABBITMQ_RAMQP");
    let q = format!("{url}/queue/{}", unique("rq"));
    ok(&["dest", "create", &q]);
    ok(&[
        "send",
        &q,
        "-d",
        "héllo",
        "-O",
        "message-type=text",
        "-H",
        "JMSCorrelationID: c-1",
        "-H",
        "priority: 7",
        "-H",
        "ttl: 5m",
        "-P",
        "n:int=5",
    ]);
    let big = vec![b'x'; 300_000];
    let o = pub_cmd(&["send", &q], Some(&big));
    assert!(o.status.success(), "{}", stderr(&o));
    let m = json_lines(&ok(&["recv", &q, "-n", "2", "-F", "json", "-m", "5s"]));
    assert_eq!(
        m[0]["body"],
        serde_json::json!({"type": "text", "data": "héllo"})
    );
    assert_eq!(
        (
            &m[0]["fields"]["correlation-id"],
            &m[0]["fields"]["priority"]
        ),
        (&Json::from("c-1"), &Json::from(7))
    );
    assert_eq!(m[0]["properties"]["n"], serde_json::json!({"int": 5}));
    assert_eq!(m[1]["body"]["base64"].as_str().unwrap().len(), 400_000);

    // Unroutable is an error for queues; topics with no listeners aren't.
    assert_eq!(
        code(&[
            "send",
            &format!("{url}/queue/{}", unique("none")),
            "-d",
            "x"
        ]),
        Some(2)
    );
    ok(&[
        "send",
        &format!("{url}/topic/{}.x", unique("nobody").replace('.', "-")),
        "-d",
        "x",
    ]);

    // Topic wildcards, durable subscriptions, unsubscribe.
    let base = unique("rqt").replace('.', "-");
    let sub = unique("rq-sub");
    let pattern = format!("{url}/topic/{base}.#");
    assert_eq!(
        code(&["recv", &pattern, "--durable", &sub, "-m", "1s"]),
        Some(28)
    );
    ok(&["send", &format!("{url}/topic/{base}.a.b"), "-d", "d1"]);
    ok(&[
        "send",
        &format!("{url}/exchange/amq.topic/{base}.c"),
        "-d",
        "d2",
    ]);
    assert_eq!(
        ok(&[
            "recv",
            &pattern,
            "--durable",
            &sub,
            "-n",
            "2",
            "-m",
            "5s",
            "--lines"
        ]),
        "d1\nd2\n"
    );
    ok(&["unsubscribe", &url, "--durable", &sub]);
    assert_eq!(code(&["unsubscribe", &url, "--durable", &sub]), Some(2));

    // Transactions and move.
    let (dlq, back) = (
        format!("{url}/queue/{}", unique("rq-dlq")),
        unique("rq-back"),
    );
    ok(&["dest", "create", &dlq]);
    ok(&["dest", "create", &format!("{url}/queue/{back}")]);
    ok(&[
        "send",
        &dlq,
        "-d",
        "m",
        "-n",
        "5",
        "--transaction",
        "--batch",
        "2",
    ]);
    let o = pub_cmd(
        &[
            "move",
            &dlq,
            "--to",
            &format!("/queue/{back}"),
            "--batch",
            "2",
            "-m",
            "2s",
        ],
        None,
    );
    assert!(stderr(&o).contains("moved 5 messages"), "{}", stderr(&o));
    assert_eq!(
        ok(&[
            "recv",
            &format!("{url}/queue/{back}"),
            "-n",
            "5",
            "-m",
            "5s",
            "--lines"
        ]),
        "m\nm\nm\nm\nm\n"
    );
    // Moving to a queue that doesn't exist fails, and the messages stay put.
    ok(&["send", &dlq, "-d", "keep", "-n", "2"]);
    let o = pub_cmd(
        &[
            "move",
            &dlq,
            "--to",
            &format!("/queue/{}", unique("nope")),
            "-m",
            "2s",
        ],
        None,
    );
    assert_eq!(o.status.code(), Some(2), "{}", stderr(&o));
    assert_eq!(
        ok(&["recv", &dlq, "-n", "0", "-m", "2s", "--lines"]),
        "keep\nkeep\n"
    );

    // Refusals.
    assert_eq!(
        code(&["ping", &url.replace("guest:guest@", "guest:nope@")]),
        Some(67)
    );
    assert_eq!(code(&["ping", &format!("{url}?vhost=nope")]), Some(67));
}

#[test]
#[ignore]
fn ramqp_and_amqp_1_0_on_the_same_rabbitmq() {
    let (ramqp, amqp) = (env("PUB_IT_RABBITMQ_RAMQP"), env("PUB_IT_RABBITMQ_AMQP"));
    let q = unique("cross-rq");
    ok(&["dest", "create", &format!("{ramqp}/queue/{q}")]);
    // 0-9-1 → 1.0: text stays text (content type).
    ok(&[
        "send",
        &format!("{ramqp}/queue/{q}"),
        "-d",
        "from 0-9-1",
        "-O",
        "message-type=text",
    ]);
    let m = &json_lines(&ok(&[
        "recv",
        &format!("{amqp}/queues/{q}"),
        "-F",
        "json",
        "-m",
        "5s",
    ]))[0];
    assert_eq!(
        m["body"],
        serde_json::json!({"type": "text", "data": "from 0-9-1"})
    );
    // 1.0 → 0-9-1: a data section with text/plain is text there (an amqp-value isn't).
    ok(&[
        "send",
        &format!("{amqp}/queues/{q}"),
        "-d",
        "from 1.0",
        "-O",
        "message-type=text",
        "-O",
        "text-body=data",
    ]);
    let m = &json_lines(&ok(&[
        "recv",
        &format!("{ramqp}/queue/{q}"),
        "-F",
        "json",
        "-m",
        "5s",
    ]))[0];
    assert_eq!(
        m["body"],
        serde_json::json!({"type": "text", "data": "from 1.0"})
    );
}

// ---- Azure Service Bus (the emulator; AZURE_SERVICEBUS=1 interop/run.sh)

#[test]
#[ignore]
fn servicebus_emulator() {
    let Ok(sb) = std::env::var("PUB_IT_AZURE_SERVICEBUS") else {
        eprintln!("skipped: AZURE_SERVICEBUS=1 interop/run.sh starts the emulator");
        return;
    };
    let at = |path: &str| format!("{sb}/{path}");
    // Entities are fixed (interop/servicebus/Config.json): start them empty.
    for e in [
        "pub.basic",
        "pub.browse",
        "pub.tx",
        "pub.delay",
        "pub.events/Subscriptions/all",
        "pub.events/Subscriptions/eu",
    ] {
        let _ = pub_cmd(&["recv", &at(e), "-n", "0", "-m", "2s"], None);
    }
    assert!(ok(&["ping", &sb]).contains("Azure Service Bus"));

    // Round trip: fields, typed properties, text and bytes, multi-frame (64 KiB frames).
    let q = at("pub.basic");
    ok(&[
        "send",
        &q,
        "-d",
        "héllo",
        "-O",
        "message-type=text",
        "-H",
        "correlation-id: c-1",
        "-H",
        "ttl: 5m",
        "-P",
        "n:int=5",
        "-P",
        "region=EU",
    ]);
    let big = vec![b'x'; 200_000];
    let o = pub_cmd(&["send", &q], Some(&big));
    assert!(o.status.success(), "{}", stderr(&o));
    let m = json_lines(&ok(&["recv", &q, "-n", "2", "-m", "10s", "-F", "json"]));
    assert_eq!(m[0]["body"]["data"], "héllo");
    assert_eq!(m[0]["fields"]["correlation-id"], "c-1");
    assert_eq!(m[0]["properties"]["n"]["int"], 5);
    assert_eq!(m[0]["properties"]["region"], "EU");
    assert!(m[0]["fields"]["expiration"].is_number());
    assert_eq!(m[1]["body"]["type"], "bytes");
    assert_eq!(m[1]["body"]["base64"].as_str().unwrap().len(), 266_668);

    // Topics: subscriptions are entities, with their own rules.
    let t = at("pub.events");
    ok(&["send", &t, "-d", "eu", "-P", "region=EU"]);
    ok(&["send", &t, "-d", "us", "-P", "region=US"]);
    assert_eq!(
        ok(&[
            "recv",
            &at("pub.events/Subscriptions/all"),
            "-n",
            "0",
            "-m",
            "3s",
            "--lines"
        ]),
        "eu\nus\n"
    );
    assert_eq!(
        ok(&[
            "recv",
            &at("pub.events/Subscriptions/eu"),
            "-n",
            "0",
            "-m",
            "3s",
            "--lines"
        ]),
        "eu\n"
    );

    // Browse peeks: nothing is locked or consumed.
    let b = at("pub.browse");
    for i in 1..=3 {
        ok(&["send", &b, "-d", &format!("b{i}")]);
    }
    assert_eq!(ok(&["browse", &b, "--lines"]), "b1\nb2\nb3\n");
    assert_eq!(ok(&["browse", &b, "-n", "2", "--lines"]), "b1\nb2\n");
    assert_eq!(
        ok(&["recv", &b, "-n", "0", "-m", "3s", "--lines"]),
        "b1\nb2\nb3\n"
    );
    assert_eq!(ok(&["browse", &b]), "");

    // Transactions within one entity.
    let tx = at("pub.tx");
    ok(&[
        "send",
        &tx,
        "-d",
        "t",
        "-n",
        "4",
        "--transaction",
        "--batch",
        "2",
    ]);
    assert_eq!(
        ok(&[
            "recv",
            &tx,
            "-n",
            "4",
            "-m",
            "5s",
            "--lines",
            "--transaction",
            "--batch",
            "1"
        ]),
        "t\nt\nt\nt\n"
    );

    // Delivery delay becomes Service Bus's scheduled enqueue time.
    let d = at("pub.delay");
    ok(&["send", &d, "-d", "later", "-H", "delivery-delay: 3s"]);
    assert_eq!(code(&["recv", &d, "-m", "1s"]), Some(28));
    assert_eq!(ok(&["recv", &d, "-m", "10s"]), "later");

    // What Service Bus can't do is refused, not half-done.
    for args in [
        vec![
            "recv".to_string(),
            q.clone(),
            "-s".into(),
            "region = 'EU'".into(),
            "-m".into(),
            "1s".into(),
        ],
        vec!["move".into(), q.clone(), "--to".into(), "pub.tx".into()],
        vec![
            "recv".into(),
            t.clone(),
            "--durable".into(),
            "x".into(),
            "-O".into(),
            "client-id=c".into(),
            "-m".into(),
            "1s".into(),
        ],
        vec![
            "unsubscribe".into(),
            t.clone(),
            "--durable".into(),
            "x".into(),
            "-O".into(),
            "client-id=c".into(),
        ],
    ] {
        let a: Vec<&str> = args.iter().map(String::as_str).collect();
        let o = pub_cmd(&a, None);
        assert_eq!(o.status.code(), Some(1), "{args:?}: {}", stderr(&o));
        assert!(
            stderr(&o).contains("Service Bus"),
            "{args:?}: {}",
            stderr(&o)
        );
    }
    assert_eq!(code(&["send", &at("missing"), "-d", "x"]), Some(2));
}
