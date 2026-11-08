use super::harness::*;
use std::os::unix::fs::PermissionsExt;

const SAVED_PROFILE: &str = "0123456789abcdef0123456789abcdef";
const OWNED_SSH: &str = r#"#!/bin/sh
printf 'ssh\n' >> "$TEST_ROOT/ssh-calls"
printf '%s\n' "$SSH_AUTH_SOCK" >> "$TEST_ROOT/auth-calls"
if [ -f "$TEST_ROOT/required-agent" ] && [ "$SSH_AUTH_SOCK" != "$(cat "$TEST_ROOT/required-agent")" ]; then
    echo 'owned SSH fixture rejected a different agent' >&2
    exit 255
fi
control=; previous=; operation=; configuration=
for arg do
    last=$arg
    printf '%s\n' "$arg" >> "$TEST_ROOT/ssh-args"
    if [ "$previous" = -S ]; then control=$(printf '%s' "$arg" | sed 's/%C/fixture/'); fi
    if [ "$previous" = -O ]; then operation=$arg; fi
    if [ "$arg" = -G ]; then configuration=yes; fi
    previous=$arg
done
if [ -n "$configuration" ]; then
    printf 'controlpath %s\n' "$control"
    exit 0
fi
if [ "$operation" = stop ]; then
    test -n "$TEST_CONTROL" && test -S "$TEST_CONTROL" || exit 255
    rm -- "$TEST_CONTROL"
    exit 0
fi
if [ "$last" = exit ]; then
    if [ "$control" = none ]; then exit 0; fi
    echo 'test reused transport failed' >&2
    exit 255
fi
printf '%s' "$control" > "$TEST_ROOT/control-path"
if [ -n "$TEST_CONTROL" ] && [ -S "$TEST_CONTROL" ]; then
    if [ "$TEST_MODE" = frozen ] && [ "$last" != '/bin/sh -s' ]; then
        exec sleep 30
    else
        echo 'test reused transport failed' >&2
        exit 255
    fi
fi
if [ "$TEST_MODE" = cold_then_broken ] && [ "$last" != '/bin/sh -s' ]; then
    case "$last" in *base64*) echo 'test new transport failed' >&2; exit 255;; esac
fi
case "$last" in
    '/bin/sh -c '*)
        case "$last" in
            *base64*) : ;;
            *) echo "script must use stdin, not a command argument: $last" >&2; exit 2 ;;
        esac
        if [ "$TEST_MODE" = offline ]; then echo 'test remote connection failed' >&2; exit 255; fi
        printf 'login banner\n'
        PATH="$TEST_ROOT/remote bin:/usr/bin:/bin" exec /bin/sh -c "$last" ;;
    '/bin/sh -s')
        script=$(cat)
        printf 'login banner\n'
        case "$script" in
            *'uname -s'*) printf 'herdr-remote-output-ready:1\n'; uname -s; uname -m ;;
            *) PATH="$TEST_ROOT/remote bin:/usr/bin:/bin" exec /bin/sh -c "$script" ;;
        esac ;;
    *) echo "unexpected command: $last" >&2; exit 2 ;;
esac
"#;

fn native_command(base: &Path, origin: &Path, caller: Option<&str>) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_herdr"));
    command
        .env("HOME", base)
        .env("TEST_ROOT", base)
        .env("XDG_CONFIG_HOME", base.join("config"))
        .env("XDG_STATE_HOME", base.join("state"))
        .env("XDG_RUNTIME_DIR", base.join("runtime"))
        .env(
            "PATH",
            format!("{}:/usr/bin:/bin", base.join("bin").display()),
        )
        .env("HERDR_SOCKET_PATH", origin)
        .env_remove("HERDR_CLIENT_SOCKET_PATH")
        .env_remove("HERDR_SESSION")
        .env_remove("HERDR_CONFIG_PATH")
        .env_remove("HERDR_REMOTE_BINARY")
        .env_remove("HERDR_ENV")
        .env_remove("SSH_AUTH_SOCK");
    if let Some(caller) = caller {
        command.env("HERDR_PANE_ID", caller);
    } else {
        command.env_remove("HERDR_PANE_ID");
    }
    command
}

fn successful(command: &mut Command) -> serde_json::Value {
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn coordination_page(socket: &Path) -> serde_json::Value {
    send_request(
        socket,
        r#"{"id":"fixture-list","method":"agent.coordination.list","params":{}}"#,
    )
}

#[test]
fn saved_machine_coordination_records_real_launch_and_concurrent_pty_submissions_before_wait() {
    run_saved_machine_case(Case::Concurrent);
}

#[derive(Clone, Copy)]
enum Case {
    Concurrent,
    ExplicitAgent,
    MissingAgent,
    SelfAlias,
    LostReply,
    HiddenOutcome,
    PrewriteRefusal,
    DisabledAgent,
    EmptyAgent,
    NonUtfAgent,
}

#[test]
fn saved_machine_coordination_uses_the_invoking_agents_override() {
    run_saved_machine_case(Case::ExplicitAgent);
}
#[test]
fn saved_machine_coordination_missing_explicit_agent_never_uses_the_daemon_agent() {
    run_saved_machine_case(Case::MissingAgent);
}
#[test]
fn saved_machine_coordination_alias_to_origin_delivers_without_a_self_edge() {
    run_saved_machine_case(Case::SelfAlias);
}
#[test]
fn saved_machine_coordination_recovers_late_actual_completion_after_lost_execute_reply() {
    run_saved_machine_case(Case::LostReply);
}
#[test]
fn saved_machine_coordination_never_replays_when_a_written_operation_stays_unconfirmed() {
    run_saved_machine_case(Case::HiddenOutcome);
}
#[test]
fn saved_machine_coordination_prewrite_refusal_falls_back_once() {
    run_saved_machine_case(Case::PrewriteRefusal);
}

#[test]
fn saved_machine_coordination_unset_agent_never_uses_the_daemon_agent() {
    run_saved_machine_case(Case::DisabledAgent);
}
#[test]
fn saved_machine_coordination_empty_agent_never_uses_the_daemon_agent() {
    run_saved_machine_case(Case::EmptyAgent);
}
#[test]
fn saved_machine_coordination_non_utf_agent_uses_native_transport_before_forwarding() {
    run_saved_machine_case(Case::NonUtfAgent);
}

struct OwnedAgent {
    path: PathBuf,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
}
impl OwnedAgent {
    fn start(path: PathBuf) -> Self {
        let socket = UnixListener::bind(&path).unwrap();
        socket.set_nonblocking(true).unwrap();
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let done = stop.clone();
        let worker = thread::spawn(move || {
            while !done.load(std::sync::atomic::Ordering::Relaxed) {
                match socket.accept() {
                    Ok((stream, _)) => drop(stream),
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5))
                    }
                    Err(error) => panic!("{error}"),
                }
            }
        });
        Self {
            path,
            stop,
            worker: Some(worker),
        }
    }
}
impl Drop for OwnedAgent {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(worker) = self.worker.take() {
            worker.join().unwrap();
        }
        let _ = fs::remove_file(&self.path);
    }
}

fn run_saved_machine_case(case: Case) {
    let base = unique_test_dir();
    let config = base.join("config");
    let runtime = base.join("runtime");
    let bin = base.join("bin");
    let remote_bin = base.join("remote bin");
    fs::create_dir_all(&bin).unwrap();
    fs::create_dir_all(&remote_bin).unwrap();
    let executable = env!("CARGO_BIN_EXE_herdr");
    fs::write(bin.join("ssh"), OWNED_SSH).unwrap();
    fs::write(
        base.join("api-proxy.py"),
        include_str!("../fixtures/coordination-api-proxy.py"),
    )
    .unwrap();
    fs::write(remote_bin.join("herdr"),format!("#!/bin/sh\ncase \"$*\" in *remote-api-bridge*--check*) exec '{0}' \"$@\" ;; *remote-api-bridge*) exec /usr/bin/python3 \"$TEST_ROOT/api-proxy.py\" ;; *) exec '{0}' \"$@\" ;; esac\n",executable)).unwrap();
    fs::set_permissions(remote_bin.join("herdr"), fs::Permissions::from_mode(0o700)).unwrap();
    let daemon_agent = OwnedAgent::start(base.join("a.sock"));
    let invoking_agent = OwnedAgent::start(base.join("b.sock"));
    fs::write(bin.join("pi"), format!(
        "#!/bin/sh\nexport HERDR_AGENT=pi\n'{0}' pane report-agent \"$HERDR_PANE_ID\" --source custom:coordination-pi --agent pi --state idle >/dev/null\nwhile IFS= read -r prompt; do printf '%s\\n' \"$prompt\" >> \"$TEST_ROOT/delivered\"; '{0}' pane report-agent \"$HERDR_PANE_ID\" --source custom:coordination-pi --agent pi --state working >/dev/null; '{0}' pane report-agent \"$HERDR_PANE_ID\" --source custom:coordination-pi --agent pi --state idle >/dev/null; done\n", executable
    )).unwrap();
    for name in ["ssh", "pi"] {
        fs::set_permissions(bin.join(name), fs::Permissions::from_mode(0o700)).unwrap();
    }
    let profiles = base.join("state").join(app_dir_name()).join("client");
    fs::create_dir_all(&profiles).unwrap();
    fs::write(profiles.join("endpoints.json"), serde_json::to_vec(&serde_json::json!({
        "version":1,"ssh":[{"id":SAVED_PROFILE,"label":"worker","target":"owned-fake-host","session":"fleet","enabled":true}]
    })).unwrap()).unwrap();
    let path = PathBuf::from(format!("{}:/usr/bin:/bin", bin.display()));
    let state = base.join("state");
    let shell = Path::new("/bin/sh");
    let envs = [
        ("HOME", base.as_path()),
        ("XDG_STATE_HOME", state.as_path()),
        ("PATH", path.as_path()),
        ("TEST_ROOT", base.as_path()),
        ("SHELL", shell),
        ("SSH_AUTH_SOCK", daemon_agent.path.as_path()),
    ];
    let origin_server = spawn_named_server_with_env(&config, &runtime, "origin", &envs);
    let target_server = spawn_named_server_with_env(&config, &runtime, "fleet", &envs);
    let origin = named_session_socket(&config, "origin");
    let target = named_session_socket(&config, "fleet");
    wait_for_socket(&origin, Duration::from_secs(5));
    wait_for_socket(&target, Duration::from_secs(5));
    fs::write(base.join("target-socket"), target.to_str().unwrap()).unwrap();
    let source_workspace = successful(native_command(&base, &origin, None).args([
        "workspace",
        "create",
        "--cwd",
        base.to_str().unwrap(),
    ]));
    let source_pane = source_workspace["result"]["root_pane"]["pane_id"]
        .as_str()
        .unwrap();
    successful(native_command(&base, &origin, None).args([
        "agent",
        "start",
        "parent",
        "--kind",
        "pi",
        "--pane",
        source_pane,
    ]));
    let target_workspace = successful(native_command(&base, &target, None).args([
        "workspace",
        "create",
        "--cwd",
        base.to_str().unwrap(),
    ]));
    let target_pane = target_workspace["result"]["root_pane"]["pane_id"]
        .as_str()
        .unwrap();
    successful(native_command(&base, &origin, Some(source_pane)).args([
        "--machine",
        "worker",
        "agent",
        "start",
        "child",
        "--kind",
        "pi",
        "--pane",
        target_pane,
    ]));

    if !matches!(case, Case::Concurrent) {
        exercise_case(
            case,
            &base,
            &origin,
            &target,
            source_pane,
            &profiles,
            invoking_agent.path.as_path(),
        );
        drop(target_server);
        drop(origin_server);
        drop(invoking_agent);
        drop(daemon_agent);
        cleanup_test_base(&base);
        return;
    }

    let mut calls = Vec::new();
    for prompt in ["first forwarded", "second forwarded"] {
        calls.push(
            native_command(&base, &origin, Some(source_pane))
                .args([
                    "--machine",
                    "worker",
                    "agent",
                    "prompt",
                    "child",
                    prompt,
                    "--wait",
                    "--until",
                    "done",
                    "--timeout",
                    "10000",
                ])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap(),
        );
    }
    let deadline = Instant::now() + Duration::from_secs(7);
    let page = loop {
        let page = coordination_page(&origin);
        if page["result"]["receipts"]
            .as_array()
            .is_some_and(|receipts| receipts.len() == 3)
        {
            break page;
        }
        assert!(
            Instant::now() < deadline,
            "successful writes were not recorded while waits were active: {page}"
        );
        thread::sleep(Duration::from_millis(10));
    };
    for call in &mut calls {
        assert!(
            call.try_wait().unwrap().is_none(),
            "the receipt must precede the later native wait timeout"
        );
    }
    for call in calls {
        let output = call.wait_with_output().unwrap();
        assert_eq!(output.status.code(), Some(1));
        let response: serde_json::Value = serde_json::from_slice(&output.stderr).unwrap();
        assert_eq!(response["error"]["code"], "timeout");
    }
    let target_page = coordination_page(&target);
    let source_id = page["result"]["server_id"].as_str().unwrap();
    let target_id = target_page["result"]["server_id"].as_str().unwrap();
    assert_ne!(source_id, target_id);
    let receipts = page["result"]["receipts"].as_array().unwrap();
    assert_eq!(
        receipts
            .iter()
            .filter(|receipt| receipt["kind"] == "delegation")
            .count(),
        1
    );
    assert_eq!(
        receipts
            .iter()
            .filter(|receipt| receipt["kind"] == "communication")
            .count(),
        2
    );
    for receipt in receipts {
        assert_eq!(receipt["source"]["server_id"], source_id);
        assert_eq!(receipt["source"]["name"], "parent");
        assert_eq!(receipt["target"]["server_id"], target_id);
        assert_eq!(receipt["target"]["name"], "child");
        assert_eq!(
            receipt["target"]["agent_instance_id"],
            target_page["result"]["agents"][0]["agent_instance_id"]
        );
    }
    let mut delivered: Vec<_> = fs::read_to_string(base.join("delivered"))
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    delivered.sort();
    assert_eq!(delivered, ["first forwarded", "second forwarded"]);
    assert!(!serde_json::to_string(receipts)
        .unwrap()
        .contains("forwarded"));
    drop(target_server);
    drop(origin_server);
    cleanup_test_base(&base);
}

fn exercise_case(
    case: Case,
    base: &Path,
    origin: &Path,
    target: &Path,
    source_pane: &str,
    profiles: &Path,
    agent_b: &Path,
) {
    let prompt = "one real delivery";
    let mut command = native_command(base, origin, Some(source_pane));
    command.args(["--machine", "worker", "agent", "prompt", "child", prompt]);
    let mut expected_receipts = 2;
    let auth_before = fs::read_to_string(base.join("auth-calls"))
        .unwrap()
        .lines()
        .count();
    match case {
        Case::ExplicitAgent => {
            let mut proxy = origin.as_os_str().to_os_string();
            proxy.push(".agent");
            successful(
                native_command(base, origin, Some(source_pane))
                    .env("SSH_AUTH_SOCK", proxy)
                    .args([
                        "--machine",
                        "worker",
                        "agent",
                        "prompt",
                        "child",
                        "default inherited agent",
                    ]),
            );
            fs::write(base.join("required-agent"), agent_b.to_str().unwrap()).unwrap();
            command.env("SSH_AUTH_SOCK", agent_b);
            expected_receipts = 3;
        }
        Case::MissingAgent => {
            let missing = base.join("missing.sock");
            fs::write(base.join("required-agent"), missing.to_str().unwrap()).unwrap();
            command.env("SSH_AUTH_SOCK", missing);
            expected_receipts = 1;
        }
        Case::SelfAlias => {
            fs::write(profiles.join("endpoints.json"),serde_json::to_vec(&serde_json::json!({"version":1,"ssh":[{"id":SAVED_PROFILE,"label":"worker","target":"owned-fake-host","session":"origin","enabled":true}]})).unwrap()).unwrap();
            fs::write(base.join("target-socket"), origin.to_str().unwrap()).unwrap();
            command = native_command(base, origin, Some(source_pane));
            command.args(["--machine", "worker", "agent", "prompt", "parent", prompt]);
            expected_receipts = 1;
        }
        Case::LostReply => {
            fs::write(base.join("transport-mode"), "lost-reply").unwrap();
            command.args(["--wait", "--until", "done", "--timeout", "1000"]);
        }
        Case::HiddenOutcome => {
            fs::write(base.join("transport-mode"), "hidden-outcome").unwrap();
            expected_receipts = 1;
        }
        Case::PrewriteRefusal => {
            fs::write(base.join("transport-mode"), "prewrite-refusal").unwrap();
            expected_receipts = 1;
        }
        Case::DisabledAgent => {}
        Case::EmptyAgent => {
            command.env("SSH_AUTH_SOCK", "");
        }
        Case::NonUtfAgent => {
            use std::os::unix::ffi::OsStringExt;
            fs::write(base.join("required-agent"), [0xff]).unwrap();
            command.env("SSH_AUTH_SOCK", std::ffi::OsString::from_vec(vec![0xff]));
            expected_receipts = 1;
        }
        Case::Concurrent => unreachable!(),
    }
    let began = Instant::now();
    let output = command.output().unwrap();
    if matches!(case, Case::LostReply | Case::HiddenOutcome) {
        assert_eq!(output.status.code(), Some(1));
        let error: serde_json::Value = serde_json::from_slice(&output.stderr).unwrap();
        assert_eq!(error["error"]["code"], "coordination_forward_unconfirmed");
        assert!(
            began.elapsed() < Duration::from_secs(9),
            "outcome recovery must be bounded"
        );
    } else {
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let delivered = fs::read_to_string(base.join("delivered")).unwrap_or_default();
        if delivered.lines().filter(|line| *line == prompt).count() == 1 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "one actual delivery missing: {delivered}"
        );
        thread::sleep(Duration::from_millis(10));
    }
    let page = coordination_page(origin);
    assert_eq!(
        page["result"]["receipts"].as_array().unwrap().len(),
        expected_receipts,
        "{page}"
    );
    if matches!(case, Case::DisabledAgent | Case::EmptyAgent) {
        let auth = fs::read_to_string(base.join("auth-calls")).unwrap();
        assert!(
            auth.lines().skip(auth_before).all(str::is_empty),
            "disabled CLI agent was replaced: {auth}"
        );
    }
    if matches!(case, Case::NonUtfAgent) {
        let auth = fs::read(base.join("auth-calls")).unwrap();
        assert!(auth
            .split(|byte| *byte == b'\n')
            .skip(auth_before)
            .filter(|line| !line.is_empty())
            .all(|line| line == [0xff]));
    }
    if matches!(case, Case::MissingAgent) {
        let auth = fs::read_to_string(base.join("auth-calls")).unwrap();
        for selected in auth.lines().skip(auth_before) {
            assert_eq!(selected, base.join("missing.sock").to_str().unwrap());
        }
    }
    if matches!(case, Case::ExplicitAgent) {
        let auth = fs::read_to_string(base.join("auth-calls")).unwrap();
        assert!(
            auth.lines()
                .filter(|line| *line == agent_b.to_str().unwrap())
                .count()
                >= 3
        );
    }
    let delivered = fs::read_to_string(base.join("delivered")).unwrap();
    assert_eq!(delivered.lines().filter(|line| *line == prompt).count(), 1);
    assert!(coordination_page(target)["result"]["receipts"].is_array());
}
