use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use crate::api::client::{ApiClient, ConnectionTarget};
use crate::api::schema::{
    AgentCoordinationDestination, AgentCoordinationEndpoint, AgentCoordinationExecuteParams,
    AgentCoordinationForwardParams, AgentCoordinationOperation, AgentCoordinationOutcome,
    AgentCoordinationResultParams, AgentTarget, Method, Request,
};
use crate::app::App;
use crate::client::endpoint::{EndpointCatalog, SavedSshEndpoint};

use super::super::responses::encode_error;

impl App {
    pub(crate) fn handle_deferred_coordination_forward(
        &mut self,
        id: String,
        params: AgentCoordinationForwardParams,
        respond_to: mpsc::Sender<String>,
    ) -> bool {
        let source = self.validate_coordination_caller(&params.caller).ok();
        let destination = match params.destination {
            AgentCoordinationDestination::SavedMachine {
                machine,
                profile_fingerprint,
            } => {
                let profiles = match EndpointCatalog::load_profiles() {
                    Ok(profiles) => profiles,
                    Err(_) => {
                        let _ = respond_to.send(encode_error(
                            id,
                            "coordination_machine_changed",
                            "saved machines cannot be read",
                        ));
                        return false;
                    }
                };
                let Some(profile) = saved_profile(&profiles, &machine).cloned() else {
                    let _ = respond_to.send(encode_error(
                        id,
                        "coordination_machine_changed",
                        "select an enabled saved machine by ID or unique label",
                    ));
                    return false;
                };
                if crate::coordination::profile_fingerprint(
                    profile.id.as_str(),
                    &profile.target,
                    &profile.session,
                ) != profile_fingerprint
                {
                    let _ = respond_to.send(encode_error(
                        id,
                        "coordination_machine_changed",
                        "the saved machine changed before forwarding",
                    ));
                    return false;
                }
                Destination::Saved(profile)
            }
            AgentCoordinationDestination::LocalSession { session } => {
                if crate::session::validate_name(&session).is_err() {
                    let _ = respond_to.send(encode_error(
                        id,
                        "invalid_request",
                        "invalid named session",
                    ));
                    return false;
                }
                Destination::Local(session)
            }
        };
        let agent_environment = match params.ssh_agent {
            Some(crate::api::schema::AgentCoordinationSshAgent::Registered { socket_path }) => {
                crate::remote::SshAgentEnvironment::Socket(socket_path.into())
            }
            Some(crate::api::schema::AgentCoordinationSshAgent::Disabled) => {
                crate::remote::SshAgentEnvironment::Disabled
            }
            None => crate::platform::registered_ssh_agent_socket()
                .map(crate::remote::SshAgentEnvironment::Socket)
                .unwrap_or_default(),
        };
        let journal = self.coordination.clone();
        let failure_id = id.clone();
        let failure_response = respond_to.clone();
        let spawned = crate::thread_spawn::spawn_named("herdr-agent-forward", move || {
            let result = (|| {
                match destination {
                    Destination::Saved(profile) => {
                        // Configured profiles choose the transport; request data supplies no host, URL or command.
                        let bridge = crate::remote::SavedSshApiBridge::start_coordination(
                            profile.id.as_str(),
                            &profile.target,
                            &profile.session,
                            agent_environment.clone(),
                        );
                        let bridge = match bridge {
                            Ok(bridge) => bridge,
                            Err(_) => return Ok(forward_error(&id, "coordination_forward_not_started", "remote transport could not be prepared; no operation was submitted")),
                        };
                        let client = ApiClient::for_target(ConnectionTarget::SocketPath(
                            bridge.socket_path().to_owned(),
                        ));
                        // A native SSH bridge owns one stream at a time. Keep receipt reads
                        // independent of an operation whose --wait may still be running.
                        let receipt_bridge = crate::remote::SavedSshApiBridge::start_coordination(
                            profile.id.as_str(),
                            &profile.target,
                            &profile.session,
                            agent_environment.clone(),
                        );
                        let receipt_bridge = match receipt_bridge {
                            Ok(bridge) => bridge,
                            Err(_) => return Ok(forward_error(&id, "coordination_forward_not_started", "receipt transport could not be prepared; no operation was submitted")),
                        };
                        let receipt_client = ApiClient::for_target(ConnectionTarget::SocketPath(
                            receipt_bridge.socket_path().to_owned(),
                        ));
                        let response = forward_operation(
                            client,
                            receipt_client,
                            source,
                            journal,
                            params.operation,
                            id.clone(),
                        );
                        drop(receipt_bridge);
                        drop(bridge);
                        response
                    }
                    Destination::Local(session) => forward_operation(
                        ApiClient::for_target(ConnectionTarget::LocalSession(Some(
                            session.clone(),
                        ))),
                        ApiClient::for_target(ConnectionTarget::LocalSession(Some(session))),
                        source,
                        journal,
                        params.operation,
                        id.clone(),
                    ),
                }
            })();
            let response = match result {
                Ok(response) => serde_json::to_string(&response).unwrap_or_else(|_| encode_error(id.clone(), "coordination_forward_failed", "remote response could not be encoded")),
                Err(_) => encode_error(id, "coordination_forward_unconfirmed", "the remote operation could not be confirmed; inspect the target before retrying"),
            };
            let _ = respond_to.send(response);
        });
        if spawned.is_err() {
            let _ = failure_response.send(encode_error(
                failure_id,
                "coordination_forward_failed",
                "remote operation was not started",
            ));
            return false;
        }
        true
    }
}

enum Destination {
    Saved(SavedSshEndpoint),
    Local(String),
}

fn saved_profile<'a>(
    profiles: &'a [SavedSshEndpoint],
    selector: &str,
) -> Option<&'a SavedSshEndpoint> {
    if let Some(profile) = profiles
        .iter()
        .find(|profile| profile.id.as_str() == selector)
    {
        return profile.enabled.then_some(profile);
    }
    let mut matches = profiles.iter().filter(|profile| profile.label == selector);
    let profile = matches.next()?;
    (profile.enabled && matches.next().is_none()).then_some(profile)
}

fn forward_error(id: &str, code: &str, message: &str) -> serde_json::Value {
    serde_json::json!({"id": id, "error": {"code": code, "message": message}})
}

fn read_outcome(client: &ApiClient, operation_id: &str) -> Option<AgentCoordinationOutcome> {
    read_outcome_with_timeout(client, operation_id, Duration::from_secs(5))
}

fn read_outcome_with_timeout(
    client: &ApiClient,
    operation_id: &str,
    timeout: Duration,
) -> Option<AgentCoordinationOutcome> {
    let response = client
        .request_value_with_timeout(
            &Request {
                id: "coordination:result".into(),
                method: Method::AgentCoordinationResult(AgentCoordinationResultParams {
                    operation_id: operation_id.into(),
                }),
            },
            timeout,
        )
        .ok()?;
    serde_json::from_value(response["result"]["outcome"].clone()).ok()
}

fn persist_outcome(
    journal: &Arc<Mutex<crate::coordination::Journal>>,
    source: &AgentCoordinationEndpoint,
    outcome: AgentCoordinationOutcome,
) {
    if let Ok(mut journal) = journal.lock() {
        if let Err(error) = journal.record_remote(source.clone(), outcome) {
            tracing::warn!(
                error,
                "could not persist successful cross-machine coordination"
            );
        }
    }
}

fn recover_outcome(
    client: &ApiClient,
    operation_id: &str,
    remote_server: &str,
    source: Option<&AgentCoordinationEndpoint>,
    journal: &Arc<Mutex<crate::coordination::Journal>>,
    operation: &AgentCoordinationOperation,
) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return;
        }
        // Reconcile an accepted launch through the existing read-only native route;
        // only its exact operation outcome can establish a relationship.
        if let AgentCoordinationOperation::Start(params) = operation {
            let _ = client.request_value_with_timeout(
                &Request {
                    id: "coordination:reconcile".into(),
                    method: Method::AgentGet(AgentTarget {
                        target: params.name.clone(),
                    }),
                },
                remaining.min(Duration::from_millis(250)),
            );
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return;
        }
        if let Some(outcome) = read_outcome_with_timeout(
            client,
            operation_id,
            remaining.min(Duration::from_millis(500)),
        )
        .filter(|outcome| outcome.id == operation_id && outcome.target.server_id == remote_server)
        {
            if let Some(source) = source {
                persist_outcome(journal, source, outcome);
            }
            return;
        }
        std::thread::sleep(
            deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(100)),
        );
    }
}

/// Observe completion independently of the CLI's connection or the native --wait result.
fn forward_operation(
    client: ApiClient,
    receipt_client: ApiClient,
    source: Option<AgentCoordinationEndpoint>,
    journal: Arc<Mutex<crate::coordination::Journal>>,
    operation: AgentCoordinationOperation,
    request_id: String,
) -> std::io::Result<serde_json::Value> {
    let capability = client
        .request_value_with_timeout(
            &Request {
                id: "coordination:capability".into(),
                method: Method::AgentCoordinationList(Default::default()),
            },
            Duration::from_secs(10),
        )
        .map_err(std::io::Error::other);
    let capability = match capability {
        Ok(capability) => capability,
        Err(_) => {
            return Ok(forward_error(
                &request_id,
                "coordination_forward_not_started",
                "remote capability could not be read; no operation was submitted",
            ))
        }
    };
    let remote_server = capability["result"]["server_id"]
        .as_str()
        .filter(|_| capability["result"]["version"] == 1);
    let Some(remote_server) = remote_server.map(str::to_owned) else {
        return Ok(forward_error(
            &request_id,
            "coordination_unsupported",
            "the remote server does not provide coordination receipts",
        ));
    };
    let receipt_namespace = receipt_client.request_value_with_timeout(
        &Request {
            id: "coordination:receipt-capability".into(),
            method: Method::AgentCoordinationList(Default::default()),
        },
        Duration::from_secs(10),
    );
    if !receipt_namespace
        .is_ok_and(|response| response["result"]["server_id"].as_str() == Some(&remote_server))
    {
        return Ok(forward_error(
            &request_id,
            "coordination_forward_not_started",
            "receipt server could not be verified; no operation was submitted",
        ));
    }
    let operation_id = crate::coordination::new_id();
    let request = Request {
        id: request_id.clone(),
        method: Method::AgentCoordinationExecute(AgentCoordinationExecuteParams {
            operation_id: operation_id.clone(),
            operation: operation.clone(),
        }),
    };
    let (reply_tx, reply_rx) = mpsc::channel();
    let remote = client.clone();
    crate::thread_spawn::spawn_named("herdr-agent-forward-call", move || {
        let _ = reply_tx.send(remote.request_value(&request));
    })?;
    let mut recorded = false;
    let response = loop {
        match reply_rx.recv_timeout(Duration::from_millis(100)) {
            Ok(response) => break response.map_err(std::io::Error::other),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if !recorded {
                    if let Some(outcome) =
                        read_outcome(&receipt_client, &operation_id).filter(|outcome| {
                            outcome.id == operation_id && outcome.target.server_id == remote_server
                        })
                    {
                        if let Some(source) = &source {
                            persist_outcome(&journal, source, outcome);
                        }
                        recorded = true;
                    }
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                break Err(std::io::Error::other("remote call closed"))
            }
        }
    };
    let mut response = match response {
        Ok(response) => response,
        Err(error) => {
            if !recorded {
                recover_outcome(
                    &receipt_client,
                    &operation_id,
                    &remote_server,
                    source.as_ref(),
                    &journal,
                    &operation,
                );
            }
            return Err(error);
        }
    };
    if let AgentCoordinationOperation::Start(params) = &operation {
        if response.get("error").is_none() {
            let terminal = response["result"]["agent"]["terminal_id"]
                .as_str()
                .unwrap_or_default()
                .to_owned();
            let deadline =
                Instant::now() + Duration::from_millis(params.timeout_ms.unwrap_or(30_000));
            loop {
                let status = match client.request_value_with_timeout(
                    &Request {
                        id: request_id.clone(),
                        method: Method::AgentGet(AgentTarget {
                            target: params.name.clone(),
                        }),
                    },
                    Duration::from_secs(5),
                ) {
                    Ok(status) => status,
                    Err(error) => {
                        if !recorded {
                            recover_outcome(
                                &receipt_client,
                                &operation_id,
                                &remote_server,
                                source.as_ref(),
                                &journal,
                                &operation,
                            );
                        }
                        return Err(std::io::Error::other(error));
                    }
                };
                let agent = &status["result"]["agent"];
                if status.get("error").is_none() && agent["terminal_id"].as_str() != Some(&terminal)
                {
                    return Ok(forward_error(
                        &request_id,
                        "agent_name_lost",
                        "the remote target changed during startup",
                    ));
                }
                let ready_state = matches!(agent["agent_status"].as_str(), Some("idle" | "done"))
                    || (crate::detect::parse_agent_label(&params.kind)
                        == Some(crate::detect::Agent::Codex)
                        && agent["agent_status"] == "unknown");
                if agent["interactive_ready"] == true
                    && agent["launch_pending"] != true
                    && ready_state
                {
                    response["result"]["agent"] = agent.clone();
                    break;
                }
                if agent["agent_status"] == "blocked" || Instant::now() >= deadline {
                    return Ok(forward_error(
                        &request_id,
                        "agent_not_ready",
                        "the remote agent did not become ready",
                    ));
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
    if !recorded {
        if let Some(outcome) = read_outcome(&receipt_client, &operation_id).filter(|outcome| {
            outcome.id == operation_id && outcome.target.server_id == remote_server
        }) {
            if let Some(source) = &source {
                persist_outcome(&journal, source, outcome);
            }
        }
    }
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn coordination_forwarded_start_accepts_native_canonical_codex_kinds() {
        use std::io::{BufRead, BufReader, Write};
        use std::os::unix::net::UnixListener;
        for kind in ["CODEX", "codex.exe"] {
            let path =
                std::env::temp_dir().join(format!("hkind-{}.sock", crate::coordination::new_id()));
            let listener = UnixListener::bind(&path).unwrap();
            listener.set_nonblocking(true).unwrap();
            let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let stopped = stop.clone();
            let (ready, pane_id) = std::sync::mpsc::channel();
            let server = std::thread::spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(async {
                        let (_, receiver) = tokio::sync::mpsc::unbounded_channel();
                        let mut app = App::new(
                            &crate::config::Config::default(),
                            crate::app::AppPolicy::TEST,
                            None,
                            receiver,
                            crate::api::EventHub::default(),
                        );
                        app.state.workspaces =
                            vec![crate::workspace::Workspace::test_new("target")];
                        app.state.ensure_test_terminals();
                        let pane = app.state.workspaces[0].tabs[0].root_pane;
                        ready.send(app.public_pane_id(0, pane).unwrap()).unwrap();
                        let terminal_id = app.state.workspaces[0].tabs[0].panes[&pane]
                            .attached_terminal_id
                            .clone();
                        let (runtime, mut input) =
                            crate::terminal::TerminalRuntime::test_with_channel(80, 24);
                        app.terminal_runtimes.insert(terminal_id.clone(), runtime);
                        let mut starts = 0;
                        while !stopped.load(std::sync::atomic::Ordering::Relaxed) {
                            let (mut stream, _) = match listener.accept() {
                                Ok(stream) => stream,
                                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                                    std::thread::sleep(Duration::from_millis(2));
                                    continue;
                                }
                                Err(error) => panic!("{error}"),
                            };
                            let mut line = String::new();
                            BufReader::new(stream.try_clone().unwrap())
                                .read_line(&mut line)
                                .unwrap();
                            let request: Request = serde_json::from_str(&line).unwrap();
                            let start = matches!(
                                &request.method,
                                Method::AgentCoordinationExecute(AgentCoordinationExecuteParams {
                                    operation: AgentCoordinationOperation::Start(_),
                                    ..
                                })
                            );
                            let response = app.handle_api_request(request);
                            if start {
                                starts += 1;
                                let value: serde_json::Value =
                                    serde_json::from_str(&response).unwrap();
                                eprintln!("native start {kind}: {response}");
                                assert_eq!(value["result"]["type"], "agent_started", "{response}");
                                assert_eq!(
                                    value["result"]["argv"][0], "codex",
                                    "{kind}: {response}"
                                );
                                let terminal = app.state.terminals.get_mut(&terminal_id).unwrap();
                                terminal.set_detected_state(
                                    Some(crate::detect::Agent::Codex),
                                    crate::detect::AgentState::Unknown,
                                );
                                terminal.observe_codex_prompt_ready(true);
                                terminal.reconcile_managed_agent_at(
                                    Instant::now() + Duration::from_millis(3100),
                                    false,
                                );
                                assert!(terminal.managed_agent_interactive_ready());
                            }
                            writeln!(stream, "{response}").unwrap();
                        }
                        assert_eq!(starts, 1);
                        assert!(!input.try_recv().unwrap().is_empty());
                        assert!(input.try_recv().is_err());
                    });
            });
            let client = ApiClient::for_target(ConnectionTarget::SocketPath(path.clone()));
            let pane_id = pane_id.recv_timeout(Duration::from_secs(5)).unwrap();
            let response = forward_operation(
                client.clone(),
                client,
                None,
                Arc::new(Mutex::new(crate::coordination::Journal::open(None))),
                AgentCoordinationOperation::Start(crate::api::schema::AgentStartParams {
                    name: "child".into(),
                    kind: kind.into(),
                    pane_id,
                    args: Vec::new(),
                    timeout_ms: Some(4000),
                }),
                "canonical".into(),
            );
            stop.store(true, std::sync::atomic::Ordering::Relaxed);
            server.join().unwrap();
            std::fs::remove_file(path).unwrap();
            let response = response.unwrap();
            assert_eq!(response["result"]["type"], "agent_started", "{response}");
            assert_eq!(response["result"]["agent"]["agent_status"], "unknown");
        }
    }

    #[test]
    fn coordination_forwarding_accepts_only_enabled_unambiguous_saved_profiles() {
        let first = SavedSshEndpoint::new("worker", "example.invalid", "default").unwrap();
        let second = SavedSshEndpoint::new("worker", "other.invalid", "default").unwrap();
        let mut disabled =
            SavedSshEndpoint::new("disabled", "disabled.invalid", "default").unwrap();
        disabled.enabled = false;
        let profiles = [first.clone(), second, disabled];
        assert!(saved_profile(&profiles, "worker").is_none());
        assert!(saved_profile(&profiles, "disabled").is_none());
        assert!(saved_profile(&profiles, "ssh://example.invalid").is_none());
        assert_eq!(saved_profile(&profiles, first.id.as_str()), Some(&first));
    }
}
