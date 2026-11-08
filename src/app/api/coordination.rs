mod forward;

use crate::api::schema::{
    AgentCoordinationEndpoint, AgentCoordinationKind, AgentCoordinationListParams,
    AgentCoordinationStartParams, AgentInfo, ResponseResult,
};
use crate::app::App;

use super::responses::{encode_error, encode_error_body, encode_success};

pub(crate) struct PendingLaunch {
    pub(super) id: String,
    source: Option<AgentCoordinationEndpoint>,
    target: AgentCoordinationEndpoint,
}

fn same_occupancy(a: &AgentCoordinationEndpoint, b: &AgentCoordinationEndpoint) -> bool {
    a.server_id == b.server_id
        && a.boot_id == b.boot_id
        && a.terminal_id == b.terminal_id
        && a.agent_instance_id == b.agent_instance_id
        && a.agent == b.agent
}

impl App {
    pub(crate) fn coordination_endpoint(
        &self,
        agent: &AgentInfo,
    ) -> Option<AgentCoordinationEndpoint> {
        let terminal = self.state.terminals.get(agent.terminal_id.as_str())?;
        let label = terminal
            .effective_agent_label()
            .map(str::to_owned)
            .or_else(|| {
                terminal
                    .managed_agent_kind()
                    .map(|kind| crate::detect::agent_label(kind).to_owned())
            })?;
        let journal = self.coordination.lock().ok()?;
        Some(AgentCoordinationEndpoint {
            server_id: journal.server_id().ok()?.to_owned(),
            boot_id: journal.boot_id.clone(),
            terminal_id: agent.terminal_id.clone(),
            agent_instance_id: terminal.coordination_instance_id().to_owned(),
            agent: label,
            pane_id: agent.pane_id.clone(),
            name: agent.name.clone(),
        })
    }

    pub(crate) fn validate_coordination_caller(
        &self,
        caller: &AgentCoordinationEndpoint,
    ) -> Result<AgentCoordinationEndpoint, String> {
        // parse_pane_id also recognizes the calling process's inherited pre-move alias.
        let (workspace, pane) = self
            .parse_pane_id(&caller.pane_id)
            .ok_or("coordination caller is unavailable")?;
        let agent = self
            .agent_info(workspace, pane)
            .ok_or("coordination caller is not an agent")?;
        if agent.launch_pending {
            return Err("coordination caller is not ready".into());
        }
        let current = self
            .coordination_endpoint(&agent)
            .ok_or("coordination caller cannot be identified")?;
        if !same_occupancy(&current, caller) {
            return Err("coordination caller has changed".into());
        }
        Ok(current)
    }

    pub(super) fn handle_agent_coordination_start(
        &mut self,
        id: String,
        params: AgentCoordinationStartParams,
    ) -> String {
        let source = self.validate_coordination_caller(&params.caller).ok();
        let (agent, argv) = match self.start_agent(params.operation) {
            Ok(started) => started,
            Err(error) => return encode_error_body(id, self.agent_start_error_body(error)),
        };
        if let (Some(source), Some(target)) = (source, self.coordination_endpoint(&agent)) {
            self.pending_coordination_launches.insert(
                agent.terminal_id.clone(),
                PendingLaunch {
                    id: crate::coordination::new_id(),
                    source: Some(source),
                    target,
                },
            );
        }
        encode_success(id, ResponseResult::AgentStarted { agent, argv })
    }

    pub(super) fn handle_agent_coordination_execute(
        &mut self,
        id: String,
        params: crate::api::schema::AgentCoordinationExecuteParams,
    ) -> String {
        let crate::api::schema::AgentCoordinationOperation::Start(operation) = params.operation
        else {
            return encode_error(
                id,
                "invalid_request",
                "coordination prompt is handled asynchronously",
            );
        };
        if params.operation_id.is_empty() || params.operation_id.len() > 128 {
            return encode_error(id, "invalid_request", "invalid coordination operation ID");
        }
        let exists = self
            .coordination
            .lock()
            .ok()
            .is_some_and(|journal| journal.operation_exists(&params.operation_id));
        if exists
            || self
                .pending_coordination_launches
                .values()
                .any(|pending| pending.id == params.operation_id)
        {
            return encode_error(
                id,
                "coordination_operation_exists",
                "inspect the existing operation instead of repeating its input",
            );
        }
        let (agent, argv) = match self.start_agent(operation) {
            Ok(started) => started,
            Err(error) => return encode_error_body(id, self.agent_start_error_body(error)),
        };
        if let Some(target) = self.coordination_endpoint(&agent) {
            self.pending_coordination_launches.insert(
                agent.terminal_id.clone(),
                PendingLaunch {
                    id: params.operation_id,
                    source: None,
                    target,
                },
            );
        }
        encode_success(id, ResponseResult::AgentStarted { agent, argv })
    }

    pub(super) fn handle_agent_coordination_result(
        &self,
        id: String,
        params: crate::api::schema::AgentCoordinationResultParams,
    ) -> String {
        let Ok(journal) = self.coordination.lock() else {
            return encode_error(
                id,
                "coordination_unavailable",
                "coordination journal is unavailable",
            );
        };
        match journal.outcome(&params.operation_id) {
            Ok(outcome) => encode_success(id, ResponseResult::AgentCoordinationResult { outcome }),
            Err(error) => encode_error(id, "coordination_unavailable", error),
        }
    }

    /// Called when readiness is reconciled, including the CLI's native startup poll.
    pub(crate) fn complete_coordination_launch(&mut self, agent: &AgentInfo) {
        let Some(pending) = self.pending_coordination_launches.get(&agent.terminal_id) else {
            return;
        };
        let Some(current) = self
            .coordination_endpoint(agent)
            .filter(|current| same_occupancy(current, &pending.target))
        else {
            self.pending_coordination_launches
                .remove(&agent.terminal_id);
            return;
        };
        let ready_state = matches!(
            agent.agent_status,
            crate::api::schema::AgentStatus::Idle | crate::api::schema::AgentStatus::Done
        ) || (agent.agent_status == crate::api::schema::AgentStatus::Unknown
            && pending.target.agent == "codex");
        if agent.launch_pending || !agent.interactive_ready || !ready_state {
            return;
        }
        let Some(pending) = self
            .pending_coordination_launches
            .remove(&agent.terminal_id)
        else {
            return;
        };
        if let Ok(mut journal) = self.coordination.lock() {
            if let Err(error) = journal.complete(
                pending.id,
                AgentCoordinationKind::Delegation,
                pending.source,
                current,
            ) {
                tracing::warn!(error, "could not persist successful agent delegation");
            }
        }
    }

    pub(super) fn handle_agent_coordination_list(
        &mut self,
        id: String,
        params: AgentCoordinationListParams,
    ) -> String {
        self.pending_coordination_launches
            .retain(|terminal_id, pending| {
                self.state
                    .terminals
                    .get(terminal_id.as_str())
                    .is_some_and(|terminal| {
                        terminal.coordination_instance_id() == pending.target.agent_instance_id
                            && terminal.managed_agent_kind().is_some()
                    })
            });
        let infos = self.collect_agent_infos();
        for info in &infos {
            self.complete_coordination_launch(info);
        }
        let agents = infos
            .iter()
            .filter(|info| !info.launch_pending)
            .filter_map(|info| self.coordination_endpoint(info))
            .collect();
        let Ok(journal) = self.coordination.lock() else {
            return encode_error(
                id,
                "coordination_unavailable",
                "coordination journal is unavailable",
            );
        };
        let server_id = match journal.server_id() {
            Ok(value) => value.to_owned(),
            Err(error) => return encode_error(id, "coordination_unavailable", error),
        };
        match journal.page(params.cursor.as_deref(), params.limit) {
            Ok((receipts, cursor)) => encode_success(
                id,
                ResponseResult::AgentCoordination {
                    version: 1,
                    server_id,
                    boot_id: journal.boot_id.clone(),
                    agents,
                    receipts,
                    cursor,
                },
            ),
            Err(error) => encode_error(id, "coordination_unavailable", error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::schema::{
        AgentCoordinationPromptParams, AgentPromptParams, AgentTarget, Method, Request,
    };
    use crate::detect::{Agent, AgentState};

    fn fixture() -> (App, crate::layout::PaneId, AgentCoordinationEndpoint) {
        let (_api_tx, api_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut app = App::new(
            &crate::config::Config::default(),
            crate::app::AppPolicy::TEST,
            None,
            api_rx,
            crate::api::EventHub::default(),
        );
        app.state.workspaces = vec![
            crate::workspace::Workspace::test_new("source"),
            crate::workspace::Workspace::test_new("target"),
        ];
        app.state.ensure_test_terminals();
        app.state.active = Some(0);
        app.state.selected = 0;
        for (index, name) in ["parent", "child"].into_iter().enumerate() {
            let pane = app.state.workspaces[index].tabs[0].root_pane;
            let terminal = app.state.workspaces[index].tabs[0].panes[&pane]
                .attached_terminal_id
                .clone();
            let terminal = app.state.terminals.get_mut(&terminal).unwrap();
            terminal.set_detected_state(Some(Agent::OpenCode), AgentState::Idle);
            terminal.set_agent_name(name.into());
        }
        let source = app.agent_info_for_target("parent").unwrap();
        let caller = app.coordination_endpoint(&source).unwrap();
        let target = app.state.workspaces[1].tabs[0].root_pane;
        (app, target, caller)
    }

    #[tokio::test]
    async fn coordination_execute_prompt_rejects_concurrent_and_completed_duplicate_ids() {
        let (mut app, target, _) = fixture();
        let (runtime, mut output) = crate::terminal::TerminalRuntime::test_with_channel(80, 24);
        app.state.insert_test_runtime(target, runtime);
        let request = || Request {
            id: "execute".into(),
            method: Method::AgentCoordinationExecute(
                crate::api::schema::AgentCoordinationExecuteParams {
                    operation_id: "one-native-operation".into(),
                    operation: crate::api::schema::AgentCoordinationOperation::Prompt(
                        AgentPromptParams {
                            target: "child".into(),
                            text: "deliver once".into(),
                            wait: None,
                        },
                    ),
                },
            ),
        };
        let (first_tx, first_rx) = std::sync::mpsc::channel();
        app.handle_deferred_agent_api_request(request(), first_tx);
        let (second_tx, second_rx) = std::sync::mpsc::channel();
        app.handle_deferred_agent_api_request(request(), second_tx);
        let duplicate: serde_json::Value = serde_json::from_str(
            &second_rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .unwrap(),
        )
        .unwrap();
        assert_eq!(duplicate["error"]["code"], "coordination_operation_exists");
        let delivered: serde_json::Value = serde_json::from_str(
            &first_rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .unwrap(),
        )
        .unwrap();
        assert_eq!(delivered["result"]["type"], "agent_prompted");
        assert_eq!(output.try_recv().unwrap().as_ref(), b"deliver once");
        assert_eq!(output.try_recv().unwrap().as_ref(), b"\r");
        let (third_tx, third_rx) = std::sync::mpsc::channel();
        app.handle_deferred_agent_api_request(request(), third_tx);
        let repeated: serde_json::Value = serde_json::from_str(
            &third_rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .unwrap(),
        )
        .unwrap();
        assert_eq!(repeated["error"]["code"], "coordination_operation_exists");
        assert!(output.try_recv().is_err());
        assert!(app
            .coordination
            .lock()
            .unwrap()
            .outcome("one-native-operation")
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn coordination_prompt_records_only_after_success_and_survives_closed_panes() {
        let (mut app, target, caller) = fixture();
        let (runtime, mut output) = crate::terminal::TerminalRuntime::test_with_channel(80, 24);
        app.state.insert_test_runtime(target, runtime);
        let (respond_to, reply) = std::sync::mpsc::channel();
        assert!(app.handle_deferred_agent_api_request(
            Request {
                id: "observed".into(),
                method: Method::AgentCoordinationPrompt(AgentCoordinationPromptParams {
                    caller: caller.clone(),
                    operation: AgentPromptParams {
                        target: "child".into(),
                        text: "private prompt must not be stored".into(),
                        wait: None
                    }
                })
            },
            respond_to
        ));
        assert!(app
            .coordination
            .lock()
            .unwrap()
            .page(None, None)
            .unwrap()
            .0
            .is_empty());
        let response = reply
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        assert!(
            serde_json::from_str::<crate::api::schema::SuccessResponse>(&response).is_ok(),
            "{response}"
        );
        assert_eq!(
            output.try_recv().unwrap().as_ref(),
            b"private prompt must not be stored"
        );
        assert_eq!(output.try_recv().unwrap().as_ref(), b"\r");
        let receipts = app.coordination.lock().unwrap().page(None, None).unwrap().0;
        assert_eq!(receipts.len(), 1);
        assert_eq!(
            receipts[0].source.agent_instance_id,
            caller.agent_instance_id
        );
        assert_ne!(
            receipts[0].source.agent_instance_id,
            receipts[0].target.agent_instance_id
        );
        assert_eq!(receipts[0].kind, AgentCoordinationKind::Communication);
        assert!(!serde_json::to_string(&receipts)
            .unwrap()
            .contains("private prompt"));
        app.state.workspaces.clear();
        app.state.terminals.clear();
        assert_eq!(
            app.coordination.lock().unwrap().page(None, None).unwrap().0,
            receipts
        );
    }

    #[tokio::test]
    async fn coordination_prompt_does_not_record_blocked_or_replaced_recipients() {
        let (mut app, target, caller) = fixture();
        let terminal_id = app.state.workspaces[1].tabs[0].panes[&target]
            .attached_terminal_id
            .clone();
        let (runtime, mut output) = crate::terminal::TerminalRuntime::test_with_channel(80, 24);
        app.state.insert_test_runtime(target, runtime);
        app.state
            .terminals
            .get_mut(&terminal_id)
            .unwrap()
            .set_detected_state(Some(Agent::OpenCode), AgentState::Blocked);
        let prompt = || Request {
            id: "observed".into(),
            method: Method::AgentCoordinationPrompt(AgentCoordinationPromptParams {
                caller: caller.clone(),
                operation: AgentPromptParams {
                    target: "child".into(),
                    text: "hello".into(),
                    wait: None,
                },
            }),
        };
        let (respond_to, reply) = std::sync::mpsc::channel();
        app.handle_deferred_agent_api_request(prompt(), respond_to);
        let response: serde_json::Value = serde_json::from_str(
            &reply
                .recv_timeout(std::time::Duration::from_secs(1))
                .unwrap(),
        )
        .unwrap();
        assert_eq!(response["error"]["code"], "agent_blocked");
        assert!(output.try_recv().is_err());
        assert!(app
            .coordination
            .lock()
            .unwrap()
            .page(None, None)
            .unwrap()
            .0
            .is_empty());
        app.state
            .terminals
            .get_mut(&terminal_id)
            .unwrap()
            .set_detected_state(Some(Agent::OpenCode), AgentState::Idle);
        let (respond_to, reply) = std::sync::mpsc::channel();
        app.handle_deferred_agent_api_request(prompt(), respond_to);
        app.state
            .terminals
            .get_mut(&terminal_id)
            .unwrap()
            .clear_agent_runtime_identity_after_respawn();
        app.state
            .terminals
            .get_mut(&terminal_id)
            .unwrap()
            .set_detected_state(Some(Agent::OpenCode), AgentState::Idle);
        let _ = reply
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        assert!(app
            .coordination
            .lock()
            .unwrap()
            .page(None, None)
            .unwrap()
            .0
            .is_empty());
    }

    #[test]
    fn coordination_source_proof_rejects_same_terminal_with_a_later_agent() {
        let (mut app, _, caller) = fixture();
        assert!(app.validate_coordination_caller(&caller).is_ok());
        let terminal = app
            .state
            .terminals
            .get_mut(caller.terminal_id.as_str())
            .unwrap();
        terminal.clear_agent_runtime_identity_after_respawn();
        terminal.set_detected_state(Some(Agent::OpenCode), AgentState::Idle);
        terminal.set_agent_name("parent".into());
        assert!(app.validate_coordination_caller(&caller).is_err());
        let source = app.agent_info_for_target("parent").unwrap();
        assert_ne!(
            app.coordination_endpoint(&source)
                .unwrap()
                .agent_instance_id,
            caller.agent_instance_id
        );
        // Normal source reads have no hidden journal mutation.
        let response = app.handle_agent_get(
            "source".into(),
            AgentTarget {
                target: "parent".into(),
            },
        );
        assert!(serde_json::from_str::<crate::api::schema::SuccessResponse>(&response).is_ok());
        assert!(app
            .coordination
            .lock()
            .unwrap()
            .page(None, None)
            .unwrap()
            .0
            .is_empty());
    }
    #[tokio::test]
    async fn coordination_is_passive_when_caller_or_journal_cannot_be_proved() {
        for unreadable in [false, true] {
            let (mut app, target, mut caller) = fixture();
            let directory = std::env::temp_dir().join(format!(
                "herdr-passive-coordination-{}",
                crate::coordination::new_id()
            ));
            if unreadable {
                std::fs::create_dir_all(&directory).unwrap();
                let path = directory.join("coordination.json");
                std::fs::write(&path, b"{\"version\":99}").unwrap();
                app.coordination = std::sync::Arc::new(std::sync::Mutex::new(
                    crate::coordination::Journal::open(Some(path)),
                ));
            } else {
                caller.agent_instance_id = "stale-source".into();
            }
            let (runtime, mut output) = crate::terminal::TerminalRuntime::test_with_channel(80, 24);
            app.state.insert_test_runtime(target, runtime);
            let (respond_to, reply) = std::sync::mpsc::channel();
            app.handle_deferred_agent_api_request(
                Request {
                    id: "passive".into(),
                    method: Method::AgentCoordinationPrompt(AgentCoordinationPromptParams {
                        caller: caller.clone(),
                        operation: AgentPromptParams {
                            target: "child".into(),
                            text: "deliver once".into(),
                            wait: None,
                        },
                    }),
                },
                respond_to,
            );
            let response = reply
                .recv_timeout(std::time::Duration::from_secs(2))
                .unwrap();
            assert!(
                serde_json::from_str::<crate::api::schema::SuccessResponse>(&response).is_ok(),
                "{response}"
            );
            assert_eq!(output.try_recv().unwrap().as_ref(), b"deliver once");
            assert_eq!(output.try_recv().unwrap().as_ref(), b"\r");
            assert!(output.try_recv().is_err());
            let records = app.coordination.lock().unwrap().page(None, None);
            assert!(records.is_err() || records.unwrap().0.is_empty());
            let terminal_id = app.state.workspaces[1].tabs[0].panes[&target]
                .attached_terminal_id
                .clone();
            app.state
                .terminals
                .get_mut(&terminal_id)
                .unwrap()
                .clear_agent_runtime_identity_after_respawn();
            let (runtime, mut launched) =
                crate::terminal::TerminalRuntime::test_with_channel(80, 24);
            app.terminal_runtimes.insert(terminal_id, runtime);
            let response = app.handle_agent_coordination_start(
                "passive-start".into(),
                AgentCoordinationStartParams {
                    caller,
                    operation: crate::api::schema::AgentStartParams {
                        name: "started-once".into(),
                        kind: "opencode".into(),
                        pane_id: app.public_pane_id(1, target).unwrap(),
                        args: Vec::new(),
                        timeout_ms: None,
                    },
                },
            );
            assert!(
                serde_json::from_str::<crate::api::schema::SuccessResponse>(&response).is_ok(),
                "{response}"
            );
            assert!(!launched.try_recv().unwrap().is_empty());
            assert!(launched.try_recv().is_err());
            let records = app.coordination.lock().unwrap().page(None, None);
            assert!(records.is_err() || records.unwrap().0.is_empty());
            if unreadable {
                std::fs::remove_dir_all(directory).unwrap();
            }
        }
    }

    #[tokio::test]
    async fn coordination_delegation_waits_for_the_same_launched_agent_to_be_ready() {
        let (mut app, target, caller) = fixture();
        let terminal_id = app.state.workspaces[1].tabs[0].panes[&target]
            .attached_terminal_id
            .clone();
        app.state
            .terminals
            .get_mut(&terminal_id)
            .unwrap()
            .clear_agent_runtime_identity_after_respawn();
        let (runtime, _output) = crate::terminal::TerminalRuntime::test_with_channel(80, 24);
        app.terminal_runtimes.insert(terminal_id.clone(), runtime);
        let pane_id = app.public_pane_id(1, target).unwrap();
        let response = app.handle_agent_coordination_start(
            "delegation".into(),
            AgentCoordinationStartParams {
                caller: caller.clone(),
                operation: crate::api::schema::AgentStartParams {
                    name: "delegated".into(),
                    kind: "opencode".into(),
                    pane_id,
                    args: Vec::new(),
                    timeout_ms: None,
                },
            },
        );
        assert!(
            serde_json::from_str::<crate::api::schema::SuccessResponse>(&response).is_ok(),
            "{response}"
        );
        assert!(app
            .coordination
            .lock()
            .unwrap()
            .page(None, None)
            .unwrap()
            .0
            .is_empty());
        let instance = app.state.terminals[&terminal_id]
            .coordination_instance_id()
            .to_owned();
        let terminal = app.state.terminals.get_mut(&terminal_id).unwrap();
        terminal.set_detected_state(Some(Agent::OpenCode), AgentState::Idle);
        terminal.reconcile_managed_agent_at(
            std::time::Instant::now() + std::time::Duration::from_secs(4),
            false,
        );
        let mut premature = app.agent_info(1, target).unwrap();
        assert!(premature.interactive_ready);
        for status in [
            crate::api::schema::AgentStatus::Blocked,
            crate::api::schema::AgentStatus::Working,
            crate::api::schema::AgentStatus::Unknown,
        ] {
            premature.agent_status = status;
            app.complete_coordination_launch(&premature);
            assert!(app
                .coordination
                .lock()
                .unwrap()
                .page(None, None)
                .unwrap()
                .0
                .is_empty());
        }
        let _ = app.handle_agent_get(
            "ready".into(),
            AgentTarget {
                target: "delegated".into(),
            },
        );
        let _ = app.handle_agent_get(
            "ready-again".into(),
            AgentTarget {
                target: "delegated".into(),
            },
        );
        let receipts = app.coordination.lock().unwrap().page(None, None).unwrap().0;
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].kind, AgentCoordinationKind::Delegation);
        assert_eq!(
            receipts[0].source.agent_instance_id,
            caller.agent_instance_id
        );
        assert_eq!(receipts[0].target.agent_instance_id, instance);
    }
}
