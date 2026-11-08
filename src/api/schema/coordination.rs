use serde::{Deserialize, Serialize};

use super::{AgentPromptParams, AgentStartParams};

/// One occupancy of a terminal. Pane and display names are labels, never identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AgentCoordinationEndpoint {
    pub server_id: String,
    pub boot_id: String,
    pub terminal_id: String,
    pub agent_instance_id: String,
    pub agent: String,
    pub pane_id: String,
    pub name: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentCoordinationKind {
    Delegation,
    Communication,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AgentCoordinationReceipt {
    pub id: String,
    pub kind: AgentCoordinationKind,
    pub source: AgentCoordinationEndpoint,
    pub target: AgentCoordinationEndpoint,
    pub occurred_at: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AgentCoordinationListParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AgentCoordinationStartParams {
    pub caller: AgentCoordinationEndpoint,
    pub operation: AgentStartParams,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AgentCoordinationPromptParams {
    pub caller: AgentCoordinationEndpoint,
    pub operation: AgentPromptParams,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "type", content = "params", rename_all = "snake_case")]
pub enum AgentCoordinationOperation {
    Start(AgentStartParams),
    Prompt(AgentPromptParams),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AgentCoordinationForwardParams {
    pub caller: AgentCoordinationEndpoint,
    pub destination: AgentCoordinationDestination,
    pub operation: AgentCoordinationOperation,
    /// Transient operation-scoped agent selection; never part of durable receipts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ssh_agent: Option<AgentCoordinationSshAgent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentCoordinationSshAgent {
    Registered { socket_path: String },
    Disabled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentCoordinationDestination {
    SavedMachine {
        machine: String,
        profile_fingerprint: String,
    },
    LocalSession {
        session: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AgentCoordinationExecuteParams {
    pub operation_id: String,
    pub operation: AgentCoordinationOperation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AgentCoordinationResultParams {
    pub operation_id: String,
}

/// A successful operation receipt returned to its originating daemon, without a claimed caller.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AgentCoordinationOutcome {
    pub id: String,
    pub kind: AgentCoordinationKind,
    pub target: AgentCoordinationEndpoint,
    pub occurred_at: String,
}
