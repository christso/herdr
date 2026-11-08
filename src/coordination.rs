//! Durable metadata from successful agent operations, independent of live pane membership.

use std::collections::HashSet;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::api::schema::{
    AgentCoordinationEndpoint, AgentCoordinationKind, AgentCoordinationOutcome,
    AgentCoordinationReceipt,
};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn same_occupancy(a: &AgentCoordinationEndpoint, b: &AgentCoordinationEndpoint) -> bool {
    a.server_id == b.server_id
        && a.boot_id == b.boot_id
        && a.terminal_id == b.terminal_id
        && a.agent_instance_id == b.agent_instance_id
}

/// Unique process-local allocations use a fresh time/process namespace, not pane labels or PIDs alone.
pub(crate) fn new_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let count = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let value = Sha256::digest(format!("{}:{nanos}:{count}", std::process::id()).as_bytes());
    let hex = format!("{value:x}");
    format!(
        "{}-{}-4{}-8{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[13..16],
        &hex[17..20],
        &hex[20..32]
    )
}

pub(crate) fn profile_fingerprint(id: &str, target: &str, session: &str) -> String {
    let value = serde_json::to_vec(&(id, target, session)).expect("string tuple serializes");
    format!("{:x}", Sha256::digest(value))
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    version: u32,
    server_id: String,
    receipts: Vec<AgentCoordinationReceipt>,
    #[serde(default)]
    outcomes: Vec<AgentCoordinationOutcome>,
}

pub(crate) struct Journal {
    path: Option<PathBuf>,
    document: Result<Document, String>,
    pub(crate) boot_id: String,
    pending_prompts: HashSet<String>,
}

/// Holds a new internal operation ID until PTY completion or submission failure.
pub(crate) struct PromptOperation {
    journal: Arc<Mutex<Journal>>,
    id: String,
}

impl PromptOperation {
    pub(crate) fn reserve(journal: Arc<Mutex<Journal>>, id: String) -> Result<Option<Self>, ()> {
        {
            let Ok(mut state) = journal.lock() else {
                // Observation must remain passive even when its lock is unavailable.
                return Ok(None);
            };
            if state.operation_exists(&id) {
                return Err(());
            }
            state.pending_prompts.insert(id.clone());
        }
        Ok(Some(Self { journal, id }))
    }
}

impl Drop for PromptOperation {
    fn drop(&mut self) {
        if let Ok(mut journal) = self.journal.lock() {
            journal.pending_prompts.remove(&self.id);
        }
    }
}

impl Journal {
    pub(crate) fn open(path: Option<PathBuf>) -> Self {
        let document = path.as_ref().map_or_else(
            || {
                Ok(Document {
                    version: 1,
                    server_id: new_id(),
                    receipts: Vec::new(),
                    outcomes: Vec::new(),
                })
            },
            |path| match std::fs::symlink_metadata(path) {
                Ok(meta) if !meta.is_file() || meta.file_type().is_symlink() => {
                    Err("coordination journal is not a regular file".into())
                }
                Ok(_) => std::fs::read(path)
                    .map_err(|_| "coordination journal cannot be read".to_string())
                    .and_then(|bytes| {
                        serde_json::from_slice::<Document>(&bytes)
                            .map_err(|_| "coordination journal format cannot be read".into())
                    })
                    .and_then(|document| {
                        if document.version == 1 && !document.server_id.is_empty() {
                            Ok(document)
                        } else {
                            Err("coordination journal version cannot be read".into())
                        }
                    }),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Document {
                    version: 1,
                    server_id: new_id(),
                    receipts: Vec::new(),
                    outcomes: Vec::new(),
                }),
                Err(_) => Err("coordination journal cannot be read".into()),
            },
        );
        let mut journal = Self {
            path,
            document,
            boot_id: new_id(),
            pending_prompts: HashSet::new(),
        };
        // Create the persistent namespace before any operation can refer to it.
        if journal.document.is_ok() && journal.path.as_ref().is_some_and(|path| !path.exists()) {
            if let Err(error) = journal.save() {
                journal.document = Err(error.to_string());
            }
        }
        journal
    }

    pub(crate) fn server_id(&self) -> Result<&str, String> {
        self.document
            .as_ref()
            .map(|document| document.server_id.as_str())
            .map_err(Clone::clone)
    }

    pub(crate) fn page(
        &self,
        cursor: Option<&str>,
        limit: Option<u32>,
    ) -> Result<(Vec<AgentCoordinationReceipt>, Option<String>), String> {
        let document = self.document.as_ref().map_err(Clone::clone)?;
        let at = cursor.map_or(Ok(0), |value| {
            value
                .parse::<usize>()
                .map_err(|_| "invalid coordination cursor".to_string())
        })?;
        if at > document.receipts.len() {
            return Err("invalid coordination cursor".into());
        }
        let limit = limit.unwrap_or(200).clamp(1, 500) as usize;
        let end = at.saturating_add(limit).min(document.receipts.len());
        Ok((
            document.receipts[at..end].to_vec(),
            (end < document.receipts.len()).then(|| end.to_string()),
        ))
    }

    pub(crate) fn outcome(&self, id: &str) -> Result<Option<AgentCoordinationOutcome>, String> {
        Ok(self
            .document
            .as_ref()
            .map_err(Clone::clone)?
            .outcomes
            .iter()
            .find(|outcome| outcome.id == id)
            .cloned())
    }

    pub(crate) fn operation_exists(&self, id: &str) -> bool {
        self.pending_prompts.contains(id) || self.outcome(id).ok().flatten().is_some()
    }

    pub(crate) fn complete(
        &mut self,
        id: String,
        kind: AgentCoordinationKind,
        source: Option<AgentCoordinationEndpoint>,
        target: AgentCoordinationEndpoint,
    ) -> Result<(), String> {
        let occurred_at = time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .map_err(|error| error.to_string())?;
        let document = self.document.as_mut().map_err(|error| error.clone())?;
        if document.outcomes.iter().any(|outcome| outcome.id == id) {
            return Ok(());
        }
        document.outcomes.push(AgentCoordinationOutcome {
            id: id.clone(),
            kind,
            target: target.clone(),
            occurred_at: occurred_at.clone(),
        });
        if let Some(source) = source {
            if !same_occupancy(&source, &target) {
                document.receipts.push(AgentCoordinationReceipt {
                    id,
                    kind,
                    source,
                    target,
                    occurred_at,
                });
            }
        }
        self.save().map_err(|error| error.to_string())
    }

    pub(crate) fn record_remote(
        &mut self,
        source: AgentCoordinationEndpoint,
        outcome: AgentCoordinationOutcome,
    ) -> Result<(), String> {
        if same_occupancy(&source, &outcome.target) {
            return Ok(());
        }
        let document = self.document.as_mut().map_err(|error| error.clone())?;
        if document
            .receipts
            .iter()
            .any(|receipt| receipt.id == outcome.id)
        {
            return Ok(());
        }
        document.receipts.push(AgentCoordinationReceipt {
            id: outcome.id,
            kind: outcome.kind,
            source,
            target: outcome.target,
            occurred_at: outcome.occurred_at,
        });
        self.save().map_err(|error| error.to_string())
    }

    fn save(&self) -> io::Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let document = self
            .document
            .as_ref()
            .map_err(|error| io::Error::other(error.clone()))?;
        if let Ok(metadata) = std::fs::symlink_metadata(path) {
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err(io::Error::other(
                    "coordination journal is not a regular file",
                ));
            }
            let existing: Document =
                serde_json::from_slice(&std::fs::read(path)?).map_err(io::Error::other)?;
            if existing.version != 1 || existing.server_id != document.server_id {
                return Err(io::Error::other(
                    "coordination journal changed format or namespace; saved bytes were preserved",
                ));
            }
        }
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent)?;
        let temporary = path.with_extension(format!("{}.tmp", new_id()));
        let mut file = crate::platform::create_private_state_file(&temporary)?;
        let result = serde_json::to_writer(&mut file, document)
            .map_err(io::Error::other)
            .and_then(|()| file.write_all(b"\n"))
            .and_then(|()| file.sync_all());
        drop(file);
        if let Err(error) = result.and_then(|()| crate::platform::replace_file(&temporary, path)) {
            let _ = std::fs::remove_file(temporary);
            return Err(error);
        }
        crate::platform::sync_parent_directory(parent)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn endpoint(server: &str, instance: &str) -> AgentCoordinationEndpoint {
        AgentCoordinationEndpoint {
            server_id: server.into(),
            boot_id: "boot".into(),
            terminal_id: "terminal".into(),
            agent_instance_id: instance.into(),
            agent: "codex".into(),
            pane_id: "w1:p1".into(),
            name: None,
        }
    }

    #[test]
    fn coordination_history_survives_restart_and_counts_each_operation_once() {
        let root = std::env::temp_dir().join(format!("herdr-coordination-{}", new_id()));
        let path = root.join("coordination.json");
        let mut first = Journal::open(Some(path.clone()));
        let server = first.server_id().unwrap().to_owned();
        let boot = first.boot_id.clone();
        first
            .complete(
                "operation".into(),
                AgentCoordinationKind::Communication,
                Some(endpoint(&server, "parent")),
                endpoint(&server, "child"),
            )
            .unwrap();
        first
            .complete(
                "operation".into(),
                AgentCoordinationKind::Communication,
                Some(endpoint(&server, "parent")),
                endpoint(&server, "child"),
            )
            .unwrap();
        let second = Journal::open(Some(path));
        assert_eq!(second.server_id().unwrap(), server);
        assert_ne!(second.boot_id, boot);
        assert_eq!(second.page(None, None).unwrap().0.len(), 1);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn coordination_paging_retains_history_and_refuses_unknown_formats_without_writing() {
        let mut journal = Journal::open(None);
        for index in 0..3 {
            journal
                .complete(
                    index.to_string(),
                    AgentCoordinationKind::Delegation,
                    Some(endpoint("server", "parent")),
                    endpoint("server", &format!("child{index}")),
                )
                .unwrap();
        }
        let (first, cursor) = journal.page(None, Some(2)).unwrap();
        assert_eq!(first.len(), 2);
        let (last, cursor) = journal.page(cursor.as_deref(), Some(2)).unwrap();
        assert_eq!(last.len(), 1);
        assert!(cursor.is_none());
        assert!(journal.page(Some("999"), None).is_err());
        let root = std::env::temp_dir().join(format!("herdr-coordination-{}", new_id()));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("coordination.json");
        let bytes = b"{\"version\":2,\"server_id\":\"future\",\"receipts\":[]}";
        std::fs::write(&path, bytes).unwrap();
        let mut future = Journal::open(Some(path.clone()));
        assert!(future
            .complete(
                "attempt".into(),
                AgentCoordinationKind::Communication,
                Some(endpoint("server", "a")),
                endpoint("server", "b")
            )
            .is_err());
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        std::fs::remove_file(&path).unwrap();
        let mut live = Journal::open(Some(path.clone()));
        std::fs::write(&path, bytes).unwrap();
        assert!(live
            .complete(
                "future-change".into(),
                AgentCoordinationKind::Communication,
                Some(endpoint("server", "a")),
                endpoint("server", "b")
            )
            .is_err());
        assert_eq!(std::fs::read(path).unwrap(), bytes);
        std::fs::remove_dir_all(root).unwrap();
    }
}
