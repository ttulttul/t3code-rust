//! The orchestration event model and its in-memory projection. Platform
//! neutral: compiled for the native server and the workers control plane.
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Seconds since the epoch, from the platform clock.
pub(crate) fn now() -> u64 {
    crate::platform::now_secs()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Project {
    pub id: String,
    pub name: String,
    pub path: String,
    pub created_at: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConversationMessage {
    pub id: String,
    pub role: String,
    pub content: String,
    pub created_at: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Run {
    pub id: Uuid,
    pub thread_id: String,
    pub provider: String,
    pub status: String,
    pub started_at: u64,
    pub finished_at: Option<u64>,
    pub error: Option<String>,
    #[serde(default)]
    pub user_message_id: Option<String>,
    #[serde(default)]
    pub queue_held: bool,
    /// How a server restart cut the run off, when one did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart_cut: Option<RestartCut>,
    /// The run a restart cut off that this run continues.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart_continuation_of: Option<Uuid>,
}

/// A run that a server restart cut off, as the reference's
/// ProviderRuntimeRecoveryService reconciles it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RestartCut {
    /// The reconcile's trigger: `shutdown` (the server stopped) or `startup`
    /// (it started again after stopping without one).
    pub trigger: String,
    /// When it was cut off (seconds).
    pub at: u64,
    /// When the run's turn should continue after the restart: how many
    /// processes in a row ended under it (the reference's
    /// `processLossCount`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation: Option<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckpointRecord {
    pub turn_count: u64,
    pub tree: String,
    pub cwd: String,
    pub captured_at: u64,
    #[serde(default)]
    pub ready: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Thread {
    #[serde(default)]
    pub(crate) row: serde_json::Value,
    pub id: String,
    pub project_id: String,
    pub title: String,
    pub cwd: String,
    pub status: String,
    pub messages: Vec<ConversationMessage>,
    pub runs: Vec<Run>,
    pub created_at: u64,
    pub archived: bool,
    pub settled: bool,
    pub snoozed_until: Option<String>,
    pub pinned: bool,
    pub unread: bool,
    pub tags: Vec<String>,
    #[serde(default)]
    pub checkpoints: Vec<CheckpointRecord>,
    /// What provider turns left behind; rebuilt from the log.
    #[serde(skip)]
    pub(crate) provider: Box<crate::provider_state::ProviderState>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum Event {
    ProjectCreated(Project),
    ProjectCommitted {
        project: Project,
        row: serde_json::Value,
        command_id: String,
        command_type: String,
        removed_threads: Vec<String>,
    },
    ThreadCreated(Thread),
    MessageAdded {
        thread_id: String,
        message: ConversationMessage,
    },
    MessageEdited {
        thread_id: String,
        message_id: String,
        content: String,
    },
    RunReordered {
        thread_id: String,
        run_id: Uuid,
        before_run_id: Option<Uuid>,
    },
    RunStarted(Run),
    RunUpdated(Run),
    RunQueued {
        message: ConversationMessage,
        run: Run,
    },
    RunOutput {
        run_id: Uuid,
        thread_id: String,
        stream: String,
        text: String,
        /// Where the output sits in the workspace's stream for the turn,
        /// when the workspace tracks positions. A restarted server resumes
        /// the turn after the last one recorded.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        position: Option<crate::workspace::StreamPosition>,
    },
    /// A provider turn's event, as the workspace reported it.
    RunProvider {
        run_id: Uuid,
        thread_id: String,
        event: crate::workspace::ProviderEvent,
        /// As [`Event::RunOutput`]'s.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        position: Option<crate::workspace::StreamPosition>,
    },
    RunFinished {
        run_id: Uuid,
        thread_id: String,
        status: String,
        error: Option<String>,
    },
    CheckpointCaptured {
        thread_id: String,
        checkpoint: CheckpointRecord,
    },
    CommandDispatched {
        command_id: String,
        command_type: String,
        thread_id: Option<String>,
        payload: serde_json::Value,
    },
    ThreadPatched {
        thread_id: String,
        patch: serde_json::Value,
    },
    ThreadDeleted {
        thread_id: String,
    },
    /// A server restart's reconciliation of a thread's provider runtime: the
    /// running runs it cut off end cancelled, the queued ones wait to be
    /// sent, and the provider session stops.
    ProviderRuntimeReconciled {
        thread_id: String,
        cut: Vec<(Uuid, RestartCut)>,
        #[serde(default)]
        held: Vec<Uuid>,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServerEvent {
    pub id: u64,
    pub at: u64,
    pub event: Event,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) receipt: Option<CommandReceipt>,
    /// The domain events a live thread subscriber gets for this commit. The
    /// commit takes one sequence number per event, ending at `id`. Not
    /// logged: a subscriber that resumes later gets a snapshot.
    #[serde(skip)]
    pub(crate) thread_events: Option<std::sync::Arc<crate::thread_events::ThreadEvents>>,
}

impl Event {
    /// The thread whose projection this event can change.
    pub(crate) fn thread_id(&self) -> Option<&str> {
        match self {
            Event::ThreadCreated(thread) => Some(&thread.id),
            Event::MessageAdded { thread_id, .. }
            | Event::MessageEdited { thread_id, .. }
            | Event::RunReordered { thread_id, .. }
            | Event::RunProvider { thread_id, .. }
            | Event::RunFinished { thread_id, .. }
            | Event::CheckpointCaptured { thread_id, .. }
            | Event::ThreadPatched { thread_id, .. }
            | Event::ProviderRuntimeReconciled { thread_id, .. } => Some(thread_id),
            Event::RunStarted(run) | Event::RunUpdated(run) | Event::RunQueued { run, .. } => {
                Some(&run.thread_id)
            }
            Event::CommandDispatched { thread_id, .. } => thread_id.as_deref(),
            // Output lines are not part of the projection.
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct CommandReceipt {
    pub(crate) command_id: String,
    pub(crate) thread_id: String,
}

#[derive(Default)]
pub struct Store {
    pub projects: HashMap<String, Project>,
    pub(crate) project_rows: indexmap::IndexMap<String, serde_json::Value>,
    pub(crate) project_receipts: HashMap<String, (String, String)>,
    pub(crate) deleted_thread_ids: std::collections::HashSet<String>,
    pub(crate) command_receipts: HashMap<String, (String, u64)>,
    pub threads: HashMap<String, Thread>,
    /// The last recorded workspace stream position of each running run.
    pub(crate) turn_positions: HashMap<Uuid, crate::workspace::StreamPosition>,
}
impl Store {
    pub(crate) fn apply_record(&mut self, record: &ServerEvent) {
        self.apply_at(&record.event, record.at);
        if let Some(receipt) = &record.receipt {
            self.command_receipts.insert(
                receipt.command_id.clone(),
                (receipt.thread_id.clone(), record.id),
            );
        }
    }

    /// Apply `event`, committed at `at` (seconds).
    fn apply_at(&mut self, event: &Event, at: u64) {
        match event {
            Event::ProjectCreated(p) => {
                self.project_rows.insert(p.id.clone(), wire_project(p));
                self.projects.insert(p.id.clone(), p.clone());
            }
            Event::ProjectCommitted {
                project,
                row,
                command_id,
                command_type,
                removed_threads,
            } => {
                self.project_rows.insert(project.id.clone(), row.clone());
                self.project_receipts.insert(
                    command_id.clone(),
                    (project.id.clone(), command_type.clone()),
                );
                if row["deletedAt"].is_null() {
                    self.projects.insert(project.id.clone(), project.clone());
                } else {
                    self.projects.remove(&project.id);
                }
                for id in removed_threads {
                    self.threads.remove(id);
                    self.deleted_thread_ids.insert(id.clone());
                }
            }
            Event::ThreadCreated(t) => {
                self.threads.insert(t.id.clone(), t.clone());
            }
            Event::MessageAdded { thread_id, message } => {
                if let Some(t) = self.threads.get_mut(thread_id) {
                    t.messages.push(message.clone());
                }
            }
            Event::MessageEdited {
                thread_id,
                message_id,
                content,
            } => {
                if let Some(t) = self.threads.get_mut(thread_id) {
                    if let Some(message) = t.messages.iter_mut().find(|m| m.id == *message_id) {
                        message.content = content.clone();
                    }
                }
            }
            Event::RunQueued { message, run } => {
                if let Some(thread) = self.threads.get_mut(&run.thread_id) {
                    thread.messages.push(message.clone());
                    thread.status = "running".into();
                    thread.runs.push(run.clone());
                }
            }
            Event::RunStarted(run) => {
                if let Some(t) = self.threads.get_mut(&run.thread_id) {
                    t.status = "running".into();
                    if let Some(existing) =
                        t.runs.iter_mut().find(|candidate| candidate.id == run.id)
                    {
                        *existing = run.clone();
                    } else {
                        t.runs.push(run.clone());
                    }
                }
            }
            Event::RunUpdated(run) => {
                if let Some(t) = self.threads.get_mut(&run.thread_id) {
                    if let Some(existing) =
                        t.runs.iter_mut().find(|candidate| candidate.id == run.id)
                    {
                        *existing = run.clone();
                    } else {
                        t.runs.push(run.clone());
                    }
                    t.status = match run.status.as_str() {
                        "completed" => "idle".into(),
                        "queued" => "running".into(),
                        status => status.into(),
                    };
                }
            }
            Event::RunReordered {
                thread_id,
                run_id,
                before_run_id,
            } => {
                if let Some(t) = self.threads.get_mut(thread_id) {
                    let Some(index) = t.runs.iter().position(|run| run.id == *run_id) else {
                        return;
                    };
                    let run = t.runs.remove(index);
                    let target = before_run_id
                        .and_then(|id| t.runs.iter().position(|candidate| candidate.id == id))
                        .unwrap_or(t.runs.len());
                    t.runs.insert(target, run);
                }
            }
            Event::RunOutput {
                run_id, position, ..
            } => {
                if let Some(position) = position {
                    self.turn_positions.insert(*run_id, *position);
                }
            }
            Event::RunProvider {
                run_id,
                thread_id,
                event,
                position,
            } => {
                if let Some(position) = position {
                    self.turn_positions.insert(*run_id, *position);
                }
                if let Some(thread) = self.threads.get_mut(thread_id) {
                    thread.provider.apply(*run_id, event, at);
                }
            }
            Event::RunFinished {
                run_id,
                thread_id,
                status,
                error,
            } => {
                self.turn_positions.remove(run_id);
                if let Some(t) = self.threads.get_mut(thread_id) {
                    t.status = if status == "completed" {
                        "idle".into()
                    } else {
                        status.clone()
                    };
                    if let Some(run) = t.runs.iter_mut().find(|r| r.id == *run_id) {
                        run.status = status.clone();
                        run.error = error.clone();
                        run.finished_at = Some(now());
                    }
                }
            }
            Event::CheckpointCaptured {
                thread_id,
                checkpoint,
            } => {
                if let Some(thread) = self.threads.get_mut(thread_id) {
                    if !thread.checkpoints.iter().any(|candidate| {
                        candidate.turn_count == checkpoint.turn_count
                            && candidate.tree == checkpoint.tree
                    }) {
                        thread.checkpoints.push(checkpoint.clone());
                        thread
                            .checkpoints
                            .sort_by_key(|candidate| candidate.turn_count);
                    }
                }
            }
            Event::CommandDispatched { .. } => {}
            Event::ThreadPatched { thread_id, patch } => {
                if let Some(thread) = self.threads.get_mut(thread_id) {
                    if let Some(fields) = patch["row"].as_object() {
                        let mut row = thread.stored_row();
                        row.as_object_mut().unwrap().extend(fields.clone());
                        thread.row = row;
                    }
                    if let Some(value) = patch.get("title").and_then(serde_json::Value::as_str) {
                        thread.title = value.to_owned();
                    }
                    if let Some(value) = patch.get("project_id").and_then(serde_json::Value::as_str)
                    {
                        thread.project_id = value.to_owned();
                    }
                    if let Some(value) = patch.get("archived").and_then(serde_json::Value::as_bool)
                    {
                        thread.archived = value;
                    }
                    if let Some(value) = patch.get("settled").and_then(serde_json::Value::as_bool) {
                        thread.settled = value;
                    }
                    if let Some(value) = patch.get("pinned").and_then(serde_json::Value::as_bool) {
                        thread.pinned = value;
                    }
                    if let Some(value) = patch.get("unread").and_then(serde_json::Value::as_bool) {
                        thread.unread = value;
                    }
                    if let Some(value) = patch.get("status").and_then(serde_json::Value::as_str) {
                        thread.status = value.to_owned();
                    }
                    if let Some(value) = patch.get("snoozed_until") {
                        thread.snoozed_until = value.as_str().map(str::to_owned);
                    }
                    if let Some(values) = patch.get("tags").and_then(serde_json::Value::as_array) {
                        thread.tags = values
                            .iter()
                            .filter_map(serde_json::Value::as_str)
                            .map(str::to_owned)
                            .collect();
                    }
                }
            }
            Event::ThreadDeleted { thread_id } => {
                self.threads.remove(thread_id);
                self.deleted_thread_ids.insert(thread_id.clone());
            }
            Event::ProviderRuntimeReconciled {
                thread_id,
                cut,
                held,
            } => {
                for (run_id, _) in cut {
                    self.turn_positions.remove(run_id);
                }
                if let Some(thread) = self.threads.get_mut(thread_id) {
                    for (run_id, restart) in cut {
                        if let Some(run) = thread.runs.iter_mut().find(|r| r.id == *run_id) {
                            run.status = "cancelled".into();
                            run.finished_at = Some(restart.at);
                            run.restart_cut = Some(restart.clone());
                        }
                    }
                    for run in thread.runs.iter_mut().filter(|r| held.contains(&r.id)) {
                        run.queue_held = true;
                    }
                    if !cut.is_empty() {
                        thread.status = "cancelled".into();
                    }
                    thread.provider.stop_session(at);
                }
            }
        }
    }
}

pub(crate) fn iso(time: time::OffsetDateTime) -> String {
    time.format(time::macros::format_description!(
        "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z"
    ))
    .expect("valid project timestamp")
}
pub(crate) fn wire_project(p: &Project) -> serde_json::Value {
    let at = iso(
        time::OffsetDateTime::from_unix_timestamp(p.created_at as i64)
            .unwrap_or(time::OffsetDateTime::UNIX_EPOCH),
    );
    serde_json::json!({"id":p.id,"title":p.name,"hasCustomTitle":false,"workspaceRoot":p.path,"repositoryIdentity":null,"faviconPath":null,"projectIcon":null,"defaultModelSelection":null,"defaultThreadEnvMode":null,"autoPull":false,"scripts":[],"pinnedAt":null,"archivedAt":null,"createdAt":at,"updatedAt":at,"deletedAt":null})
}
