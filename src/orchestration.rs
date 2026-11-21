//! Orchestration commands (`orchestration.dispatchCommand`): checks, then the
//! events each command commits. Dispatch runs over the control state; what
//! it needs beyond that (policy, run execution, guest shares) comes from a
//! [`CommandHost`].
use super::{now, CommandReceipt, ConversationMessage, Event, Run, Thread};
use crate::control_state::ControlState;
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{oneshot, Mutex};
use uuid::Uuid;

/// What command dispatch needs from the server beyond the control state.
pub(crate) trait CommandHost:
    std::ops::Deref<Target = ControlState> + crate::workspace::MaybeSendSync + Sized
{
    /// Held while a command checks and commits, so commands apply one at a time.
    fn mutation_lock(&self) -> &Mutex<()>;
    /// The managed policy's summary, when a policy applies.
    fn policy_summary(&self) -> Option<&Value>;
    fn runtime_denial(&self, mode: &str) -> Option<String>;
    fn provider_denial(&self, driver: &str) -> Option<String>;
    /// The effective settings.
    fn effective_settings(&self) -> anyhow::Result<Value>;
    fn workspace(&self) -> &dyn crate::workspace::Workspace;
    /// Running runs by id; resolving a sender stops its run.
    fn cancellations(&self) -> &Mutex<HashMap<Uuid, oneshot::Sender<()>>>;
    /// Execute `run` in the background until it ends or `cancel` resolves,
    /// recording a failure to start or run as the run's end.
    fn launch_run(
        self: Arc<Self>,
        run: Run,
        cwd: String,
        text: String,
        cancel: oneshot::Receiver<()>,
    );
    fn close_guest_thread(&self, thread_id: &str) -> Result<(), String>;
    fn resolve_guest_message(
        &self,
        thread_id: &str,
        message_id: &str,
        outcome: &str,
    ) -> Result<(), String>;
}

pub(crate) async fn dispatch_orchestration_command<H: CommandHost>(
    state: Arc<H>,
    command: Value,
) -> Result<u64, Value> {
    dispatch_command(state, command, None).await
}

/// Dispatch the server's own `message.dispatch` that continues `source`, a
/// run a restart cut off, with the checks a client's message gets.
pub(crate) async fn dispatch_restart_continuation<H: CommandHost>(
    state: Arc<H>,
    command: Value,
    source: Uuid,
) -> Result<u64, Value> {
    dispatch_command(state, command, Some(source)).await
}

async fn dispatch_command<H: CommandHost>(
    state: Arc<H>,
    command: Value,
    continuation_of: Option<Uuid>,
) -> Result<u64, Value> {
    let kind = command["type"].as_str().unwrap_or_default();
    let command_id = command["commandId"].as_str().unwrap_or_default();
    let id = command["threadId"]
        .as_str()
        .or_else(|| {
            matches!(kind, "thread.fork" | "thread.merge_back")
                .then(|| command["targetThreadId"].as_str())
                .flatten()
        })
        .or_else(|| command["parentThreadId"].as_str())
        .unwrap_or_default();
    let error = |detail: String, cause: Value| json!({"_tag":"OrchestrationV2DispatchCommandError","commandId":command_id,"commandType":kind,"message":detail,"detail":detail,"cause":cause});
    let dispatch_error = |detail: String| {
        error(
            detail.clone(),
            json!({"name":"OrchestratorDispatchError","message":format!("Failed to dispatch orchestration command {kind} ({command_id})."),"cause":detail}),
        )
    };
    let _guard = state.mutation_lock().lock().await;
    let store = state.store.read().await;
    if let Some((prior_id, seq)) = store.command_receipts.get(command_id) {
        if id == prior_id {
            return Ok(*seq);
        }
        let msg = format!("Command {command_id} was already handled for thread {prior_id} and cannot be replayed for {id}.");
        return Err(error(
            msg.clone(),
            json!({"name":"OrchestratorCommandIdConflictError","message":msg}),
        ));
    }
    if matches!(kind, "thread.fork" | "delegated_task.request") {
        let target = command["targetThreadId"]
            .as_str()
            .or_else(|| command["childThreadId"].as_str())
            .unwrap_or_default();
        if !target.is_empty()
            && (store.threads.contains_key(target) || store.deleted_thread_ids.contains(target))
        {
            let msg = format!("Thread {target} already exists. Use a new thread id.");
            return Err(error(
                msg.clone(),
                json!({"name":"OrchestratorThreadAlreadyExistsError","message":msg}),
            ));
        }
    }
    if kind == "thread.create"
        && (store.threads.contains_key(id) || store.deleted_thread_ids.contains(id))
    {
        let msg = format!("Thread {id} already exists. Use a new thread id.");
        return Err(error(
            msg.clone(),
            json!({"name":"OrchestratorThreadAlreadyExistsError","message":msg}),
        ));
    }
    if let Some(id) = command["threadId"].as_str().filter(|_| {
        !matches!(
            kind,
            "thread.create" | "thread.fork" | "delegated_task.request"
        )
    }) {
        let Some(thread) = store.threads.get(id) else {
            let msg = format!("No orchestration projection exists for thread {id}.");
            return Err(error(
                msg.clone(),
                json!({"name":"OrchestratorProjectionError","message":format!("Failed to load orchestration projection for thread {id}."),"cause":{"name":"ProjectionStoreThreadNotFoundError","message":msg}}),
            ));
        };
        if kind == "thread.archive" && thread.archived {
            return Err(dispatch_error(format!("Thread {id} is already archived.")));
        }
        if kind == "thread.unarchive" && !thread.archived {
            return Err(dispatch_error(format!("Thread {id} is not archived.")));
        }
        if kind == "thread.mark-unread" && !thread.runs.iter().any(|r| r.finished_at.is_some()) {
            return Err(dispatch_error(format!(
                "Thread {id} has no completed run to mark unread."
            )));
        }
        if thread.archived
            && matches!(
                kind,
                "thread.settle"
                    | "thread.auto-settle"
                    | "thread.unsettle"
                    | "thread.snooze"
                    | "thread.unsnooze"
                    | "thread.auto-settle.set"
                    | "thread.pin"
                    | "thread.unpin"
                    | "thread.pin.reorder"
                    | "thread.active.reorder"
                    | "queue.resume"
                    | "queued-message.promote-to-steer"
                    | "thread.pull-request-watch.sync"
                    | "thread.pull-request.sync"
            )
        {
            return Err(dispatch_error(format!("Thread {id} is archived.")));
        }
        if kind == "message.dispatch" && !store.projects.contains_key(&thread.project_id) {
            let runtime = json!({
                "name":"RuntimePolicyResolveError",
                "message":format!("Failed to resolve runtime policy for provider instance {} in project {}.", thread.row["providerInstanceId"].as_str().unwrap_or("codex"), thread.project_id),
                "cause":"Project not found."
            });
            let nested = json!({
                "name":"OrchestratorDispatchError",
                "message":format!("Failed to dispatch orchestration command {kind} ({command_id})."),
                "cause":runtime
            });
            return Err(error(
                "Project not found.".into(),
                json!({"name":"OrchestratorDispatchError","message":format!("Failed to dispatch orchestration command {kind} ({command_id})."),"cause":nested}),
            ));
        }
    }
    if let Some(summary) = state.policy_summary() {
        let row = store
            .threads
            .get(id)
            .map(Thread::stored_row)
            .unwrap_or(Value::Null);
        let starts_work = kind == "message.dispatch";
        let sets_mode = matches!(kind, "thread.create" | "thread.runtime-mode.set");
        if sets_mode || starts_work {
            let mode = if sets_mode {
                &command["runtimeMode"]
            } else {
                &row["runtimeMode"]
            };
            if let Some(mut message) = mode.as_str().and_then(|mode| state.runtime_denial(mode)) {
                if starts_work {
                    message.push_str(" Switch this thread's mode to continue.");
                }
                return Err(error(
                    message.clone(),
                    json!({"name":"OrchestratorRuntimeModeAboveCeilingError","message":message}),
                ));
            }
        }
        if starts_work && summary["limits"]["providers"]["allowedDrivers"].is_array() {
            let id = command["modelSelection"]["instanceId"]
                .as_str()
                .or(row["providerInstanceId"].as_str())
                .unwrap_or("codex");
            let settings = state
                .effective_settings()
                .map_err(|e| dispatch_error(e.to_string()))?;
            let driver = settings["providerInstances"][id]["driver"]
                .as_str()
                .unwrap_or(id);
            if let Some(reason) = state.provider_denial(driver) {
                return Err(dispatch_error(reason));
            }
        }
    }
    // A message, or a metadata update that continues the thread, starts
    // work on a provider instance; the reference refuses a disabled one.
    let continues = kind == "thread.metadata.update" && !command["continuation"].is_null();
    if kind == "message.dispatch" || continues {
        if let Some(thread) = store.threads.get(id) {
            let row = thread.stored_row();
            let instance = (!continues)
                .then(|| command["modelSelection"]["instanceId"].as_str())
                .flatten()
                .or(row["modelSelection"]["instanceId"].as_str())
                .or(row["providerInstanceId"].as_str())
                .unwrap_or("codex");
            let settings = state
                .effective_settings()
                .map_err(|e| dispatch_error(e.to_string()))?;
            if !provider_instance_enabled(&settings, &thread.project_id, instance) {
                return Err(dispatch_error(format!(
                    "Provider instance \"{instance}\" is disabled for project {}.",
                    thread.project_id
                )));
            }
        }
    }
    drop(store);
    let receipt = CommandReceipt {
        command_id: command_id.to_string(),
        thread_id: id.to_string(),
    };
    apply_command(state.clone(), &command, &receipt, continuation_of)
        .await
        .map_err(|e| dispatch_error(e.to_string()))
}

/// Drivers whose instances are off unless enabled, as the reference decodes
/// them.
const DEFAULT_DISABLED_DRIVERS: &[&str] = &[
    "cursor",
    "grok",
    "fx",
    "kiro",
    "muse",
    "pi",
    "opencode",
    "antigravity",
];

/// Whether `instance` may run in `project`: the project's override, else the
/// instance's own setting, else its driver's default. An instance that is
/// not configured is enabled only if it is a built-in default (`codex`,
/// `claudeAgent`). The reference's `resolveProjectProviderInstanceEnabled`.
pub(crate) fn provider_instance_enabled(settings: &Value, project: &str, instance: &str) -> bool {
    let overridden =
        &settings["projectSettingsOverrides"][project]["providerInstances"][instance]["enabled"];
    if let Some(enabled) = overridden.as_bool() {
        return enabled;
    }
    let Some(config) = settings["providerInstances"]
        .as_object()
        .and_then(|instances| instances.get(instance))
    else {
        return matches!(instance, "codex" | "claudeAgent");
    };
    let own = config["enabled"].as_bool();
    let nested = config["config"]["enabled"].as_bool();
    if own == Some(false) || nested == Some(false) {
        return false;
    }
    own.or(nested).unwrap_or_else(|| {
        let driver = config["driver"].as_str().unwrap_or(instance);
        !DEFAULT_DISABLED_DRIVERS.contains(&driver)
    })
}

async fn start_run<H: CommandHost>(
    state: Arc<H>,
    receipt: &CommandReceipt,
    thread_id: &str,
    text: String,
    message_id: &Value,
    queued: bool,
    continues: Option<Uuid>,
) -> anyhow::Result<u64> {
    let (cwd, needs_baseline) = state
        .store
        .read()
        .await
        .threads
        .get(thread_id)
        .map(|t| (t.cwd.clone(), t.checkpoints.is_empty()))
        .ok_or_else(|| anyhow::anyhow!("thread not found"))?;
    let baseline = needs_baseline.then(|| cwd.clone());
    let run = Run {
        id: crate::platform::uuid_v4(),
        thread_id: thread_id.to_owned(),
        provider: "default".into(),
        status: if queued { "queued" } else { "running" }.into(),
        started_at: now(),
        finished_at: None,
        error: None,
        user_message_id: Some(message_id.as_str().unwrap_or_default().to_owned()),
        queue_held: queued,
        restart_cut: None,
        restart_continuation_of: continues,
    };
    let message = ConversationMessage {
        id: message_id.as_str().unwrap_or_default().to_owned(),
        // The prompt that continues a run shows as a notification.
        role: if continues.is_some() {
            crate::thread_projection::NOTIFICATION_ROLE
        } else {
            "user"
        }
        .into(),
        content: text.clone(),
        created_at: now(),
    };
    let seq = state
        .commit_receipted(
            Event::RunQueued {
                message,
                run: run.clone(),
            },
            Some(receipt.clone()),
        )
        .await?
        .id;
    if let Some(baseline_cwd) = baseline {
        if let Some(tree) = state.workspace().capture_git_tree(&baseline_cwd).await {
            let _ = state
                .commit(Event::CheckpointCaptured {
                    thread_id: thread_id.to_owned(),
                    checkpoint: crate::CheckpointRecord {
                        turn_count: 0,
                        tree,
                        cwd: baseline_cwd,
                        captured_at: now(),
                        ready: true,
                    },
                })
                .await;
        }
    }
    if queued {
        return Ok(seq);
    }
    let (tx, rx) = oneshot::channel();
    state.cancellations().lock().await.insert(run.id, tx);
    state.launch_run(run, cwd, text, rx);
    Ok(seq)
}

async fn start_queued_run<H: CommandHost>(
    state: Arc<H>,
    receipt: &CommandReceipt,
    thread_id: &str,
    run_id: &str,
) -> anyhow::Result<u64> {
    start_existing_run(state, receipt, thread_id, run_id, "running").await
}

async fn start_existing_run<H: CommandHost>(
    state: Arc<H>,
    receipt: &CommandReceipt,
    thread_id: &str,
    run_id: &str,
    start_status: &str,
) -> anyhow::Result<u64> {
    let (mut run, cwd, text) = {
        let store = state.store.read().await;
        let thread = store
            .threads
            .get(thread_id)
            .ok_or_else(|| anyhow::anyhow!("thread not found"))?;
        let run = thread
            .runs
            .iter()
            .find(|candidate| {
                candidate.id.to_string() == run_id
                    && (candidate.status == "queued" || candidate.status == "preparing")
            })
            .cloned()
            .ok_or_else(|| anyhow::anyhow!(format!("Run {run_id} is not queued.")))?;
        if thread
            .runs
            .iter()
            .any(|candidate| candidate.status == "running")
        {
            return Err(anyhow::anyhow!(
                "A queued user message can only be sent now while the thread is stopped."
            ));
        }
        let message_id = run
            .user_message_id
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!(format!("Run {run_id} has no queued message.")))?;
        let text = thread
            .messages
            .iter()
            .find(|message| message.id == message_id)
            .map(|message| message.content.clone())
            .ok_or_else(|| anyhow::anyhow!(format!("Run {run_id} has no queued message.")))?;
        (run, thread.cwd.clone(), text)
    };
    run.status = start_status.into();
    run.queue_held = false;
    let sequence = state
        .commit_receipted(Event::RunStarted(run.clone()), Some(receipt.clone()))
        .await?
        .id;
    let (tx, rx) = oneshot::channel();
    state.cancellations().lock().await.insert(run.id, tx);
    state.launch_run(run, cwd, text, rx);
    Ok(sequence)
}

async fn apply_command<H: CommandHost>(
    state: Arc<H>,
    command: &Value,
    receipt: &CommandReceipt,
    continues: Option<Uuid>,
) -> anyhow::Result<u64> {
    let command_type = command
        .get("type")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let thread_id = command.get("threadId").and_then(serde_json::Value::as_str);
    match command_type {
        "thread.create" => {
            let project_id = command
                .get("projectId")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| anyhow::anyhow!("thread.create requires projectId"))?;
            let cwd = state
                .store
                .read()
                .await
                .projects
                .get(&project_id)
                .map(|project| project.path.clone())
                .unwrap_or_default();
            let id = receipt.thread_id.clone();
            let thread = Thread {
                row: crate::thread_projection::created(
                    command,
                    &crate::auth_service::iso(crate::auth_service::now()),
                ),
                id,
                project_id,
                title: command
                    .get("title")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("New thread")
                    .to_owned(),
                cwd: command
                    .get("worktreePath")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or(&cwd)
                    .to_owned(),
                status: "idle".into(),
                messages: vec![],
                runs: vec![],
                created_at: now(),
                archived: false,
                settled: false,
                snoozed_until: None,
                pinned: false,
                unread: false,
                tags: vec![],
                checkpoints: vec![],
                provider: Default::default(),
            };
            return Ok(state
                .commit_receipted(Event::ThreadCreated(thread), Some(receipt.clone()))
                .await?
                .id);
        }
        "message.dispatch" => {
            let thread_id =
                thread_id.ok_or_else(|| anyhow::anyhow!("message.dispatch requires threadId"))?;
            let text = command
                .get("text")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let queued = command["dispatchMode"]["type"] == "queue_after_active"
                && state
                    .store
                    .read()
                    .await
                    .threads
                    .get(thread_id)
                    .is_some_and(|thread| thread.runs.iter().any(|run| run.status == "running"));
            return start_run(
                state.clone(),
                receipt,
                thread_id,
                text,
                &command["messageId"],
                queued,
                continues,
            )
            .await;
        }
        "thread.stop" | "run.interrupt" | "subagent.stop" => {
            if let Some(thread_id) = thread_id {
                let run_ids = state
                    .store
                    .read()
                    .await
                    .threads
                    .get(thread_id)
                    .map(|thread| {
                        thread
                            .runs
                            .iter()
                            .filter(|run| run.status == "running")
                            .map(|run| run.id)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                let mut cancellations = state.cancellations().lock().await;
                for run_id in run_ids {
                    if let Some(cancel) = cancellations.remove(&run_id) {
                        let _ = cancel.send(());
                    }
                }
                return Ok(state
                    .commit_receipted(
                        Event::ThreadPatched {
                            thread_id: thread_id.to_owned(),
                            patch: serde_json::json!({"status":"cancelled"}),
                        },
                        Some(receipt.clone()),
                    )
                    .await?
                    .id);
            }
        }
        "thread.pull-request-watch.checks-reopen" => {
            let id = thread_id.ok_or_else(|| anyhow::anyhow!("Missing thread id."))?;
            let store = state.store.read().await;
            let thread = store
                .threads
                .get(id)
                .ok_or_else(|| anyhow::anyhow!("thread not found"))?;
            let mut row = thread.stored_row();
            if let Some(links) = row["pullRequests"].as_array_mut() {
                for link in links {
                    if let Some(watch) = link.get_mut("watch").and_then(Value::as_object_mut) {
                        watch.remove("checksPending");
                    }
                }
            }
            drop(store);
            return Ok(state
                .commit_receipted(
                    Event::ThreadPatched {
                        thread_id: id.to_owned(),
                        patch: json!({"row":row}),
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "thread.pull-request-watch.sync" => {
            let id = thread_id.ok_or_else(|| anyhow::anyhow!("Missing thread id."))?;
            let store = state.store.read().await;
            let thread = store
                .threads
                .get(id)
                .ok_or_else(|| anyhow::anyhow!("thread not found"))?;
            let mut row = thread.stored_row();
            let host = command["host"].as_str().unwrap_or_default();
            let repository = command["repository"].as_str().unwrap_or_default();
            let number = command["number"].clone();
            let Some(link) = row["pullRequests"].as_array_mut().and_then(|links| {
                links.iter_mut().find(|link| {
                    link["host"]
                        .as_str()
                        .unwrap_or_default()
                        .eq_ignore_ascii_case(host)
                        && link["repository"]
                            .as_str()
                            .unwrap_or_default()
                            .eq_ignore_ascii_case(repository)
                        && link["number"] == number
                })
            }) else {
                drop(store);
                return Ok(state
                    .commit_receipted(
                        Event::CommandDispatched {
                            command_id: receipt.command_id.clone(),
                            command_type: command_type.into(),
                            thread_id: Some(id.to_owned()),
                            payload: command.clone(),
                        },
                        Some(receipt.clone()),
                    )
                    .await?
                    .id);
            };
            let existing_watch = link["watch"].clone();
            if existing_watch["startedAt"] != command["startedAt"] {
                drop(store);
                return Ok(state
                    .commit_receipted(
                        Event::CommandDispatched {
                            command_id: receipt.command_id.clone(),
                            command_type: command_type.into(),
                            thread_id: Some(id.to_owned()),
                            payload: command.clone(),
                        },
                        Some(receipt.clone()),
                    )
                    .await?
                    .id);
            }
            if command["watch"].is_null() {
                let ended = command
                    .get("endedWatch")
                    .cloned()
                    .or_else(|| (!existing_watch.is_null()).then_some(existing_watch));
                link.as_object_mut().unwrap().remove("watch");
                if let Some(ended) = ended {
                    link["endedWatch"] = ended;
                }
            } else {
                let mut watch = command["watch"].clone();
                if command.get("checksPendingBefore").is_none()
                    || existing_watch.get("checksPending").is_some()
                {
                    link["watch"] = watch;
                } else if let Some(object) = watch.as_object_mut() {
                    object.remove("checksPending");
                    link["watch"] = Value::Object(object.clone());
                }
                link.as_object_mut().unwrap().remove("endedWatch");
            }
            drop(store);
            return Ok(state
                .commit_receipted(
                    Event::ThreadPatched {
                        thread_id: id.to_owned(),
                        patch: json!({"row":row}),
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "thread.issue.link"
        | "thread.issue.unlink"
        | "thread.pull-request.link"
        | "thread.pull-request.unlink"
        | "thread.pull-request.watch"
        | "thread.pull-request.sync"
        | "thread.pull-request-link.sync" => {
            let id = &receipt.thread_id;
            let store = state.store.read().await;
            let thread = store
                .threads
                .get(id)
                .ok_or_else(|| anyhow::anyhow!("thread not found"))?;
            let at = crate::auth_service::iso(crate::auth_service::now());
            let row = if command_type == "thread.pull-request.watch" {
                crate::thread_projection::watch_patch(thread, command, &at)
            } else if command_type == "thread.pull-request.sync" {
                let mut row = thread.stored_row();
                row["branchPullRequest"] = command["branchPullRequest"].clone();
                if command.get("linkedPullRequest").is_some() {
                    row["linkedPullRequest"] = command["linkedPullRequest"].clone();
                }
                row
            } else {
                crate::thread_projection::link_patch(thread, command, &at)
            };
            drop(store);
            return Ok(state
                .commit_receipted(
                    Event::ThreadPatched {
                        thread_id: id.clone(),
                        patch: json!({"row":row}),
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "thread.guest-share.close" | "thread.guest-message.dismiss" => {
            let id = thread_id.ok_or_else(|| anyhow::anyhow!("Missing thread id."))?;
            if command_type == "thread.guest-share.close" {
                state.close_guest_thread(id)
            } else {
                state.resolve_guest_message(
                    id,
                    command["guestMessageId"].as_str().unwrap_or(""),
                    "dismissed",
                )
            }
            .map_err(anyhow::Error::msg)?;
        }
        "thread.guest-share.open" | "thread.guest-share.redeem" | "thread.guest-message.submit" => {
            let id = thread_id.ok_or_else(|| anyhow::anyhow!("Missing thread id."))?;
            let mut row = state
                .store
                .read()
                .await
                .threads
                .get(id)
                .ok_or_else(|| anyhow::anyhow!("thread not found"))?
                .stored_row();
            match command_type {
                "thread.guest-share.open" => {
                    row["guestShare"] = json!({"id":command["shareId"],"host":command["host"],"createdAt":crate::auth_service::iso(crate::auth_service::now()),"expiresAt":command["expiresAt"],"guestName":Value::Null,"messages":[]});
                }
                "thread.guest-share.redeem" => {
                    if !row["guestShare"].is_object() {
                        return Err(anyhow::anyhow!("Guest share is not open."));
                    }
                    row["guestShare"]["guestName"] = command["guestName"].clone();
                }
                _ => {
                    let mut messages = row["guestShare"]["messages"]
                        .as_array()
                        .cloned()
                        .unwrap_or_default();
                    let mut message = json!({"id":command["guestMessageId"],"text":command["text"],"status":"pending","submittedAt":crate::auth_service::iso(crate::auth_service::now())});
                    if let Some(attachments) = command.get("attachments") {
                        message["attachments"] = attachments.clone();
                    }
                    messages.push(message);
                    row["guestShare"]["messages"] = json!(messages);
                }
            }
            return Ok(state
                .commit_receipted(
                    Event::ThreadPatched {
                        thread_id: id.to_owned(),
                        patch: json!({"row":row}),
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "thread.project.adopt-folder" => {
            let id = thread_id.ok_or_else(|| anyhow::anyhow!("Missing thread id."))?;
            let project_id = command["projectId"].as_str().unwrap_or_default();
            let store = state.store.read().await;
            let project = store
                .projects
                .get(project_id)
                .ok_or_else(|| anyhow::anyhow!(format!("Project {project_id} was not found.")))?;
            let thread = store
                .threads
                .get(id)
                .ok_or_else(|| anyhow::anyhow!("thread not found"))?;
            if thread
                .runs
                .iter()
                .any(|run| matches!(run.status.as_str(), "preparing" | "starting" | "running"))
            {
                return Err(anyhow::anyhow!(format!(
                    "Thread {id} still has work in progress. Finish or stop it first."
                )));
            }
            if thread.cwd != project.path {
                return Err(anyhow::anyhow!(format!(
                    "Thread {id} does not work in {}.",
                    project.path
                )));
            }
            let mut row = thread.stored_row();
            row["projectId"] = json!(project_id);
            row["worktreePath"] = Value::Null;
            drop(store);
            return Ok(state
                .commit_receipted(
                    Event::ThreadPatched {
                        thread_id: id.to_owned(),
                        patch: json!({"row":row}),
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "thread.archive"
        | "thread.unarchive"
        | "thread.auto-settle"
        | "thread.settle"
        | "thread.unsettle"
        | "thread.pin"
        | "thread.unpin"
        | "thread.mark-unread"
        | "thread.visit"
        | "thread.snooze"
        | "thread.unsnooze"
        | "thread.metadata.update"
        | "thread.title.regeneration.complete"
        | "thread.runtime-mode.set"
        | "thread.interaction-mode.set"
        | "thread.model-selection.set"
        | "thread.tag.set"
        | "thread.auto-settle.set"
        | "thread.pin.reorder"
        | "thread.active.reorder"
        | "provider.switch" => {
            if let Some(thread_id) = thread_id {
                let current = state.store.read().await;
                let existing_thread = current.threads.get(thread_id);
                let existing_row = existing_thread
                    .map(|thread| thread.stored_row())
                    .unwrap_or_else(|| json!({}));
                if command_type == "thread.metadata.update" {
                    if let Some(expected) = command.get("expectedWorktreePath") {
                        if expected != &existing_row["worktreePath"] {
                            return Err(anyhow::anyhow!(format!(
                                "Thread {thread_id} worktree changed before the metadata update could be applied."
                            )));
                        }
                    }
                    if let Some(expected) = command.get("expectedBranch") {
                        if expected != &existing_row["branch"] {
                            return Err(anyhow::anyhow!(format!(
                                "Thread {thread_id} branch changed before the metadata update could be applied."
                            )));
                        }
                    }
                    if command["expectedEmpty"] == true
                        && existing_thread.is_some_and(|thread| {
                            !thread.messages.is_empty() || !thread.runs.is_empty()
                        })
                    {
                        return Err(anyhow::anyhow!(format!(
                            "Thread {thread_id} is no longer empty."
                        )));
                    }
                }
                drop(current);
                let mut patch = serde_json::Map::new();
                let at = json!(crate::auth_service::iso(crate::auth_service::now()));
                let existing_title_regeneration = state
                    .store
                    .read()
                    .await
                    .threads
                    .get(thread_id)
                    .map(|thread| thread.stored_row()["titleRegeneration"].clone())
                    .unwrap_or(Value::Null);
                let existing_row = state
                    .store
                    .read()
                    .await
                    .threads
                    .get(thread_id)
                    .map(|thread| thread.stored_row())
                    .unwrap_or_else(|| json!({}));
                let mut row = json!({"updatedAt":at});
                match command_type {
                    "thread.archive" => {
                        patch.insert("archived".into(), true.into());
                        row["archivedAt"] = at;
                        row["titleRegeneration"] = Value::Null;
                        row["guestShare"] = Value::Null;
                    }
                    "thread.unarchive" => {
                        patch.insert("archived".into(), false.into());
                        row["archivedAt"] = Value::Null;
                    }
                    "thread.auto-settle" => {
                        let snapshot = command["snapshotAt"].as_str().unwrap_or_default();
                        let changed_at = state
                            .store
                            .read()
                            .await
                            .threads
                            .get(thread_id)
                            .map(|thread| {
                                thread.stored_row()["updatedAt"]
                                    .as_str()
                                    .unwrap_or_default()
                                    .to_owned()
                            })
                            .unwrap_or_default();
                        if !snapshot.is_empty() && changed_at.as_str() > snapshot {
                            return Err(anyhow::anyhow!(format!(
                                "Thread {thread_id} changed before automatic settlement."
                            )));
                        }
                        patch.insert("settled".into(), true.into());
                        patch.insert("pinned".into(), false.into());
                        row["settledOverride"] = "settled".into();
                        row["settledAt"] = command.get("settledAt").cloned().unwrap_or(at.clone());
                        row["unsettledAt"] = Value::Null;
                        row["pinnedAt"] = Value::Null;
                        row["pinOrderKey"] = Value::Null;
                        row["activeOrderKey"] = Value::Null;
                        row["snoozedUntil"] = Value::Null;
                        row["snoozedAt"] = Value::Null;
                    }
                    "thread.settle" => {
                        patch.insert("settled".into(), true.into());
                        patch.insert("pinned".into(), false.into());
                        row["settledOverride"] = "settled".into();
                        row["settledAt"] = command.get("settledAt").cloned().unwrap_or(at);
                        for k in [
                            "unsettledAt",
                            "pinnedAt",
                            "pinOrderKey",
                            "activeOrderKey",
                            "snoozedUntil",
                            "snoozedAt",
                        ] {
                            row[k] = Value::Null;
                        }
                    }
                    "thread.unsettle" => {
                        patch.insert("settled".into(), false.into());
                        row["settledOverride"] = "active".into();
                        row["settledAt"] = Value::Null;
                        row["unsettledAt"] = at;
                    }
                    "thread.pin" => {
                        patch.insert("pinned".into(), true.into());
                        row["pinnedAt"] = at;
                        row["settledOverride"] = if existing_row["settledOverride"] == "settled" {
                            Value::String("active".into())
                        } else {
                            existing_row["settledOverride"].clone()
                        };
                        if row["settledOverride"] == "active" {
                            row["settledAt"] = Value::Null;
                            row["unsettledAt"] =
                                json!(crate::auth_service::iso(crate::auth_service::now()));
                        }
                        row["snoozedUntil"] = Value::Null;
                        row["snoozedAt"] = Value::Null;
                        if let Some(key) = command.get("orderKey") {
                            row["pinOrderKey"] = key.clone();
                        }
                    }
                    "thread.unpin" => {
                        patch.insert("pinned".into(), false.into());
                        row["pinnedAt"] = Value::Null;
                        row["pinOrderKey"] = Value::Null;
                    }
                    "thread.mark-unread" => {
                        patch.insert("unread".into(), true.into());
                        if let Some(completed) = state
                            .store
                            .read()
                            .await
                            .threads
                            .get(thread_id)
                            .and_then(|thread| {
                                thread.runs.iter().filter_map(|run| run.finished_at).max()
                            })
                        {
                            row["lastVisitedAt"] = json!(crate::auth_service::iso(
                                (completed as i64).saturating_mul(1000).saturating_sub(1)
                            ));
                        }
                    }
                    "thread.snooze" => {
                        let snoozed_until =
                            command.get("snoozedUntil").cloned().unwrap_or(Value::Null);
                        patch.insert("snoozed_until".into(), snoozed_until.clone());
                        row["snoozedUntil"] = snoozed_until;
                        row["snoozedAt"] = at.clone();
                    }
                    "thread.unsnooze" => {
                        patch.insert("snoozed_until".into(), serde_json::Value::Null);
                        row["snoozedUntil"] = Value::Null;
                        row["snoozedAt"] = Value::Null;
                    }
                    "thread.visit" => {
                        if let Some(visited_at) = command.get("visitedAt") {
                            row["lastVisitedAt"] = visited_at.clone();
                        }
                        patch.insert("unread".into(), false.into());
                    }
                    "thread.title.regeneration.complete" => {
                        let request_id = command["requestId"].as_str().unwrap_or_default();
                        if existing_title_regeneration["requestId"] == request_id {
                            if let Some(title) = command.get("title") {
                                row["title"] = title.clone();
                                row["titleRenamed"] = false.into();
                                patch.insert("title".into(), title.clone());
                            }
                            if let Some(outcome) = command.get("outcome") {
                                row["lastTitleRegeneration"] = json!({
                                    "requestId": request_id,
                                    "completedAt": at,
                                    "outcome": outcome
                                });
                            }
                            row["titleRegeneration"] = Value::Null;
                        }
                    }
                    "thread.metadata.update" => {
                        if let Some(title) = command.get("title") {
                            patch.insert("title".into(), title.clone());
                            row["title"] = title.clone();
                            row["titleRenamed"] = true.into();
                            row["titleRegeneration"] = Value::Null;
                        }
                        for field in ["branch", "worktreePath", "linkedPullRequest"] {
                            if let Some(value) = command.get(field) {
                                row[field] = value.clone();
                                if field == "linkedPullRequest" {
                                    patch.insert(field.into(), value.clone());
                                    let mut links = existing_row["pullRequests"]
                                        .as_array()
                                        .cloned()
                                        .unwrap_or_default();
                                    if value.is_null() {
                                        if let Some(previous) =
                                            existing_row["linkedPullRequest"].as_object()
                                        {
                                            let repository =
                                                previous["repository"].as_str().unwrap_or_default();
                                            let number =
                                                previous["number"].as_i64().unwrap_or_default();
                                            links.retain(|link| {
                                                link["repository"] != repository
                                                    || link["number"] != number
                                            });
                                        }
                                    } else if let Some(linked) = value.as_object() {
                                        let repository =
                                            linked["repository"].as_str().unwrap_or_default();
                                        let number = linked["number"].as_i64().unwrap_or_default();
                                        links.retain(|link| {
                                            link["repository"] != repository
                                                || link["number"] != number
                                        });
                                        let host = linked["url"]
                                            .as_str()
                                            .and_then(|url| url::Url::parse(url).ok())
                                            .and_then(|url| url.host_str().map(str::to_owned))
                                            .unwrap_or_else(|| "github.com".into());
                                        links.push(json!({
                                            "host":host,"repository":repository,"number":number,
                                            "url":linked["url"],"source":"manual","linkedAt":at,
                                            "snapshot":null,"stack":null
                                        }));
                                    }
                                    row["pullRequests"] = json!(links);
                                }
                            }
                        }
                        if command.get("regenerateTitle") == Some(&Value::Bool(true)) {
                            row["titleRegeneration"] = json!({
                                "requestId": command["commandId"],
                                "startedAt": at
                            });
                        } else if command.get("regenerateTitle") == Some(&Value::Bool(false)) {
                            row["titleRegeneration"] = Value::Null;
                        }
                    }
                    "thread.runtime-mode.set" => {
                        row["runtimeMode"] = command["runtimeMode"].clone()
                    }
                    "thread.interaction-mode.set" => {
                        row["interactionMode"] = command["interactionMode"].clone()
                    }
                    "thread.model-selection.set" | "provider.switch" => {
                        row["modelSelection"] = command["modelSelection"].clone();
                        row["providerInstanceId"] = command["modelSelection"]["instanceId"].clone();
                    }
                    "thread.auto-settle.set" => {
                        row["autoSettleDisabledAt"] = if command["enabled"] == true {
                            Value::Null
                        } else {
                            at
                        }
                    }
                    "thread.pin.reorder" => row["pinOrderKey"] = command["orderKey"].clone(),
                    "thread.active.reorder" => row["activeOrderKey"] = command["orderKey"].clone(),
                    "thread.tag.set" => {
                        let tag = command.get("tag").and_then(serde_json::Value::as_str);
                        let tagged = command
                            .get("tagged")
                            .and_then(serde_json::Value::as_bool)
                            .unwrap_or(false);
                        let mut tags = state
                            .store
                            .read()
                            .await
                            .threads
                            .get(thread_id)
                            .map(|thread| thread.tags.clone())
                            .unwrap_or_default();
                        if let Some(tag) = tag {
                            if tagged && !tags.iter().any(|value| value == tag) {
                                tags.push(tag.into());
                            }
                            if !tagged {
                                tags.retain(|value| value != tag);
                            }
                        }
                        row["tags"] = json!(tags);
                        patch.insert("tags".into(), serde_json::json!(tags));
                    }
                    _ => {}
                }
                patch.insert("row".into(), row);
                return Ok(state
                    .commit_receipted(
                        Event::ThreadPatched {
                            thread_id: thread_id.to_owned(),
                            patch: patch.into(),
                        },
                        Some(receipt.clone()),
                    )
                    .await?
                    .id);
            }
        }
        "thread.delete" => {
            if let Some(thread_id) = thread_id {
                if command["onlyIfUnused"] == true {
                    let used =
                        state
                            .store
                            .read()
                            .await
                            .threads
                            .get(thread_id)
                            .is_some_and(|thread| {
                                !thread.messages.is_empty() || !thread.runs.is_empty()
                            });
                    if used {
                        return Ok(state
                            .commit_receipted(
                                Event::CommandDispatched {
                                    command_id: receipt.command_id.clone(),
                                    command_type: command_type.into(),
                                    thread_id: Some(thread_id.to_owned()),
                                    payload: command.clone(),
                                },
                                Some(receipt.clone()),
                            )
                            .await?
                            .id);
                    }
                }
                return Ok(state
                    .commit_receipted(
                        Event::ThreadDeleted {
                            thread_id: thread_id.to_owned(),
                        },
                        Some(receipt.clone()),
                    )
                    .await?
                    .id);
            }
        }
        "prepared-run.progress" => {
            let run_id = command["runId"].as_str().unwrap_or_default();
            let valid = state
                .store
                .read()
                .await
                .threads
                .get(thread_id.unwrap_or_default())
                .and_then(|thread| thread.runs.iter().find(|run| run.id.to_string() == run_id))
                .is_some_and(|run| run.status == "preparing");
            if !valid {
                return Err(anyhow::anyhow!(format!(
                    "Run {run_id} is not awaiting workspace preparation."
                )));
            }
            return Ok(state
                .commit_receipted(
                    Event::CommandDispatched {
                        command_id: receipt.command_id.clone(),
                        command_type: command_type.into(),
                        thread_id: Some(thread_id.unwrap_or_default().to_owned()),
                        payload: command.clone(),
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "prepared-run.release" => {
            let run_id = command["runId"].as_str().unwrap_or_default();
            let valid = state
                .store
                .read()
                .await
                .threads
                .get(thread_id.unwrap_or_default())
                .and_then(|thread| thread.runs.iter().find(|run| run.id.to_string() == run_id))
                .is_some_and(|run| run.status == "preparing");
            if !valid {
                return Err(anyhow::anyhow!(format!(
                    "Run {run_id} is not awaiting workspace preparation."
                )));
            }
            return start_existing_run(
                state,
                receipt,
                thread_id.unwrap_or_default(),
                run_id,
                "starting",
            )
            .await;
        }
        "prepared-run.fail" => {
            let run_id = command["runId"].as_str().unwrap_or_default();
            let thread_id = thread_id.unwrap_or_default();
            let valid = state
                .store
                .read()
                .await
                .threads
                .get(thread_id)
                .and_then(|thread| thread.runs.iter().find(|run| run.id.to_string() == run_id))
                .is_some_and(|run| run.status == "preparing");
            if !valid {
                return Err(anyhow::anyhow!(format!(
                    "Run {run_id} is not awaiting workspace preparation."
                )));
            }
            return Ok(state
                .commit_receipted(
                    Event::RunFinished {
                        run_id: Uuid::parse_str(run_id)?,
                        thread_id: thread_id.to_owned(),
                        status: "failed".into(),
                        error: command["failure"]["message"].as_str().map(str::to_owned),
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "prepared-run.retry" => {
            let run_id = command["runId"].as_str().unwrap_or_default();
            let thread_id = thread_id.unwrap_or_default();
            let current = state
                .store
                .read()
                .await
                .threads
                .get(thread_id)
                .and_then(|thread| thread.runs.iter().find(|run| run.id.to_string() == run_id))
                .cloned();
            let Some(mut run) = current else {
                return Err(anyhow::anyhow!(format!(
                    "Run {run_id} has no failed workspace preparation to retry."
                )));
            };
            if run.status != "failed" {
                return Err(anyhow::anyhow!(format!(
                    "Run {run_id} has no failed workspace preparation to retry."
                )));
            }
            run.status = "preparing".into();
            run.finished_at = None;
            run.error = None;
            return Ok(state
                .commit_receipted(Event::RunStarted(run), Some(receipt.clone()))
                .await?
                .id);
        }
        "provider-session.detach" => {
            let provider_session_id = command["providerSessionId"].as_str().unwrap_or_default();
            let thread_id = thread_id.unwrap_or_default();
            return Err(anyhow::anyhow!(format!(
                "Provider session {provider_session_id} does not belong to thread {thread_id}."
            )));
        }
        "queued-run.cancel" => {
            let run_id = command["runId"].as_str().unwrap_or_default();
            let valid = state
                .store
                .read()
                .await
                .threads
                .get(thread_id.unwrap_or_default())
                .and_then(|thread| thread.runs.iter().find(|run| run.id.to_string() == run_id))
                .is_some_and(|run| run.status == "queued");
            if !valid {
                return Err(anyhow::anyhow!(format!("Run {run_id} is not queued.")));
            }
            return Ok(state
                .commit_receipted(
                    Event::RunFinished {
                        run_id: Uuid::parse_str(run_id)?,
                        thread_id: thread_id.unwrap_or_default().to_owned(),
                        status: "cancelled".into(),
                        error: None,
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "queued-run.edit" => {
            let run_id = command["runId"].as_str().unwrap_or_default();
            let Some((message_id, thread_id)) = state
                .store
                .read()
                .await
                .threads
                .get(thread_id.unwrap_or_default())
                .and_then(|thread| thread.runs.iter().find(|run| run.id.to_string() == run_id))
                .filter(|run| run.status == "queued")
                .and_then(|run| run.user_message_id.clone())
                .map(|message_id| (message_id, thread_id.unwrap_or_default().to_owned()))
            else {
                return Err(anyhow::anyhow!(format!("Run {run_id} is not queued.")));
            };
            return Ok(state
                .commit_receipted(
                    Event::MessageEdited {
                        thread_id,
                        message_id,
                        content: command["text"].as_str().unwrap_or_default().to_owned(),
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "queued-run.reorder" => {
            let run_id = command["runId"].as_str().unwrap_or_default();
            let thread_id = thread_id.unwrap_or_default();
            let valid = state
                .store
                .read()
                .await
                .threads
                .get(thread_id)
                .and_then(|thread| thread.runs.iter().find(|run| run.id.to_string() == run_id))
                .is_some_and(|run| run.status == "queued");
            if !valid {
                return Err(anyhow::anyhow!(format!("Run {run_id} is not queued.")));
            }
            let before_run_id = match command.get("beforeRunId") {
                Some(Value::Null) | None => None,
                Some(value) => Some(Uuid::parse_str(value.as_str().unwrap_or_default())?),
            };
            return Ok(state
                .commit_receipted(
                    Event::RunReordered {
                        thread_id: thread_id.to_owned(),
                        run_id: Uuid::parse_str(run_id)?,
                        before_run_id,
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "queued-run.start" => {
            let thread_id = thread_id.unwrap_or_default();
            let run_id = command["runId"].as_str().unwrap_or_default();
            return start_queued_run(state, receipt, thread_id, run_id).await;
        }
        "queue.resume" => {
            let thread_id = thread_id.unwrap_or_default();
            let (queued, next, active) = {
                let store = state.store.read().await;
                let thread = store
                    .threads
                    .get(thread_id)
                    .ok_or_else(|| anyhow::anyhow!("thread not found"))?;
                let active = thread
                    .runs
                    .iter()
                    .any(|run| matches!(run.status.as_str(), "preparing" | "starting" | "running"));
                let queued = thread
                    .runs
                    .iter()
                    .filter(|run| run.status == "queued")
                    .cloned()
                    .collect::<Vec<_>>();
                let next = queued.first().cloned();
                (queued, next, active)
            };
            let held = queued
                .iter()
                .filter(|run| run.queue_held)
                .cloned()
                .collect::<Vec<_>>();
            let mut sequence = None;
            for (index, mut run) in held.into_iter().enumerate() {
                run.queue_held = false;
                sequence = Some(
                    state
                        .commit_receipted(
                            Event::RunUpdated(run),
                            (index + 1 == queued.iter().filter(|run| run.queue_held).count())
                                .then(|| receipt.clone()),
                        )
                        .await?
                        .id,
                );
            }
            if !active {
                if let Some(run) = next {
                    if queued.iter().all(|run| !run.queue_held) {
                        sequence = Some(
                            state
                                .commit_receipted(
                                    Event::CommandDispatched {
                                        command_id: receipt.command_id.clone(),
                                        command_type: command_type.into(),
                                        thread_id: Some(thread_id.to_owned()),
                                        payload: command.clone(),
                                    },
                                    Some(receipt.clone()),
                                )
                                .await?
                                .id,
                        );
                    }
                    start_existing_run(
                        state.clone(),
                        &CommandReceipt {
                            command_id: format!("{}:start", receipt.command_id),
                            thread_id: thread_id.to_owned(),
                        },
                        thread_id,
                        &run.id.to_string(),
                        "running",
                    )
                    .await?;
                }
            }
            if sequence.is_none() {
                sequence = Some(
                    state
                        .commit_receipted(
                            Event::CommandDispatched {
                                command_id: receipt.command_id.clone(),
                                command_type: command_type.into(),
                                thread_id: Some(thread_id.to_owned()),
                                payload: command.clone(),
                            },
                            Some(receipt.clone()),
                        )
                        .await?
                        .id,
                );
            }
            return Ok(sequence.unwrap_or(0));
        }
        "queued-message.promote-to-steer" => {
            let thread_id = thread_id.unwrap_or_default();
            let queued_run_id = command["queuedRunId"].as_str().unwrap_or_default();
            let target_run_id = command["targetRunId"].as_str().unwrap_or_default();
            let (queued_run, target_run, message) = {
                let store = state.store.read().await;
                let thread = store
                    .threads
                    .get(thread_id)
                    .ok_or_else(|| anyhow::anyhow!("thread not found"))?;
                let queued_run = thread
                    .runs
                    .iter()
                    .find(|run| run.id.to_string() == queued_run_id && run.status == "queued")
                    .cloned()
                    .ok_or_else(|| {
                        anyhow::anyhow!(format!("Queued run {queued_run_id} is not queued."))
                    })?;
                let target_run = thread
                    .runs
                    .iter()
                    .find(|run| run.id.to_string() == target_run_id)
                    .cloned()
                    .ok_or_else(|| {
                        anyhow::anyhow!(format!("Target run {target_run_id} was not found."))
                    })?;
                let message_id = queued_run.user_message_id.as_deref().ok_or_else(|| {
                    anyhow::anyhow!(format!("Queued run {queued_run_id} has no queued message."))
                })?;
                let message = thread
                    .messages
                    .iter()
                    .find(|message| message.id == message_id)
                    .cloned()
                    .ok_or_else(|| {
                        anyhow::anyhow!(format!(
                            "Queued run {queued_run_id} has no queued message."
                        ))
                    })?;
                (queued_run, target_run, message)
            };
            if message.content.trim_start().starts_with("/goal") {
                return Err(anyhow::anyhow!(
                    "Goal commands must run as a separate turn. Queue it or wait for the active turn to finish."
                ));
            }
            if message.content.trim_start().starts_with("/compact") {
                return Err(anyhow::anyhow!(
                    "Context compaction must run as a separate turn. Queue it or wait for the active turn to finish."
                ));
            }
            if target_run.status == "running" {
                if let Some(cancel) = state.cancellations().lock().await.remove(&target_run.id) {
                    let _ = cancel.send(());
                }
            }
            let mut cancelled = queued_run;
            cancelled.status = "cancelled".into();
            cancelled.finished_at = Some(now());
            cancelled.queue_held = false;
            return Ok(state
                .commit_receipted(Event::RunUpdated(cancelled), Some(receipt.clone()))
                .await?
                .id);
        }
        "runtime-request.respond" => {
            let request_id = command["requestId"].as_str().unwrap_or_default();
            let thread_id = thread_id.unwrap_or_default();
            if let Some(sequence) =
                respond_provider_request(&state, thread_id, request_id, command, receipt).await?
            {
                return Ok(sequence);
            }
            let current = state
                .store
                .read()
                .await
                .threads
                .get(thread_id)
                .and_then(|thread| {
                    thread
                        .row
                        .get("runtimeRequests")
                        .and_then(Value::as_array)
                        .and_then(|requests| {
                            requests.iter().find(|request| request["id"] == request_id)
                        })
                })
                .cloned();
            let Some(request) = current else {
                return Err(anyhow::anyhow!(format!(
                    "Runtime request {request_id} was not found."
                )));
            };
            if request["status"]
                .as_str()
                .is_some_and(|status| status != "pending")
            {
                return Err(anyhow::anyhow!(format!(
                    "Runtime request {request_id} is {}.",
                    request["status"].as_str().unwrap_or("resolved")
                )));
            }
            let mut requests = state
                .store
                .read()
                .await
                .threads
                .get(thread_id)
                .and_then(|thread| thread.row["runtimeRequests"].as_array().cloned())
                .unwrap_or_default();
            if let Some(existing) = requests
                .iter_mut()
                .find(|candidate| candidate["id"] == request_id)
            {
                existing["status"] = json!("resolved");
                existing["decision"] = command.get("decision").cloned().unwrap_or(Value::Null);
                existing["feedback"] = command.get("feedback").cloned().unwrap_or(Value::Null);
                existing["answers"] = command.get("answers").cloned().unwrap_or(Value::Null);
            }
            return Ok(state
                .commit_receipted(
                    Event::ThreadPatched {
                        thread_id: thread_id.to_owned(),
                        patch: json!({"row":{"runtimeRequests":requests}}),
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "thread.user-input.dismiss" => {
            return Err(anyhow::anyhow!("This question has already been answered."));
        }
        "thread.fork" => {
            let source_thread_id = command["sourceThreadId"].as_str().unwrap_or_default();
            let source_point = command["sourcePoint"]["type"].as_str().unwrap_or_default();
            let target_thread_id = command["targetThreadId"].as_str().unwrap_or_default();
            let source = state
                .store
                .read()
                .await
                .threads
                .get(source_thread_id)
                .cloned();
            let Some(source) = source else {
                return Err(anyhow::anyhow!(format!(
                    "No orchestration projection exists for thread {source_thread_id}."
                )));
            };
            if source_point == "latest_stable"
                && !source.runs.iter().any(|run| run.status == "completed")
            {
                return Err(anyhow::anyhow!(
                    "No stable source run was found for fork source latest_stable."
                ));
            }
            let at = crate::auth_service::iso(crate::auth_service::now());
            let mut row = source.stored_row();
            row["id"] = json!(target_thread_id);
            row["title"] = command
                .get("title")
                .cloned()
                .unwrap_or_else(|| json!(source.title));
            row["createdAt"] = json!(at.clone());
            row["updatedAt"] = json!(at);
            row["archivedAt"] = Value::Null;
            row["deletedAt"] = Value::Null;
            row["lineage"] = json!({"parentThreadId":source_thread_id,"relationshipToParent":"fork","rootThreadId":source.row["lineage"]["rootThreadId"].as_str().unwrap_or(source_thread_id),"sideChat":command.get("sideChat").and_then(Value::as_bool).unwrap_or(false)});
            let forked_from = source
                .runs
                .iter()
                .rev()
                .find(|run| run.status == "completed")
                .map(|run| json!({"type":"run","threadId":source_thread_id,"runId":run.id}))
                .unwrap_or(Value::Null);
            row["forkedFrom"] = forked_from;
            let thread = Thread {
                row,
                id: target_thread_id.to_owned(),
                project_id: source.project_id,
                title: command
                    .get("title")
                    .and_then(Value::as_str)
                    .unwrap_or(&source.title)
                    .to_owned(),
                cwd: source.cwd,
                status: "idle".into(),
                messages: source.messages,
                runs: vec![],
                created_at: now(),
                archived: false,
                settled: false,
                snoozed_until: None,
                pinned: false,
                unread: false,
                tags: source.tags,
                checkpoints: vec![],
                provider: Default::default(),
            };
            return Ok(state
                .commit_receipted(Event::ThreadCreated(thread), Some(receipt.clone()))
                .await?
                .id);
        }
        "thread.merge_back" => {
            let source_thread_id = command["sourceThreadId"].as_str().unwrap_or_default();
            let target_thread_id = command["targetThreadId"].as_str().unwrap_or_default();
            let store = state.store.read().await;
            let source = store
                .threads
                .get(source_thread_id)
                .cloned()
                .ok_or_else(|| {
                    anyhow::anyhow!(format!(
                        "No orchestration projection exists for thread {source_thread_id}."
                    ))
                })?;
            let target = store
                .threads
                .get(target_thread_id)
                .cloned()
                .ok_or_else(|| {
                    anyhow::anyhow!(format!(
                        "No orchestration projection exists for thread {target_thread_id}."
                    ))
                })?;
            let existing = target
                .messages
                .iter()
                .map(|m| m.id.as_str())
                .collect::<std::collections::HashSet<_>>();
            let additions = source
                .messages
                .into_iter()
                .filter(|m| !existing.contains(m.id.as_str()))
                .collect::<Vec<_>>();
            drop(store);
            for message in additions {
                state
                    .commit(Event::MessageAdded {
                        thread_id: target_thread_id.to_owned(),
                        message,
                    })
                    .await?;
            }
            let sequence = state
                .commit_receipted(
                    Event::CommandDispatched {
                        command_id: receipt.command_id.clone(),
                        command_type: command_type.into(),
                        thread_id: Some(target_thread_id.to_owned()),
                        payload: command.clone(),
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id;
            return Ok(sequence);
        }
        "delegated_task.request" => {
            let parent_thread_id = command["parentThreadId"].as_str().unwrap_or_default();
            let generated_child_id;
            let target_thread_id = if let Some(value) = command["childThreadId"]
                .as_str()
                .or_else(|| command["targetThreadId"].as_str())
            {
                value
            } else {
                generated_child_id = format!(
                    "delegated-task:{}",
                    command["commandId"].as_str().unwrap_or_default()
                );
                generated_child_id.as_str()
            };
            let parent = state
                .store
                .read()
                .await
                .threads
                .get(parent_thread_id)
                .cloned()
                .ok_or_else(|| {
                    anyhow::anyhow!(format!(
                        "No orchestration projection exists for thread {parent_thread_id}."
                    ))
                })?;
            let at = crate::auth_service::iso(crate::auth_service::now());
            let mut row = parent.stored_row();
            row["id"] = json!(target_thread_id);
            row["title"] = command
                .get("title")
                .cloned()
                .unwrap_or_else(|| json!("Subagent"));
            row["createdAt"] = json!(at.clone());
            row["updatedAt"] = json!(at);
            row["lineage"] = json!({"parentThreadId":parent_thread_id,"relationshipToParent":"subagent","rootThreadId":parent.row["lineage"]["rootThreadId"].as_str().unwrap_or(parent_thread_id),"sideChat":false});
            row["forkedFrom"] = json!({"type":"node","nodeId":command["parentNodeId"]});
            let prompt = command["task"].as_str().unwrap_or_default();
            let task_id = format!(
                "node:delegated-task:{}",
                command["commandId"].as_str().unwrap_or_default()
            );
            let task = json!({
                "id": task_id,
                "threadId": parent_thread_id,
                "runId": command["parentRunId"],
                "parentNodeId": command["parentNodeId"],
                "origin": "app_owned",
                "createdBy": command["createdBy"].as_str().unwrap_or("user"),
                "driver": command["modelSelection"]["instanceId"],
                "providerInstanceId": command["modelSelection"]["instanceId"],
                "providerThreadId": Value::Null,
                "childThreadId": target_thread_id,
                "nativeTaskRef": Value::Null,
                "prompt": prompt,
                "title": command.get("title").cloned().unwrap_or(Value::Null),
                "model": command["modelSelection"]["model"],
                "modelSelection": command["modelSelection"],
                "completionWake": command.get("completionWake").cloned().unwrap_or(json!("settled_only")),
                "completionDelivery": {"state":"pending","observedByRunId":Value::Null},
                "status": "running",
                "result": Value::Null,
                "startedAt": at,
                "completedAt": Value::Null,
                "updatedAt": at
            });
            let message = ConversationMessage {
                id: format!(
                    "{}:task",
                    command["commandId"].as_str().unwrap_or(target_thread_id)
                ),
                role: "user".into(),
                content: prompt.into(),
                created_at: now(),
            };
            let thread = Thread {
                row,
                id: target_thread_id.to_owned(),
                project_id: parent.project_id,
                title: command
                    .get("title")
                    .and_then(Value::as_str)
                    .unwrap_or("Subagent")
                    .to_owned(),
                cwd: parent.cwd,
                status: "idle".into(),
                messages: vec![message],
                runs: vec![],
                created_at: now(),
                archived: false,
                settled: false,
                snoozed_until: None,
                pinned: false,
                unread: false,
                tags: vec![],
                checkpoints: vec![],
                provider: Default::default(),
            };
            let mut subagents = parent.row["subagents"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            subagents.push(task);
            state
                .commit(Event::ThreadPatched {
                    thread_id: parent_thread_id.to_owned(),
                    patch: json!({"row":{"subagents":subagents}}),
                })
                .await?;
            return Ok(state
                .commit_receipted(Event::ThreadCreated(thread), Some(receipt.clone()))
                .await?
                .id);
        }
        "delegated_task.wake-policy"
        | "delegated_task.completion-delivery.acknowledge"
        | "delegated_task.completion-delivery.dispose" => {
            let parent_thread_id = command["parentThreadId"].as_str().unwrap_or_default();
            let task_id = command["taskId"].as_str().unwrap_or_default();
            let mut subagents = state
                .store
                .read()
                .await
                .threads
                .get(parent_thread_id)
                .and_then(|thread| thread.row["subagents"].as_array().cloned())
                .unwrap_or_default();
            let Some(task) = subagents.iter_mut().find(|task| task["id"] == task_id) else {
                return Err(anyhow::anyhow!(format!(
                    "Delegated task {task_id} is not an app-owned task of thread {parent_thread_id}."
                )));
            };
            if command_type == "delegated_task.wake-policy" {
                let wake = command["completionWake"].as_str().unwrap_or_default();
                if task["completionWake"].as_str() == Some(wake) {
                    return Err(anyhow::anyhow!(format!(
                        "Delegated task {task_id} already wakes the parent with completionWake {wake}."
                    )));
                }
                task["completionWake"] = json!(wake);
            } else {
                let delivery_state = if command_type.ends_with("acknowledge") {
                    "acknowledged"
                } else {
                    "disposed"
                };
                let current = task["completionDelivery"]["state"].as_str();
                if current == Some(delivery_state)
                    || (delivery_state == "acknowledged" && current == Some("disposed"))
                {
                    return Ok(state
                        .commit_receipted(
                            Event::ThreadPatched {
                                thread_id: parent_thread_id.to_owned(),
                                patch: json!({"row":{"subagents":subagents}}),
                            },
                            Some(receipt.clone()),
                        )
                        .await?
                        .id);
                }
                task["completionDelivery"] = json!({
                    "state": delivery_state,
                    "observedByRunId": command.get("observedByRunId").cloned().unwrap_or(Value::Null)
                });
            }
            return Ok(state
                .commit_receipted(
                    Event::ThreadPatched {
                        thread_id: parent_thread_id.to_owned(),
                        patch: json!({"row":{"subagents":subagents}}),
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "delegated_task.remote.request" => {
            let parent_thread_id = command["parentThreadId"].as_str().unwrap_or_default();
            let task_id = format!(
                "node:delegated-task:{}",
                command["commandId"].as_str().unwrap_or_default()
            );
            let mut subagents = state
                .store
                .read()
                .await
                .threads
                .get(parent_thread_id)
                .map(|thread| {
                    thread.row["subagents"]
                        .as_array()
                        .cloned()
                        .unwrap_or_default()
                })
                .ok_or_else(|| {
                    anyhow::anyhow!(format!(
                        "No orchestration projection exists for thread {parent_thread_id}."
                    ))
                })?;
            subagents.push(json!({"id":task_id,"threadId":parent_thread_id,"runId":command["parentRunId"],"parentNodeId":command["parentNodeId"],"origin":"app_owned","driver":command["driver"],"providerInstanceId":command["modelSelection"]["instanceId"],"providerThreadId":Value::Null,"childThreadId":Value::Null,"remoteChild":command["remoteChild"],"prompt":command["task"],"title":command.get("title").cloned().unwrap_or(Value::Null),"model":command["modelSelection"]["model"],"modelSelection":command["modelSelection"],"completionWake":command.get("completionWake").cloned().unwrap_or(json!("settled_only")),"status":"running","result":Value::Null,"startedAt":crate::auth_service::iso(crate::auth_service::now()),"completedAt":Value::Null,"updatedAt":crate::auth_service::iso(crate::auth_service::now())}));
            return Ok(state
                .commit_receipted(
                    Event::ThreadPatched {
                        thread_id: parent_thread_id.to_owned(),
                        patch: json!({"row":{"subagents":subagents}}),
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "delegated_task.remote.complete" | "delegated_task.remote.stop" => {
            let parent_thread_id = command["parentThreadId"].as_str().unwrap_or_default();
            let task_id = command["taskId"].as_str().unwrap_or_default();
            let mut subagents = state
                .store
                .read()
                .await
                .threads
                .get(parent_thread_id)
                .map(|thread| {
                    thread.row["subagents"]
                        .as_array()
                        .cloned()
                        .unwrap_or_default()
                })
                .ok_or_else(|| {
                    anyhow::anyhow!(format!(
                        "No orchestration projection exists for thread {parent_thread_id}."
                    ))
                })?;
            let task = subagents
                .iter_mut()
                .find(|task| task["id"] == task_id)
                .ok_or_else(|| {
                    anyhow::anyhow!(format!("Delegated task {task_id} was not found."))
                })?;
            if command_type == "delegated_task.remote.complete" {
                if task["result"].is_null() {
                    task["status"] = command["status"].clone();
                    task["result"] = command["result"].clone();
                    task["completedAt"] =
                        json!(crate::auth_service::iso(crate::auth_service::now()));
                }
                if let Some(stopped) = command.get("stoppedThere") {
                    task["remoteChild"]["stoppedThere"] = json!(stopped == "stopped");
                }
            } else {
                let count = task["remoteChild"]["stopCount"].as_u64().unwrap_or(0) + 1;
                task["remoteChild"]["stopCount"] = json!(count);
            }
            return Ok(state
                .commit_receipted(
                    Event::ThreadPatched {
                        thread_id: parent_thread_id.to_owned(),
                        patch: json!({"row":{"subagents":subagents}}),
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "checkpoint.rollback.fail" => {
            let id = thread_id.unwrap_or_default();
            let row = state
                .store
                .read()
                .await
                .threads
                .get(id)
                .map(|t| t.stored_row())
                .ok_or_else(|| anyhow::anyhow!("thread not found"))?;
            if row["rollbackRequestId"].as_str() != command["requestId"].as_str() {
                return Err(anyhow::anyhow!(format!(
                    "Rollback {} is no longer the thread's current rollback.",
                    command["requestId"].as_str().unwrap_or_default()
                )));
            }
            let mut next = row;
            next["rollbackFailure"] =
                json!({"requestId":command["requestId"],"message":command["message"]});
            return Ok(state
                .commit_receipted(
                    Event::ThreadPatched {
                        thread_id: id.to_owned(),
                        patch: json!({"row":next}),
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "runtime-request.delivery.fail" => {
            let id = thread_id.unwrap_or_default();
            let mut row = state
                .store
                .read()
                .await
                .threads
                .get(id)
                .map(|t| t.stored_row())
                .ok_or_else(|| anyhow::anyhow!("thread not found"))?;
            let request_id = command["requestId"].as_str().unwrap_or_default();
            if let Some(request) = row["runtimeRequests"]
                .as_array_mut()
                .and_then(|items| items.iter_mut().find(|item| item["id"] == request_id))
            {
                request["deliveryError"] = json!(true);
            }
            return Ok(state
                .commit_receipted(
                    Event::ThreadPatched {
                        thread_id: id.to_owned(),
                        patch: json!({"row":row}),
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "thread.interrupt.unresponsive" => {
            let id = thread_id.unwrap_or_default();
            return Ok(state.commit_receipted(Event::ThreadPatched {thread_id:id.to_owned(),patch:json!({"row":{"interruptUnresponsive":true,"providerThreadId":command["providerThreadId"],"providerTurnId":command["providerTurnId"]}})},Some(receipt.clone())).await?.id);
        }
        "thread.background-work.settle" => {
            let id = thread_id.unwrap_or_default();
            return Ok(state.commit_receipted(Event::ThreadPatched {thread_id:id.to_owned(),patch:json!({"row":{"backgroundWorkSettled":true,"backgroundWorkSettleReason":command.get("reason").cloned().unwrap_or(Value::Null)}})},Some(receipt.clone())).await?.id);
        }
        "secret_request.record" | "link_request.record" => {
            let id = thread_id.unwrap_or_default();
            let mut row = state
                .store
                .read()
                .await
                .threads
                .get(id)
                .map(|t| t.stored_row())
                .ok_or_else(|| anyhow::anyhow!("thread not found"))?;
            let now = crate::auth_service::iso(crate::auth_service::now());
            let mut items = row["turnItems"].as_array().cloned().unwrap_or_default();
            let turn_item_id = command["turnItemId"].as_str().unwrap_or_default();
            let existing = items
                .iter()
                .find(|item| item["id"].as_str() == Some(turn_item_id))
                .cloned();
            let unchanged = existing.as_ref().is_some_and(|item| {
                (command_type == "secret_request.record"
                    && item["secretStatus"] != "pending"
                    && command["secretStatus"] != "pending")
                    || (command_type == "link_request.record"
                        && item["linkStatus"] != "pending"
                        && command["linkStatus"] != "pending")
            });
            let item = if unchanged {
                existing.unwrap()
            } else if command_type == "secret_request.record" {
                let pending = command["secretStatus"] == "pending";
                json!({
                    "id":turn_item_id,
                    "threadId":id,
                    "runId":command["runId"],
                    "nodeId":command["nodeId"],
                    "providerThreadId":Value::Null,
                    "providerTurnId":Value::Null,
                    "nativeItemRef":Value::Null,
                    "parentItemId":Value::Null,
                    "ordinal":existing.as_ref().and_then(|item| item["ordinal"].as_u64()).unwrap_or(items.len() as u64),
                    "status":if pending {"waiting"} else if command["secretStatus"] == "saved" {"completed"} else {"cancelled"},
                    "title":command["label"],
                    "startedAt":existing.as_ref().and_then(|item| item["startedAt"].as_str()).unwrap_or(&now),
                    "completedAt":if pending {Value::Null} else {json!(now)},
                    "updatedAt":now,
                    "type":"secret_request",
                    "label":command["label"],
                    "reason":command["reason"],
                    "url":command.get("url").cloned().unwrap_or(Value::Null),
                    "placeholder":command.get("placeholder").cloned().unwrap_or(Value::Null),
                    "delivery":command.get("delivery").cloned().unwrap_or(Value::Null),
                    "secretStatus":command["secretStatus"]
                })
            } else {
                let pending = command["linkStatus"] == "pending";
                let status = if pending {
                    "waiting"
                } else if command["linkStatus"] == "linked" {
                    "completed"
                } else if command["linkStatus"] == "failed" {
                    "failed"
                } else {
                    "cancelled"
                };
                let mut item = json!({
                    "id":turn_item_id,
                    "threadId":id,
                    "runId":command["runId"],
                    "nodeId":command["nodeId"],
                    "providerThreadId":Value::Null,
                    "providerTurnId":Value::Null,
                    "nativeItemRef":Value::Null,
                    "parentItemId":Value::Null,
                    "ordinal":existing.as_ref().and_then(|item| item["ordinal"].as_u64()).unwrap_or(items.len() as u64),
                    "status":status,
                    "title":command.get("label").cloned().unwrap_or_else(|| command.get("url").cloned().unwrap_or(json!("Link an environment"))),
                    "startedAt":existing.as_ref().and_then(|item| item["startedAt"].as_str()).unwrap_or(&now),
                    "completedAt":if pending {Value::Null} else {json!(now)},
                    "updatedAt":now,
                    "type":"link_request",
                    "url":command.get("url").cloned().unwrap_or(Value::Null),
                    "reason":command["reason"],
                    "linkStatus":command["linkStatus"]
                });
                for key in [
                    "environmentId",
                    "label",
                    "hint",
                    "requestedAccess",
                    "linkedEnvironmentId",
                    "linkedLabel",
                    "linkedAccess",
                    "failure",
                ] {
                    if let Some(value) = command.get(key) {
                        item[key] = value.clone();
                    }
                }
                item
            };
            items.retain(|candidate| candidate["id"] != turn_item_id);
            items.push(item);
            row["turnItems"] = json!(items);
            return Ok(state
                .commit_receipted(
                    Event::ThreadPatched {
                        thread_id: id.to_owned(),
                        patch: json!({"row":row}),
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "notification.delivery.accept" => {
            let thread_id = thread_id.unwrap_or_default();
            let message_id = command["messageId"].as_str().unwrap_or_default();
            let exists = state
                .store
                .read()
                .await
                .threads
                .get(thread_id)
                .is_some_and(|thread| {
                    thread
                        .messages
                        .iter()
                        .any(|message| message.id == message_id)
                });
            if !exists {
                return Err(anyhow::anyhow!(format!(
                    "Notification message {message_id} was not found."
                )));
            }
            return Ok(state
                .commit_receipted(
                    Event::CommandDispatched {
                        command_id: receipt.command_id.clone(),
                        command_type: command_type.into(),
                        thread_id: Some(thread_id.to_owned()),
                        payload: command.clone(),
                    },
                    Some(receipt.clone()),
                )
                .await?
                .id);
        }
        "checkpoint.rollback" => {
            let thread_id = thread_id.unwrap_or_default();
            let checkpoint_id = command["checkpointId"].as_str().unwrap_or_default();
            let target = state
                .store
                .read()
                .await
                .threads
                .get(thread_id)
                .and_then(|thread| {
                    thread.checkpoints.iter().find(|checkpoint| {
                        crate::thread_projection::checkpoint_id(thread_id, checkpoint.turn_count)
                            == checkpoint_id
                    })
                })
                .cloned();
            let Some(target) = target else {
                return Err(anyhow::anyhow!(format!(
                    "Checkpoint {checkpoint_id} was not found."
                )));
            };
            if !target.ready && command.get("restoreFiles").and_then(Value::as_bool) != Some(false)
            {
                return Err(anyhow::anyhow!(format!(
                    "Checkpoint {checkpoint_id} is missing and cannot be restored."
                )));
            }
            let expected_scope = crate::thread_projection::checkpoint_scope_id(thread_id);
            if command["scopeId"].as_str() != Some(expected_scope.as_str()) {
                return Err(anyhow::anyhow!(format!(
                    "Checkpoint {checkpoint_id} belongs to scope {expected_scope}, not {}.",
                    command["scopeId"].as_str().unwrap_or_default()
                )));
            }
            let second = state
                .commit_receipted(
                    Event::ThreadPatched {
                        thread_id: thread_id.to_owned(),
                        patch: json!({"row":{"rollbackRequestId":receipt.command_id,"rollbackFailure":Value::Null}}),
                    },
                    Some(receipt.clone()),
                )
                .await?;
            return Ok(second.id);
        }
        "thread.created.record" => {
            let parent_thread_id = command["parentThreadId"].as_str().unwrap_or_default();
            let target_thread_id = command["targetThreadId"].as_str().unwrap_or_default();
            let store = state.store.read().await;
            let parent = store.threads.get(parent_thread_id).ok_or_else(|| {
                anyhow::anyhow!(format!(
                    "No orchestration projection exists for thread {parent_thread_id}."
                ))
            })?;
            let target = store.threads.get(target_thread_id).ok_or_else(|| {
                anyhow::anyhow!(format!(
                    "No orchestration projection exists for thread {target_thread_id}."
                ))
            })?;
            if parent.project_id != target.project_id {
                return Err(anyhow::anyhow!(format!(
                    "Target thread {target_thread_id} belongs to another project."
                )));
            }
            let parent_run_id = command["parentRunId"].as_str().unwrap_or_default();
            if !parent
                .runs
                .iter()
                .any(|run| run.id.to_string() == parent_run_id)
            {
                return Err(anyhow::anyhow!(format!(
                    "Parent node {} is not the root of run {parent_run_id}.",
                    command["parentNodeId"].as_str().unwrap_or_default()
                )));
            }
            if let Some(target_run_id) = command["targetRunId"].as_str() {
                if !target
                    .runs
                    .iter()
                    .any(|run| run.id.to_string() == target_run_id)
                {
                    return Err(anyhow::anyhow!(format!(
                        "Target run {target_run_id} does not belong to thread {target_thread_id}."
                    )));
                }
            }
            drop(store);
        }
        // Every command discriminator has a registered wire contract. Commands
        // whose provider-specific state is not available locally still need a
        // durable receipt so retries remain idempotent and clients observe the
        // same dispatch sequence shape as the reference server.
        _ => {}
    }
    Ok(state
        .commit_receipted(
            Event::CommandDispatched {
                command_id: receipt.command_id.clone(),
                command_type: command_type.into(),
                thread_id: thread_id.map(str::to_owned),
                payload: command.clone(),
            },
            Some(receipt.clone()),
        )
        .await?
        .id)
}

/// Answer a runtime request a provider turn waits on, as the reference's
/// `runtime-request.respond` does: check it can be answered, record the
/// answer, then hand it to the turn. `None` when the thread's provider turns
/// made no request `request_id`.
async fn respond_provider_request<H: CommandHost>(
    state: &Arc<H>,
    thread_id: &str,
    request_id: &str,
    command: &Value,
    receipt: &crate::CommandReceipt,
) -> anyhow::Result<Option<u64>> {
    let found = {
        let store = state.store.read().await;
        let Some(thread) = store.threads.get(thread_id) else {
            return Ok(None);
        };
        let Some(record) = thread
            .provider_records()
            .runtime_requests
            .into_iter()
            .find(|record| record["id"] == request_id)
        else {
            return Ok(None);
        };
        let uuid = request_id.rsplit(':').next().unwrap_or_default().to_owned();
        let Some((run_id, _)) = thread.provider.request(&uuid) else {
            return Ok(None);
        };
        (record, uuid, run_id)
    };
    let (record, uuid, run_id) = found;
    let status = record["status"].as_str().unwrap_or("resolved");
    if status != "pending" {
        anyhow::bail!("Runtime request {request_id} is {status}.");
    }
    let capability = &record["responseCapability"];
    if capability["type"] == "not_resumable" {
        anyhow::bail!(
            "{}",
            capability["reason"]
                .as_str()
                .unwrap_or("This request cannot be answered.")
        );
    }
    let decision = command["decision"].as_str().map(str::to_owned);
    let feedback = command["feedback"].as_str().map(str::to_owned);
    if feedback.is_some()
        && (capability["type"] != "live"
            || capability["supportsFeedback"] != true
            || record["kind"] == "user_input"
            || decision.as_deref() != Some("decline")
            || feedback.as_deref().is_some_and(|f| f.trim().is_empty()))
    {
        anyhow::bail!("Feedback requires a live approval that supports it and a decline decision.");
    }
    if capability["type"] == "message" {
        // The CLI that asked is gone; the reference sends the answers as a
        // new message, which this server does not do yet.
        anyhow::bail!("This question can only be answered with a new message.");
    }
    // Each answer as its plain value, as the reference decodes them.
    let answers = command
        .get("answers")
        .filter(|a| !a.is_null())
        .map(|answers| {
            let flat: serde_json::Map<String, Value> = answers
                .as_object()
                .into_iter()
                .flatten()
                .map(|(id, answer)| {
                    let value = answer
                        .get("answers")
                        .cloned()
                        .unwrap_or_else(|| answer.clone());
                    (id.clone(), value)
                })
                .collect();
            Value::Object(flat)
        });
    let sequence = state
        .commit_receipted(
            Event::RunProvider {
                run_id,
                thread_id: thread_id.to_owned(),
                event: crate::workspace::ProviderEvent::RuntimeRequestAnswered {
                    id: uuid.clone(),
                    decision: decision.clone(),
                    feedback: feedback.clone(),
                    answers: answers.clone(),
                },
                position: None,
            },
            Some(receipt.clone()),
        )
        .await?
        .id;
    let delivered = state
        .workspace()
        .respond_turn(
            &run_id.to_string(),
            crate::workspace::provider::RuntimeResponse {
                id: uuid.clone(),
                decision,
                feedback,
                answers,
            },
        )
        .await;
    if !delivered {
        let _ = state
            .commit(Event::RunProvider {
                run_id,
                thread_id: thread_id.to_owned(),
                event: crate::workspace::ProviderEvent::RuntimeRequestDeliveryFailed { id: uuid },
                position: None,
            })
            .await;
    }
    Ok(Some(sequence))
}

#[cfg(test)]
mod tests {
    use super::provider_instance_enabled;
    use serde_json::json;

    #[test]
    fn provider_instances_are_enabled_as_the_reference_resolves_them() {
        let settings = json!({
            "providerInstances": {
                "codex": {"driver": "codex"},
                "work": {"driver": "codex", "enabled": false},
                "nested": {"driver": "claudeAgent", "config": {"enabled": false}},
                "cursor": {"driver": "cursor"},
                "cursorOn": {"driver": "cursor", "enabled": true}
            },
            "projectSettingsOverrides": {
                "p2": {"providerInstances": {"codex": {"enabled": false}, "work": {"enabled": true}}}
            }
        });
        let enabled =
            |project: &str, instance: &str| provider_instance_enabled(&settings, project, instance);
        assert!(enabled("p1", "codex"));
        assert!(!enabled("p1", "work"));
        assert!(!enabled("p1", "nested"));
        assert!(!enabled("p1", "cursor"));
        assert!(enabled("p1", "cursorOn"));
        // Unconfigured: only the built-in defaults.
        assert!(enabled("p1", "claudeAgent"));
        assert!(!enabled("p1", "parity-absent"));
        // A project's override wins.
        assert!(!enabled("p2", "codex"));
        assert!(enabled("p2", "work"));
    }
}
