//! The projection records of a thread's provider turns, in the reference's
//! shapes: for each run, its attempt, root node, provider turn (with token
//! usage), the items the provider reported with their nodes, assistant
//! messages, the todo list, and the error or interrupt items its end adds;
//! and the thread's provider thread.
//!
//! Ids follow the reference's id allocator (`joinId`: parts joined with
//! `:`, each percent-encoded, so nested ids are encoded again). Item
//! ordinals follow its position store: run N owns
//! `N * 1_000_000 ..= N * 1_000_000 + 999_999`, its user message takes the
//! first slot and each later item the next free one, in the order the items
//! were first seen.
use crate::{
    provider_state::{ItemRecord, RequestRecord, RunRecord, Terminal},
    thread_projection::{iso, uri_component},
    workspace::provider::{self, TurnStatus},
    Run, Thread,
};
use serde_json::{json, Map, Value};

fn join(prefix: &str, parts: &[&str]) -> String {
    let mut id = prefix.to_owned();
    for part in parts {
        id.push(':');
        id.push_str(&uri_component(part));
    }
    id
}

/// The ids one provider run's records share.
pub(crate) struct RunIds {
    pub run: String,
    pub attempt: String,
    pub root_node: String,
    pub provider_thread: String,
    pub provider_turn: String,
    pub driver: String,
}

fn item_id(prefix: &str, driver: &str, native: &str) -> String {
    format!(
        "{prefix}:provider:{}:native-item:{}",
        uri_component(driver),
        uri_component(native)
    )
}

fn native_ref(driver: &str, native: &str, strength: &str) -> Value {
    json!({"driver": driver, "nativeId": native, "strength": strength})
}

/// What the projection adds for a thread's provider turns.
#[derive(Default)]
pub(crate) struct ProviderRecords {
    pub attempts: Vec<Value>,
    pub nodes: Vec<Value>,
    pub provider_threads: Vec<Value>,
    pub provider_sessions: Vec<Value>,
    pub provider_turns: Vec<Value>,
    pub messages: Vec<Value>,
    pub plans: Vec<Value>,
    pub runtime_requests: Vec<Value>,
    /// Turn items by the index of their run, in the order they take slots.
    pub items: Vec<(usize, Value)>,
}

/// The reference's RUNTIME_REQUEST_DELIVERY_FAILED_MESSAGE.
const DELIVERY_FAILED: &str = "T3 Code could not confirm that your answer reached the agent. If the run stays stuck, stop it and try again.";

/// A failure as the projection carries it.
fn failure_json(failure: &provider::ProviderFailure) -> Value {
    let mut out = json!({
        "class": failure.class,
        "message": failure.message,
        "code": failure.code,
        "retryable": failure.retryable
    });
    if let Some(reset) = &failure.reset_at {
        out["resetAt"] = json!(reset);
    }
    out
}

/// What the server says when a restart cut off the turn a request waited in.
const RESTART_REASON: &str = "The server restarted before this runtime request was resolved.";
const SHUTDOWN_REASON: &str = "The server shut down before this runtime request was resolved.";

/// How a request stands, after a restart may have cut its turn off: its
/// status, how it can be answered, and when it stopped waiting.
pub(crate) struct RequestState {
    pub status: &'static str,
    pub capability: Value,
    pub resolved_at: Option<u64>,
}

impl Thread {
    /// `request`'s state in `run`, whose session is `session_id`. A restart
    /// loses the CLI that waited for it: an approval expires, and a question
    /// stays open to be answered by a message, as the reference recovers
    /// them.
    pub(crate) fn request_state(
        &self,
        run: &Run,
        request: &RequestRecord,
        session_id: &Value,
    ) -> RequestState {
        let restarted = request.pending()
            && (run.restart_cut.is_some()
                || run.error.as_deref() == Some(crate::turn::RESTART_INTERRUPTION));
        let shut_down = run
            .restart_cut
            .as_ref()
            .is_some_and(|cut| cut.trigger == "shutdown");
        if restarted && request.request.kind == "user_input" {
            return RequestState {
                status: "pending",
                capability: json!({"type": "message"}),
                resolved_at: None,
            };
        }
        if restarted {
            return RequestState {
                status: if shut_down { "cancelled" } else { "expired" },
                capability: json!({"type": "not_resumable", "reason": if shut_down { SHUTDOWN_REASON } else { RESTART_REASON }}),
                resolved_at: run.finished_at.or(Some(request.created_at)),
            };
        }
        let mut capability = json!({"type": "live", "providerSessionId": session_id});
        if request.request.supports_feedback {
            capability["supportsFeedback"] = json!(true);
        }
        RequestState {
            status: request.status,
            capability,
            resolved_at: request.resolved_at,
        }
    }

    /// The reference's pendingRuntimeRequestSummary: the newest pending
    /// approval, or else the newest pending question.
    pub(crate) fn pending_runtime_request(&self) -> Option<Value> {
        let records = self.provider_records();
        records
            .runtime_requests
            .iter()
            .filter(|request| request["status"] == "pending")
            .min_by(|a, b| {
                let question = |r: &Value| r["kind"] == "user_input";
                question(a)
                    .cmp(&question(b))
                    .then_with(|| b["createdAt"].as_str().cmp(&a["createdAt"].as_str()))
            })
            .map(|request| {
                json!({"id": request["id"], "kind": request["kind"], "createdAt": request["createdAt"],
                    "blocking": request["responseCapability"]["type"] != "message"})
            })
    }

    /// The projection id of a request of `run`.
    pub(crate) fn request_id(&self, ids: &RunIds, request: &RequestRecord) -> String {
        format!(
            "{}:{}",
            join(
                "runtime-request:provider",
                &[
                    &ids.driver,
                    "provider-turn",
                    &ids.provider_turn,
                    "native-request",
                    &request.request.native_request_id
                ]
            ),
            request.request.id
        )
    }
}

impl Thread {
    fn instance_id(&self) -> String {
        let row = self.stored_row();
        row["modelSelection"]["instanceId"]
            .as_str()
            .or(row["providerInstanceId"].as_str())
            .unwrap_or(provider::CODEX)
            .to_owned()
    }

    /// The provider thread record's id, named after the run that first used
    /// it (`pending:<run id>`), as the reference names a new thread's record.
    pub(crate) fn provider_thread_id(&self) -> Option<String> {
        let thread = self.provider.thread.as_ref()?;
        let run = self.runs.iter().find(|run| run.id == thread.first_run)?;
        let run_id = run.id.to_string();
        Some(format!(
            "provider-thread:provider:{}:native-thread:{}",
            uri_component(&thread.driver),
            uri_component(&format!("pending:{run_id}"))
        ))
    }

    /// The ids of `run`'s provider records, when it ran a provider turn.
    pub(crate) fn provider_run_ids(&self, run: &Run) -> Option<RunIds> {
        let record = self.provider.runs.get(&run.id)?;
        let run_id = run.id.to_string();
        let attempt = join("run-attempt:run", &[&run_id]) + ":attempt:1";
        let native_turn = record
            .native_turn_id
            .clone()
            .unwrap_or_else(|| format!("turn:{attempt}"));
        Some(RunIds {
            root_node: join("node:run", &[&run_id]) + ":root",
            provider_thread: self.provider_thread_id().unwrap_or_default(),
            provider_turn: format!(
                "provider-turn:provider:{}:native-turn:{}",
                uri_component(&record.driver),
                uri_component(&native_turn)
            ),
            driver: record.driver.clone(),
            run: run_id,
            attempt,
        })
    }

    /// Every provider record of the thread.
    pub(crate) fn provider_records(&self) -> ProviderRecords {
        let mut out = ProviderRecords::default();
        let instance = self.instance_id();
        let session = self.provider_session(&instance);
        let session_id = session
            .as_ref()
            .map_or(Value::Null, |session| session["id"].clone());
        let mut turn_ordinal = 0;
        let mut first_ordinal = None;
        let mut last_ordinal = 0;
        for (index, run) in self.runs.iter().enumerate() {
            let (Some(record), Some(ids)) =
                (self.provider.runs.get(&run.id), self.provider_run_ids(run))
            else {
                continue;
            };
            turn_ordinal += 1;
            first_ordinal.get_or_insert(index + 1);
            last_ordinal = index + 1;
            let terminal = self.provider.terminals.get(&run.id);
            self.provider_run(
                &mut out,
                index,
                run,
                record,
                terminal,
                &ids,
                &instance,
                turn_ordinal,
            );
            for request in &record.requests {
                self.provider_request(&mut out, index, run, &ids, request, &session_id);
            }
        }
        out.provider_sessions.extend(session);
        if let (Some(thread), Some(id)) = (&self.provider.thread, self.provider_thread_id()) {
            out.provider_threads.push(json!({
                "id": id,
                "driver": thread.driver,
                "providerInstanceId": instance,
                "providerSessionId": session_id,
                "appThreadId": self.id,
                "ownerNodeId": null,
                "nativeThreadRef": thread.native_id.as_deref()
                    .map_or(Value::Null, |native| native_ref(&thread.driver, native, "strong")),
                "nativeConversationHeadRef": null,
                "status": thread.status,
                "firstRunOrdinal": first_ordinal,
                "lastRunOrdinal": last_ordinal,
                "handoffIds": [],
                "forkedFrom": null,
                "pendingBackgroundTasks": [],
                "contextUsage": self.context_usage(),
                "nativeMetadata": self.claude_native_metadata(&thread.driver),
                "goal": null,
                "createdAt": iso(thread.created_at),
                "updatedAt": iso(thread.updated_at)
            }));
        }
        out.messages
            .sort_by(|a, b| a["createdAt"].as_str().cmp(&b["createdAt"].as_str()));
        out.items
            .retain(|(_, item)| item.get("requestSlot").is_none());
        out
    }

    /// The provider thread's context usage, as the reference sets it when a
    /// turn starts on a thread whose earlier turns reported usage: the
    /// usage they reported last (`latestNativeContextUsage`).
    fn context_usage(&self) -> Value {
        let Some(last) = self
            .runs
            .iter()
            .rposition(|run| self.provider.runs.contains_key(&run.id))
        else {
            return Value::Null;
        };
        self.runs[..last]
            .iter()
            .filter_map(|run| self.provider.runs.get(&run.id)?.usage.as_ref())
            .max_by_key(|(_, at)| *at)
            .map_or(Value::Null, |(usage, _)| {
                let mut out = json!({"usedTokens": usage.used_tokens});
                if let Some(max) = usage.max_tokens.filter(|max| *max > 0) {
                    out["maxTokens"] = json!(max);
                }
                out
            })
    }

    /// A Claude session's metadata: its title (the thread's) and what its
    /// prompts used of its attachment budget.
    fn claude_native_metadata(&self, driver: &str) -> Value {
        if driver != provider::CLAUDE_AGENT {
            return Value::Null;
        }
        let mut metadata = Map::new();
        if let Some((bytes, pages)) = self.provider.attachment_budget {
            metadata.insert(
                "claudeAttachmentBudget".into(),
                json!({"bytesUsed": bytes, "pagesUsed": pages}),
            );
        }
        let title = self.title.trim();
        if !title.is_empty() {
            metadata.insert("title".into(), json!(title));
        }
        if metadata.is_empty() {
            Value::Null
        } else {
            Value::Object(metadata)
        }
    }

    /// The provider session the thread's turns ran in, once one opened: the
    /// shared app-server session of a Codex instance, or the thread's own
    /// Claude session (which the reference opens even when the CLI is
    /// missing). A Codex session whose app-server closed mid-turn is `error`.
    fn provider_session(&self, instance: &str) -> Option<Value> {
        let thread = self.provider.thread.as_ref()?;
        let first = self.runs.iter().find(|run| run.id == thread.first_run)?;
        let claude = thread.driver == provider::CLAUDE_AGENT;
        if !claude && thread.native_id.is_none() {
            return None;
        }
        let id = if claude {
            format!(
                "provider-session:provider-instance:{}:thread:{}:{}",
                uri_component(instance),
                uri_component(&self.id),
                first.id
            )
        } else {
            format!(
                "provider-session:provider-instance:{}:shared",
                uri_component(instance)
            )
        };
        let last = self
            .runs
            .iter()
            .rev()
            .find_map(|run| self.provider.terminals.get(&run.id));
        let crashed = !claude
            && last
                .and_then(|t| t.failure.as_ref())
                .is_some_and(|failure| failure.message == provider::CODEX_STREAM_CLOSED);
        // A restart stops a session that has not failed.
        let stopped = self.provider.session_stopped_at.filter(|_| !crashed);
        let capabilities: Value =
            serde_json::from_str(include_str!("provider_capabilities.json")).unwrap_or_default();
        let row = self.stored_row();
        Some(json!({
            "capabilities": capabilities[thread.driver.as_str()],
            "createdAt": iso(thread.created_at),
            "cwd": self.cwd,
            "driver": thread.driver,
            "extensionStatuses": [],
            "id": id,
            "lastError": if crashed { json!(provider::CODEX_STREAM_CLOSED) } else { Value::Null },
            "model": row["modelSelection"]["model"],
            "providerInstanceId": instance,
            "status": if crashed { "error" } else if stopped.is_some() { "stopped" } else { "ready" },
            "updatedAt": iso(if crashed { thread.updated_at } else { stopped.unwrap_or(thread.created_at) })
        }))
    }

    #[allow(clippy::too_many_arguments)]
    fn provider_run(
        &self,
        out: &mut ProviderRecords,
        index: usize,
        run: &Run,
        record: &RunRecord,
        terminal: Option<&Terminal>,
        ids: &RunIds,
        instance: &str,
        turn_ordinal: usize,
    ) {
        let driver = ids.driver.as_str();
        // A turn a restart cut off before it ended is cancelled then.
        let cut = run
            .restart_cut
            .as_ref()
            .filter(|_| terminal.is_none())
            .map(|cut| cut.at);
        let ended = terminal
            .map(|_| record.ended_at.unwrap_or(record.started_at))
            .or(cut);
        let status = match terminal.map(|terminal| terminal.status) {
            None if cut.is_some() => "cancelled",
            None | Some(TurnStatus::Running) => "running",
            Some(TurnStatus::Completed) => "completed",
            Some(TurnStatus::Interrupted) => "interrupted",
            Some(TurnStatus::Failed) => "failed",
        };
        let started = iso(record.started_at);
        let completed = ended.map(iso);
        out.attempts.push(json!({
            "id": ids.attempt,
            "runId": ids.run,
            "attemptOrdinal": 1,
            "rootNodeId": ids.root_node,
            "providerInstanceId": instance,
            "providerThreadId": ids.provider_thread,
            "providerTurnId": null,
            "reason": "initial",
            "status": status,
            "startedAt": started,
            "completedAt": completed,
            "nativeThreadId": self.provider.thread.as_ref().and_then(|t| t.native_id.clone())
        }));
        out.nodes.push(json!({
            "id": ids.root_node,
            "threadId": self.id,
            "runId": ids.run,
            "parentNodeId": null,
            "rootNodeId": ids.root_node,
            "kind": "root_turn",
            "status": status,
            "countsForRun": true,
            "providerThreadId": ids.provider_thread,
            "providerTurnId": null,
            "nativeItemRef": null,
            "runtimeRequestId": null,
            "checkpointScopeId": null,
            "startedAt": started,
            "completedAt": completed
        }));
        let native_turn = record
            .native_turn_id
            .clone()
            .unwrap_or_else(|| format!("turn:{}", ids.attempt));
        let strength = if record.native_turn_id.is_some() {
            "strong"
        } else {
            "weak"
        };
        let mut turn = json!({
            "id": ids.provider_turn,
            "providerThreadId": ids.provider_thread,
            "nodeId": ids.root_node,
            "runAttemptId": ids.attempt,
            "nativeTurnRef": native_ref(driver, &native_turn, strength),
            "ordinal": turn_ordinal,
            "status": status,
            "startedAt": started,
            "completedAt": completed
        });
        if let Some((usage, at)) = &record.usage {
            turn["tokenUsage"] = json!({
                "usedTokens": usage.used_tokens,
                "maxTokens": usage.max_tokens,
                "inputTokens": usage.input_tokens,
                "cachedInputTokens": usage.cached_input_tokens,
                "outputTokens": usage.output_tokens,
                "reasoningOutputTokens": usage.reasoning_output_tokens,
                "updatedAt": iso(*at)
            });
            if let Some(ttl) = usage.prompt_cache_ttl_ms {
                turn["tokenUsage"]["promptCacheTtlMs"] = json!(ttl);
            }
            if let Some(expires) = &usage.prompt_cache_expires_at {
                turn["tokenUsage"]["promptCacheExpiresAt"] = json!(expires);
            }
        }
        if let Some(usage) = &record.turn_usage {
            turn["turnTokenUsage"] = usage.clone();
        }
        if let Some(model) = &record.model {
            turn["model"] = json!(model);
        }
        // The reference records a provider turn once the CLI starts one.
        if record.turn_started || record.native_turn_id.is_some() || !record.items.is_empty() {
            out.provider_turns.push(turn);
        }

        for item in &record.items {
            if matches!(
                item.item.kind.as_str(),
                "approval_request" | "user_input_request"
            ) {
                // Placed by provider_request, in this slot.
                out.items
                    .push((index, json!({"requestSlot": item.item.id})));
                continue;
            }
            self.provider_item(out, index, ids, item, terminal.map(|t| t.status), cut);
        }
        if let Some(plan) = &record.plan {
            let native = format!("turn-plan:{native_turn}");
            let node = item_id("node", driver, &native);
            let plan_id = format!(
                "plan:thread:{}:provider:{}:run:{}:{}",
                uri_component(&self.id),
                uri_component(driver),
                uri_component(&ids.run),
                uri_component(&native)
            );
            let claude = driver == provider::CLAUDE_AGENT;
            let steps: Vec<Value> = plan
                .steps
                .iter()
                .enumerate()
                .map(|(i, step)| {
                    let text = step.step.trim();
                    json!({
                        "id": if claude { format!("todo-{i}") } else { format!("step-{}", i + 1) },
                        "text": if text.is_empty() { format!("Step {}", i + 1) } else { text.to_owned() },
                        "status": match step.status.as_str() {
                            "inProgress" | "in_progress" | "running" => "running",
                            "completed" => "completed",
                            _ => "pending",
                        }
                    })
                })
                .collect();
            let done = !steps.is_empty() && steps.iter().all(|s| s["status"] == "completed");
            let mut plan_row = json!({
                "id": plan_id,
                "threadId": self.id,
                "runId": ids.run,
                "nodeId": node,
                "kind": "todo_list",
                "status": if claude && done { "completed" } else { "active" },
                "steps": steps
            });
            if let Some(explanation) = &plan.explanation {
                plan_row["explanation"] = json!(explanation);
            }
            out.plans.push(plan_row);
            let mut item = json!({
                "id": item_id("turn-item", driver, &native),
                "threadId": self.id,
                "runId": ids.run,
                "nodeId": node,
                "providerThreadId": ids.provider_thread,
                "providerTurnId": ids.provider_turn,
                "nativeItemRef": native_ref(driver, &native, "strong"),
                "parentItemId": null,
                "ordinal": 0,
                "status": "completed",
                "title": null,
                "startedAt": iso(plan.first_at),
                "completedAt": iso(plan.updated_at),
                "updatedAt": iso(plan.updated_at),
                "type": "todo_list",
                "planId": plan_id,
                "steps": steps
            });
            if let Some(explanation) = &plan.explanation {
                item["explanation"] = json!(explanation);
            }
            out.items.push((index, item));
        }

        if let Some(restart) = &run.restart_cut {
            out.items
                .push((index, self.restart_notice(run, restart, Some(ids))));
        }
        let Some(terminal) = terminal else { return };
        let at = completed.clone().unwrap_or_else(|| started.clone());
        match terminal.status {
            // A Codex turn whose app-server never opened a session: the
            // reference reports the session, not the turn, as failed.
            TurnStatus::Failed
                if driver == provider::CODEX
                    && !record.turn_started
                    && self
                        .provider
                        .thread
                        .as_ref()
                        .is_some_and(|thread| thread.native_id.is_none()) =>
            {
                let session = join("provider-session:provider-instance", &[instance]) + ":shared";
                out.items.push((index, json!({
                    "id": join("turn-item:run", &[&ids.run]) + ":signal:provider-session-open-failure",
                    "threadId": self.id,
                    "runId": ids.run,
                    "nodeId": ids.root_node,
                    "providerThreadId": ids.provider_thread,
                    "providerTurnId": null,
                    "nativeItemRef": null,
                    "parentItemId": null,
                    "ordinal": 0,
                    "status": "failed",
                    "title": "Provider session failed to open",
                    "startedAt": at,
                    "completedAt": at,
                    "updatedAt": at,
                    "type": "error",
                    "failure": {
                        "class": "provider_error",
                        "message": format!("Failed to open {driver} provider session {session}."),
                        "code": null,
                        "retryable": null
                    }
                })));
            }
            TurnStatus::Failed => {
                let failure = terminal.failure.clone().unwrap_or_else(|| {
                    provider::ProviderFailure::new("unknown", "Provider turn failed.")
                });
                let native = format!("terminal-failure:{}", ids.provider_turn);
                out.items.push((index, json!({
                    "id": item_id("turn-item", driver, &native),
                    "threadId": self.id,
                    "runId": ids.run,
                    "nodeId": ids.root_node,
                    "providerThreadId": ids.provider_thread,
                    "providerTurnId": ids.provider_turn,
                    "nativeItemRef": null,
                    "parentItemId": null,
                    "ordinal": 0,
                    "status": "failed",
                    "title": if failure.class == "usage_limit" { "Usage limit reached" } else { "Provider error" },
                    "startedAt": at,
                    "completedAt": at,
                    "updatedAt": at,
                    "type": "error",
                    "failure": failure_json(&failure)
                })));
            }
            TurnStatus::Interrupted => {
                let request = join("turn-item:run", &[&ids.run]) + ":signal:interrupt-request";
                let signal = |id: String, parent: Value, status: &str, title: &str, kind: &str| {
                    json!({
                        "id": id,
                        "threadId": self.id,
                        "runId": ids.run,
                        "nodeId": ids.root_node,
                        "providerThreadId": ids.provider_thread,
                        "providerTurnId": null,
                        "nativeItemRef": null,
                        "parentItemId": parent,
                        "ordinal": 0,
                        "status": status,
                        "title": title,
                        "startedAt": at,
                        "completedAt": at,
                        "updatedAt": at,
                        "type": kind,
                        "message": "Stopped by user"
                    })
                };
                out.items.push((
                    index,
                    signal(
                        request.clone(),
                        Value::Null,
                        "completed",
                        "Interrupt requested",
                        "run_interrupt_request",
                    ),
                ));
                out.items.push((
                    index,
                    signal(
                        join("turn-item:run", &[&ids.run]) + ":signal:interrupt-result",
                        json!(request),
                        "interrupted",
                        "Interrupted",
                        "run_interrupt_result",
                    ),
                ));
            }
            TurnStatus::Completed | TurnStatus::Running => {}
        }
    }

    /// The notice the reference adds to a run a restart cut off
    /// (`server-recovery`), after the run's items.
    pub(crate) fn restart_notice(
        &self,
        run: &Run,
        restart: &crate::RestartCut,
        ids: Option<&RunIds>,
    ) -> Value {
        let at = iso(restart.at);
        let startup = restart.trigger == "startup";
        json!({
            "id": join("turn-item:run", &[&run.id.to_string()]) + ":signal:server-recovery",
            "threadId": self.id,
            "runId": run.id.to_string(),
            "nodeId": ids.map(|ids| ids.root_node.clone()),
            "providerThreadId": ids.map(|ids| ids.provider_thread.clone()),
            "providerTurnId": null,
            "nativeItemRef": null,
            "parentItemId": null,
            "ordinal": 0,
            "status": "completed",
            "title": if startup { "Server restarted" } else { "Server shut down" },
            "startedAt": at,
            "completedAt": at,
            "updatedAt": at,
            "type": "system_notice",
            "message": format!(
                "Cancelled because the server {} before the provider work completed.",
                if startup { "restarted" } else { "shut down" }
            )
        })
    }

    /// A runtime request's record, node and turn item, as the reference's
    /// adapters build them (`buildApprovalRequestArtifacts`,
    /// `buildUserInputRequestArtifacts`) and its respond command updates
    /// them.
    fn provider_request(
        &self,
        out: &mut ProviderRecords,
        index: usize,
        run: &Run,
        ids: &RunIds,
        request: &RequestRecord,
        session_id: &Value,
    ) {
        let driver = ids.driver.as_str();
        let open = &request.request;
        let id = self.request_id(ids, request);
        let state = self.request_state(run, request, session_id);
        let question = open.kind == "user_input";
        // Codex questions are items of their tool call; everything else
        // hangs under the call it asks about.
        let codex_question = question && driver == provider::CODEX;
        let (item_row_id, node_id, parent) = if codex_question {
            (
                item_id("turn-item", driver, &open.native_item_id),
                item_id("node", driver, &open.native_item_id),
                ids.root_node.clone(),
            )
        } else {
            (
                join("turn-item:runtime-request", &[&id]),
                join("node:runtime-request", &[&id]),
                item_id("node", driver, &open.native_item_id),
            )
        };
        let native_item = if driver == provider::CODEX {
            &open.native_item_id
        } else {
            &open.native_request_id
        };
        let reference = native_ref(driver, native_item, "strong");
        // The node and item end with the answer: completed when accepted or
        // answered, cancelled when declined, withdrawn or expired.
        let done = match state.status {
            "resolved" => Some(
                if matches!(request.decision.as_deref(), Some("decline" | "cancel")) {
                    "cancelled"
                } else {
                    "completed"
                },
            ),
            "cancelled" | "expired" => Some("cancelled"),
            _ => None,
        };
        let status = done.unwrap_or("waiting");
        let created = iso(request.created_at);
        let completed = done.and(state.resolved_at).map(iso);
        let updated = completed.clone().unwrap_or_else(|| created.clone());
        let mut record = json!({
            "id": id,
            "nodeId": node_id,
            "providerTurnId": ids.provider_turn,
            "nativeRequestRef": native_ref(driver, &open.native_request_id, "strong"),
            "kind": open.kind,
            "status": state.status,
            "responseCapability": state.capability,
            "createdAt": created,
            "resolvedAt": state.resolved_at.map(iso)
        });
        if state.status == "resolved" {
            for (key, value) in [
                ("decision", &request.decision),
                ("feedback", &request.feedback),
            ] {
                if let Some(value) = value {
                    record[key] = json!(value);
                }
            }
            if let Some(answers) = &request.answers {
                record["answers"] = answers.clone();
            }
        }
        out.runtime_requests.push(record);
        out.nodes.push(json!({
            "id": node_id,
            "threadId": self.id,
            "runId": ids.run,
            "parentNodeId": parent,
            "rootNodeId": ids.root_node,
            "kind": if question { "user_input_request" } else { "approval_request" },
            "status": status,
            "countsForRun": false,
            "providerThreadId": ids.provider_thread,
            "providerTurnId": ids.provider_turn,
            "nativeItemRef": reference,
            "runtimeRequestId": id,
            "checkpointScopeId": null,
            "startedAt": created,
            "completedAt": completed
        }));
        let mut item = Map::new();
        for (key, value) in [
            ("id", json!(item_row_id)),
            ("threadId", json!(self.id)),
            ("runId", json!(ids.run)),
            ("nodeId", json!(node_id)),
            ("providerThreadId", json!(ids.provider_thread)),
            ("providerTurnId", json!(ids.provider_turn)),
            ("nativeItemRef", reference),
            ("parentItemId", Value::Null),
            ("ordinal", json!(0)),
            ("status", json!(status)),
            ("title", Value::Null),
            ("startedAt", json!(created)),
            ("completedAt", json!(completed)),
            ("updatedAt", json!(updated)),
            (
                "type",
                json!(if question {
                    "user_input_request"
                } else {
                    "approval_request"
                }),
            ),
            ("requestId", json!(id)),
        ] {
            item.insert(key.into(), value);
        }
        if !question {
            item.insert("requestKind".into(), json!(open.kind));
        }
        for (key, value) in &open.fields {
            item.insert(key.clone(), value.clone());
        }
        if let (true, "resolved", Some(answers)) = (question, state.status, &request.answers) {
            let texts: Map<String, Value> = open.fields["questions"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|q| Some((q["id"].as_str()?.to_owned(), q["question"].clone())))
                .collect();
            item.insert(
                "questionAnswer".into(),
                json!({"requestId": id, "answers": answers, "attachmentsByQuestionId": {},
                    "questionTextById": texts}),
            );
        }
        if let Some(at) = request.delivery_failed_at {
            let at = iso(at);
            let failure = json!({
                "id": format!("turn-item:runtime-request-delivery-failure:{id}"),
                "threadId": self.id,
                "runId": ids.run,
                "nodeId": node_id,
                "providerThreadId": ids.provider_thread,
                "providerTurnId": ids.provider_turn,
                "nativeItemRef": null,
                "parentItemId": null,
                "ordinal": 0,
                "status": "failed",
                "title": "Answer delivery not confirmed",
                "startedAt": at,
                "completedAt": at,
                "updatedAt": at,
                "type": "error",
                "failure": {"class": "transport_error", "message": DELIVERY_FAILED, "code": null, "retryable": null}
            });
            let slot = format!("delivery:{}", open.id);
            if let Some(existing) = out
                .items
                .iter_mut()
                .find(|(_, existing)| existing["requestSlot"] == slot.as_str())
            {
                existing.1 = failure;
            }
        }
        // Into the slot the request took among its run's items.
        let slot = format!("request:{}", open.id);
        let row = Value::Object(item);
        match out
            .items
            .iter_mut()
            .find(|(_, existing)| existing["requestSlot"] == slot.as_str())
        {
            Some(existing) => existing.1 = row,
            None => out.items.push((index, row)),
        }
    }

    fn provider_item(
        &self,
        out: &mut ProviderRecords,
        index: usize,
        ids: &RunIds,
        record: &ItemRecord,
        ended: Option<TurnStatus>,
        cut: Option<u64>,
    ) {
        let driver = ids.driver.as_str();
        let item = &record.item;
        // The reference shows no reasoning that has no text.
        if item.kind == "reasoning"
            && item
                .fields
                .get("text")
                .and_then(Value::as_str)
                .is_none_or(|text| text.trim().is_empty())
        {
            return;
        }
        let native = item.id.as_str();
        // An item still running when its turn ended ends with the turn.
        let status = match (item.status.as_str(), ended) {
            ("running", Some(TurnStatus::Interrupted)) => "interrupted",
            ("running", Some(TurnStatus::Failed)) => "failed",
            ("running", Some(TurnStatus::Completed)) => "completed",
            ("pending" | "running" | "waiting", None) if cut.is_some() => "cancelled",
            (status, _) => status,
        }
        .to_owned();
        let running = status == "running";
        let node = item_id("node", driver, native);
        let reference = native_ref(driver, native, "strong");
        let started = iso(record.first_at);
        // An item a restart cancelled changed then.
        let cancelled_at = cut.filter(|_| status == "cancelled" && item.status != "cancelled");
        let updated = iso(cancelled_at.unwrap_or(record.updated_at));
        let completed = (!running).then(|| updated.clone());
        let mut row = Map::new();
        for (key, value) in [
            ("id", json!(item_id("turn-item", driver, native))),
            ("threadId", json!(self.id)),
            ("runId", json!(ids.run)),
            ("nodeId", json!(node)),
            ("providerThreadId", json!(ids.provider_thread)),
            ("providerTurnId", json!(ids.provider_turn)),
            ("nativeItemRef", reference.clone()),
            ("parentItemId", Value::Null),
            ("ordinal", json!(0)),
            ("status", json!(status)),
            ("title", json!(item.title)),
            ("startedAt", json!(started)),
            ("completedAt", json!(completed)),
            ("updatedAt", json!(updated)),
            ("type", json!(item.kind)),
        ] {
            row.insert(key.into(), value);
        }
        for (key, value) in &item.fields {
            row.insert(key.clone(), value.clone());
        }
        let streams = matches!(item.kind.as_str(), "assistant_message" | "reasoning");
        if streams {
            row.insert(
                "text".into(),
                json!(item
                    .fields
                    .get("text")
                    .and_then(Value::as_str)
                    .unwrap_or_default()),
            );
            row.insert("streaming".into(), json!(running));
        }
        if item.kind == "assistant_message" {
            let message = item_id("message", driver, native);
            row.insert("messageId".into(), json!(message));
            out.messages.push(json!({
                "createdBy": "agent",
                "creationSource": "provider",
                "id": message,
                "threadId": self.id,
                "runId": ids.run,
                "nodeId": node,
                "role": "assistant",
                "text": row["text"],
                "attachments": [],
                "streaming": running,
                "createdAt": started,
                "updatedAt": updated
            }));
        }
        out.nodes.push(json!({
            "id": node,
            "threadId": self.id,
            "runId": ids.run,
            "parentNodeId": ids.root_node,
            "rootNodeId": ids.root_node,
            "kind": match item.kind.as_str() {
                "assistant_message" => "assistant_message",
                "reasoning" => "reasoning",
                _ => "tool_call",
            },
            "status": status,
            "countsForRun": false,
            "providerThreadId": ids.provider_thread,
            "providerTurnId": ids.provider_turn,
            "nativeItemRef": reference,
            "runtimeRequestId": null,
            "checkpointScopeId": null,
            "startedAt": started,
            "completedAt": completed
        }));
        out.items.push((index, Value::Object(row)));
    }
}

#[cfg(test)]
mod tests {
    use crate::workspace::provider::{ProviderEvent, RuntimeRequestOpen, TurnStatus};
    use crate::{Run, Thread};
    use serde_json::{json, Map, Value};
    use uuid::Uuid;

    fn thread(run: Run) -> Thread {
        Thread {
            row: json!({"modelSelection": {"instanceId": "codex", "model": "m"}}),
            id: "t1".into(),
            project_id: "p1".into(),
            title: "T".into(),
            cwd: "/w".into(),
            status: "running".into(),
            messages: vec![],
            runs: vec![run],
            created_at: 1,
            archived: false,
            settled: false,
            snoozed_until: None,
            pinned: false,
            unread: false,
            tags: vec![],
            checkpoints: vec![],
            provider: Box::default(),
        }
    }

    fn opened(id: &str, kind: &str) -> ProviderEvent {
        let mut fields = Map::new();
        fields.insert("prompt".into(), json!("touch x"));
        ProviderEvent::RuntimeRequestOpened {
            request: RuntimeRequestOpen {
                id: id.into(),
                native_request_id: format!("call_{id}"),
                native_item_id: format!("call_{id}"),
                kind: kind.into(),
                runtime_mode: "full-access".into(),
                supports_feedback: false,
                fields,
            },
        }
    }

    fn requests(thread: &Thread) -> Vec<Value> {
        thread.provider_records().runtime_requests
    }

    #[test]
    fn requests_wait_resolve_and_expire_as_the_reference_records_them() {
        let run_id = Uuid::new_v4();
        let mut run = Run {
            id: run_id,
            thread_id: "t1".into(),
            provider: "codex".into(),
            status: "running".into(),
            started_at: 1,
            finished_at: None,
            error: None,
            user_message_id: None,
            queue_held: false,
            restart_cut: None,
            restart_continuation_of: None,
        };
        let mut live = thread(run.clone());
        for event in [
            ProviderEvent::ProviderThread {
                driver: "codex".into(),
                native_thread_id: "thr".into(),
                resumed: false,
                model: None,
            },
            ProviderEvent::ProviderTurn {
                native_turn_id: Some("turn".into()),
                status: TurnStatus::Running,
                model: None,
            },
            opened("a", "command"),
            opened("q", "user_input"),
        ] {
            live.provider.apply(run_id, &event, 2);
        }
        let pending = requests(&live);
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0]["status"], "pending");
        assert_eq!(
            pending[0]["id"],
            "runtime-request:provider:codex:provider-turn:provider-turn%3Aprovider%3Acodex%3Anative-turn%3Aturn:native-request:call_a:a"
        );
        assert_eq!(pending[0]["responseCapability"]["type"], "live");
        // An approval comes before a question in the shell's summary.
        assert_eq!(live.pending_runtime_request().unwrap()["kind"], "command");
        let items = live.provider_records().items;
        let approval = items
            .iter()
            .find(|(_, item)| item["type"] == "approval_request")
            .unwrap();
        assert_eq!(approval.1["status"], "waiting");
        assert_eq!(approval.1["prompt"], "touch x");

        // A restart: the approval expires and the question waits for a message.
        run.status = "failed".into();
        run.finished_at = Some(5);
        run.error = Some(crate::turn::RESTART_INTERRUPTION.into());
        let mut restarted = thread(run);
        restarted.provider = live.provider.clone();
        let after = requests(&restarted);
        assert_eq!(after[0]["status"], "expired");
        assert_eq!(after[0]["responseCapability"]["type"], "not_resumable");
        assert_eq!(after[1]["status"], "pending");
        assert_eq!(after[1]["responseCapability"], json!({"type": "message"}));

        // An answer resolves the request and completes or cancels its card.
        live.provider.apply(
            run_id,
            &ProviderEvent::RuntimeRequestAnswered {
                id: "a".into(),
                decision: Some("decline".into()),
                feedback: None,
                answers: None,
            },
            3,
        );
        let answered = requests(&live);
        assert_eq!(answered[0]["status"], "resolved");
        assert_eq!(answered[0]["decision"], "decline");
        let items = live.provider_records().items;
        let approval = items
            .iter()
            .find(|(_, item)| item["type"] == "approval_request")
            .unwrap();
        assert_eq!(approval.1["status"], "cancelled");
        assert_eq!(
            live.pending_runtime_request().unwrap()["kind"],
            "user_input"
        );
    }
}
