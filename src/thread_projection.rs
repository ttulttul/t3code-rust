use crate::{auth_service, Store, Thread};
use serde_json::{json, Map, Value};

const APP_FIELDS: &[&str] = include!(concat!(env!("OUT_DIR"), "/OrchestrationV2AppThread.rs"));
const SHELL_FIELDS: &[&str] = include!(concat!(env!("OUT_DIR"), "/OrchestrationV2ThreadShell.rs"));

fn ordered(fields: &[&str], row: &Value) -> Value {
    Value::Object(
        fields
            .iter()
            .filter_map(|&k| row.get(k).map(|v| (k.to_string(), v.clone())))
            .collect(),
    )
}

pub(crate) fn iso(seconds: u64) -> String {
    auth_service::iso((seconds as i64).saturating_mul(1000))
}

/// JavaScript's encodeURIComponent, which the reference uses to nest ids in ids.
pub(crate) fn uri_component(value: &str) -> String {
    const RESERVED: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'_')
        .remove(b'.')
        .remove(b'!')
        .remove(b'~')
        .remove(b'*')
        .remove(b'\'')
        .remove(b'(')
        .remove(b')');
    percent_encoding::utf8_percent_encode(value, RESERVED).to_string()
}

fn merge(target: &mut Value, fields: Value) {
    if let (Some(target), Value::Object(fields)) = (target.as_object_mut(), fields) {
        target.extend(fields);
    }
}

/// The role of a message the server sent as a notification: the prompt that
/// continues a run a restart cut off.
pub(crate) const NOTIFICATION_ROLE: &str = "notification";
/// The notification's summary, as the reference words it.
pub(crate) const RESTART_CONTINUATION_SUMMARY: &str = "T3 Code restarted and resumed this turn";

/// The reference's run statuses; older stores can hold others.
fn run_status(status: &str) -> &'static str {
    match status {
        "preparing" => "preparing",
        "queued" => "queued",
        "starting" => "starting",
        "running" => "running",
        "waiting" => "waiting",
        "completed" => "completed",
        "interrupted" => "interrupted",
        "cancelled" => "cancelled",
        "rolled_back" => "rolled_back",
        _ => "failed",
    }
}

fn message_role(role: &str) -> &'static str {
    match role {
        "assistant" => "assistant",
        "system" => "system",
        _ => "user",
    }
}

/// The thread's model selection; rows from before selections were stored name no model.
fn model_selection(row: &Value) -> Value {
    if row["modelSelection"]["instanceId"].is_string() && row["modelSelection"]["model"].is_string()
    {
        return row["modelSelection"].clone();
    }
    json!({"instanceId":row["providerInstanceId"].as_str().unwrap_or("codex"),"model":"default"})
}

/// The reference's dedupeSnapshotBodies: a message item repeats its message's text, so the
/// item carries `textOmitted` instead and the client restores the text from the message.
fn dedupe_bodies(projection: &mut Value) {
    let texts: std::collections::HashMap<String, String> = projection["messages"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|m| Some((m["id"].as_str()?.to_owned(), m["text"].as_str()?.to_owned())))
        .collect();
    let dedupe = |item: &mut Value| {
        if !matches!(
            item["type"].as_str(),
            Some("user_message" | "assistant_message")
        ) {
            return;
        }
        let text = item["text"].as_str().unwrap_or_default();
        if !text.is_empty()
            && item["messageId"]
                .as_str()
                .and_then(|id| texts.get(id))
                .map(String::as_str)
                == Some(text)
        {
            item["text"] = json!("");
            item["textOmitted"] = json!(true);
        }
    };
    if let Some(items) = projection["turnItems"].as_array_mut() {
        items.iter_mut().for_each(dedupe);
    }
    if let Some(rows) = projection["visibleTurnItems"].as_array_mut() {
        rows.iter_mut().for_each(|row| dedupe(&mut row["item"]));
    }
}

/// The id of a thread's root checkpoint scope, as the reference names it.
pub(crate) fn checkpoint_scope_id(thread_id: &str) -> String {
    format!(
        "checkpoint-scope:thread:{}:name:root",
        uri_component(thread_id)
    )
}

/// The id of a thread's checkpoint after `turn_count` turns.
pub(crate) fn checkpoint_id(thread_id: &str, turn_count: u64) -> String {
    format!(
        "checkpoint:scope:{}:name:{turn_count}",
        uri_component(&checkpoint_scope_id(thread_id))
    )
}

pub(crate) fn created(command: &Value, at: &str) -> Value {
    json!({"createdBy":command["createdBy"],"creationSource":command["creationSource"],
        "id":command["threadId"],"projectId":command["projectId"],"title":command["title"],
        "providerInstanceId":command["modelSelection"]["instanceId"],"modelSelection":command["modelSelection"],
        "runtimeMode":command["runtimeMode"],"interactionMode":command["interactionMode"],
        "branch":command["branch"],"worktreePath":command["worktreePath"],"pullRequests":[],
        "activeProviderThreadId":null,"lineage":{"parentThreadId":null,"relationshipToParent":null,"rootThreadId":command["threadId"]},
        "forkedFrom":null,"createdAt":at,"updatedAt":at,"archivedAt":null,"settledOverride":null,
        "settledAt":null,"snoozedUntil":null,"snoozedAt":null,"lastVisitedAt":null,"deletedAt":null})
}

impl Thread {
    /// Each run as `OrchestrationV2Run`. Runs have no provider records here, so the ids of
    /// those (provider thread, root node, attempt, checkpoint) are null.
    fn run_rows(&self, row: &Value) -> Vec<Value> {
        let selection = model_selection(row);
        let instance = row["providerInstanceId"]
            .as_str()
            .or(selection["instanceId"].as_str())
            .unwrap_or("codex")
            .to_owned();
        let mut queue_position = 0;
        self.runs
            .iter()
            .enumerate()
            .map(|(index, run)| {
                let status = run_status(&run.status);
                let waiting = matches!(status, "queued" | "preparing");
                let requested_at = self
                    .messages
                    .iter()
                    .find(|message| Some(&message.id) == run.user_message_id.as_ref())
                    .map_or(run.started_at, |message| message.created_at);
                let mut out = json!({
                    "id": run.id.to_string(),
                    "threadId": self.id,
                    "ordinal": index + 1,
                    "providerInstanceId": instance,
                    "modelSelection": selection,
                    "providerThreadId": null,
                    "userMessageId": run.user_message_id.clone().filter(|id| !id.trim().is_empty()).unwrap_or_else(|| run.id.to_string()),
                    "rootNodeId": null,
                    "activeAttemptId": null,
                    "status": status,
                    "queuePosition": null,
                    "requestedAt": iso(requested_at),
                    "startedAt": if waiting { Value::Null } else { iso(run.started_at).into() },
                    "completedAt": run.finished_at.map(iso),
                    "checkpointId": null,
                    "contextHandoffId": null
                });
                let turn_count = index as u64 + 1;
                if self.checkpoints.iter().any(|c| c.turn_count == turn_count) {
                    out["checkpointId"] = json!(checkpoint_id(&self.id, turn_count));
                }
                if let Some(ids) = self.provider_run_ids(run) {
                    out["providerThreadId"] = json!(ids.provider_thread);
                    out["rootNodeId"] = json!(ids.root_node);
                    out["activeAttemptId"] = json!(ids.attempt);
                }
                if let Some(source) = run.restart_continuation_of {
                    out["restartContinuationOfRunId"] = json!(source.to_string());
                    out["workStartedAt"] = json!(iso(self.work_started_at(run)));
                }
                if status == "queued" {
                    queue_position += 1;
                    out["queuePosition"] = json!(queue_position);
                    out["queueHeld"] = json!(run.queue_held);
                }
                out
            })
            .collect()
    }

    /// When the work `run` continues began: the start of the first run of
    /// its chain of restart continuations.
    fn work_started_at(&self, run: &crate::Run) -> u64 {
        let mut current = run;
        for _ in 0..self.runs.len() {
            let Some(source) = current
                .restart_continuation_of
                .and_then(|id| self.runs.iter().find(|r| r.id == id))
            else {
                break;
            };
            current = source;
        }
        current.started_at
    }

    fn run_of(&self, message_id: &str) -> Option<(usize, &crate::Run)> {
        self.runs
            .iter()
            .enumerate()
            .find(|(_, run)| run.user_message_id.as_deref() == Some(message_id))
    }

    /// Each message as `OrchestrationV2ConversationMessage`.
    fn message_rows(&self) -> Vec<Value> {
        self.messages
            .iter()
            // A notification shows as its turn item only.
            .filter(|message| message.role != NOTIFICATION_ROLE)
            .map(|message| {
                let at = iso(message.created_at);
                json!({
                    "createdBy": if message.role == "user" { "user" } else { "agent" },
                    "creationSource": if message.role == "user" { "web" } else { "provider" },
                    "id": message.id,
                    "threadId": self.id,
                    "runId": self.run_of(&message.id).map(|(_, run)| run.id.to_string()),
                    "nodeId": null,
                    "role": message_role(&message.role),
                    "text": message.content,
                    "attachments": [],
                    "streaming": false,
                    "createdAt": at,
                    "updatedAt": at
                })
            })
            .collect()
    }

    /// The timeline as `OrchestrationV2TurnItem`s: each message, then its run's failure,
    /// then the `provider` items (by run index) the run's provider turn reported.
    /// Ordinals follow the reference's TurnItemPositionStore: a run's items start after
    /// `ordinal * 1_000_000`, so items stay ordered when earlier runs gain items later;
    /// each provider item takes the next free slot of its run.
    fn turn_items_with(&self, provider: Vec<(usize, Value)>) -> Vec<Value> {
        let mut items = Vec::new();
        let mut ordinal: u64 = 0;
        for message in &self.messages {
            let run = self.run_of(&message.id);
            ordinal = match run {
                Some((index, _)) => ((index as u64 + 1) * 1_000_000).max(ordinal) + 1,
                None => ordinal + 1,
            };
            let run_id = run.map(|(_, run)| run.id.to_string());
            let at = iso(message.created_at);
            let base = |id: String,
                        ordinal: u64,
                        status: &str,
                        title: Value,
                        started: &str,
                        completed: Value,
                        updated: &str| {
                json!({
                    "id": id,
                    "threadId": self.id,
                    "runId": run_id,
                    "nodeId": null,
                    "providerThreadId": null,
                    "providerTurnId": null,
                    "nativeItemRef": null,
                    "parentItemId": null,
                    "ordinal": ordinal,
                    "status": status,
                    "title": title,
                    "startedAt": started,
                    "completedAt": completed,
                    "updatedAt": updated
                })
            };
            let mut item = base(
                format!("turn-item:message:{}", uri_component(&message.id)),
                ordinal,
                "completed",
                Value::Null,
                &at,
                at.clone().into(),
                &at,
            );
            let fields = match message.role.as_str() {
                "user" => json!({
                    "type": "user_message",
                    "createdBy": "user",
                    "creationSource": "web",
                    "messageId": message.id,
                    "inputIntent": "turn_start",
                    "text": message.content,
                    "attachments": []
                }),
                "assistant" => json!({
                    "type": "assistant_message",
                    "messageId": message.id,
                    "text": message.content,
                    "streaming": false
                }),
                // The prompt that continues a run a restart cut off.
                NOTIFICATION_ROLE => {
                    if let Some(ids) = run.and_then(|(_, run)| self.provider_run_ids(run)) {
                        item["nodeId"] = json!(ids.root_node);
                        item["providerThreadId"] = json!(ids.provider_thread);
                    }
                    json!({
                        "type": "notification",
                        "source": {"kind": "system"},
                        "outcome": "updated",
                        "summary": RESTART_CONTINUATION_SUMMARY,
                        "detail": message.content
                    })
                }
                _ => continue,
            };
            merge(&mut item, fields);
            items.push(item);
            let Some((_, run)) = run else { continue };
            // A provider turn's end adds its own items.
            if self.provider.terminals.contains_key(&run.id) {
                continue;
            }
            // So does a provider turn a restart cut off.
            if let Some(restart) = &run.restart_cut {
                if !self.provider.runs.contains_key(&run.id) {
                    ordinal += 1;
                    let mut notice = self.restart_notice(run, restart, None);
                    notice["ordinal"] = json!(ordinal);
                    items.push(notice);
                }
                continue;
            }
            let Some(error) = run
                .error
                .as_deref()
                .map(str::trim)
                .filter(|e| !e.is_empty())
            else {
                continue;
            };
            ordinal += 1;
            let failed_at = iso(run.finished_at.unwrap_or(run.started_at));
            let mut failure = base(
                format!(
                    "turn-item:run:{}:signal:provider-failure",
                    uri_component(&run.id.to_string())
                ),
                ordinal,
                "failed",
                json!("Provider failed"),
                &failed_at,
                failed_at.clone().into(),
                &failed_at,
            );
            merge(
                &mut failure,
                json!({"type":"error","failure":{
                    "class":"provider_error",
                    "message": error.chars().take(4096).collect::<String>().trim_end(),
                    "code":null,
                    "retryable":null
                }}),
            );
            items.push(failure);
        }
        for (index, mut item) in provider {
            let lower = (index as u64 + 1) * 1_000_000;
            let upper = lower + 999_999;
            let next = items
                .iter()
                .filter_map(|i| i["ordinal"].as_u64())
                .filter(|o| (lower..=upper).contains(o))
                .max()
                .unwrap_or(lower)
                + 1;
            item["ordinal"] = json!(next);
            items.push(item);
        }
        items.sort_by(|a, b| {
            (a["ordinal"].as_u64(), a["id"].as_str())
                .cmp(&(b["ordinal"].as_u64(), b["id"].as_str()))
        });
        items
    }

    pub(crate) fn stored_row(&self) -> Value {
        if self.row.is_object() {
            return self.row.clone();
        }
        let at = auth_service::iso((self.created_at as i64).saturating_mul(1000));
        created(
            &json!({"createdBy":"user","creationSource":"web","threadId":self.id,"projectId":self.project_id,
            "title":self.title,"modelSelection":null,"runtimeMode":"full-access","interactionMode":"default","worktreePath":self.cwd}),
            &at,
        )
    }

    /// `OrchestrationV2ThreadProjection`, as the reference's getThreadProjection returns it.
    pub(crate) fn projection(&self) -> Value {
        self.projection_with(true)
    }

    /// A turn item with everything it holds, bounded for transport, as
    /// `orchestration.getTurnItem` returns it.
    pub(crate) fn turn_item_detail(&self, item_id: &str) -> Option<Value> {
        self.projection_with(false)["turnItems"]
            .as_array()?
            .iter()
            .find(|item| item["id"] == item_id)
            .map(crate::wire_items::detail_item)
    }

    /// The projection; with `wire`, its turn items as snapshots and events
    /// carry them (output omitted).
    fn projection_with(&self, wire: bool) -> Value {
        let row = self.stored_row();
        let array = |key: &str| {
            row.get(key)
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
        };
        // The root scope and each checkpoint hang off the run they belong to
        // (the first run for the scope and the baseline), as in the reference.
        let scope_id = checkpoint_scope_id(&self.id);
        let run_ids = |turn_count: u64| {
            let index = turn_count.max(1) as usize - 1;
            self.runs
                .get(index)
                .map(|run| (run.id.to_string(), self.provider_run_ids(run)))
        };
        let node_of = |turn_count: u64| {
            run_ids(turn_count)
                .and_then(|(_, ids)| ids.map(|ids| ids.root_node))
                .unwrap_or_else(|| format!("node:{}", self.id))
        };
        let checkpoint_scopes = self
            .checkpoints
            .first()
            .map(|checkpoint| {
                let first = run_ids(1);
                vec![json!({
                    "id":scope_id,
                    "threadId":self.id,
                    "runId":first.as_ref().map(|(id, _)| id.clone()),
                    "nodeId":node_of(1),
                    "parentScopeId":null,
                    "providerThreadId":first.and_then(|(_, ids)| ids.map(|ids| ids.provider_thread)),
                    "kind":"root_run",
                    "ordinalWithinParent":0,
                    "advancesAppRunCount":true,
                    "cwd":checkpoint.cwd,
                    "createdAt":iso(checkpoint.captured_at)
                })]
            })
            .unwrap_or_default();
        // The reference records the baseline with the first turn's checkpoint,
        // when a run ends; until then only the scope exists.
        let captured_turn = self.checkpoints.iter().any(|c| c.turn_count > 0);
        let checkpoints = self.checkpoints.iter().enumerate()
            .filter(|(_, checkpoint)| captured_turn || checkpoint.turn_count > 0)
            .map(|(position, checkpoint)| json!({
            "id":checkpoint_id(&self.id, checkpoint.turn_count),
            "threadId":self.id,
            "scopeId":scope_id,
            "runId":if checkpoint.turn_count == 0 { None } else { run_ids(checkpoint.turn_count).map(|(id, _)| id) },
            "nodeId":node_of(checkpoint.turn_count),
            "parentCheckpointId":position.checked_sub(1)
                .map(|previous| checkpoint_id(&self.id, self.checkpoints[previous].turn_count)),
            "ordinalWithinScope":checkpoint.turn_count,
            "appRunOrdinal":if checkpoint.turn_count == 0 {Value::Null} else {json!(checkpoint.turn_count)},
            "ref":checkpoint.tree,
            "status":if checkpoint.ready {"ready"} else {"missing"},
            "files":[],
            "capturedAt":iso(checkpoint.captured_at)
        })).collect::<Vec<_>>();
        let mut provider = self.provider_records();
        // A provider turn that captured a checkpoint ends with its item.
        for checkpoint in self.checkpoints.iter().filter(|c| c.turn_count > 0) {
            let index = checkpoint.turn_count as usize - 1;
            let Some(ids) = self
                .runs
                .get(index)
                .and_then(|run| self.provider_run_ids(run))
            else {
                continue;
            };
            let id = checkpoint_id(&self.id, checkpoint.turn_count);
            let at = iso(checkpoint.captured_at);
            provider.items.push((
                index,
                json!({
                    "id": format!(
                        "turn-item:provider:{}:native-item:{}",
                        uri_component(&ids.driver),
                        uri_component(&format!("checkpoint:{id}"))
                    ),
                    "threadId": self.id,
                    "runId": ids.run,
                    "nodeId": ids.root_node,
                    "providerThreadId": ids.provider_thread,
                    "providerTurnId": null,
                    "nativeItemRef": null,
                    "parentItemId": null,
                    "ordinal": 0,
                    "status": "completed",
                    "title": null,
                    "startedAt": at,
                    "completedAt": at,
                    "updatedAt": at,
                    "type": "checkpoint",
                    "checkpointId": id,
                    "scopeId": scope_id,
                    "files": []
                }),
            ));
        }
        let mut turn_items = self.turn_items_with(provider.items);
        if wire {
            turn_items = turn_items
                .iter()
                .map(crate::wire_items::wire_item)
                .collect();
        }
        let mut app = ordered(APP_FIELDS, &row);
        if let Some(id) = self.provider_thread_id() {
            app["activeProviderThreadId"] = json!(id);
        }
        let with = |key: &str, extra: Vec<Value>| {
            let mut records = array(key);
            records.extend(extra);
            Value::Array(records)
        };
        let mut messages = self.message_rows();
        messages.extend(provider.messages);
        let visible = turn_items
            .iter()
            .enumerate()
            .map(|(position, item)| {
                json!({"position":position,"visibility":"local","sourceThreadId":self.id,
                    "sourceItemId":item["id"],"item":item})
            })
            .collect::<Vec<_>>();
        json!({"thread":app,"runs":self.run_rows(&row),"attempts":with("attempts",provider.attempts),"nodes":with("nodes",provider.nodes),"subagents":array("subagents"),
            "providerSessions":with("providerSessions",provider.provider_sessions),"providerThreads":with("providerThreads",provider.provider_threads),"providerTurns":with("providerTurns",provider.provider_turns),"runtimeRequests":with("runtimeRequests",provider.runtime_requests),
            "messages":messages,"plans":provider.plans,"turnItems":turn_items,"checkpointScopes":checkpoint_scopes,"checkpoints":checkpoints,
            "contextHandoffs":[],"contextTransfers":[],"visibleTurnItems":visible,"updatedAt":row["updatedAt"]})
    }

    /// The thread's whole timeline as `OrchestrationV2ThreadTranscript` items.
    pub(crate) fn transcript(&self) -> Value {
        let row = self.stored_row();
        json!({"threadId":self.id,"title":self.title,"updatedAt":row["updatedAt"],
            "items":self.projection()["visibleTurnItems"]})
    }

    /// One `snapshot` item of `orchestration.subscribeThread`, shaped by the options the
    /// client sent as the reference's ThreadStream shapes it. The whole timeline always fits
    /// one window, so a bounded snapshot has no earlier history.
    pub(crate) fn stream_snapshot(&self, sequence: u64, options: &Value) -> Value {
        let mut projection = self.projection();
        if options["acceptBoundedSnapshot"] != true {
            return json!({"kind":"snapshot","snapshotSequence":sequence,"projection":projection});
        }
        let latest_local_turn_ordinal = projection["turnItems"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|item| item["ordinal"].as_u64())
            .max();
        if options["acceptDedupedBodies"] == true {
            dedupe_bodies(&mut projection);
        }
        let mut omitted = false;
        if options["acceptCompactTurnItems"] == true {
            // Every turn item is a local visible row, so the rows alone restore them.
            let rows = projection["visibleTurnItems"]
                .as_array()
                .map_or(0, Vec::len);
            if rows > 0 {
                let items = projection["turnItems"].as_array_mut().expect("turn items");
                items.drain(..rows.min(items.len()));
                omitted = true;
            }
        }
        // The reference's field order (ThreadStream.boundedSnapshotResponseFields).
        let mut item = json!({"kind":"snapshot","snapshotSequence":sequence,"projection":projection,
            "historyCursor":null,"hasMoreHistory":false,"latestLocalTurnOrdinal":latest_local_turn_ordinal});
        if omitted {
            item["turnItemsOmitLocalVisible"] = json!(true);
        }
        item
    }

    pub(crate) fn shell(&self) -> Value {
        let mut row = self.stored_row();
        for key in [
            "latestRunId",
            "latestRunRequestedAt",
            "latestRunStartedAt",
            "latestRunCompletedAt",
            "activeRunId",
            "activityRunStartedAt",
            "activityRunStatus",
            "lastError",
            "lastErrorClass",
            "lastErrorCode",
            "usageLimitResetAt",
            "pendingRuntimeRequest",
            "latestVisibleMessage",
            "latestUserMessageAt",
            "latestUserAuthoredMessageAt",
            "goal",
            "unsettledAt",
            "limitRecovery",
            "pinnedAt",
            "autoSettleDisabledAt",
            "pinOrderKey",
            "opens",
            "titleRegeneration",
            "lastTitleRegeneration",
        ] {
            row.as_object_mut()
                .unwrap()
                .entry(key)
                .or_insert(Value::Null);
        }
        for key in [
            "latestRunStopRequested",
            "latestRunBackgroundWake",
            "blockedOnThreadWait",
            "hasActionableProposedPlan",
        ] {
            row[key] = false.into();
        }
        for key in ["pendingBackgroundTasks", "providerInstanceHistory"] {
            row[key] = json!([]);
        }
        row["status"] = json!(self.status);
        if let Some(pending) = self.pending_runtime_request() {
            row["pendingRuntimeRequest"] = pending;
        }
        row["pendingGuestMessageCount"] = json!(row["guestShare"]["messages"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|m| m["status"] == "pending")
            .count());
        row["itemCount"] = json!(self.messages.len());
        row["visibleItemCount"] = row["itemCount"].clone();
        ordered(SHELL_FIELDS, &row)
    }
}

fn field<'a>(row: &'a Value, key: &str) -> &'a str {
    row[key].as_str().unwrap_or_default().trim()
}

fn same_link(a: &Value, b: &Value, issue: bool) -> bool {
    field(a, "host").eq_ignore_ascii_case(field(b, "host"))
        && if issue {
            a["tracker"] == b["tracker"]
                && field(a, "scope").eq_ignore_ascii_case(field(b, "scope"))
                && field(a, "key") == field(b, "key")
        } else {
            field(a, "repository").eq_ignore_ascii_case(field(b, "repository"))
                && a["number"] == b["number"]
        }
}

pub(crate) fn link_patch(thread: &Thread, cmd: &Value, at: &str) -> Value {
    let kind = field(cmd, "type");
    let issue = kind.starts_with("thread.issue.");
    let key = if issue { "issues" } else { "pullRequests" };
    let mut links = thread.row[key].as_array().cloned().unwrap_or_default();
    let i = links.iter().position(|l| same_link(l, cmd, issue));
    if kind.ends_with(".unlink") {
        let Some(i) = i else {
            return json!({});
        };
        if !issue && (links[i]["source"] == "stack" || !links[i]["stack"].is_null()) {
            links[i]["source"] = "stack-dismissed".into();
            links[i].as_object_mut().unwrap().shift_remove("watch");
        } else {
            links.remove(i);
        }
    } else if kind == "thread.pull-request-link.sync" {
        let Some(i) = i else {
            return json!({});
        };
        links[i]["snapshot"] = cmd["snapshot"].clone();
        links[i]["stack"] = cmd["stack"].clone();
    } else if let Some(i) = i {
        if issue || links[i]["source"] != "stack-dismissed" || cmd["source"] == "stack" {
            return json!({});
        }
        links[i]["source"] = cmd["source"].clone();
        links[i]["url"] = cmd["url"].clone();
    } else if issue {
        links.push(json!({"tracker":cmd["tracker"],"host":field(cmd,"host").to_lowercase(),
            "scope":field(cmd,"scope"),"key":field(cmd,"key"),"projectId":cmd["projectId"],
            "url":cmd["url"],"source":cmd["source"],"linkedAt":at,"displayKey":cmd["displayKey"],"title":cmd["title"]}));
    } else {
        let mut link = json!({"host":field(cmd,"host").to_lowercase(),"repository":field(cmd,"repository").to_lowercase(),"number":cmd["number"],"url":cmd["url"]});
        if let Some(url) = cmd.get("repositoryWebUrl") {
            link["repositoryWebUrl"] = url.clone();
        }
        link["source"] = cmd["source"].clone();
        link["linkedAt"] = json!(at);
        link["snapshot"] = Value::Null;
        link["stack"] = Value::Null;
        links.push(link);
    }
    let mut patch = Map::new();
    patch.insert(key.into(), Value::Array(links));
    if !issue {
        patch.insert("linkedPullRequest".into(), Value::Null);
    }
    if !kind.ends_with(".sync") {
        patch.insert("updatedAt".into(), json!(at));
    }
    Value::Object(patch)
}

pub(crate) fn watch_patch(thread: &Thread, cmd: &Value, at: &str) -> Value {
    let mut links = thread.row["pullRequests"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let host = field(cmd, "host");
    let repository = field(cmd, "repository");
    let number = cmd["number"].clone();
    let matches = |link: &Value| {
        field(link, "host").eq_ignore_ascii_case(host)
            && field(link, "repository").eq_ignore_ascii_case(repository)
            && link["number"] == number
            && link["source"] != "stack-dismissed"
    };
    let key_matches = |link: &Value| {
        field(link, "host").eq_ignore_ascii_case(host)
            && field(link, "repository").eq_ignore_ascii_case(repository)
            && link["number"] == number
    };
    let mut index = links.iter().position(matches);
    if cmd["watching"] == true {
        if index.is_none() {
            let Some(link) = cmd.get("link").cloned() else {
                return json!({});
            };
            let ended_watch = links
                .iter()
                .find(|candidate| {
                    key_matches(candidate) && candidate["source"] == "stack-dismissed"
                })
                .and_then(|candidate| candidate.get("endedWatch").cloned());
            links.retain(|candidate| !key_matches(candidate));
            let mut created = json!({
                "host": host.to_lowercase(),
                "repository": repository.to_lowercase(),
                "number": number,
                "url": link["url"],
                "source": link["source"],
                "linkedAt": at,
                "snapshot": Value::Null,
                "stack": Value::Null
            });
            if let Some(repository_web_url) = link.get("repositoryWebUrl") {
                created["repositoryWebUrl"] = repository_web_url.clone();
            }
            if let Some(ended_watch) = ended_watch {
                created["endedWatch"] = ended_watch;
            }
            links.push(created);
            index = Some(links.len() - 1);
        }
        let index = index.expect("watch link index exists");
        if links[index].get("watch").is_some() {
            return json!({});
        }
        let previous = links[index].get("endedWatch").cloned();
        let mut watch = previous.unwrap_or_else(|| {
            json!({
                "startedAt": at,
                "headSha": Value::Null,
                "failedChecks": [],
                "passed": false,
                "passedChecks": [],
                "remarksThrough": at,
                "remarkIds": [],
                "conflicting": false,
                "wakes": 0
            })
        });
        watch["startedAt"] = json!(at);
        watch["remarksThrough"] = json!(at);
        watch["remarkIds"] = json!([]);
        watch["wakes"] = json!(0);
        if let Some(object) = links[index].as_object_mut() {
            object.remove("endedWatch");
            object.insert("watch".into(), watch);
        }
    } else if let Some(index) = index {
        let Some(link) = links[index].as_object_mut() else {
            return json!({});
        };
        let Some(previous) = link.remove("watch") else {
            return json!({});
        };
        link.insert("endedWatch".into(), previous);
    } else {
        return json!({});
    }
    json!({"pullRequests":links,"updatedAt":at})
}

pub(crate) fn linked(store: &Store, input: &Value, issue: bool) -> Value {
    let key = if issue { "issues" } else { "pullRequests" };
    let mut rows: Vec<_> = store
        .threads
        .values()
        .filter(|t| {
            t.row["lineage"]["sideChat"] != true
                && t.row[key]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .any(|l| l["source"] != "stack-dismissed" && same_link(l, input, issue))
        })
        .collect();
    rows.sort_by(|a, b| {
        field(&b.row, "updatedAt")
            .cmp(field(&a.row, "updatedAt"))
            .then_with(|| a.id.cmp(&b.id))
    });
    json!({"threads":rows.into_iter().map(|t|json!({"id":t.id,"projectId":t.project_id,"title":t.title,"archivedAt":t.row["archivedAt"],"settled":t.settled})).collect::<Vec<_>>()})
}
