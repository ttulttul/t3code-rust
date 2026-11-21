//! What a server restart does to provider turns, as the reference's
//! ProviderRuntimeRecoveryService and RestartContinuation do it.
//!
//! No provider CLI survives the server that started it. When the server stops
//! (`shutdown`), or starts after stopping without doing so (`startup`), each
//! running run is cut off: it ends cancelled with a notice, its unfinished
//! records end with it, waiting runtime requests close, queued runs wait to
//! be sent, and the provider session stops. A run whose provider turn was
//! still going in a live session is remembered for continuation; once the
//! server is up again, it sends the turn's native thread a prompt to
//! continue, as a notification in a new run.
use crate::{Event, RestartCut, Run, Store, Thread};
use serde_json::{json, Value};
use uuid::Uuid;

/// The prompt that continues a cut-off turn, as the reference words it.
pub(crate) const CONTINUE_PROMPT: &str = "Continue where you left off. Your last turn was cut short, so a command or other action it started may have finished or may still be running. Before you repeat one that changes state, confirm it did not take effect and is not still running; if you cannot tell, say so instead of repeating it.";

/// A turn that keeps taking the server down is not continued after this
/// many lost processes in a row (the reference's `MAX_CONTINUATION_CHAIN`).
const MAX_PROCESS_LOSSES: u32 = 3;

/// The events that reconcile every thread's provider runtime after a
/// restart's `trigger` (`shutdown` or `startup`) at `at` (seconds).
/// `now_iso` is the time a snooze is compared with.
pub(crate) fn reconcile(store: &Store, trigger: &str, at: u64, now_iso: &str) -> Vec<Event> {
    let mut threads: Vec<&Thread> = store.threads.values().collect();
    threads.sort_by(|a, b| a.id.cmp(&b.id));
    threads
        .into_iter()
        .filter_map(|thread| reconcile_thread(thread, trigger, at, now_iso))
        .collect()
}

fn reconcile_thread(thread: &Thread, trigger: &str, at: u64, now_iso: &str) -> Option<Event> {
    let continued = continuation_source(thread, now_iso);
    let cut: Vec<(Uuid, RestartCut)> = thread
        .runs
        .iter()
        .filter(|run| run.status == "running")
        .map(|run| {
            let continuation = (continued == Some(run.id)).then(|| {
                // A graceful stop is no loss. A loss counts on from the
                // losses of the run this one continued.
                if trigger == "shutdown" {
                    0
                } else {
                    1 + run
                        .restart_continuation_of
                        .and_then(|source| thread.runs.iter().find(|r| r.id == source))
                        .and_then(|source| source.restart_cut.as_ref())
                        .and_then(|cut| cut.continuation)
                        .unwrap_or(0)
                }
            });
            (
                run.id,
                RestartCut {
                    trigger: trigger.to_owned(),
                    at,
                    continuation,
                },
            )
        })
        .collect();
    let held: Vec<Uuid> = thread
        .runs
        .iter()
        .filter(|run| run.status == "queued" && !run.queue_held)
        .map(|run| run.id)
        .collect();
    let provider = &thread.provider;
    let session_open = provider.session_live()
        || provider
            .thread
            .as_ref()
            .is_some_and(|record| record.status == "active");
    if cut.is_empty() && held.is_empty() && !session_open {
        return None;
    }
    Some(Event::ProviderRuntimeReconciled {
        thread_id: thread.id.clone(),
        cut,
        held,
    })
}

/// The run whose turn continues after the restart, as the reference's
/// `restartContinuationRun` picks it before cutting it off: the thread's
/// latest started run, still running, in a thread nobody put away, whose
/// provider turn was still going in a live session on a known native thread
/// with nothing waiting for an answer.
fn continuation_source(thread: &Thread, now_iso: &str) -> Option<Uuid> {
    let run = thread
        .runs
        .iter()
        .rev()
        .find(|run| run.status != "queued")?;
    if run.status != "running" || put_away(thread, now_iso) {
        return None;
    }
    let provider = &thread.provider;
    let record = provider.runs.get(&run.id)?;
    let turn_running = !provider.terminals.contains_key(&run.id)
        && (record.turn_started || record.native_turn_id.is_some() || !record.items.is_empty());
    let native_thread = provider
        .thread
        .as_ref()
        .is_some_and(|t| t.native_id.is_some() && t.status == "active");
    let waiting = provider
        .runs
        .values()
        .any(|record| record.requests.iter().any(|request| request.pending()));
    (turn_running && native_thread && provider.session_live() && !waiting).then_some(run.id)
}

/// Archived, deleted, settled or snoozed: a restart does not wake it.
fn put_away(thread: &Thread, now_iso: &str) -> bool {
    let row = thread.stored_row();
    thread.archived
        || thread.settled
        || !row["deletedAt"].is_null()
        || row["settledOverride"] == "settled"
        || row["snoozedUntil"]
            .as_str()
            .or(thread.snoozed_until.as_deref())
            .is_some_and(|until| until > now_iso)
}

/// The `message.dispatch` commands that continue the runs a restart cut off,
/// with the run each continues, as the reference's `continueRestartedRun`
/// sends them. `settings` are the effective settings.
pub(crate) fn continuations(store: &Store, settings: &Value, now_iso: &str) -> Vec<(Uuid, Value)> {
    let mut out = Vec::new();
    let mut threads: Vec<&Thread> = store.threads.values().collect();
    threads.sort_by(|a, b| a.id.cmp(&b.id));
    for thread in threads {
        let Some((index, run, losses)) = thread
            .runs
            .iter()
            .enumerate()
            .filter_map(|(index, run)| {
                let losses = run.restart_cut.as_ref()?.continuation?;
                Some((index, run, losses))
            })
            .next_back()
        else {
            continue;
        };
        // A later run supersedes it.
        if thread.runs[index + 1..]
            .iter()
            .any(|later| later.status != "queued")
        {
            continue;
        }
        let message_id = format!("message:restart-continuation:{}", run.id);
        if thread
            .messages
            .iter()
            .any(|message| message.id == message_id)
            || !continues_after_restart(settings, &thread.project_id)
            || put_away(thread, now_iso)
        {
            continue;
        }
        if losses > MAX_PROCESS_LOSSES {
            tracing::warn!(
                thread_id = %thread.id,
                source_run_id = %run.id,
                "Not continuing a run restarted too many times in a row"
            );
            continue;
        }
        out.push((run.id, command(thread, run, &message_id)));
    }
    out
}

/// `continueThreadsAfterServerUpdate` for `project`: its override, else the
/// setting, which is on by default.
fn continues_after_restart(settings: &Value, project: &str) -> bool {
    settings["projectSettingsOverrides"][project]["continueThreadsAfterServerUpdate"]
        .as_bool()
        .or(settings["continueThreadsAfterServerUpdate"].as_bool())
        .unwrap_or(true)
}

fn command(thread: &Thread, source: &Run, message_id: &str) -> Value {
    let mut command = json!({
        "type": "message.dispatch",
        "commandId": format!("command:restart-continuation:{}", source.id),
        "threadId": thread.id,
        "messageId": message_id,
        "text": CONTINUE_PROMPT,
        "notification": {
            "source": {"kind": "system"},
            "outcome": "updated",
            "summary": crate::thread_projection::RESTART_CONTINUATION_SUMMARY,
            "detail": CONTINUE_PROMPT
        },
        "attachments": [],
        "dispatchMode": {"type": "start_immediately"},
        "createdBy": "agent",
        "creationSource": "server",
        "restartContinuationOfRunId": source.id.to_string()
    });
    let selection = &thread.stored_row()["modelSelection"];
    if selection.is_object() {
        command["modelSelection"] = selection.clone();
    }
    command
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace::provider::{
        ProviderEvent, ProviderItem, RuntimeRequestOpen, ThreadDisposition, TurnStatus,
    };

    fn run(status: &str) -> Run {
        Run {
            id: Uuid::new_v4(),
            thread_id: "thread:a".into(),
            provider: "default".into(),
            status: status.into(),
            started_at: 10,
            finished_at: None,
            error: None,
            user_message_id: Some("message:a".into()),
            queue_held: false,
            restart_cut: None,
            restart_continuation_of: None,
        }
    }

    fn thread(runs: Vec<Run>) -> Thread {
        Thread {
            row: json!({"id": "thread:a", "modelSelection": {"instanceId": "claudeAgent", "model": "m"},
                "deletedAt": null, "snoozedUntil": null}),
            id: "thread:a".into(),
            project_id: "project:a".into(),
            title: "A".into(),
            cwd: "/tmp".into(),
            status: "running".into(),
            messages: vec![],
            runs,
            created_at: 1,
            archived: false,
            settled: false,
            snoozed_until: None,
            pinned: false,
            unread: false,
            tags: vec![],
            checkpoints: vec![],
            provider: Default::default(),
        }
    }

    /// A provider turn that started in a session on native thread `sess-1`
    /// and reported a running command.
    fn started(thread: &mut Thread, run: Uuid) {
        let events = [
            ProviderEvent::ProviderThread {
                driver: "claudeAgent".into(),
                native_thread_id: "sess-1".into(),
                resumed: false,
                model: None,
            },
            ProviderEvent::ProviderTurn {
                native_turn_id: None,
                status: TurnStatus::Running,
                model: None,
            },
            ProviderEvent::TurnItem {
                item: ProviderItem::new("tool-1", "command_execution", "running"),
            },
        ];
        for event in &events {
            thread.provider.apply(run, event, 11);
        }
    }

    fn store(thread: Thread) -> Store {
        let mut store = Store::default();
        store.threads.insert(thread.id.clone(), thread);
        store
    }

    fn apply(store: &mut Store, events: Vec<Event>) {
        for (id, event) in events.into_iter().enumerate() {
            store.apply_record(&crate::ServerEvent {
                id: id as u64 + 1,
                at: 20,
                event,
                receipt: None,
                thread_events: None,
            });
        }
    }

    #[test]
    fn a_running_turn_is_cut_off_and_continued_after_a_shutdown() {
        let first = run("running");
        let id = first.id;
        let mut a = thread(vec![first]);
        started(&mut a, id);
        let mut store = store(a);
        let events = reconcile(&store, "shutdown", 20, "2026-01-01T00:00:00Z");
        apply(&mut store, events);
        let thread = &store.threads["thread:a"];
        let cut = thread.runs[0].restart_cut.clone().unwrap();
        assert_eq!(thread.runs[0].status, "cancelled");
        assert_eq!(
            (cut.trigger.as_str(), cut.continuation),
            ("shutdown", Some(0))
        );
        assert!(!thread.provider.session_live());

        let projection = thread.projection();
        assert_eq!(projection["providerSessions"][0]["status"], "stopped");
        assert_eq!(projection["providerThreads"][0]["status"], "idle");
        assert_eq!(projection["providerTurns"][0]["status"], "cancelled");
        assert_eq!(projection["attempts"][0]["status"], "cancelled");
        let items: Vec<_> = projection["turnItems"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| {
                (
                    i["type"].as_str().unwrap(),
                    i["status"].as_str().unwrap(),
                    i["title"].clone(),
                )
            })
            .collect();
        assert_eq!(
            items,
            [
                ("command_execution", "cancelled", Value::Null),
                ("system_notice", "completed", json!("Server shut down"))
            ]
        );

        let commands = continuations(&store, &json!({}), "2026-01-01T00:00:00Z");
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].0, id);
        assert_eq!(commands[0].1["text"], CONTINUE_PROMPT);
        assert_eq!(commands[0].1["restartContinuationOfRunId"], id.to_string());
        // The setting, or a project's override, turns it off.
        let off = json!({"continueThreadsAfterServerUpdate": false});
        assert!(continuations(&store, &off, "2026-01-01T00:00:00Z").is_empty());
        let project_off = json!({"projectSettingsOverrides": {"project:a": {"continueThreadsAfterServerUpdate": false}}});
        assert!(continuations(&store, &project_off, "2026-01-01T00:00:00Z").is_empty());
    }

    #[test]
    fn an_idle_thread_only_has_its_session_stopped() {
        let mut first = run("completed");
        first.finished_at = Some(12);
        let id = first.id;
        let mut a = thread(vec![first]);
        started(&mut a, id);
        a.provider.apply(
            id,
            &ProviderEvent::TurnTerminal {
                status: TurnStatus::Completed,
                failure: None,
                thread_disposition: ThreadDisposition::Reusable,
                driver: None,
            },
            12,
        );
        let mut store = store(a);
        let events = reconcile(&store, "startup", 20, "2026-01-01T00:00:00Z");
        assert!(
            matches!(&events[..], [Event::ProviderRuntimeReconciled { cut, .. }] if cut.is_empty())
        );
        apply(&mut store, events);
        let projection = store.threads["thread:a"].projection();
        assert_eq!(projection["runs"][0]["status"], "completed");
        assert_eq!(projection["providerSessions"][0]["status"], "stopped");
        assert!(continuations(&store, &json!({}), "2026-01-01T00:00:00Z").is_empty());
        // Nothing is left to reconcile the next time.
        assert!(reconcile(&store, "startup", 30, "2026-01-01T00:00:00Z").is_empty());
    }

    #[test]
    fn a_turn_waiting_on_the_user_or_put_away_is_not_continued() {
        let first = run("running");
        let id = first.id;
        let mut a = thread(vec![first]);
        started(&mut a, id);
        let mut waiting = a.clone();
        waiting.provider.apply(
            id,
            &ProviderEvent::RuntimeRequestOpened {
                request: RuntimeRequestOpen {
                    id: "perm-1".into(),
                    native_request_id: "perm-1".into(),
                    native_item_id: "tool-1".into(),
                    kind: "command".into(),
                    runtime_mode: "full-access".into(),
                    supports_feedback: false,
                    fields: Default::default(),
                },
            },
            12,
        );
        let mut snoozed = a.clone();
        snoozed.row["snoozedUntil"] = json!("2026-02-01T00:00:00Z");
        for thread in [waiting, snoozed] {
            let events = reconcile(
                &store(thread.clone()),
                "startup",
                20,
                "2026-01-01T00:00:00Z",
            );
            let [Event::ProviderRuntimeReconciled { cut, .. }] = &events[..] else {
                panic!("one reconcile event");
            };
            assert_eq!(cut[0].1.continuation, None);
            assert_eq!(cut[0].1.trigger, "startup");
        }
    }

    #[test]
    fn a_turn_that_keeps_crashing_the_server_stops_being_continued() {
        let mut source = run("cancelled");
        source.restart_cut = Some(RestartCut {
            trigger: "startup".into(),
            at: 5,
            continuation: Some(MAX_PROCESS_LOSSES),
        });
        let mut next = run("running");
        next.restart_continuation_of = Some(source.id);
        let id = next.id;
        let mut a = thread(vec![source, next]);
        started(&mut a, id);
        let mut store = store(a);
        let events = reconcile(&store, "startup", 20, "2026-01-01T00:00:00Z");
        apply(&mut store, events);
        let cut = store.threads["thread:a"].runs[1]
            .restart_cut
            .clone()
            .unwrap();
        assert_eq!(cut.continuation, Some(MAX_PROCESS_LOSSES + 1));
        assert!(continuations(&store, &json!({}), "2026-01-01T00:00:00Z").is_empty());
    }
}
