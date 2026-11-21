//! What a thread's provider turns leave behind in the store, rebuilt from
//! the recorded [`Event::RunProvider`](crate::Event::RunProvider) events:
//! the native thread (Codex) or session (Claude) the next turn resumes, and
//! each run's items, plan, usage and end, which `thread_projection` turns
//! into the reference's projection records.
use crate::workspace::provider::{self, ProviderTurn};
use crate::workspace::provider::{
    PlanStep, ProviderEvent, ProviderFailure, ProviderItem, RuntimeRequestOpen, ThreadDisposition,
    TokenUsage, TurnStatus,
};
use serde_json::Value;
use std::collections::HashMap;
use uuid::Uuid;

/// A thread's provider state. Not serialized: the store rebuilds it from
/// the event log.
#[derive(Clone, Debug, Default)]
pub struct ProviderState {
    /// The native thread the next turn resumes, if it may.
    pub native_thread: Option<NativeThread>,
    /// The thread's provider thread record, once a provider turn ran.
    pub thread: Option<ThreadRecord>,
    /// Each run's provider turn, by run.
    pub runs: HashMap<Uuid, RunRecord>,
    /// How each run's provider turn ended, from its `turn.terminal`.
    pub terminals: HashMap<Uuid, Terminal>,
    /// What the native session's prompts used of its attachment budget
    /// (bytes, pages), once a prompt reached it.
    pub attachment_budget: Option<(u64, u64)>,
    /// When a server restart stopped the provider session, until a turn
    /// starts it again.
    pub session_stopped_at: Option<u64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Terminal {
    pub status: TurnStatus,
    pub failure: Option<ProviderFailure>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeThread {
    pub driver: String,
    pub id: String,
}

/// The provider thread record: named after the run that first used it, as
/// the reference names a new thread's record before the native thread is
/// known.
#[derive(Clone, Debug)]
pub struct ThreadRecord {
    pub driver: String,
    pub first_run: Uuid,
    pub created_at: u64,
    pub updated_at: u64,
    pub native_id: Option<String>,
    /// `not_loaded`, `active`, `idle` or `error`.
    pub status: &'static str,
}

/// One run's provider turn.
#[derive(Clone, Debug)]
pub struct RunRecord {
    pub driver: String,
    pub started_at: u64,
    /// The CLI started the turn (`provider_turn.updated`), so the run has a
    /// provider turn record.
    pub turn_started: bool,
    pub native_turn_id: Option<String>,
    pub usage: Option<(TokenUsage, u64)>,
    /// The whole turn's usage (`turnTokenUsage`).
    pub turn_usage: Option<Value>,
    /// The model the turn ran on.
    pub model: Option<String>,
    /// The CLI received the run's prompt.
    pub prompt_delivered: bool,
    /// Items in the order first seen.
    pub items: Vec<ItemRecord>,
    pub plan: Option<PlanRecord>,
    pub ended_at: Option<u64>,
    /// Runtime requests the CLI made, in order.
    pub requests: Vec<RequestRecord>,
}

/// A runtime request and what became of it.
#[derive(Clone, Debug)]
pub struct RequestRecord {
    pub request: RuntimeRequestOpen,
    pub created_at: u64,
    /// `pending`, `resolved` or `cancelled`.
    pub status: &'static str,
    pub resolved_at: Option<u64>,
    pub decision: Option<String>,
    pub feedback: Option<String>,
    pub answers: Option<Value>,
    /// When the answer could not be handed to the turn.
    pub delivery_failed_at: Option<u64>,
}

impl RequestRecord {
    /// Whether the request still waits for the user.
    pub fn pending(&self) -> bool {
        self.status == "pending"
    }
}

#[derive(Clone, Debug)]
pub struct ItemRecord {
    pub item: ProviderItem,
    pub first_at: u64,
    pub updated_at: u64,
}

#[derive(Clone, Debug)]
pub struct PlanRecord {
    pub steps: Vec<PlanStep>,
    pub explanation: Option<String>,
    pub first_at: u64,
    pub updated_at: u64,
}

impl ProviderState {
    /// Apply `run`'s provider `event`, recorded at `at` (seconds).
    pub(crate) fn apply(&mut self, run: Uuid, event: &ProviderEvent, at: u64) {
        // A turn after a restart runs in a session started again.
        self.session_stopped_at = None;
        let driver = match event {
            ProviderEvent::ProviderThread { driver, .. } => driver.clone(),
            _ => self
                .runs
                .get(&run)
                .map(|record| record.driver.clone())
                .or_else(|| self.thread.as_ref().map(|thread| thread.driver.clone()))
                .unwrap_or_else(|| provider::CODEX.into()),
        };
        let record = self.runs.entry(run).or_insert_with(|| RunRecord {
            driver: driver.clone(),
            started_at: at,
            turn_started: false,
            native_turn_id: None,
            usage: None,
            turn_usage: None,
            model: None,
            prompt_delivered: false,
            items: Vec::new(),
            plan: None,
            ended_at: None,
            requests: Vec::new(),
        });
        match event {
            ProviderEvent::ProviderThread {
                driver,
                native_thread_id,
                resumed,
                ..
            } => {
                record.driver = driver.clone();
                // A run that starts a new native session starts a new
                // attachment budget.
                if !resumed && !record.prompt_delivered {
                    self.attachment_budget = None;
                }
                self.native_thread = Some(NativeThread {
                    driver: driver.clone(),
                    id: native_thread_id.clone(),
                });
                let thread = self.thread.get_or_insert_with(|| ThreadRecord {
                    driver: driver.clone(),
                    first_run: run,
                    created_at: at,
                    updated_at: at,
                    native_id: None,
                    status: "active",
                });
                thread.native_id = Some(native_thread_id.clone());
                thread.status = "active";
                thread.updated_at = at;
            }
            ProviderEvent::ProviderTurn {
                native_turn_id,
                model,
                ..
            } => {
                record.turn_started = true;
                if model.is_some() {
                    record.model = model.clone();
                }
                if native_turn_id.is_some() {
                    record.native_turn_id = native_turn_id.clone();
                }
            }
            ProviderEvent::TurnItem { item } => {
                match record
                    .items
                    .iter_mut()
                    .find(|known| known.item.id == item.id)
                {
                    Some(known) => {
                        known.item = item.clone();
                        known.updated_at = at;
                    }
                    None => record.items.push(ItemRecord {
                        item: item.clone(),
                        first_at: at,
                        updated_at: at,
                    }),
                }
            }
            ProviderEvent::TurnItemDelta { item_id, delta } => {
                if let Some(known) = record
                    .items
                    .iter_mut()
                    .find(|known| &known.item.id == item_id)
                {
                    let text = known
                        .item
                        .fields
                        .get("text")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let text = format!("{text}{delta}");
                    known.item.fields.insert("text".into(), text.into());
                    known.item.fields.insert("streaming".into(), true.into());
                    known.updated_at = at;
                }
            }
            ProviderEvent::Plan { steps, explanation } => {
                let first_at = record.plan.as_ref().map_or(at, |plan| plan.first_at);
                record.plan = Some(PlanRecord {
                    steps: steps.clone(),
                    explanation: explanation.clone(),
                    first_at,
                    updated_at: at,
                });
            }
            ProviderEvent::Usage { usage } => record.usage = Some((usage.clone(), at)),
            ProviderEvent::TurnUsage { usage } => record.turn_usage = Some(usage.clone()),
            ProviderEvent::PromptDelivered { bytes, pages } => {
                record.prompt_delivered = true;
                let (used_bytes, used_pages) = self.attachment_budget.unwrap_or_default();
                self.attachment_budget = Some((used_bytes + bytes, used_pages + pages));
            }
            ProviderEvent::TurnTerminal {
                status,
                failure,
                thread_disposition,
                driver,
            } => {
                record.ended_at = Some(at);
                if let (None, Some(driver)) = (&self.thread, driver) {
                    record.driver = driver.clone();
                }
                // A turn that ended before its CLI opened a thread still has
                // the provider thread record the reference creates for the
                // run, not loaded.
                let thread = self.thread.get_or_insert_with(|| ThreadRecord {
                    driver: record.driver.clone(),
                    first_run: run,
                    created_at: at,
                    updated_at: at,
                    native_id: None,
                    status: "not_loaded",
                });
                self.terminals.insert(
                    run,
                    Terminal {
                        status: *status,
                        failure: failure.clone(),
                    },
                );
                let broken = *thread_disposition == ThreadDisposition::Broken;
                if broken {
                    self.native_thread = None;
                }
                if thread.native_id.is_some() {
                    thread.status = if broken { "error" } else { "idle" };
                    thread.updated_at = at;
                }
            }
            ProviderEvent::RuntimeRequestOpened { request } => {
                record.requests.push(RequestRecord {
                    request: request.clone(),
                    created_at: at,
                    status: "pending",
                    resolved_at: None,
                    decision: None,
                    feedback: None,
                    answers: None,
                    delivery_failed_at: None,
                });
                // A question or approval takes the next item slot, as the
                // reference orders it among the turn's items.
                let kind = if request.kind == "user_input" {
                    "user_input_request"
                } else {
                    "approval_request"
                };
                record.items.push(ItemRecord {
                    item: ProviderItem::new(format!("request:{}", request.id), kind, "waiting"),
                    first_at: at,
                    updated_at: at,
                });
            }
            ProviderEvent::RuntimeRequestAnswered {
                id,
                decision,
                feedback,
                answers,
            } => {
                if let Some(request) = record
                    .requests
                    .iter_mut()
                    .find(|known| &known.request.id == id && known.pending())
                {
                    request.status = "resolved";
                    request.resolved_at = Some(at);
                    request.decision = decision.clone();
                    request.feedback = feedback.clone();
                    request.answers = answers.clone();
                }
            }
            ProviderEvent::RuntimeRequestDeliveryFailed { id } => {
                if let Some(request) = record
                    .requests
                    .iter_mut()
                    .find(|known| &known.request.id == id && known.delivery_failed_at.is_none())
                {
                    request.delivery_failed_at = Some(at);
                    record.items.push(ItemRecord {
                        item: ProviderItem::new(
                            format!("delivery:{id}"),
                            "approval_request",
                            "failed",
                        ),
                        first_at: at,
                        updated_at: at,
                    });
                }
            }
            ProviderEvent::RuntimeRequestClosed { id, .. } => {
                if let Some(request) = record
                    .requests
                    .iter_mut()
                    .find(|known| &known.request.id == id && known.pending())
                {
                    request.status = "cancelled";
                    request.resolved_at = Some(at);
                }
            }
            ProviderEvent::CommandOutputDelta { .. } | ProviderEvent::RuntimeRequest { .. } => {}
        }
    }

    /// A server restart stopped the provider session at `at`: no CLI
    /// survives the server, so an active provider thread is idle.
    pub(crate) fn stop_session(&mut self, at: u64) {
        self.session_stopped_at = Some(at);
        if let Some(thread) = self.thread.as_mut().filter(|t| t.status == "active") {
            thread.status = "idle";
            thread.updated_at = at;
        }
    }

    /// Whether the thread's provider session is open: a turn opened it and
    /// no restart stopped it since.
    pub fn session_live(&self) -> bool {
        self.thread.is_some() && self.session_stopped_at.is_none()
    }

    /// The run and record of the request `id`.
    pub fn request(&self, id: &str) -> Option<(Uuid, &RequestRecord)> {
        self.runs.iter().find_map(|(run, record)| {
            record
                .requests
                .iter()
                .find(|request| request.request.id == id)
                .map(|request| (*run, request))
        })
    }

    /// The native thread a turn of `driver` resumes.
    pub fn resume_for(&self, driver: &str) -> Option<String> {
        self.native_thread
            .as_ref()
            .filter(|thread| thread.driver == driver)
            .map(|thread| thread.id.clone())
    }
}

/// How a turn of a thread runs its provider: the CLI of the thread's
/// provider instance (from `settings.providerInstances`, or the built-in
/// instance of the same name) with the thread's model selection, resuming
/// the thread's native thread. `None` for an instance whose driver the
/// workspace does not run.
pub(crate) fn provider_turn(
    settings: &Value,
    manifest: &Value,
    row: &Value,
    state: &ProviderState,
) -> Option<(ProviderTurn, Vec<(String, String)>)> {
    let selection = &row["modelSelection"];
    let instance_id = selection["instanceId"]
        .as_str()
        .or(row["providerInstanceId"].as_str())
        .unwrap_or(provider::CODEX);
    let instance = &settings["providerInstances"][instance_id];
    let driver = instance["driver"].as_str().unwrap_or(instance_id);
    if driver != provider::CODEX && driver != provider::CLAUDE_AGENT {
        return None;
    }
    let config = &instance["config"];
    let text = |key: &str| config[key].as_str().unwrap_or_default().trim().to_owned();
    let reasoning_effort = selection["options"].as_array().and_then(|options| {
        options
            .iter()
            .find(|option| option["id"] == "reasoningEffort")
            .and_then(|option| option["value"].as_str())
            .map(str::to_owned)
    });
    let env = instance["environment"]
        .as_array()
        .map(|variables| {
            variables
                .iter()
                .filter_map(|variable| {
                    Some((
                        variable["name"].as_str()?.to_owned(),
                        variable["value"].as_str()?.to_owned(),
                    ))
                })
                .collect()
        })
        .unwrap_or_default();
    let claude_launch = if driver == provider::CLAUDE_AGENT {
        crate::model_manifest::claude_launch(manifest, selection)
    } else {
        (None, Value::Null)
    };
    let turn = ProviderTurn {
        driver: driver.to_owned(),
        binary_path: text("binaryPath"),
        launch_args: split_args(&text("launchArgs")),
        home_path: text("homePath"),
        model: selection["model"]
            .as_str()
            .filter(|model| !model.is_empty() && *model != "default")
            .unwrap_or_default()
            .to_owned(),
        reasoning_effort: reasoning_effort.or_else(|| claude_launch.0.clone()),
        resume: state.resume_for(driver),
        runtime_mode: row["runtimeMode"].as_str().unwrap_or_default().to_owned(),
        app_thread_id: row["id"].as_str().unwrap_or_default().to_owned(),
        interaction_mode: row["interactionMode"]
            .as_str()
            .unwrap_or_default()
            .to_owned(),
        extra_dirs: Vec::new(),
        claude_settings: claude_launch.1,
        context_window: (driver == provider::CLAUDE_AGENT)
            .then(|| crate::model_manifest::claude_context_window(manifest, selection))
            .flatten(),
    };
    Some((turn, env))
}

/// Split launch arguments as a shell would split plain words and quoted
/// strings (no expansion).
fn split_args(text: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut started = false;
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        match (quote, c) {
            (Some(q), c) if c == q => quote = None,
            (Some('"'), '\\') => current.extend(chars.next()),
            (Some(_), c) => current.push(c),
            (None, '"' | '\'') => {
                quote = Some(c);
                started = true;
            }
            (None, '\\') => {
                current.extend(chars.next());
                started = true;
            }
            (None, c) if c.is_whitespace() => {
                if started {
                    args.push(std::mem::take(&mut current));
                    started = false;
                }
            }
            (None, c) => {
                current.push(c);
                started = true;
            }
        }
    }
    if started {
        args.push(current);
    }
    args
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_turn_takes_its_instance_settings_selection_and_native_thread() {
        let settings = json!({"providerInstances": {"work": {
            "driver": "codex",
            "config": {"binaryPath": "/opt/codex", "launchArgs": "--profile 'my work' -c x=1", "homePath": "/h"},
            "environment": [{"name": "A", "value": "1", "sensitive": false}]
        }}});
        let row = json!({"modelSelection": {"instanceId": "work", "model": "gpt-5",
            "options": [{"id": "reasoningEffort", "value": "high"}]}});
        let mut state = ProviderState::default();
        state.apply(
            Uuid::nil(),
            &ProviderEvent::ProviderThread {
                driver: "codex".into(),
                native_thread_id: "thr_1".into(),
                resumed: false,
                model: None,
            },
            1,
        );
        let (turn, env) = provider_turn(&settings, &json!({}), &row, &state).unwrap();
        assert_eq!(turn.driver, "codex");
        assert_eq!(turn.binary_path, "/opt/codex");
        assert_eq!(turn.launch_args, ["--profile", "my work", "-c", "x=1"]);
        assert_eq!(turn.home_path, "/h");
        assert_eq!(turn.model, "gpt-5");
        assert_eq!(turn.reasoning_effort.as_deref(), Some("high"));
        assert_eq!(turn.resume.as_deref(), Some("thr_1"));
        assert_eq!(env, [("A".to_string(), "1".to_string())]);

        // A built-in instance needs no settings; another driver's native
        // thread is not resumed.
        let claude = json!({"modelSelection": {"instanceId": "claudeAgent", "model": "default"}});
        let (turn, _) = provider_turn(&json!({}), &json!({}), &claude, &state).unwrap();
        assert_eq!(
            (turn.driver.as_str(), turn.model.as_str()),
            ("claudeAgent", "")
        );
        assert_eq!(turn.resume, None);
        let other = json!({"modelSelection": {"instanceId": "cursor", "model": "x"}});
        assert!(provider_turn(&json!({}), &json!({}), &other, &state).is_none());
    }
}
