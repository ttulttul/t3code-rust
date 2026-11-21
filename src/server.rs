//! The native server: AppState, the axum router and the process-backed run
//! loop. Only built with the `native` feature.
use crate::*;

use axum::{
    body::Bytes,
    extract::{FromRequest, Path, Request, State, WebSocketUpgrade},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{any, get, post},
    Json, Router,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use hmac::{Hmac, Mac};

use serde_json::json;
use sha2::Sha256;
use std::{
    collections::HashMap,
    path::{Path as FsPath, PathBuf},
    sync::Arc,
};
use tokio::sync::{broadcast, oneshot, Mutex};
use tracing::info;
use uuid::Uuid;

pub struct AppState {
    /// The event log, store, auth and state documents; AppState dereferences
    /// to it.
    pub control: crate::control_state::ControlState,
    pub(crate) config_started_at: String,
    pub(crate) environment_id: String,
    pub oauth: mcp_oauth::OAuth,
    pub(crate) connect: access_http::connect::Service,
    pub data_dir: PathBuf,
    pub paths: config::ServerPaths,
    pub runtime_config: config::RuntimeConfig,
    pub settings: Arc<settings::Settings>,
    pub provider_command: Vec<String>,
    pub cancellations: Mutex<HashMap<Uuid, oneshot::Sender<()>>>,
    pub guest: guest_service::GuestService,
    pub voice: voice::VoiceService,
    pub pull_requests: pull_requests::PullRequestService,
    pub issues: tokio::sync::Mutex<issues::IssueService>,
    /// The workspace plane: files, git, terminals, processes and previews.
    pub(crate) workspace: Arc<dyn crate::workspace::Workspace>,
    /// The same workspace, for the native-only HTTP routes that reach into
    /// its services (media, browser pages).
    pub(crate) local: Arc<crate::workspace::LocalWorkspace>,
    pub worktree_setup: worktree_setup::WorktreeSetupTracker,
    pub(crate) project_mutation_lock: Mutex<()>,
    pub(crate) storage_cleanup_report: Mutex<Option<serde_json::Value>>,
    pub(crate) pull_request_refresh: Mutex<u64>,
    pub(crate) provider_install_states: Mutex<HashMap<String, serde_json::Value>>,
    pub(crate) provider_auth_states: Mutex<HashMap<String, serde_json::Value>>,
    pub(crate) project_clone_states: Mutex<HashMap<String, serde_json::Value>>,
    pub(crate) project_clone_events: broadcast::Sender<()>,
    pub(crate) project_clone_cancellations: Mutex<HashMap<String, (u64, oneshot::Sender<()>)>>,
    pub(crate) project_clone_sequence: Mutex<u64>,
    /// The provider model manifest, refreshed from upstream.
    pub(crate) model_manifest:
        Arc<crate::model_manifest::ModelManifest<crate::model_manifest::NativeHost>>,
    /// Background tasks, timers and the open sockets.
    pub(crate) runtime: Arc<crate::runtime::TokioRuntime>,
    /// Set once the server starts to stop; see [`crate::turn::TurnHost::stopping`].
    pub(crate) stopping: std::sync::atomic::AtomicBool,
}
impl AppState {
    /// The server is stopping: record how its running turns are cut off,
    /// then stop their CLIs, which do not outlive it.
    pub(crate) async fn stop_provider_runtime(&self) {
        self.stopping
            .store(true, std::sync::atomic::Ordering::SeqCst);
        {
            let _guard = self.project_mutation_lock.lock().await;
            reconcile_provider_runtime(&self.control, "shutdown").await;
        }
        for (_, cancel) in self.cancellations.lock().await.drain() {
            let _ = cancel.send(());
        }
    }

    /// Continue the turns a restart cut off, as the reference's
    /// `provider-runtime.continue` effects do once the server is up.
    pub(crate) async fn continue_restarted_runs(self: Arc<Self>) {
        let settings = match self.settings.internal() {
            Ok(settings) => settings,
            Err(error) => {
                tracing::warn!(%error, "could not read settings to continue restarted turns");
                return;
            }
        };
        let commands = {
            let store = self.store.read().await;
            crate::runtime_recovery::continuations(
                &store,
                &settings,
                &auth_service::iso(auth_service::now()),
            )
        };
        for (source, command) in commands {
            let thread_id = command["threadId"].as_str().unwrap_or_default().to_owned();
            if let Err(error) =
                crate::orchestration::dispatch_restart_continuation(self.clone(), command, source)
                    .await
            {
                tracing::info!(
                    %thread_id,
                    source_run_id = %source,
                    error = %error["message"],
                    "orchestration-v2.restart-continuation.not-sent"
                );
            }
        }
    }

    /// The settings workspace method `method` takes, as configured now. Only
    /// the fields that method reads are filled in; reading them costs a
    /// settings load or a lock.
    pub(crate) async fn workspace_settings(
        &self,
        method: &str,
    ) -> crate::dispatch::WorkspaceSettings {
        let mut settings = crate::dispatch::WorkspaceSettings {
            automatic_git_fetch: true,
            ..Default::default()
        };
        if method.starts_with("git.") {
            settings.git = crate::workspace::GitTools {
                gh: self.issues.lock().await.gh.clone(),
                text_generation: self.provider_command.clone(),
            };
        }
        if matches!(method, "vcs.refreshStatus" | "subscribeVcsStatus") {
            // `VcsAutomaticFetchInterval`: a zero interval turns off the fetch a status read starts.
            settings.automatic_git_fetch = self.settings.internal().map_or(true, |settings| {
                settings["automaticGitFetchInterval"].as_f64() != Some(0.0)
            });
        }
        settings
    }

    pub async fn new(
        data_dir: impl Into<PathBuf>,
        provider_command: Vec<String>,
    ) -> anyhow::Result<Arc<Self>> {
        Self::with_auth(data_dir, provider_command, auth_service::AuthService::new()).await
    }

    pub async fn with_auth(
        data_dir: impl Into<PathBuf>,
        provider_command: Vec<String>,
        auth: auth_service::AuthService,
    ) -> anyhow::Result<Arc<Self>> {
        Self::with_config(
            data_dir,
            provider_command,
            auth,
            config::RuntimeConfig::load()?,
        )
        .await
    }

    pub async fn with_config(
        data_dir: impl Into<PathBuf>,
        provider_command: Vec<String>,
        auth: auth_service::AuthService,
        runtime_config: config::RuntimeConfig,
    ) -> anyhow::Result<Arc<Self>> {
        let dir = data_dir.into();
        let paths = config::ServerPaths::from_state_dir(dir.clone());
        paths.ensure()?;
        let documents: Arc<dyn persistence::DocumentStore> = Arc::new(persistence::FsStore);
        let settings = Arc::new(settings::Settings::open_in(&paths, documents.clone())?);
        let auth = auth.open_in(documents.clone(), &dir)?;
        auth.revoke_disallowed(
            settings.policy.allows_client("remote"),
            settings.policy.allows_connect(),
        )?;
        let connect = access_http::connect::Service::open(&dir)?;
        let guest = guest_service::GuestService::open(&dir)?;
        if !settings.policy.allows_connect() {
            connect.clear_disabled_link()?;
            guest.set_tunnel_hostname(None);
        }
        let log: Box<dyn event_log::EventLog + Send> =
            Box::new(event_log::NdjsonLog::open(dir.join("events.jsonl"))?);
        let control = crate::control_state::ControlState::open(auth, documents, log, &dir)?;
        // Turns run as this process's children, so none survives a restart.
        reconcile_provider_runtime(&control, "startup").await;
        let (project_clone_events, _) = broadcast::channel(64);
        let local = Arc::new(crate::workspace::LocalWorkspace::new(
            dir.clone(),
            local_services::TerminalService::with_logs(dir.join("logs/terminals")),
            local_services::PreviewService::default(),
            local_services::DeviceService::new(settings.clone(), &dir)?,
        ));
        Ok(Arc::new(Self {
            control,
            config_started_at: auth_service::iso(auth_service::now()),
            environment_id: environment_http::identity(&dir)?,
            oauth: mcp_oauth::OAuth::open(&dir)?,
            connect,
            data_dir: dir.clone(),
            paths: paths.clone(),
            runtime_config,
            settings: settings.clone(),
            provider_command,
            cancellations: Mutex::new(HashMap::new()),
            guest,
            voice: voice::VoiceService::new(settings.clone()),
            pull_requests: pull_requests::PullRequestService::default(),
            issues: tokio::sync::Mutex::new(issues::IssueService::new(dir.clone())),
            workspace: local.clone(),
            local,
            worktree_setup: worktree_setup::WorktreeSetupTracker::default(),
            project_mutation_lock: Mutex::new(()),
            storage_cleanup_report: Mutex::new(None),
            pull_request_refresh: Mutex::new(0),
            provider_install_states: Mutex::new(HashMap::new()),
            provider_auth_states: Mutex::new(HashMap::new()),
            project_clone_states: Mutex::new(HashMap::new()),
            project_clone_events,
            project_clone_cancellations: Mutex::new(HashMap::new()),
            project_clone_sequence: Mutex::new(0),
            model_manifest: Arc::new(crate::model_manifest::ModelManifest::new(
                crate::model_manifest::NativeHost::new(&paths.state_dir, settings.clone()),
            )),
            runtime: Arc::default(),
            stopping: Default::default(),
        }))
    }
}

impl crate::orchestration::CommandHost for AppState {
    fn mutation_lock(&self) -> &Mutex<()> {
        &self.project_mutation_lock
    }
    fn policy_summary(&self) -> Option<&serde_json::Value> {
        self.settings.policy.summary()
    }
    fn runtime_denial(&self, mode: &str) -> Option<String> {
        self.settings.policy.runtime_denial(mode)
    }
    fn provider_denial(&self, driver: &str) -> Option<String> {
        self.settings.policy.provider_denial(driver)
    }
    fn effective_settings(&self) -> anyhow::Result<serde_json::Value> {
        self.settings.internal()
    }
    fn workspace(&self) -> &dyn crate::workspace::Workspace {
        &*self.workspace
    }
    fn cancellations(&self) -> &Mutex<HashMap<Uuid, oneshot::Sender<()>>> {
        &self.cancellations
    }
    fn launch_run(
        self: Arc<Self>,
        run: Run,
        cwd: String,
        text: String,
        cancel: oneshot::Receiver<()>,
    ) {
        let env_remove = if self.settings.policy.summary().is_some() {
            vec!["T3CODE_CODEX_LAUNCH_ARGS".into()]
        } else {
            vec![]
        };
        let runtime = self.runtime.clone();
        crate::runtime::Runtime::spawn(
            &*runtime,
            Box::pin(async move {
                // A thread whose provider instance runs Codex or Claude runs
                // that CLI, as the control plane does; any other runs the
                // configured command.
                let provider = match self.settings.internal() {
                    Ok(settings) => {
                        let store = self.store.read().await;
                        store.threads.get(&run.thread_id).and_then(|thread| {
                            crate::provider_state::provider_turn(
                                &settings,
                                &self.model_manifest.current(),
                                &thread.stored_row(),
                                &thread.provider,
                            )
                        })
                    }
                    Err(error) => {
                        tracing::warn!(%error, "could not read settings for a turn");
                        None
                    }
                };
                let mut spec = crate::workspace::TurnSpec {
                    prompt: text,
                    cwd,
                    env_remove,
                    turn_id: run.id.to_string(),
                    ..Default::default()
                };
                match provider {
                    Some((mut provider, env)) => {
                        // Claude may read the thread's attachments, as the
                        // reference lets it.
                        provider.extra_dirs = vec![self.paths.attachments.display().to_string()];
                        spec.provider = Some(provider);
                        spec.env = env;
                    }
                    None => {
                        spec.command = if self.provider_command.is_empty() {
                            crate::turn::echo_command()
                        } else {
                            self.provider_command.clone()
                        };
                    }
                }
                crate::turn::launch(self, run, spec, cancel).await
            }),
        );
    }
    fn close_guest_thread(&self, thread_id: &str) -> Result<(), String> {
        self.guest.close_thread(thread_id).map_err(str::to_owned)
    }
    fn resolve_guest_message(
        &self,
        thread_id: &str,
        message_id: &str,
        outcome: &str,
    ) -> Result<(), String> {
        self.guest
            .resolve_message(thread_id, message_id, outcome)
            .map_err(|error| error.to_string())
    }
}

impl crate::turn::TurnHost for AppState {
    fn workspace(&self) -> &dyn crate::workspace::Workspace {
        &*self.workspace
    }
    fn cancellations(&self) -> &Mutex<HashMap<Uuid, oneshot::Sender<()>>> {
        &self.cancellations
    }
    fn stopping(&self) -> bool {
        self.stopping.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// Record a restart's reconciliation of every thread's provider runtime
/// ([`crate::runtime_recovery::reconcile`]).
async fn reconcile_provider_runtime(control: &crate::control_state::ControlState, trigger: &str) {
    let events = {
        let store = control.store.read().await;
        crate::runtime_recovery::reconcile(
            &store,
            trigger,
            crate::now(),
            &auth_service::iso(auth_service::now()),
        )
    };
    for event in events {
        if let Err(error) = control.commit(event).await {
            tracing::error!(%error, trigger, "could not record a provider runtime reconciliation");
        }
    }
}

impl std::ops::Deref for AppState {
    type Target = crate::control_state::ControlState;
    fn deref(&self) -> &Self::Target {
        &self.control
    }
}

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .merge(provider_http::routes())
        .route("/api/projects", get(projects::snapshot))
        .route("/api/projects/mutate", post(projects::mutate_http))
        .route("/api/observability/v1/traces", post(otlp_traces_proxy))
        .route(
            "/api/orchestration/shell",
            get(environment_http::shell_snapshot),
        )
        .route(
            "/api/orchestration/threads/:thread_id",
            get(environment_http::thread_snapshot),
        )
        .route(
            "/api/orchestration/threads/:thread_id/transcript",
            get(environment_http::thread_snapshot),
        )
        .route(
            "/api/orchestration/threads/:thread_id/bounded",
            get(environment_http::thread_snapshot),
        )
        .route(
            "/api/orchestration/threads/:thread_id/history",
            get(environment_http::thread_snapshot),
        )
        .route_layer(axum::middleware::from_fn_with_state(
            state.clone(),
            require_session,
        ))
        // Hook credentials are carried in the URL. The public webhook route
        // must bypass the environment session middleware above.
        .route("/api/hooks/:hook_id/:token", any(webhook_handler))
        .merge(asset_http::routes())
        .merge(mcp_oauth::routes())
        .merge(access_http::routes())
        .merge(host_rpc::routes())
        .route(
            "/.well-known/t3/environment",
            get(environment_http::descriptor),
        )
        .route("/ws", get(websocket))
        .fallback(any(contract_fallback))
        .layer(axum::middleware::from_fn(access_http::contextualize))
        // Outermost, like the reference's global CORS middleware: preflights
        // never reach a route and every response, errors included, is tagged.
        .layer(axum::middleware::from_fn(cors::browser_api))
        .with_state(state)
}

pub(crate) async fn otlp_traces_proxy(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Response {
    let session = match access_http::principal(&state, &headers) {
        Ok(session) => session,
        Err(reason) => return auth_error(&headers, reason),
    };
    if !session.has("orchestration:operate") {
        return access_http::scope_error(&headers, "orchestration:operate");
    }
    if serde_json::from_slice::<serde_json::Value>(&body).is_err() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let exporter_disabled = std::env::var("OTEL_TRACES_EXPORTER")
        .ok()
        .map(|value| {
            value
                .split(',')
                .any(|item| item.trim().eq_ignore_ascii_case("none"))
        })
        .unwrap_or(false)
        && !std::env::var("OTEL_TRACES_EXPORTER")
            .ok()
            .map(|value| {
                value
                    .split(',')
                    .any(|item| item.trim().eq_ignore_ascii_case("otlp"))
            })
            .unwrap_or(false);
    let endpoint = if exporter_disabled {
        None
    } else {
        let explicit = std::env::var("T3CODE_OTLP_TRACES_URL")
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .or_else(|| {
                std::env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
                    .ok()
                    .map(|value| value.trim().to_owned())
                    .filter(|value| !value.is_empty())
            })
            .or_else(|| {
                state
                    .settings
                    .internal()
                    .ok()
                    .and_then(|settings| {
                        settings["observability"]["otlpTracesUrl"]
                            .as_str()
                            .map(str::to_owned)
                    })
                    .filter(|value| !value.trim().is_empty())
            });
        let generic = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .and_then(|value| {
                let mut parsed = url::Url::parse(&value).ok()?;
                if !matches!(parsed.scheme(), "http" | "https") {
                    return None;
                }
                parsed.set_path(&format!(
                    "{}/v1/traces",
                    parsed.path().trim_end_matches('/')
                ));
                Some(parsed.to_string())
            });
        explicit.or(generic)
    };
    let Some(endpoint) = endpoint else {
        return StatusCode::NO_CONTENT.into_response();
    };
    let mut request = reqwest::Client::new()
        .post(endpoint)
        .header("content-type", "application/json");
    let headers_value = std::env::var("OTEL_EXPORTER_OTLP_TRACES_HEADERS")
        .ok()
        .or_else(|| std::env::var("OTEL_EXPORTER_OTLP_HEADERS").ok());
    if let Some(headers_value) = headers_value {
        for pair in headers_value.split(',') {
            let Some((name, value)) = pair.split_once('=') else {
                continue;
            };
            let Ok(value) = percent_encoding::percent_decode_str(value).decode_utf8() else {
                continue;
            };
            if let (Ok(name), Ok(value)) = (
                reqwest::header::HeaderName::from_bytes(name.trim().as_bytes()),
                reqwest::header::HeaderValue::from_str(value.trim()),
            ) {
                request = request.header(name, value);
            }
        }
    }
    let response = request.body(body).send().await;
    match response {
        Ok(response) if response.status().is_success() => StatusCode::NO_CONTENT.into_response(),
        _ => (StatusCode::BAD_GATEWAY, "Trace export failed.").into_response(),
    }
}

pub(crate) async fn webhook_handler(
    State(state): State<Arc<AppState>>,
    Path((hook_id, token)): Path<(String, String)>,
    request: Request,
) -> Response {
    let outcome = |status: StatusCode, body: serde_json::Value, name: &'static str| {
        let mut response = (status, Json(body)).into_response();
        response.headers_mut().insert(
            "x-t3-hook-outcome",
            axum::http::HeaderValue::from_static(name),
        );
        response
    };
    if request.method() == axum::http::Method::HEAD {
        let mut response = outcome(
            StatusCode::METHOD_NOT_ALLOWED,
            serde_json::json!({"error":"method_not_allowed"}),
            "method_not_allowed",
        );
        response.headers_mut().insert(
            axum::http::header::ALLOW,
            axum::http::HeaderValue::from_static("GET, POST, PUT, PATCH"),
        );
        return response;
    }
    let headers = request.headers().clone();
    let method = request.method().as_str().to_owned();
    let query = request.uri().query().map(str::to_owned).unwrap_or_default();
    let body = match axum::body::to_bytes(request.into_body(), 1024 * 1024 + 1).await {
        Ok(bytes) if bytes.len() <= 1024 * 1024 => bytes,
        _ => {
            return outcome(
                StatusCode::PAYLOAD_TOO_LARGE,
                serde_json::json!({"error":"body_too_large"}),
                "body_too_large",
            )
        }
    };
    let tasks = state.scheduled_tasks.lock().await;
    let task = tasks
        .values()
        .find(|task| {
            task["schedule"]["type"] == "webhook"
                && task["id"].as_str().is_some_and(|id| id == hook_id)
                && task["_webhookToken"].as_str() == Some(token.as_str())
        })
        .cloned();
    let Some(task) = task else {
        return outcome(
            StatusCode::NOT_FOUND,
            serde_json::json!({"error":"hook_not_found"}),
            "not_found",
        );
    };
    let delivery_id = format!("delivery:{}", uuid::Uuid::new_v4());
    let headers_json = headers
        .iter()
        .filter_map(|(name, value)| {
            Some((
                name.as_str().to_ascii_lowercase(),
                json!(value.to_str().ok()?.to_owned()),
            ))
        })
        .collect::<serde_json::Map<_, _>>();
    let content_type = headers_json
        .get("content-type")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let record_delivery = |outcome_name: &str, signature_verified: bool, error: Option<String>| {
        let state = state.clone();
        let delivery_id = delivery_id.clone();
        let hook_id = hook_id.clone();
        let method = method.clone();
        let query = query.clone();
        let headers_json = headers_json.clone();
        let content_type = content_type.clone();
        let outcome_name = outcome_name.to_owned();
        let delivery_key = delivery_id.clone();
        let delivery = json!({
            "id":delivery_id,
            "taskId":hook_id,
            "receivedAt":crate::auth_service::iso(crate::auth_service::now()),
            "method":method,
            "query":query,
            "headers":headers_json,
            "body":String::from_utf8_lossy(&body),
            "bodyBytes":body.len(),
            "bodyTruncated":false,
            "outcome":outcome_name,
            "signatureVerified":signature_verified,
            "pendingWindow":false,
            "missingFields":[],
            "error":error,
            "renderedPrompt":null,
            "contentType":content_type
        });
        async move {
            state
                .scheduled_webhook_deliveries
                .lock()
                .await
                .insert(delivery_key, delivery);
            let _ = state.persist_scheduled_webhook_deliveries().await;
        }
    };
    if task["enabled"] != true {
        record_delivery("disabled", false, None).await;
        return outcome(
            StatusCode::CONFLICT,
            serde_json::json!({"error":"hook_disabled"}),
            "disabled",
        );
    }
    if let Some(signature) = task["schedule"]["signature"].as_object() {
        let valid =
            webhook_signature_valid(signature, task["_webhookSecret"].as_str(), &headers, &body);
        if !valid {
            record_delivery("rejected_signature", false, None).await;
            return outcome(
                StatusCode::UNAUTHORIZED,
                serde_json::json!({"error":"invalid_signature"}),
                "rejected_signature",
            );
        }
    }
    record_delivery("accepted", task["schedule"]["signature"].is_object(), None).await;
    let _ = body;
    outcome(
        StatusCode::ACCEPTED,
        serde_json::json!({"deliveryId":delivery_id}),
        "accepted",
    )
}

pub(crate) fn webhook_signature_valid(
    signature: &serde_json::Map<String, serde_json::Value>,
    secret: Option<&str>,
    headers: &HeaderMap,
    body: &[u8],
) -> bool {
    let Some(secret) = secret else { return false };
    let Some(header) = signature["header"].as_str() else {
        return false;
    };
    let Some(received) = headers.get(header).and_then(|v| v.to_str().ok()) else {
        return false;
    };
    let prefix = signature["prefix"].as_str().unwrap_or_default();
    let value = received.trim();
    if value.len() < prefix.len() {
        return false;
    }
    let Some(head) = value.get(..prefix.len()) else {
        return false;
    };
    if !head.eq_ignore_ascii_case(prefix) {
        return false;
    }
    let candidate = &value[prefix.len()..];
    let decoded = match signature["encoding"].as_str().unwrap_or("base64") {
        "hex" => hex::decode(candidate.to_ascii_lowercase()).ok(),
        _ => BASE64.decode(candidate).ok(),
    };
    let Some(decoded) = decoded else { return false };
    let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(secret.as_bytes()) else {
        return false;
    };
    mac.update(body);
    mac.verify_slice(&decoded).is_ok()
}

pub(crate) async fn contract_fallback(
    State(state): State<Arc<AppState>>,
    request: Request,
) -> Response {
    let method = request.method().as_str();
    let path = request.uri().path();
    if HTTP_CONTRACTS
        .iter()
        .any(|c| c.environment_auth && c.method == method && contracts::path_matches(c.path, path))
    {
        if let Err(reason) = access_http::authenticate(&state, request.headers()).await {
            return auth_error(request.headers(), reason);
        }
    }
    let (status, code, message) = if HTTP_CONTRACTS
        .iter()
        .any(|c| !c.relay && c.method == method && contracts::path_matches(c.path, path))
    {
        (
            StatusCode::NOT_IMPLEMENTED,
            "contract_not_implemented",
            "This contract endpoint is registered but its provider integration is unavailable.",
        )
    } else {
        return static_http::client(
            state.runtime_config.static_dir.as_deref(),
            request.method(),
            path,
        )
        .await;
    };
    let body = serde_json::json!({ "code": code, "message": message });
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .header("access-control-allow-origin", "*")
        .body(body.to_string().into())
        .expect("valid response")
}
pub(crate) async fn require_session(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: axum::middleware::Next,
) -> Response {
    use auth_service::permissions::{http_scopes, project_scopes};
    let session = match access_http::principal(&state, request.headers()) {
        Ok(session) => session,
        Err(e) => return auth_error(request.headers(), e),
    };
    for scope in http_scopes(request.uri().path(), request.method().as_str()) {
        if !session.has(scope) {
            return access_http::scope_error(request.headers(), scope);
        }
    }
    if request.uri().path() == "/api/projects/mutate" {
        let (parts, body) = request.into_parts();
        let bytes = match axum::body::to_bytes(body, 2 * 1024 * 1024).await {
            Ok(bytes) => bytes,
            Err(_) => return StatusCode::PAYLOAD_TOO_LARGE.into_response(),
        };
        if let Ok(body) = serde_json::from_slice(&bytes) {
            if let Some(scope) = project_scopes(&body)
                .into_iter()
                .find(|scope| !session.has(scope))
            {
                return access_http::scope_error(&parts.headers, scope);
            }
        }
        return next
            .run(Request::from_parts(parts, axum::body::Body::from(bytes)))
            .await;
    }
    next.run(request).await
}

pub async fn authorize_upgrade(
    state: &AppState,
    headers: &HeaderMap,
    uri: &axum::http::Uri,
    audience: &str,
) -> Result<auth_service::Session, &'static str> {
    let ticket = url::form_urlencoded::parse(uri.query().unwrap_or("").as_bytes())
        .filter(|(key, _)| key == "wsTicket")
        .last()
        .map(|(_, value)| value);
    if let Some(ticket) = ticket.filter(|v| !v.trim().is_empty()) {
        access_http::authenticate_ticket(state, headers, uri, &ticket, audience)
    } else {
        access_http::principal(state, headers)
    }
}

pub(crate) async fn websocket(State(state): State<Arc<AppState>>, request: Request) -> Response {
    let query = url::form_urlencoded::parse(request.uri().query().unwrap_or("").as_bytes());
    let references = query
        .clone()
        .find(|(k, _)| k == "clientCapabilities")
        .is_some_and(|(_, v)| v.split(',').any(|c| c == "settingsSecretReferences"));
    // The reference server checks only that the parameter is present.
    let frame_parts = query
        .clone()
        .any(|(key, _)| key == rpc::SOCKET_FRAME_PARTS_QUERY_PARAM);
    let compatible = query
        .clone()
        .find(|(key, _)| key == "orchestrationProtocol")
        .is_some_and(|(_, value)| value == "2");
    if !compatible {
        let body = serde_json::json!({
            "code": "orchestration_protocol_incompatible",
            "message": "Update this client to one that supports orchestration protocol 2.",
            "orchestrationProtocolVersion": 2
        });
        return Response::builder()
            .status(StatusCode::UPGRADE_REQUIRED)
            .header("content-type", "application/json")
            .header("vary", "Accept-Encoding")
            .header("access-control-allow-origin", "*")
            .body(body.to_string().into())
            .expect("valid response");
    }
    let auth = authorize_upgrade(&state, request.headers(), request.uri(), "rpc").await;
    let principal = match auth {
        Ok(s) => s,
        Err(reason) => return auth_error(request.headers(), reason),
    };
    match WebSocketUpgrade::from_request(request, &state).await {
        Ok(upgrade) => upgrade
            .on_upgrade(move |socket| {
                rpc::session(state, socket, principal, references, frame_parts)
            })
            .into_response(),
        Err(rejection) => rejection.into_response(),
    }
}
pub async fn run_server(state: Arc<AppState>, listen: std::net::SocketAddr) -> anyhow::Result<()> {
    run_server_owned(state, listen, None).await
}

/// [`run_server`] for a process that holds the state directory's ownership
/// lock: the runtime record is published once listening and cleared on
/// SIGINT/SIGTERM.
pub async fn run_server_owned(
    state: Arc<AppState>,
    listen: std::net::SocketAddr,
    ownership: Option<server_runtime::ServerOwnership>,
) -> anyhow::Result<()> {
    let policy = &state.settings.policy;
    let client = if !listen.ip().is_loopback() {
        "remote"
    } else if state.auth.desktop() {
        "desktop"
    } else {
        "local-browser"
    };
    if !policy.allows_client(client) {
        let action = match client {
            "remote" => "remote clients",
            "desktop" => "the desktop app",
            _ => "browsers on this machine",
        };
        let refusal = policy.denial(action);
        if client == "remote" {
            anyhow::bail!(
                "{}, so the server cannot listen on {}.",
                refusal.trim_end_matches('.'),
                listen.ip()
            );
        }
        anyhow::bail!(refusal);
    }
    if !policy.allows_client("remote") && state.runtime_config.tailscale_serve == Some(true) {
        anyhow::bail!(
            "{}, so Tailscale Serve cannot be turned on.",
            policy.denial("remote clients").trim_end_matches('.')
        );
    }
    policy.close_tailscale_routes(listen).await?;
    let listener = tokio::net::TcpListener::bind(listen).await?;
    serve_bound(
        state,
        listener,
        ownership,
        server_runtime::shutdown_signal(),
    )
    .await
}

/// How long a graceful shutdown waits for open connections before the
/// process exits anyway.
const SHUTDOWN_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

/// Serve on an already bound listener until `shutdown` resolves. When this
/// process owns the state directory, the runtime record is published with
/// the listener's real port (so port 0 records the chosen one) and is
/// cleared again, if still ours, when serving stops.
pub async fn serve_bound<F>(
    state: Arc<AppState>,
    listener: tokio::net::TcpListener,
    ownership: Option<server_runtime::ServerOwnership>,
    shutdown: F,
) -> anyhow::Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    use std::future::IntoFuture;
    let bound = listener.local_addr()?;
    info!(listen = %bound, "t3code server listening");
    if let Some(owner) = &ownership {
        let record = server_runtime::RuntimeRecord::new(
            state.runtime_config.requested_host,
            bound.port(),
            Some(state.settings.policy.loaded()),
        );
        if let Err(error) = owner.publish(record) {
            tracing::warn!(
                path = %owner.state_path().display(),
                %error,
                "failed to persist server runtime state"
            );
        }
    }
    // Refresh the model manifest now and then on the provider-check cadence;
    // each refresh is TTL-gated, so most ticks read nothing.
    let manifest = state.model_manifest.clone();
    let manifest_refresh = tokio::spawn(async move {
        let period =
            std::time::Duration::from_millis(crate::model_manifest::MANIFEST_RETRY_MS as u64);
        loop {
            manifest.refresh().await;
            tokio::time::sleep(period).await;
        }
    });
    // T3 Connect: run the stored managed tunnel's relay client while this server listens.
    let policy = &state.settings.policy;
    let endpoint = crate::managed_endpoint::ManagedEndpointRuntime::open(
        crate::managed_endpoint::RuntimeOptions::for_paths(
            &state.paths,
            policy.allows_connect() && policy.allows_client("remote"),
        ),
    )
    .await;
    // `server.ts`'s cloud link fiber: register (or provision) the tunnel with T3 Connect,
    // serve recovery requests, and release a CLI-provisioned tunnel on shutdown.
    let cloud_link = policy.allows_connect().then(|| {
        let descriptor_state = state.clone();
        Arc::new(crate::cloud_link::CloudLink::new(
            endpoint.clone(),
            crate::cloud_link::CloudLinkOptions {
                environment_id: state.environment_id.clone(),
                public: crate::cloud_link::PublicConfig::from_env(),
                descriptor: Arc::new(move || {
                    let state = descriptor_state.clone();
                    Box::pin(async move {
                        crate::environment_http::descriptor(axum::extract::State(state))
                            .await
                            .0
                    })
                }),
                timings: crate::cloud_link::Timings::default(),
            },
        ))
    });
    let endpoint_startup = cloud_link.clone().map(|cloud_link| {
        let origin = crate::managed_endpoint::LocalOrigin::for_address(bound);
        tokio::spawn(async move {
            let recoveries = cloud_link.serve_recovery_requests(origin.clone());
            let startup = cloud_link.run_startup(&origin);
            tokio::join!(recoveries, startup);
        })
    });
    // Continue what the last restart cut off, now that clients can watch it.
    tokio::spawn(state.clone().continue_restarted_runs());
    let (stopping_tx, stopping_rx) = oneshot::channel::<()>();
    let stopping_endpoint = endpoint.clone();
    let stopping_state = state.clone();
    let server = axum::serve(
        listener,
        router(state).into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        shutdown.await;
        info!("shutting down t3code server");
        stopping_state.stop_provider_runtime().await;
        stopping_endpoint.begin_shutdown();
        let _ = stopping_tx.send(());
    });
    let grace = async move {
        match stopping_rx.await {
            Ok(()) => tokio::time::sleep(SHUTDOWN_GRACE).await,
            Err(_) => std::future::pending().await,
        }
    };
    let result = tokio::select! {
        result = server.into_future() => result.map_err(anyhow::Error::from),
        () = grace => {
            tracing::warn!("open connections did not close within the shutdown grace period");
            Ok(())
        }
    };
    manifest_refresh.abort();
    if let Some(startup) = endpoint_startup {
        startup.abort();
    }
    if let Some(cloud_link) = &cloud_link {
        endpoint.begin_shutdown();
        cloud_link.release_on_shutdown_logged().await;
    }
    endpoint.shutdown().await;
    drop(ownership);
    result
}
pub fn default_data_dir() -> PathBuf {
    std::env::var_os("T3CODE_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| config::default_base_dir().join("userdata"))
}
pub fn default_provider_command() -> Vec<String> {
    std::env::var("T3_PROVIDER_COMMAND")
        .ok()
        .map(|s| s.split_whitespace().map(str::to_owned).collect())
        .unwrap_or_else(crate::turn::echo_command)
}
pub fn ensure_path(path: &str) -> anyhow::Result<()> {
    let p = FsPath::new(path);
    if !p.exists() {
        anyhow::bail!("working directory does not exist: {path}");
    }
    Ok(())
}

impl access_http::AuthHost for AppState {
    fn auth(&self) -> &auth_service::AuthService {
        &self.auth
    }
}
