//! What a server restart does to runs that were running.
use serde_json::json;
use t3code_server::{
    auth_service::AuthService,
    config::{RuntimeConfig, ServerPaths},
    AppState, Event, Run,
};

async fn open(paths: &ServerPaths) -> std::sync::Arc<AppState> {
    AppState::with_config(
        &paths.state_dir,
        Vec::new(),
        AuthService::new(),
        RuntimeConfig::default(),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn a_restart_ends_runs_it_cut_off() {
    let home = tempfile::tempdir().unwrap();
    let paths = ServerPaths::from_base(home.path());
    std::fs::create_dir_all(&paths.state_dir).unwrap();
    let run: Run = serde_json::from_value(json!({
        "id": "6f1c1a52-8c4a-4d3e-9a0e-2b8f3c1d7e01", "thread_id": "t1", "provider": "default",
        "status": "running", "started_at": 1, "finished_at": null, "error": null
    }))
    .unwrap();
    {
        let state = open(&paths).await;
        state
            .commit_now(Event::ThreadCreated(
                serde_json::from_value(json!({
                    "id": "t1", "project_id": "p1", "title": "One", "cwd": "/tmp",
                    "status": "idle", "messages": [], "runs": [], "created_at": 1,
                    "archived": false, "settled": false, "snoozed_until": null,
                    "pinned": false, "unread": false, "tags": []
                }))
                .unwrap(),
            ))
            .unwrap();
        state.commit_now(Event::RunStarted(run.clone())).unwrap();
    }
    let state = open(&paths).await;
    let store = state.store_now();
    let thread = &store.threads["t1"];
    let ended = &thread.runs[0];
    // As the reference's startup reconciliation ends them: cancelled, with a
    // notice that the server restarted.
    assert_eq!(ended.status, "cancelled");
    let cut = ended.restart_cut.as_ref().unwrap();
    assert_eq!(cut.trigger, "startup");
    // It ran no provider turn, so there is nothing to continue.
    assert_eq!(cut.continuation, None);
    assert_eq!(ended.error, None);
}
