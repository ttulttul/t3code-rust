#!/usr/bin/env python3
"""Open a seeded thread with the pinned reference client against both servers.

Each server runs in its own temporary home with two projects on git checkouts
whose remotes are on Bitbucket and GitHub, and one thread that has had a user
message dispatched. The Node driver (client_thread.mjs) uses the reference
client-runtime, so every reply is decoded with the client's own schemas, and
records:

- `orchestration.subscribeThread` up to `synchronized`, with the options the
  0.1.10 client sends and without them, and for a missing thread;
- `subscribeWorktreeSetup` and `worktreeSetupCancel` for the thread;
- each project's repository identity on the shell subscription;
- `pullRequests.list` for each project, without host credentials.

Each step must end the same way on both servers. Decoded values are compared
by shape (every key path and value type) and, where the value does not depend
on time or generated ids, exactly.

Then each provider turn scenario (PROVIDER_SCENARIOS) runs on a fresh pair of
servers with fake `codex` and `claude` executables on PATH. They replay the
recorded protocol sessions in tests/fixtures/providers, so no model is called.
The driver subscribes to the thread, dispatches a message, and keeps what
streams in until the run settles; the turn's projection is then compared the
same way, along with the turn as the client renders it: items in order with
their types, statuses and text, the messages, and the run, attempt, provider
turn, provider thread and provider session states. Scenarios with runtime
requests set the thread's runtime mode and answer (`respond`) or interrupt the
first request once its card shows; the `cli approvals` check compares what each
server told the fake CLI: the runtime policy of its thread and turn, and its
answer to each request.

Restart scenarios (those with "restart") stop the server the way a service
manager does (SIGTERM) once the turn is running or has settled, change what
the scenario says (settings, or the CLI removed), start the server again on the
same home, wait for whatever it continues to settle, and then compare the
thread: how the cut-off run ended, the notice and the continuation run, the
provider session, and each turn launch of the CLI (`cli launches`: a new
session or a resumed one). Known differences are listed
in client_thread_parity_xfail.json; an entry that starts matching fails the
suite until it is removed.
"""
import argparse
import contextlib
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import subprocess
import tempfile
import time

from client_initial_sync_parity import rpc
from parity_harness import PROVIDER_CLIS, RELAY_TOOLS, encode, http, isolated_git_env, managed_server, path_without

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests/fixtures/providers"
# Each scenario: the thread's provider instance and model, and the recorded session its CLI replays.
PROVIDER_SCENARIOS = [
    {"name": "codex turn", "instance": "codex", "model": "fixture", "scenario": "codex_turn.json"},
    {"name": "codex failed turn", "instance": "codex", "model": "fixture", "scenario": "codex_failed.json"},
    # Runtime requests: what each server asks the user, and what it answers the CLI.
    {"name": "codex approval", "instance": "codex", "model": "fixture", "scenario": "codex_approval.json",
     "respond": {"decision": "accept"}},
    {"name": "codex approval for session", "instance": "codex", "model": "fixture",
     "scenario": "codex_approval.json", "respond": {"decision": "acceptForSession"}},
    {"name": "codex approval declined", "instance": "codex", "model": "fixture", "scenario": "codex_approval.json",
     "respond": {"decision": "decline"}},
    {"name": "codex approval interrupted", "instance": "codex", "model": "fixture",
     "scenario": "codex_approval_interrupt.json", "interrupt": "request"},
    {"name": "codex approval-required", "instance": "codex", "model": "fixture", "scenario": "codex_approval.json",
     "runtimeMode": "approval-required", "respond": {"decision": "accept"}},
    {"name": "codex auto-accept-edits", "instance": "codex", "model": "fixture", "scenario": "codex_approval.json",
     "runtimeMode": "auto-accept-edits", "respond": {"decision": "decline"}},
    {"name": "codex user input", "instance": "codex", "model": "fixture", "scenario": "codex_user_input.json",
     "respond": {"answers": {"color": "Blue"}}},
    {"name": "codex crash", "instance": "codex", "model": "fixture", "scenario": "codex_crash.json"},
    {"name": "codex missing", "instance": "codex", "model": "fixture", "scenario": None},
    {"name": "codex interrupt", "instance": "codex", "model": "fixture", "scenario": "codex_interrupt.json",
     "interrupt": True},
    {"name": "claude turn", "instance": "claudeAgent", "model": "claude-opus-5-5", "scenario": "claude_turn.json"},
    {"name": "claude crash", "instance": "claudeAgent", "model": "claude-opus-5-5", "scenario": "claude_crash.json"},
    {"name": "claude missing", "instance": "claudeAgent", "model": "claude-opus-5-5", "scenario": None},
    {"name": "claude interrupt", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_interrupt.json", "interrupt": True},
    {"name": "claude approval", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_approval.json", "respond": {"decision": "accept"}},
    {"name": "claude approval declined", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_approval.json", "respond": {"decision": "decline"}},
    {"name": "claude approval interrupted", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_approval_interrupt.json", "interrupt": "request"},
    {"name": "claude question", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_question.json", "respond": {"answers": {"Which color?": "Blue"}}},
    {"name": "claude approval-required", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_approval.json", "runtimeMode": "approval-required", "respond": {"decision": "acceptForSession"}},
    {"name": "claude auto-accept-edits", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_approval.json", "runtimeMode": "auto-accept-edits", "respond": {"decision": "cancel"}},
    # Server restarts: the reference's ProviderRuntimeRecoveryService and restart continuation.
    {"name": "claude restart mid-turn", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_restart.json", "restart": {"after": "running"}},
    {"name": "claude restart idle", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_restart_idle.json", "restart": {"after": "settled"}},
    {"name": "claude restart continuation off", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_restart.json",
     "restart": {"after": "running", "settings": {"continueThreadsAfterServerUpdate": False}}},
    {"name": "claude restart instance disabled", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_restart.json", "restart": {"after": "running", "settings": {
         "providerInstances": {"claudeAgent": {"driver": "claudeAgent", "enabled": False}}}}},
    {"name": "claude restart cli missing", "instance": "claudeAgent", "model": "claude-opus-5-5",
     "scenario": "claude_restart.json", "restart": {"after": "running", "removeCli": True}},
]
# The fake CLIs: version probes answer as a supported release would, and protocol sessions replay
# the scenario. Anything else exits quietly.
FAKE_CODEX = """#!/bin/sh
case "$1 $2" in
  "--version "*) echo "codex-cli 0.170.0"; exit 0;;
  "app-server --help") echo "Usage: codex app-server"; exit 0;;
  "app-server "*) exec python3 "$FAKE_PROVIDER_CLI" "$@";;
esac
exit 0
"""
FAKE_CLAUDE = """#!/bin/sh
case "$*" in
  --version*) echo "2.1.200 (Claude Code)"; exit 0;;
  *--output-format*stream-json*) exec python3 "$FAKE_PROVIDER_CLI" "$@";;
esac
exit 0
"""
THREAD = "thread:client-thread"
REMOTES = {
    "project:bitbucket": ("Bitbucket checkout", "git@ssh.bitbucket.org:example-org/example-repo.git"),
    "project:github": ("GitHub checkout", "https://github.com/Example-Org/Example.Repo.git"),
}
GENERATED = re.compile(r"^(\d{4}-\d\d-\d\dT[\d:.]+Z|[0-9a-f]{40}|[0-9a-f-]{36}|.*:[0-9a-f-]{36}.*)$")


# Set in main: the Git configuration both servers and the seeded checkouts use.
GIT_ISOLATION = {}


def git(*args, cwd=None):
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, env={**os.environ, **GIT_ISOLATION})


def make_checkout(path, remote):
    git("init", "-qb", "main", str(path))
    git("-C", str(path), "remote", "add", "origin", remote)
    (path / "README.md").write_text("fixture\n")
    git("-C", str(path), "add", "README.md")
    git("-C", str(path), "-c", "user.email=test@example.test", "-c", "user.name=Parity", "commit", "-qm", "Fixture")


def seed(url, cookie, root):
    headers = {"Cookie": cookie, "Content-Type": "application/json"}
    for project_id, (title, remote) in REMOTES.items():
        checkout = root / project_id.split(":")[1]
        make_checkout(checkout, remote)
        status, _, body = http(url, "POST", "/api/projects/mutate", encode({
            "type": "project.create", "commandId": f"create-{project_id}", "projectId": project_id,
            "title": title, "workspaceRoot": str(checkout)}), headers)
        if status != 200:
            raise RuntimeError(f"project.create failed on {url}: HTTP {status} {body[:400]!r}")
    rpc(url, cookie, "orchestration.dispatchCommand", {
        "type": "thread.create", "commandId": "seed-thread", "threadId": THREAD, "projectId": "project:bitbucket",
        "title": "Thread", "modelSelection": {"instanceId": "codex", "model": "fixture"},
        "runtimeMode": "full-access", "interactionMode": "default", "branch": None, "worktreePath": None,
        "createdBy": "user", "creationSource": "web"})
    # No provider is installed in the sandbox, so the turn ends as soon as it starts.
    rpc(url, cookie, "orchestration.dispatchCommand", {
        "type": "message.dispatch", "commandId": "seed-message", "threadId": THREAD,
        "messageId": "message:seed", "text": "Hello from the parity fixture", "attachments": [],
        "createdBy": "user", "creationSource": "web", "dispatchMode": {"type": "start_immediately"}})
    return {"threadId": THREAD, "projectIds": list(REMOTES), "hosts": ["ssh.bitbucket.org", "github.com"]}


def run_client(args, url, cookie, seeded):
    env = {**os.environ, "T3_CLIENT_SYNC_COOKIE": cookie, "T3_CLIENT_THREAD_SEED": json.dumps(seeded),
           "T3_CLIENT_THREAD_WAIT_MS": str(args.reply_ms)}
    result = subprocess.run([args.node, str(ROOT / "scripts/client_thread.mjs"), args.reference_root, url],
                            capture_output=True, text=True, env=env, timeout=args.reply_ms / 1000 + 120)
    lines = [line for line in result.stdout.splitlines() if line.startswith("{")]
    if not lines:
        raise RuntimeError(f"client driver produced no result (exit {result.returncode}):\n{result.stderr[-4000:]}")
    return json.loads(lines[-1])


def shape(value, path="$", out=None):
    """Every key path with its value type; array elements share one path."""
    out = {} if out is None else out
    if isinstance(value, dict):
        out.setdefault(path, set()).add("object")
        for key, item in value.items():
            shape(item, f"{path}.{key}", out)
    elif isinstance(value, list):
        out.setdefault(path, set()).add("array")
        for item in value:
            shape(item, f"{path}[]", out)
    else:
        out.setdefault(path, set()).add(type(value).__name__)
    return out


def stable(value, root):
    """The value with times, generated ids and the temporary home masked."""
    if isinstance(value, dict):
        return {key: stable(item, root) for key, item in value.items()}
    if isinstance(value, list):
        return [stable(item, root) for item in value]
    if isinstance(value, str):
        value = value.replace(str(root), "<home>")
        return "<generated>" if GENERATED.match(value) else value
    return value


def variants(projection):
    """Each projection record by collection and, for turn items, by type."""
    out = {"thread": [projection.get("thread")]}
    for key, value in projection.items():
        if not isinstance(value, list):
            continue
        for record in value:
            if key == "turnItems":
                out.setdefault(f"turnItems:{record.get('type')}", []).append(record)
            elif key == "visibleTurnItems":
                item = record.get("item", {})
                row = {k: v for k, v in record.items() if k != "item"}
                out.setdefault(f"visibleTurnItems:{item.get('type')}", []).append(row)
                out.setdefault(f"visibleTurnItems.item:{item.get('type')}", []).append(item)
            else:
                out.setdefault(key, []).append(record)
    return out


def comparable_shape(records):
    """Key paths with value types, where a null matches any type."""
    merged = {}
    for record in records:
        for path, types in shape(record).items():
            merged.setdefault(path, set()).update(types)
    return merged


# Fields a client shows that do not depend on time, generated ids or how far the run got.
STABLE_FIELDS = {
    "messages": ["role", "text", "attachments", "streaming", "createdBy", "creationSource"],
    "turnItems:user_message": ["text", "textOmitted", "inputIntent", "attachments", "createdBy",
                               "creationSource", "status", "title"],
    "visibleTurnItems:user_message": ["position", "visibility"],
    "visibleTurnItems:error": ["position", "visibility"],
    "turnItems:error": ["status"],
    "runs": ["status", "ordinal", "queuePosition"],
    "thread": ["title", "projectId", "runtimeMode", "interactionMode", "modelSelection", "branch", "worktreePath"],
}
STABLE_FIELDS["visibleTurnItems.item:user_message"] = STABLE_FIELDS["turnItems:user_message"]


def compare_thread(original, rust):
    """Problems with Rust's thread stream as the reference client decodes it.

    The reference records its provider runtime's work in the projection (attempts, nodes,
    provider threads), which Rust does not model. Each kind of record both servers send must
    have the same fields and types; values are compared where they do not depend on timing.
    """
    problems = []
    kinds = [[item.get("kind") for item in side] for side in (original, rust)]
    if kinds[0] != kinds[1]:
        return [f"items {kinds[0]} != {kinds[1]}"]
    for left, right in zip(original, rust):
        if left.get("kind") != "snapshot":
            continue
        outer = [sorted(k for k in item if k != "projection") for item in (left, right)]
        if outer[0] != outer[1]:
            problems.append(f"snapshot fields {outer[0]} != {outer[1]}")
        for field in ("historyCursor", "hasMoreHistory", "turnItemsOmitLocalVisible", "idTable"):
            if left.get(field) != right.get(field):
                problems.append(f"snapshot {field}: original {left.get(field)!r} rust {right.get(field)!r}")
        projections = left["projection"], right["projection"]
        if sorted(projections[0]) != sorted(projections[1]):
            problems.append(f"projection fields {sorted(projections[0])} != {sorted(projections[1])}")
        ours, theirs = variants(projections[0]), variants(projections[1])
        for name in sorted(set(ours) & set(theirs)):
            a, b = comparable_shape(ours[name]), comparable_shape(theirs[name])
            for path in sorted(set(a) | set(b)):
                left_types, right_types = a.get(path, set()) - {"NoneType"}, b.get(path, set()) - {"NoneType"}
                if path not in a or path not in b or (left_types and right_types and left_types != right_types):
                    problems.append(f"{name} {path}: original {sorted(a.get(path, []))} rust {sorted(b.get(path, []))}")
            for field in STABLE_FIELDS.get(name, []):
                values = [[record.get(field) for record in side[name]] for side in (ours, theirs)]
                if values[0] != values[1]:
                    problems.append(f"{name}.{field}: original {values[0]!r} rust {values[1]!r}")
        for name in sorted(set(ours) - set(theirs)):
            if name.endswith(("user_message", "error")) or name in ("messages", "runs", "thread"):
                problems.append(f"rust sends no {name}")
    return problems


def write_fake_clis(directory, drivers):
    directory.mkdir(parents=True, exist_ok=True)
    for name, script in (("codex", FAKE_CODEX), ("claude", FAKE_CLAUDE)):
        if name in drivers:
            (directory / name).write_text(script)
            (directory / name).chmod(0o755)


def seed_turn(url, cookie, root, scenario):
    """A project on a git checkout and a thread on the scenario's provider, with the message to send."""
    checkout = root / "checkout"
    make_checkout(checkout, REMOTES["project:github"][1])
    status, _, body = http(url, "POST", "/api/projects/mutate", encode({
        "type": "project.create", "commandId": "create-project", "projectId": "project:github",
        "title": "Provider turn", "workspaceRoot": str(checkout)}),
        {"Cookie": cookie, "Content-Type": "application/json"})
    if status != 200:
        raise RuntimeError(f"project.create failed on {url}: HTTP {status} {body[:400]!r}")
    rpc(url, cookie, "orchestration.dispatchCommand", {
        "type": "thread.create", "commandId": "seed-thread", "threadId": THREAD, "projectId": "project:github",
        "title": "Thread", "modelSelection": {"instanceId": scenario["instance"], "model": scenario["model"]},
        "runtimeMode": scenario.get("runtimeMode", "full-access"), "interactionMode": "default", "branch": None,
        "worktreePath": None, "createdBy": "user", "creationSource": "web"})
    return {"threadId": THREAD, "turnMs": 30000, "interrupt": scenario.get("interrupt", False),
            "respond": scenario.get("respond"), "dispatch": {
        "type": "message.dispatch", "commandId": "turn-message", "threadId": THREAD,
        "messageId": "message:turn", "text": "Run echo hi and write notes.txt", "attachments": [],
        "createdBy": "user", "creationSource": "web", "dispatchMode": {"type": "start_immediately"}}}


def rendered_turn(projection):
    """The turn as a client renders it, without times and generated ids."""
    items = sorted(projection.get("turnItems", []), key=lambda item: item.get("ordinal", 0))
    def item(record):
        row = [record.get("type"), record.get("status")]
        for field in ("text", "title", "command", "toolName"):
            if record.get(field) not in (None, ""):
                row.append(f"{field}={str(record[field])[:80]}")
        if record.get("failure"):
            row.append("failure=" + str(record["failure"].get("message"))[:120])
        return " ".join(str(part) for part in row)
    return {
        "items": [item(record) for record in items],
        "messages": [f"{m.get('role')} {m.get('createdBy')}/{m.get('creationSource')}: {str(m.get('text'))[:80]}"
                     for m in projection.get("messages", [])],
        "runs": [run.get("status") for run in projection.get("runs", [])],
        "runtimeRequests": [f"{r.get('kind')} {r.get('status')} {r.get('decision')} {r.get('answers')}"
                            for r in projection.get("runtimeRequests", [])],
        "attempts": [attempt.get("status") for attempt in projection.get("attempts", [])],
        "providerTurns": [turn.get("status") for turn in projection.get("providerTurns", [])],
        "providerThreads": [thread.get("status") for thread in projection.get("providerThreads", [])],
        "providerSessions": [session.get("status") for session in projection.get("providerSessions", [])],
        "plans": [plan.get("kind") for plan in projection.get("plans", [])],
        # Token usage as the client's context meter and usage views read it, without times.
        "usage": [{"model": turn.get("model"), "turnTokenUsage": turn.get("turnTokenUsage"),
                   "tokenUsage": {k: v for k, v in (turn.get("tokenUsage") or {}).items()
                                  if k not in ("updatedAt", "promptCacheExpiresAt")} or None}
                  for turn in projection.get("providerTurns", [])],
        "nativeMetadata": [thread.get("nativeMetadata") for thread in projection.get("providerThreads", [])],
    }


def live_summary(live):
    """What streamed in after the dispatch: item kinds and event types in order, repeats collapsed."""
    out = []
    for item in (live or {}).get("items", []):
        if not item.get("dispatched"):
            continue
        label = item["kind"] if item["kind"] != "event" else f"event {item['event'].get('type')}"
        if item["kind"] == "event" and item["event"].get("type") == "turn-item.updated":
            label += f" {item['event'].get('payload', {}).get('type')}"
        if not out or out[-1] != label:
            out.append(label)
    return out


# Codex request params that carry the thread's runtime mode.
CODEX_POLICY_KEYS = ("approvalPolicy", "approvalsReviewer", "sandbox", "sandboxPolicy")
# Claude Code flags that carry it.
CLAUDE_POLICY_FLAGS = ("--permission-mode", "--permission-prompt-tool", "--allow-dangerously-skip-permissions")


def cli_approvals(log):
    """What a server told the fake CLI about approvals: the runtime policy it started the thread
    and turn with, and its answer to each request the CLI made (fixture request ids are strings
    such as "approval-1" or "perm-1")."""
    out = []
    args_path = Path(str(log) + ".args")
    for line in (args_path.read_text().splitlines() if args_path.exists() else []):
        words = line.split()
        for index, word in enumerate(words):
            name = word.split("=", 1)[0]
            if name in CLAUDE_POLICY_FLAGS:
                value = word.split("=", 1)[1] if "=" in word else (
                    words[index + 1] if index + 1 < len(words) and not words[index + 1].startswith("--") else True)
                out.append(f"arg {name}={value}")
    for line in (Path(log).read_text().splitlines() if Path(log).exists() else []):
        try:
            message = json.loads(line)
        except ValueError:
            continue
        method = message.get("method")
        if method in ("thread/start", "thread/resume", "turn/start"):
            params = message.get("params") or {}
            out.append(f"{method} " + json.dumps({k: params[k] for k in CODEX_POLICY_KEYS if k in params},
                                                 sort_keys=True))
        elif method is None and isinstance(message.get("id"), str) and ("result" in message or "error" in message):
            out.append(f"reply {message['id']} " + json.dumps(message.get("result", message.get("error")),
                                                              sort_keys=True))
        elif message.get("type") == "control_response":
            response = message.get("response") or {}
            if str(response.get("request_id", "")).startswith("perm-"):
                out.append(f"control_response {response['request_id']} "
                           + json.dumps(response.get("response", response.get("error")), sort_keys=True))
    return out


UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


def cli_launch(log, root):
    """How a server launched the fake CLI for the turn: its arguments, the environment it set,
    and the Codex initialize, thread and turn parameters, with generated ids and the server's
    temporary home masked. The reference's own agent MCP server (`mcp__t3-code__*`,
    `mcp_servers.t3-code*`) and the orchestration context that describes its tools are left out:
    Rust serves no agent MCP tools, so it does not offer them."""
    def mask(value):
        if isinstance(value, str):
            for server in ("original", "rust"):
                value = value.replace(f"{root}/{server}", "<home>/<server>")
            return UUID.sub("<uuid>", value.replace(str(root), "<home>"))
        if isinstance(value, list):
            return [mask(item) for item in value]
        if isinstance(value, dict):
            return {key: mask(item) for key, item in value.items()}
        return value
    out = []
    launch = Path(str(log) + ".launch")
    launches = []
    for line in (launch.read_text().splitlines() if launch.exists() else []):
        entry = json.loads(line)
        # Claude Code probes at startup, not for a turn.
        if "--no-session-persistence" in entry["argv"]:
            continue
        argv = [arg for arg in entry["argv"] if "mcp__t3-code" not in arg]
        env = {k: v for k, v in entry["env"].items() if k != "T3CODE_HOME"}
        launches.append(f"launch {json.dumps(mask(argv))} env {json.dumps(mask(env), sort_keys=True)}")
    # The reference also starts a Codex app-server to read the provider's status; the turn's
    # session is the last one started.
    out.extend(launches[-1:])
    initializes = []
    for line in (Path(log).read_text().splitlines() if Path(log).exists() else []):
        try:
            message = json.loads(line)
        except ValueError:
            continue
        if message.get("method") == "initialize":
            initializes.append(message)
            continue
        if message.get("method") in ("thread/start", "thread/resume", "turn/start"):
            params = dict(message.get("params") or {})
            params.pop("input", None)
            params.pop("additionalContext", None)
            config = params.get("config")
            if isinstance(config, dict):
                params["config"] = {k: v for k, v in config.items() if not k.startswith("mcp_servers.t3-code")}
            out.append(f"{message['method']} {json.dumps(mask(params), sort_keys=True)}")
    for message in initializes[-1:]:
        out.insert(1, f"initialize {json.dumps(mask(message.get('params')), sort_keys=True)}")
    return out


def turn_launches(log):
    """Each turn launch of a fake CLI, in order: a new session, or one it resumes. Claude Code
    resumes with --resume; Codex with thread/resume."""
    out = []
    launch = Path(str(log) + ".launch")
    for line in (launch.read_text().splitlines() if launch.exists() else []):
        argv = json.loads(line)["argv"]
        if "--no-session-persistence" in argv or argv[:1] == ["app-server"]:
            continue
        out.append("resume" if any(arg.startswith("--resume") for arg in argv) else "new")
    for line in (Path(log).read_text().splitlines() if Path(log).exists() else []):
        with contextlib.suppress(ValueError):
            method = json.loads(line).get("method")
            if method in ("thread/start", "thread/resume"):
                out.append("resume" if method == "thread/resume" else "new")
    return out


def thread_projection(url, cookie):
    return rpc(url, cookie, "orchestration.getThreadProjection", {"threadId": THREAD})


ACTIVE_RUN = {"preparing", "starting", "running", "waiting"}


def wait_for(url, cookie, done, seconds):
    deadline = time.monotonic() + seconds
    while True:
        projection = thread_projection(url, cookie)
        if done(projection) or time.monotonic() > deadline:
            return projection
        time.sleep(0.5)


def restart_turn(args, command, root, credentials, env, checkouts, fake_bin, scenario):
    """A turn on a server that is stopped and started again: what the client sees afterwards."""
    restart = scenario["restart"]
    with contextlib.ExitStack() as first:
        url, cookie = managed_server(first, command, root, *credentials, env=env)
        seeded = seed_turn(url, cookie, checkouts, scenario)
        rpc(url, cookie, "orchestration.dispatchCommand", seeded["dispatch"])
        if restart["after"] == "running":
            # The turn has run a command and holds on.
            wait_for(url, cookie, lambda p: any(i.get("type") == "command_execution"
                                                for i in p.get("turnItems", [])), 30)
        else:
            wait_for(url, cookie, lambda p: bool(p.get("runs")) and not any(
                r.get("status") in ACTIVE_RUN for r in p["runs"]), 30)
        time.sleep(1)
        if restart.get("settings"):
            rpc(url, cookie, "server.updateSettings", {"patch": restart["settings"]})
    # Stopped as a service manager stops it.
    if restart.get("removeCli"):
        for cli in fake_bin.iterdir():
            cli.unlink()
    with contextlib.ExitStack() as second:
        url, cookie = managed_server(second, command, root, *credentials, env=env)
        # The reference continues a turn a few seconds after it starts; let that run settle.
        started = time.monotonic()
        wait_for(url, cookie, lambda p: time.monotonic() - started > 8 and not any(
            r.get("status") in ACTIVE_RUN for r in p.get("runs", [])), 40)
        time.sleep(1)
        return run_client(args, url, cookie, {"threadId": THREAD, "inspect": True})


def compare_turn(original, rust):
    """Problems with Rust's provider turn as the reference client sees it, by check name."""
    problems = {}
    launch = [original.get("launch", []), rust.get("launch", [])]
    if launch[0] != launch[1]:
        problems["cli launch"] = [f"original {line}" for line in launch[0]] + [f"rust     {line}" for line in launch[1]]
    cli = [original.get("cli", []), rust.get("cli", [])]
    if cli[0] != cli[1]:
        problems["cli approvals"] = [f"original {line}" for line in cli[0]] + [f"rust     {line}" for line in cli[1]]
    launches = [original.get("launches", []), rust.get("launches", [])]
    if launches[0] != launches[1]:
        problems["cli launches"] = [f"original {launches[0]}", f"rust     {launches[1]}"]
    failed_steps = False
    live_step = "orchestration.subscribeThread (live turn)"
    inspected = live_step not in original["steps"] and live_step not in rust["steps"]
    for step in ((() if inspected else (live_step,))
                 + ("orchestration.subscribeThread", "orchestration.getThreadProjection")):
        a, b = original["steps"].get(step), rust["steps"].get(step)
        if a is None or b is None or a["outcome"] != b["outcome"]:
            failed_steps = True
            problems[step] = [f"outcome {a and a['outcome']} != {b and b['outcome']}: {(b or {}).get('message', '')[:600]}"]
    if failed_steps:
        return problems
    # A restart scenario reads the thread afterwards; nothing streamed live.
    live = None if inspected else [original["steps"][live_step]["value"], rust["steps"][live_step]["value"]]
    if live is None:
        pass
    elif live[0] is None or live[1] is None:
        problems["live"] = [f"run settled: original {live[0] is not None} rust {live[1] is not None}"]
    else:
        for reply in ("respondReply", "interruptReply"):
            outcomes = [None if side.get(reply) is None else sorted(side[reply]) for side in live]
            if outcomes[0] != outcomes[1]:
                problems[reply] = [f"original {live[0].get(reply)}", f"rust     {live[1].get(reply)}"]
        summaries = [live_summary(side) for side in live]
        if summaries[0] != summaries[1]:
            problems["live"] = [f"original {summaries[0]}", f"rust     {summaries[1]}"]
        # The kinds of item a client applies, whatever their order and grouping.
        kinds = [sorted(set(summary)) for summary in summaries]
        if kinds[0] != kinds[1]:
            problems["live kinds"] = [f"original only {sorted(set(kinds[0]) - set(kinds[1]))}",
                                      f"rust only     {sorted(set(kinds[1]) - set(kinds[0]))}"]
    shapes = compare_thread(original["steps"]["orchestration.subscribeThread"]["value"],
                            rust["steps"]["orchestration.subscribeThread"]["value"])
    if shapes:
        problems["shape"] = shapes
    fetched = [side["steps"].get("orchestration.getTurnItem (omitted output)", {}).get("value")
               for side in (original, rust)]
    if fetched[0] != fetched[1]:
        problems["omitted output"] = [f"original {fetched[0]}", f"rust     {fetched[1]}"]
    rendered = [rendered_turn(side["steps"]["orchestration.getThreadProjection"]["value"])
                for side in (original, rust)]
    for key in rendered[0]:
        if rendered[0][key] != rendered[1][key]:
            problems[key] = [f"original {rendered[0][key]}", f"rust     {rendered[1][key]}"]
    return problems


def provider_turns(args, credentials):
    """Run each provider scenario on both servers; the number of unexpected results."""
    xfail = json.loads((ROOT / "scripts/client_thread_parity_xfail.json").read_text())
    failures = 0
    for scenario in PROVIDER_SCENARIOS:
        if args.scenario and scenario["name"] not in args.scenario:
            continue
        outcomes = {}
        with tempfile.TemporaryDirectory() as temporary, contextlib.ExitStack() as stack:
            temporary = Path(temporary)
            drivers = set() if scenario["scenario"] is None else {"codex" if scenario["instance"] == "codex" else "claude"}
            for name, command in (("original", shlex.split(args.original_command)),
                                  ("rust", shlex.split(args.rust_command))):
                # Each server has its own fake CLIs, which a restart scenario may remove.
                fake_bin = temporary / f"{name}-fake-bin"
                write_fake_clis(fake_bin, drivers)
                path = os.pathsep.join([str(fake_bin), path_without(PROVIDER_CLIS | RELAY_TOOLS, temporary / "bin")])
                log = temporary / f"{name}-provider.log"
                env = {**GIT_ISOLATION, "PATH": path, "FAKE_PROVIDER_CLI": str(FIXTURES / "fake_cli.py"),
                       "FAKE_PROVIDER_LOG": str(log),
                       "FAKE_PROVIDER_SCENARIO": str(FIXTURES / (scenario["scenario"] or "codex_turn.json"))}
                root = temporary / name
                if "restart" in scenario:
                    outcomes[name] = restart_turn(args, command, root, credentials, env,
                                                  temporary / f"{name}-checkouts", fake_bin, scenario)
                    outcomes[name]["launches"] = turn_launches(log)
                else:
                    url, cookie = managed_server(stack, command, root, *credentials, env=env)
                    seeded = seed_turn(url, cookie, temporary / f"{name}-checkouts", scenario)
                    outcomes[name] = run_client(args, url, cookie, seeded)
                outcomes[name]["cli"] = cli_approvals(log)
                outcomes[name]["launch"] = cli_launch(log, temporary)
        if args.dump:
            for name, outcome in outcomes.items():
                slug = scenario["name"].replace(" ", "-")
                (args.dump / f"turn-{slug}-{name}.json").write_text(json.dumps(outcome, indent=2, sort_keys=True))
        problems = compare_turn(outcomes["original"], outcomes["rust"])
        expected = set(xfail.get(scenario["name"], {}))
        for check in sorted(set(problems) | expected):
            label = f"{scenario['name']}: {check}"
            if check in problems and check in expected:
                print(f"XFAIL {label}: {xfail[scenario['name']][check]}")
            elif check in problems:
                failures += 1
                print(f"FAIL {label}")
                for problem in problems[check][:40]:
                    print(f"  {problem}")
            else:
                failures += 1
                print(f"XPASS {label}: remove it from client_thread_parity_xfail.json")
        if not problems:
            print(f"PASS {scenario['name']}")
    return failures


def thread_checks(args, credentials, temporary):
    """The seeded thread's checks; the number that failed."""
    outcomes, roots = {}, {}
    with contextlib.ExitStack() as stack:
        os.environ["PATH"] = path_without(PROVIDER_CLIS | RELAY_TOOLS, Path(temporary) / "bin")
        # Rust runs its provider command for a turn; one that fails ends the turn as the
        # reference's does without a provider CLI.
        commands = {"original": shlex.split(args.original_command),
                    "rust": shlex.split(args.rust_command) + ["--provider-command", "false"]}
        for name, command in commands.items():
            root = Path(temporary) / name
            url, cookie = managed_server(stack, command, root, *credentials, env=GIT_ISOLATION)
            seeded = seed(url, cookie, Path(temporary) / f"{name}-checkouts")
            roots[name] = Path(temporary) / f"{name}-checkouts"
            time.sleep(2)
            outcomes[name] = run_client(args, url, cookie, seeded)
    if args.dump:
        args.dump.mkdir(parents=True, exist_ok=True)
        for name, outcome in outcomes.items():
            (args.dump / f"{name}.json").write_text(json.dumps(outcome, indent=2, sort_keys=True))
    failures = total = 0
    names = sorted(set(outcomes["original"]["steps"]) | set(outcomes["rust"]["steps"]))
    for name in names:
        total += 1
        original = outcomes["original"]["steps"].get(name)
        rust = outcomes["rust"]["steps"].get(name)
        problems = []
        if original is None or rust is None:
            problems.append("missing on one server")
        elif original["outcome"] != rust["outcome"] or original.get("tag") != rust.get("tag"):
            problems.append(f"outcome {original['outcome']} {original.get('tag', '')} != "
                            f"{rust['outcome']} {rust.get('tag', '')}: {rust.get('message', '')[:600]}")
        elif name.startswith("orchestration.subscribeThread") and original["outcome"] == "reply":
            problems = compare_thread(original["value"], rust["value"])
        else:
            left, right = shape(original.get("value")), shape(rust.get("value"))
            for key in sorted(set(left) | set(right)):
                if left.get(key) != right.get(key):
                    problems.append(f"{key}: original {sorted(left.get(key, []))} rust {sorted(right.get(key, []))}")
            if not problems:
                comparable = [stable(original.get("value", original.get("error")), roots["original"]),
                              stable(rust.get("value", rust.get("error")), roots["rust"])]
                if comparable[0] != comparable[1]:
                    problems.append("values differ:\n    original " + json.dumps(comparable[0], sort_keys=True)[:3000]
                                    + "\n    rust     " + json.dumps(comparable[1], sort_keys=True)[:3000])
        if problems:
            failures += 1
            print(f"FAIL {name}")
            for problem in problems[:40]:
                print(f"  {problem}")
        else:
            print(f"PASS {name}: {rust['outcome']} {rust.get('tag', '')}")
    print(f"{total - failures}/{total} client thread checks matched")
    return failures


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-command", required=True)
    parser.add_argument("--rust-command", required=True)
    parser.add_argument("--reference-root", required=True,
                        help="checkout of the pinned upstream with dependencies installed")
    parser.add_argument("--node", default="node")
    parser.add_argument("--reply-ms", type=int, default=8000)
    parser.add_argument("--dump", type=Path, help="write each server's decoded results here")
    parser.add_argument("--scenario", action="append", help="run only this provider scenario (repeatable)")
    parser.add_argument("--skip-thread-checks", action="store_true", help="run only the provider scenarios")
    args = parser.parse_args()
    credentials = secrets.token_urlsafe(32), secrets.token_bytes(32)
    if args.dump:
        args.dump.mkdir(parents=True, exist_ok=True)
    failures = 0
    with tempfile.TemporaryDirectory() as temporary:
        GIT_ISOLATION.update(isolated_git_env(Path(temporary) / "git"))
        if not args.skip_thread_checks:
            failures += thread_checks(args, credentials, Path(temporary) / "thread")
        failures += provider_turns(args, credentials)
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
