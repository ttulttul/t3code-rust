#!/usr/bin/env python3
"""A stand-in for the Codex and Claude Code CLIs in provider turn tests.

It plays the scenario named by FAKE_PROVIDER_SCENARIO: messages recorded from
a real `codex app-server` or `claude --output-format stream-json` session,
with steps that wait for what the client sends. Every line it receives is
appended to FAKE_PROVIDER_LOG, its arguments to FAKE_PROVIDER_LOG.args, and its
arguments and launch environment as JSON to FAKE_PROVIDER_LOG.launch.

A scenario is {"steps": [...]} or {"variants": [{"argsContain": "...",
"steps": [...]}, ...]}, where the first variant whose argsContain is in the
arguments (or that has none) plays. A variant may name another scenario file
in this directory ("scenario": "claude_turn.json") to play its steps instead.
Steps:

  {"expect": "turn/start"}   read until a message whose method, type or
                             request.subtype matches; remember it
  {"expectReply": "id"}      read until the reply to request "id"
  {"send": {...}}            write a message; "$id" becomes the remembered
                             message's id and "$params.x" its params.x
  {"stderr": "text"}         write a line to standard error
  {"sleep": seconds}
  {"exit": code}

A request the scenario does not expect is answered from SIDE_REPLIES (or the
scenario's own "replies" by method), so a client that reads its config or
account first still reaches the scripted steps.

After the last step the CLI waits for its input to close, as the real ones do.
"""
import json
import os
import sys
import time

args = sys.argv[1:]
log_path = os.environ["FAKE_PROVIDER_LOG"]
with open(log_path + ".args", "a") as out:
    out.write(" ".join(args) + "\n")
# The launch as JSON: the arguments and the environment a server sets for its CLIs.
LAUNCH_ENV = ("T3CODE_THREAD_ID", "CLAUDE_CODE_", "CLAUDE_AGENT_SDK_", "CODEX_INTERNAL_", "ENABLE_CLAUDEAI_")
with open(log_path + ".launch", "a") as out:
    out.write(json.dumps({"argv": args, "env": {k: v for k, v in os.environ.items()
                                                if k.startswith(LAUNCH_ENV)}}) + "\n")
with open(os.environ["FAKE_PROVIDER_SCENARIO"]) as source:
    scenario = json.load(source)

if "variants" in scenario:
    variant = next(
        v
        for v in scenario["variants"]
        if "argsContain" not in v or v["argsContain"] in " ".join(args)
    )
    if "scenario" in variant:
        directory = os.path.dirname(os.path.abspath(os.environ["FAKE_PROVIDER_SCENARIO"]))
        with open(os.path.join(directory, variant["scenario"])) as source:
            played = json.load(source)
        steps = played["steps"]
        scenario = {**played, **{k: v for k, v in scenario.items() if k != "variants"}}
    else:
        steps = variant["steps"]
else:
    steps = scenario["steps"]

remembered = {}

# Answers to requests a client may make between the scripted ones.
SIDE_REPLIES = {
    "account/read": {"account": {"type": "apiKey"}, "requiresOpenaiAuth": False},
    "config/read": {"config": {}, "origins": {}},
    "thread/unsubscribe": {},
    "turn/interrupt": {},
    **scenario.get("replies", {}),
}
# Answers to Claude control requests by subtype; any other succeeds empty.
CONTROL_REPLIES = {
    "initialize": {"commands": [], "agents": [], "output_style": "default", "available_output_styles": ["default"],
                   "models": [], "account": {}},
    "mcp_set_servers": {"added": [], "removed": [], "errors": {}},
    **scenario.get("controlReplies", {}),
}


def answer(message):
    """Reply to an unscripted request this CLI knows an answer to."""
    method = message.get("method")
    if "id" in message and method in SIDE_REPLIES:
        sys.stdout.write(json.dumps({"id": message["id"], "result": SIDE_REPLIES[method]}) + "\n")
        sys.stdout.flush()
    subtype = (message.get("request") or {}).get("subtype")
    if message.get("type") == "control_request":
        reply = {"type": "control_response", "response": {
            "subtype": "success", "request_id": message.get("request_id"),
            "response": CONTROL_REPLIES.get(subtype, {})}}
        sys.stdout.write(json.dumps(reply) + "\n")
        sys.stdout.flush()


def receive():
    line = sys.stdin.readline()
    if not line:
        sys.exit(0)
    with open(log_path, "a") as out:
        out.write(line if line.endswith("\n") else line + "\n")
    try:
        return json.loads(line)
    except ValueError:
        return {}


def fill(value):
    if isinstance(value, str):
        if value == "$id":
            return remembered.get("id")
        if value.startswith("$params."):
            return remembered.get("params", {}).get(value[len("$params."):])
        return value
    if isinstance(value, list):
        return [fill(v) for v in value]
    if isinstance(value, dict):
        return {k: fill(v) for k, v in value.items()}
    return value


for step in steps:
    if "expect" in step:
        while True:
            message = receive()
            names = (
                message.get("method"),
                message.get("type"),
                (message.get("request") or {}).get("subtype"),
            )
            if step["expect"] in names:
                remembered = message
                break
            answer(message)
    elif "expectReply" in step:
        while True:
            message = receive()
            if message.get("id") == step["expectReply"] and "method" not in message:
                break
            answer(message)
    elif "send" in step:
        sys.stdout.write(json.dumps(fill(step["send"])) + "\n")
        sys.stdout.flush()
    elif "stderr" in step:
        sys.stderr.write(step["stderr"] + "\n")
        sys.stderr.flush()
    elif "sleep" in step:
        time.sleep(step["sleep"])
    elif "exit" in step:
        sys.exit(step["exit"])

while True:
    answer(receive())
