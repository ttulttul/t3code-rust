#!/usr/bin/env node
// Drives the pinned reference client-runtime against one server for what opening a thread
// needs: the thread subscription, its worktree setup subscription, the projects' repository
// identity, and the change request browser's first reads.
//
//   node scripts/client_thread.mjs <reference-checkout> <http-base-url>
//
// Environment:
//   T3_CLIENT_SYNC_COOKIE     session cookie (kept out of the process listing)
//   T3_CLIENT_THREAD_SEED     JSON {"threadId": ..., "projectIds": [...]} of the seeded state
//   T3_CLIENT_THREAD_WAIT_MS  wait for each reply or stream value (default 8000)
//   T3_CLIENT_SYNC_TRACE=1    log every frame to stderr
//
// Prints one JSON object on stdout: each step's decoded value, or how it failed.
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";
import path from "node:path";

const [reference, base] = process.argv.slice(2);
if (!reference || !base) {
  console.error("usage: client_thread.mjs <reference-checkout> <http-base-url>");
  process.exit(2);
}
const cookie = process.env.T3_CLIENT_SYNC_COOKIE;
const seed = JSON.parse(process.env.T3_CLIENT_THREAD_SEED ?? "{}");
const waitMs = Number(process.env.T3_CLIENT_THREAD_WAIT_MS ?? 8000);
const trace = process.env.T3_CLIENT_SYNC_TRACE === "1";

const runtimeDir = path.join(path.resolve(reference), "packages/client-runtime");
const require = createRequire(path.join(runtimeDir, "package.json"));
const load = (specifier) => import(pathToFileURL(require.resolve(specifier)).href);
const loadSource = (relative) => import(pathToFileURL(path.join(runtimeDir, "src", relative)).href);

const Cause = await load("effect/Cause");
const Effect = await load("effect/Effect");
const Layer = await load("effect/Layer");
const Option = await load("effect/Option");
const Stream = await load("effect/Stream");
const Socket = await load("effect/socket/Socket");
const FetchHttpClient = await load("effect/http/FetchHttpClient");
const Deferred = await load("effect/Deferred");
const Fiber = await load("effect/Fiber");
const RpcSession = await loadSource("rpc/session.ts");

const authHeaders = cookie ? { Cookie: cookie } : {};
const descriptor = await (await fetch(new URL("/.well-known/t3/environment", base))).json();
const socketUrl = new URL("/ws?orchestrationProtocol=2", base.replace(/^http/, "ws")).toString();
const webSocket = (url, options) => {
  const socket = new globalThis.WebSocket(url, {
    protocols: typeof options === "string" || Array.isArray(options) ? options : options?.protocols,
    headers: authHeaders,
  });
  if (trace) {
    const send = socket.send.bind(socket);
    socket.send = (data) => {
      process.stderr.write(`>> ${String(data).slice(0, 2000)}\n`);
      send(data);
    };
    socket.addEventListener("message", (event) => process.stderr.write(`<< ${String(event.data).slice(0, 4000)}\n`));
  }
  return socket;
};
const cookieFetch = (input, init = {}) =>
  globalThis.fetch(input, { ...init, headers: { ...Object.fromEntries(new Headers(init.headers)), ...authHeaders } });

/** Decoded values as JSON: Options, dates and class instances by their visible fields. */
function plain(value) {
  if (Option.isOption?.(value)) return Option.isSome(value) ? { some: plain(value.value) } : { none: true };
  if (value instanceof Date) return value.toISOString();
  if (value && typeof value === "object" && typeof value.epochMilliseconds === "number") {
    return new Date(value.epochMilliseconds).toISOString();
  }
  if (Array.isArray(value)) return value.map(plain);
  if (value instanceof Map) return Object.fromEntries([...value].map(([k, v]) => [String(k), plain(v)]));
  if (value && typeof value === "object") {
    return Object.fromEntries(Object.keys(value).sort().map((key) => [key, plain(value[key])]));
  }
  return value;
}

const results = { environmentId: descriptor.environmentId, steps: {} };

/** Records a step's decoded value or its failure, as the client sees it. */
const record = (name, effect) =>
  effect.pipe(
    Effect.timeoutOption(waitMs),
    Effect.matchCauseEffect({
      onSuccess: (outcome) =>
        Effect.sync(() => {
          results.steps[name] = Option.isNone(outcome)
            ? { outcome: "silent" }
            : { outcome: "reply", value: plain(outcome.value) };
        }),
      onFailure: (cause) =>
        Effect.sync(() => {
          const failure = Cause.findErrorOption(cause);
          const error = Option.isSome(failure) ? failure.value : Cause.squash(cause);
          results.steps[name] = {
            outcome: Option.isSome(failure) ? "error" : "defect",
            tag: String(error?._tag ?? error?.name ?? "unknown"),
            ...(Option.isSome(failure) ? { error: plain({ ...error }) } : {}),
            message: String(error?.message ?? error).slice(0, 2000),
          };
        }),
    }),
  );

/** Items until `synchronized`, as EnvironmentThreadState waits for them. */
const untilSynchronized = (stream) =>
  stream.pipe(
    Stream.takeUntil((item) => item.kind === "synchronized"),
    Stream.runCollect,
    Effect.map((items) => Array.from(items)),
  );

/** The stream's values within the wait, and whether it ended in it. */
const valuesWithin = (stream, ms) =>
  Effect.gen(function* () {
    const values = [];
    const ended = yield* stream.pipe(
      Stream.runForEach((value) => Effect.sync(() => values.push(value))),
      Effect.as(true),
      Effect.timeoutOption(ms),
      Effect.map(Option.isSome),
    );
    return { values, ended };
  });

const threadInput = (threadId) => ({
  threadId,
  requestCompletionMarker: true,
  acceptBoundedSnapshot: true,
  acceptDeferredWork: true,
  acceptCompactTurnItems: true,
  acceptCompactIds: true,
  acceptDedupedBodies: true,
});

const TERMINAL_RUN = new Set(["completed", "failed", "interrupted", "cancelled"]);

/** Whether a stream item shows the thread's run settled. */
const settles = (item) =>
  (item.kind === "event" && item.event.type === "run.updated" && TERMINAL_RUN.has(item.event.payload?.status)) ||
  (item.kind === "snapshot" && (item.projection?.runs ?? []).some((run) => TERMINAL_RUN.has(run.status)));

/** The run a stream item shows, and whether its provider turn is running. */
const runOf = (item) => {
  if (item.kind === "event" && item.event.type.startsWith("run.")) return { runId: item.event.payload?.id };
  if (item.kind === "event" && item.event.type === "provider-turn.updated") {
    return { turnRunning: item.event.payload?.status === "running" };
  }
  if (item.kind === "snapshot") {
    const runs = item.projection?.runs ?? [];
    return {
      runId: runs.at(-1)?.id,
      turnRunning: (item.projection?.providerTurns ?? []).some((turn) => turn.status === "running"),
    };
  }
  return {};
};

/** Pending runtime requests and the request ids that have a turn item, from a stream item. */
const requestsOf = (item, pending, withItems) => {
  const requests =
    item.kind === "event" && item.event.type === "runtime-request.updated"
      ? [item.event.payload]
      : item.kind === "snapshot"
        ? (item.projection?.runtimeRequests ?? [])
        : [];
  for (const request of requests) {
    if (request?.status === "pending") pending.set(request.id, request);
    else if (request) pending.delete(request.id);
  }
  const turnItems =
    item.kind === "event" && item.event.type === "turn-item.updated"
      ? [item.event.payload]
      : item.kind === "snapshot"
        ? [
            ...(item.projection?.turnItems ?? []),
            ...(item.projection?.visibleTurnItems ?? []).map((row) => row.item),
          ]
        : [];
  for (const turnItem of turnItems) if (turnItem?.requestId) withItems.add(turnItem.requestId);
};

/**
 * subscribeThread's items from before a message is dispatched until its run settles. With
 * `seed.interrupt`, the run is interrupted once its provider turn is running; with
 * `seed.interrupt === "request"`, once a runtime request is pending. With `seed.respond`
 * ({decision} or {answers}), the first pending runtime request is answered with it.
 */
const liveTurn = (client, seed) =>
  Effect.gen(function* () {
    const items = [];
    const synchronized = yield* Deferred.make();
    const settled = yield* Deferred.make();
    const turnRunning = yield* Deferred.make();
    const requestPending = yield* Deferred.make();
    const pending = new Map();
    const withItems = new Set();
    let dispatched = false;
    let runId;
    const fiber = yield* client["orchestration.subscribeThread"](threadInput(seed.threadId)).pipe(
      Stream.runForEach((item) =>
        Effect.gen(function* () {
          items.push({ ...plain(item), dispatched });
          if (item.kind === "synchronized") yield* Deferred.succeed(synchronized, undefined);
          if (dispatched && settles(item)) yield* Deferred.succeed(settled, undefined);
          const run = runOf(item);
          if (dispatched && run.runId) runId = run.runId;
          if (dispatched && run.turnRunning) yield* Deferred.succeed(turnRunning, undefined);
          if (dispatched) {
            requestsOf(item, pending, withItems);
            // Answer once the request's card is there too, as a user would.
            const ready = [...pending.values()].find((request) => withItems.has(request.id));
            if (ready) yield* Deferred.succeed(requestPending, ready);
          }
        }),
      ),
      Effect.forkScoped,
    );
    yield* Deferred.await(synchronized);
    dispatched = true;
    const reply = yield* client["orchestration.dispatchCommand"](seed.dispatch);
    let interruptReply = null;
    let respondReply = null;
    if (seed.respond) {
      const request = yield* Deferred.await(requestPending);
      respondReply = yield* client["orchestration.dispatchCommand"]({
        type: "runtime-request.respond",
        commandId: "turn-respond",
        threadId: seed.threadId,
        requestId: request.id,
        ...seed.respond,
      }).pipe(
        Effect.map(plain),
        Effect.catch((error) => Effect.succeed({ error: String(error?._tag ?? error) })),
      );
    }
    if (seed.interrupt === "request") {
      yield* Deferred.await(requestPending);
      interruptReply = plain(
        yield* client["orchestration.dispatchCommand"]({
          type: "run.interrupt",
          commandId: "turn-interrupt",
          threadId: seed.threadId,
          runId,
        }),
      );
    } else if (seed.interrupt) {
      yield* Deferred.await(turnRunning);
      interruptReply = plain(
        yield* client["orchestration.dispatchCommand"]({
          type: "run.interrupt",
          commandId: "turn-interrupt",
          threadId: seed.threadId,
          runId,
        }),
      );
    }
    yield* Deferred.await(settled);
    // Let the run's last events arrive.
    yield* Effect.sleep(1500);
    yield* Fiber.interrupt(fiber);
    return { dispatchReply: plain(reply), interruptReply, respondReply, items };
  });

const program = Effect.gen(function* () {
  const factory = yield* RpcSession.make({ environmentThemes: true, usageLimitSources: true, usageLimitsCommand: true });
  const session = yield* factory.connect({
    environmentId: descriptor.environmentId,
    label: "parity",
    httpBaseUrl: base,
    socketUrl,
    httpAuthorization: null,
    target: { _tag: "PrimaryConnectionTarget", environmentId: descriptor.environmentId },
  });
  yield* session.ready;
  const client = session.client;
  if (seed.dispatch || seed.inspect) {
    // A provider turn: subscribe first, dispatch the message once synchronized, and keep what
    // streams in until the run settles (or the wait ends). Then read the thread afresh. With
    // `seed.inspect` (a thread whose turns already ran), only read it.
    if (seed.dispatch) {
      yield* record(
        "orchestration.subscribeThread (live turn)",
        liveTurn(client, seed).pipe(Effect.timeoutOption(seed.turnMs ?? 30000), Effect.map(Option.getOrNull)),
      );
    }
    yield* record("orchestration.subscribeThread", untilSynchronized(client["orchestration.subscribeThread"](threadInput(seed.threadId))));
    yield* record("orchestration.getThreadProjection", client["orchestration.getThreadProjection"]({ threadId: seed.threadId }));
    // What a client fetches for each item whose output the projection left out.
    const projection = results.steps["orchestration.getThreadProjection"]?.value;
    const omitted = (projection?.turnItems ?? []).filter((item) => item.outputOmitted === true);
    yield* record(
      "orchestration.getTurnItem (omitted output)",
      Effect.forEach(omitted, (item) =>
        client["orchestration.getTurnItem"]({ threadId: seed.threadId, itemId: item.id }).pipe(
          Effect.map(({ item: detail }) => ({
            type: item.type,
            output: detail?.output ?? null,
            diffStr: detail?.diffStr ?? null,
          })),
        ),
      ),
    );
    return;
  }
  const steps = [];
  if (seed.threadId) {
    steps.push(
      ["orchestration.subscribeThread", untilSynchronized(client["orchestration.subscribeThread"](threadInput(seed.threadId)))],
      [
        "orchestration.subscribeThread (plain)",
        untilSynchronized(client["orchestration.subscribeThread"]({ threadId: seed.threadId, requestCompletionMarker: true })),
      ],
      ["subscribeWorktreeSetup", valuesWithin(client.subscribeWorktreeSetup({ threadId: seed.threadId }), 1500)],
      ["worktreeSetup.cancel", client["worktreeSetup.cancel"]({ threadId: seed.threadId })],
    );
  }
  steps.push(
    ["orchestration.subscribeThread (missing)", untilSynchronized(client["orchestration.subscribeThread"](threadInput("thread:missing")))],
    // Enrichment snapshots fill in identities after the first one; the last one wins.
    [
      "orchestration.subscribeShell",
      valuesWithin(client["orchestration.subscribeShell"]({ requestCompletionMarker: true }), 3000).pipe(
        Effect.map(({ values }) => {
          const identities = {};
          for (const item of values) {
            for (const project of item.snapshot?.projects ?? []) {
              if (item.resolvedRepositoryIdentityRoots === undefined || project.repositoryIdentity != null) {
                identities[project.id] = project.repositoryIdentity ?? null;
              }
            }
          }
          return identities;
        }),
      ),
    ],
  );
  for (const host of seed.hosts ?? []) {
    steps.push([`pullRequests.routingIdentity ${host}`, client["pullRequests.routingIdentity"]({ host })]);
  }
  for (const projectId of seed.projectIds ?? []) {
    steps.push([
      `pullRequests.list ${projectId}`,
      client["pullRequests.list"]({ state: "open", projectId }),
    ]);
  }
  yield* Effect.all(steps.map(([name, effect]) => record(name, effect)), { concurrency: "unbounded", discard: true });
}).pipe(
  Effect.scoped,
  Effect.provide(FetchHttpClient.layer),
  Effect.provideService(FetchHttpClient.Fetch, cookieFetch),
  Effect.provide(Layer.succeed(Socket.WebSocketConstructor, webSocket)),
);

await Effect.runPromise(program);
// Exit only once a large result is flushed to the pipe.
process.stdout.write(JSON.stringify(results) + "\n", () => process.exit(0));
