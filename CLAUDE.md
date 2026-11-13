# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A celery-like job queue in Nim (>= 2.0). Jobs are ordinary Nim procs compiled
into the server binary and registered by name; callers submit a name plus JSON
args over HTTP and poll for the result. `PLAN.md` holds the design decisions,
known limitations and the roadmap; read it before changing behaviour.

## Commands

```sh
nimble test                                   # all tests (tests/t*.nim)
nim c -r tests/test_server.nim                # one test file
nim c -r tests/test_server.nim "server lifecycle::*"   # one suite (unittest filter)
nim c -r tests/test_store.nim "*takeNextJob*"          # tests matching a pattern
nph src tests examples                        # format (nph style is required)
nph --check src tests examples
nim r examples/pipeline_server.nim            # then, in another terminal:
nim r examples/pipeline_client.nim
```

`tests/config.nims` and `examples/config.nims` add `src/` to the path, so
`import jobqueue` works without installing. `tests/helpers.nim` holds shared
test handlers and temp-database helpers; it is not prefixed with `t` so
`nimble test` does not run it on its own.

Dependency: `db_connector` (SQLite bindings) and the system `libsqlite3`.

## Architecture

The server runs HTTP on the main thread (asynchttpserver) and jobs on a pool
of OS threads. Remote worker processes (`remote_worker.nim`) can also claim
jobs over HTTP; a server with no jobs registered starts no threads and is
purely a queue. `PLAN.md` has the claim/heartbeat/result protocol.

- `server.nim`: `JobQueue` has a lifecycle `qsNew -> qsRunning -> qsStopped`.
  Jobs can only be registered while new, and a stopped queue cannot restart.
  `routeRequest` is the whole HTTP API as a pure function (no sockets), so
  tests call it directly. It never raises: unexpected errors become 500 and
  requests after `stop()` get 503; request-body helpers raise
  `BadRequestError`, which becomes 400. `serve()` = `start()` + poll loop +
  ctrl-c handling; `start()`/`stop()`/`port()` exist for tests and
  embedding. `maintenanceLoop` re-queues jobs whose remote worker stopped
  heartbeating and wakes the threads for due retries, so both only happen
  while something drives the async loop.
- `store.nim`: the only state shared between threads. It owns the lock and
  the "job available" condition variable, and every public proc takes the
  lock itself. Storage is always `sqlite_backend.nim`: `:memory:` by
  default, a file when `databasePath` is set.
- `worker.nim`: `runJob` is pure and never raises (handler exceptions,
  including Defects, become a failed outcome). `workerLoop` catches store
  errors, because an exception escaping a thread kills the process.
- `registry.nim`: name -> handler table, written before start-up and read
  without a lock afterwards.
- `client.nim`: blocking HTTP client (`submitJob`, `getJob`, `waitForJob`,
  `getStatus`). Job ids are percent-encoded in URLs and decoded by the
  server.
- `remote_worker.nim`: N job threads (claim, `runJob`, report) plus one
  heartbeat thread, sharing a lock-guarded `SharedState` through a `ptr`
  into the `RemoteWorker`, the same pattern the server uses for its store.

Invariants to preserve:

- Job args and results cross threads as JSON **text** (`JobRecord.argsJson`,
  `resultJson`), never `JsonNode`, which is a ref type and not thread-safe.
- Handlers must be `proc(args: JsonNode): JsonNode {.gcsafe, nimcall.}`: no
  closures, no global ref/string/seq state.
- Every running job belongs to a worker id. Only that worker may finish it or
  extend its lease (the store checks `worker_id`), so a worker that lost its
  lease gets 409. The server's own threads use the reserved id `local` and
  `leaseExpiresAt = 0` (never expires). Remote worker ids must be unique
  across processes: `newWorkerId` uses host, pid and OS randomness, because
  `std/oids` collides between processes started in the same second. Each
  remote job thread appends its index (`threadWorkerId`), so ownership is
  per thread and a sibling thread cannot report a job it lost.
- `JobQueueError.httpCode` separates refusals (4xx, final) from server
  failures (5xx, retried by the remote worker). Every `HttpClient` comes from
  `newJsonHttpClient`, which sets a timeout; never block for ever.
- Jobs run at least once: running jobs are re-queued (ahead of newer jobs) on
  server restart and when a lease expires.
- Retries: only a handler raising `RetryableJobError`, or an exception whose
  exact type name is in the registration's `retryOn`, causes one, and
  `runJob` alone decides (from `JobRecord.retryCount` and the registration's
  `maxRetries`), for server threads and remote workers alike. A retried job
  is queued with a `run_after` time that `claimNextJob` respects; blocked
  server threads are woken for it by `wakeForDueJobs`, called from
  `maintenanceLoop`. Lease expiry and restart re-runs do not count as retries.
- `maxFinishedJobs` removes finished jobs in finish order, not submission
  order. The SQLite backend relies on `finished_at > 0` meaning "finished"
  (its partial index and retention query use it) and keeps
  `finishedJobCount` in step itself rather than counting each time.
- The `acceptLoop` request callback is the one deliberate closure
  (asynchttpserver requires it); keep it a thin wrapper around `routeRequest`.
- The pattern for sharing jobs between a Nim server and client is a shared
  module (see `examples/text_jobs.nim`): name constants, a register proc for
  the server and typed submit wrappers for the client.

## Style

Simple Nim readable by intermediate programmers: no templates or macros
unless necessary, descriptive names, `nph` formatting.
