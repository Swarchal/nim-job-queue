# Plan

## Decisions

| Question | Decision |
| --- | --- |
| What is a job? | A Nim proc compiled into the server or a remote worker and registered by name. Callers send the name plus JSON arguments. |
| Architecture | The server holds the queue and can run jobs on its own pool of threads. Remote worker processes, on any machine, can also claim jobs over HTTP, so jobs can be added or changed without restarting the server. |
| Caller API | HTTP + JSON, so curl/Python can use it. A small Nim client module wraps it. |
| Persistence | Always SQLite: an in-memory database by default, or a file when `databasePath` is set, so jobs survive a restart. (There used to be a separate memory backend; it was dropped when remote workers arrived, to avoid writing leases and claims twice.) |
| Dependencies | Standard library, plus `db_connector` (the official SQLite bindings) and the system `libsqlite3`. |
| Style | No macros or templates: jobs are registered with an explicit `registerJob(name, handler)` call. |

## How it fits together

```
caller ──HTTP──> server.nim ──addJob──> store.nim <──takeNextJob── worker.nim (N threads)
                 (main thread)          (lock + cond)               │
caller <──HTTP── server.nim <──getJob── store.nim <──markSucceeded/markFailed
                                                                    │
                                                  registry.nim ─────┘ (name -> handler)
```

1. `POST /jobs` — the server checks the name is registered, stores the job as `queued`, and returns its id straight away.
2. A worker thread blocked in `takeNextJob` wakes up, marks the job `running`, and calls the handler.
3. The handler's return value (or the message of the exception it raised) is stored, and the job becomes `succeeded` or `failed`.
4. The caller polls `GET /jobs/{id}` until the status is `succeeded` or `failed`.

### Remote workers

```
remote_worker.nim ──POST /workers/claim──────> server.nim ──claimJob──────> store.nim
 (N job threads)  ──POST /jobs/{id}/result───>            ──markSucceeded/markFailed
 (1 heartbeat     ──POST /jobs/{id}/heartbeat>            ──extendLease
  thread)                                      leaseLoop ──requeueExpiredLeases (every second)
```

- A worker claims jobs by listing the names it can run. The server remembers those names, so callers can then submit them even though the server cannot run them itself.
- A claimed job belongs to that worker id and has a lease (`leaseSeconds`, default 30). The worker sends a heartbeat every third of a lease; if they stop, the server puts the job back in the queue, in its original order, for another worker. Only the worker holding a job can report its result, so a late result from a worker that lost its lease is refused with `409`.
- The server's own threads use the same claim rules, but only for jobs registered on the server, under the reserved worker id `local`, and with no lease. A server with no jobs registered starts no threads and is purely a queue.
- Deploying new or changed jobs: start workers with the new code, then press ctrl-c on the old ones. They finish their current job, report it, and exit.

## HTTP API

| Request | Reply |
| --- | --- |
| `POST /jobs` with `{"name": "add", "args": {"a": 1, "b": 2}}` | `202 {"id": "...", "status": "queued"}`; `400` for bad JSON or an unknown name |
| `GET /jobs/{id}` | `200 {"id", "name", "status", "result", "error", "submittedAt", "startedAt", "finishedAt"}`; `404` if unknown |
| `GET /job-names` | `200 ["add", ...]` |
| `GET /status` | `200 {"workers": 4, "remoteWorkers": 2, "queued": 3, "running": 4, "succeeded": 120, "failed": 2}` |
| `GET /health` | `200 {"status": "ok", "queued": 3}` |
| `POST /workers/claim` with `{"workerId": "w1", "jobNames": ["add"]}` | `200 {"job": {"id", "name", "args"} or null, "leaseSeconds": 30}` |
| `POST /jobs/{id}/heartbeat` with `{"workerId": "w1"}` | `200`; `409` if the job is no longer running on that worker |
| `POST /jobs/{id}/result` with `{"workerId": "w1", "succeeded": true, "result": ...}` or `{..., "succeeded": false, "error": "..."}` | `200`; `409` if the job is no longer running on that worker |

## Modules

| File | Responsibility |
| --- | --- |
| `src/jobqueue.nim` | Re-exports the public API. |
| `src/jobqueue/types.nim` | `JobStatus`, `JobRecord`, `JobHandler`, `JobQueueConfig`. |
| `src/jobqueue/registry.nim` | Job name → handler table. Written before start-up, read-only afterwards. |
| `src/jobqueue/store.nim` | Owns the lock and the wake-up signal; claims, leases and worker ownership. |
| `src/jobqueue/sqlite_backend.nim` | Jobs in one `jobs` table, in a file or in memory. |
| `src/jobqueue/worker.nim` | `runJob` (pure, testable, also used by remote workers) and the server's thread loop around it. |
| `src/jobqueue/server.nim` | `JobQueue` object, `routeRequest` (pure, testable), the lease check, and `serve`. |
| `src/jobqueue/remote_worker.nim` | `RemoteWorker`: job threads that claim, run and report, plus a heartbeat thread. |
| `src/jobqueue/client.nim` | `submitJob`, `getJob`, `waitForJob`, `getStatus`. |

## Threading model

- The HTTP server and the lease check run on the main thread; jobs run on `workerCount` threads.
- The only state shared between threads is the `JobStore`. Every store proc takes the lock itself, so the single SQLite connection is only ever used by one thread at a time.
- Job arguments and results cross threads as JSON text, not `JsonNode`, because `JsonNode` is a ref type and is not safe to share.
- Handlers must be `{.gcsafe, nimcall.}`: they cannot use global `ref`/`string`/`seq` variables or capture anything.

## Status

Implemented: every module above, the tests, and the examples. The store tests run against both an in-memory and a file database.

Two additions to the original plan, both in `server.nim`:

- `start()` / `stop()` / `port()` sit underneath `serve()`. They let the end-to-end test run a real server on an OS-chosen port without blocking.
- A known path with the wrong method returns `405`.

## SQLite storage

- Enabled by `JobQueueConfig(databasePath: "jobs.sqlite")`. The file and table are created if missing.
- **A job that was running when the server died is run again** on the next start, ahead of newer jobs. A job can therefore run more than once, so jobs with side effects should be safe to repeat.
- Jobs still queued at shutdown stay queued and run on the next start.
- Only one server process may use a database file at a time. Nothing enforces this yet.
- Every state change is its own write. Write-ahead logging keeps that cheap.
- A job whose name is no longer registered after an upgrade fails with a clear message when a worker picks it up.

## Known limitations of this design

- **No hard job timeout.** A Nim thread cannot be killed safely, so a handler that hangs occupies its thread forever. With remote workers the hung worker process can at least be killed by hand; its heartbeats then stop and the job goes to another worker after its lease.
- **Jobs run at least once, not exactly once.** A job can run twice if the server restarts while it is running, or if a remote worker loses contact for longer than a lease while still running it (its late result is then refused). Jobs with side effects should be safe to repeat.
- **Remote-only job names are learned, not configured.** Until a worker that runs a name has asked for work since the server started, submitting that name gives 400. Queued jobs whose workers have all gone wait until one comes back.
- **Remote workers poll** for work every `pollIntervalSeconds` (0.5 by default) when idle, rather than being told when a job arrives.
- **Finished jobs are kept forever by default**, so memory (or the database file) grows with every job. Set `maxFinishedJobs` to cap them: once more than that many jobs have succeeded or failed, the ones that finished longest ago are removed, and `GET /jobs/{id}` gives 404 for them. A caller that polls slowly while many jobs finish can miss its result, so keep the cap well above the number of jobs finishing per polling interval.
- **Without `databasePath`, jobs are lost on restart**, including ones that were running. On ctrl-c, running jobs finish but queued jobs are dropped.
- **No authentication.** Anyone who can reach the port can submit jobs, read results, or pose as a worker and report false results. Bind to `127.0.0.1`, or keep the server on a trusted network or behind a proxy that adds authentication. This matters more now that workers on other machines need to reach it.
- `asynchttpserver` requires its callback to be a closure. That is the one place a closure is unavoidable; it will be a three-line wrapper around `routeRequest`.

## Later, in rough priority order

1. Result retention by age (drop finished jobs after N seconds). Retention by count is done: `maxFinishedJobs`.
2. Long-poll `GET /jobs/{id}?wait=10` and `POST /workers/claim` so callers and workers don't have to poll.
3. `DELETE /jobs/{id}` to cancel a job that is still queued.
4. Retries with a per-job maximum.
5. Delayed / scheduled jobs.
6. Priorities or multiple named queues.
7. A lock file so two servers cannot open the same database.
8. Authentication: at least a shared secret for workers.

## Open questions

- Is polling acceptable for the first version, or is long-polling needed from the start?
- How long should results be kept?
- Licence: the `.nimble` file says MIT as a placeholder.
