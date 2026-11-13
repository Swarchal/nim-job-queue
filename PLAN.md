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
  thread)                                      maintenanceLoop ──requeueExpiredLeases (every 0.1 s)
```

- A worker claims jobs by listing the names it can run. The server remembers those names, so callers can then submit them even though the server cannot run them itself.
- Each job thread of a remote worker has its own worker id (`<process id>-<thread index>`), so `remoteWorkers` in `GET /status` counts job threads, like `workers` does for the server. A per-process id would let one thread report a job that a sibling thread had re-claimed after the first thread's lease expired.
- A claimed job belongs to that worker id and has a lease (`leaseSeconds`, default 30). The worker sends a heartbeat every third of a lease; if they stop, the server puts the job back in the queue, in its original order, for another worker. Only the worker holding a job can report its result, so a late result from a worker that lost its lease is refused with `409`.
- The server's own threads use the same claim rules, but only for jobs registered on the server, under the reserved worker id `local`, and with no lease. A server with no jobs registered starts no threads and is purely a queue.
- Every HTTP request has a timeout (`defaultRequestTimeoutSeconds`, 30 s, per wait for data, not for the whole request); heartbeats use a third of the lease. Connecting has no timeout of its own beyond the operating system's.
- A worker sends a result again, with a growing pause, when the server cannot be reached or answers 5xx (a database error, or a proxy's 502). A 4xx answer, such as 409 for a lost lease, is final.
- Deploying new or changed jobs: start workers with the new code, then press ctrl-c on the old ones. They finish their current job, report it, and exit.

## HTTP API

| Request | Reply |
| --- | --- |
| `POST /jobs` with `{"name": "add", "args": {"a": 1, "b": 2}}` | `202 {"id": "...", "status": "queued"}`; `400` for bad JSON or an unknown name |
| `GET /jobs/{id}` | `200 {"id", "name", "status", "result", "error", "retries", "submittedAt", "startedAt", "finishedAt"}`; `404` if unknown |
| `GET /job-names` | `200 ["add", ...]` |
| `GET /status` | `200 {"workers": 4, "remoteWorkers": 2, "queued": 3, "running": 4, "succeeded": 120, "failed": 2}` |
| `GET /health` | `200 {"status": "ok", "queued": 3}` |
| `POST /workers/claim` with `{"workerId": "w1", "jobNames": ["add"]}` | `200 {"job": {"id", "name", "args", "retries"} or null, "leaseSeconds": 30}` |
| `POST /jobs/{id}/heartbeat` with `{"workerId": "w1"}` | `200`; `409` if the job is no longer running on that worker |
| `POST /jobs/{id}/result` with `{"workerId": "w1", "succeeded": true, "result": ...}` or `{..., "succeeded": false, "error": "..."}` (`error` may be empty or left out). A failure may add `"retry": true` and `"retryDelaySeconds": 2.0` to put the job back in the queue instead of failing it | `200`; `409` if the job is no longer running on that worker |

## Retries

- A handler raises `RetryableJobError` (or a type that inherits from it) to say a failure is worth another try. Every other exception fails the job at once.
- `registerJob(..., retryOn = @["TimeoutError"])` names further exception types to retry, so a handler need not catch and re-raise them. Names are compared with the exact type raised (`error.name`), not by inheritance: Nim does not expose an exception's parent types by name at run time. `retryOn` without `maxRetries` is rejected, since it would never retry.
- The limit belongs to the registration, not the submission: `registerJob(name, handler, maxRetries = 3, retryDelaySeconds = 1.0)`. The job's author knows which errors are transient and how long they last; callers do not have to.
- `runJob` decides. It compares the job's `retryCount` with `maxRetries` and returns an outcome with `shouldRetry` and a delay: `retryDelaySeconds`, doubled for every retry already made, capped at an hour (`maxRetryDelaySeconds`). Once the retries are used up the job fails with the last error.
- The store's `retryJob` puts the job back in the queue with `retry_count + 1` and a `run_after` time. `claimNextJob` skips queued jobs whose `run_after` has not passed. The job keeps its place (`sequence`), so once due it runs ahead of newer jobs.
- Remote workers decide the same way: the claim reply carries `retries`, and the worker reports `"retry": true` with the delay. The server trusts the worker, as it does for results; two workers registered with different `maxRetries` each apply their own.
- The server's threads block on a condition variable, which cannot time out, so `maintenanceLoop` calls `wakeForDueJobs` on each tick (every 0.1 s). A retry on the server's own threads can therefore start up to a tenth of a second late, and a store used without a running server only wakes for retries with no delay.
- Only `RetryableJobError` counts as a retry. Re-runs after a lost lease or a restart do not touch `retry_count`.
- Database files from before retries get the two new columns added on opening.

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
4. Delayed / scheduled jobs (the `run_after` column that retries use is most of this).
5. Priorities or multiple named queues.
6. A lock file so two servers cannot open the same database.
7. Authentication: at least a shared secret for workers.

Done: retries (see above).

## Open questions

- Is polling acceptable for the first version, or is long-polling needed from the start?
- How long should results be kept?
- Licence: the `.nimble` file says MIT as a placeholder.
