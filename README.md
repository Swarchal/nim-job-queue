# nim job queue

This is a server which accepts nim jobs, runs them, and returns the results to a caller.

It acts much like celery in python.

Jobs are ordinary Nim procs compiled into the server, or into remote worker
processes, and registered by name.
Callers submit a job name with JSON arguments over HTTP and poll for the result.

## Server

```nim
import std/json
import jobqueue

proc addNumbers(args: JsonNode): JsonNode {.gcsafe, nimcall.} =
  return %(args["a"].getInt() + args["b"].getInt())

var queue = newJobQueue(JobQueueConfig(host: "127.0.0.1", port: 8080, workerCount: 4))
queue.registerJob("add", addNumbers)
queue.serve() # blocks until ctrl-c
```

A job fails if its proc raises; the exception message is returned to the caller.

## Retrying on certain errors

A job that raises `RetryableJobError` (or an error type that inherits from
it) is put back in the queue and run again, if it was registered with
`maxRetries`. Any other exception still fails the job at once, so the job
decides which of its errors are worth another try:

```nim
proc fetchPage(args: JsonNode): JsonNode {.gcsafe, nimcall.} =
  try:
    return %download(args["url"].getStr())
  except TimeoutError as error:
    # The site may answer next time.
    raise newException(RetryableJobError, error.msg)
  # A ValueError for a malformed url, say, is not caught: no retry.

queue.registerJob("fetch-page", fetchPage, maxRetries = 3, retryDelaySeconds = 2.0)
```

Errors can also be named when the job is registered, so the handler does
not have to catch them:

```nim
queue.registerJob(
  "fetch-page", fetchPage, maxRetries = 3, retryOn = @["TimeoutError", "OSError"]
)
```

The names are compared with the exact type of the exception raised, so
`"OSError"` does not cover an `IOError` or a type that inherits from
`OSError`, and a misspelt name never matches. `RetryableJobError` keeps
working alongside `retryOn`.

The job waits `retryDelaySeconds` (default 1) before the first retry and
twice as long before each later one (2, 4, 8 seconds here), up to an hour.
After `maxRetries` retries it fails with the last error. `GET /jobs/{id}`
and `JobInfo.retries` say how many retries a job has had. Remote workers
take the same arguments in `worker.registerJob`.

`examples/example_server.nim` has a `flaky-greeting` job that does this, and
`examples/example_client.nim` submits it.

## Remote workers

Jobs can also run in separate worker processes, on any machine that can
reach the server. The server can then run no jobs of its own and act purely
as a queue, and you can add or change jobs without restarting it: start
workers with the new code, then press ctrl-c on the old ones, which finish
their current job and exit.

```nim
# On the server machine: no jobs registered, so it only queues them.
var queue = newJobQueue(JobQueueConfig(host: "", port: 8080))
queue.serve()

# On each worker machine:
var worker = newRemoteWorker(
  RemoteWorkerConfig(serverUrl: "http://queue-host:8080", threadCount: 4)
)
worker.registerJob("add", addNumbers)
worker.run() # blocks until ctrl-c
```

Workers announce the job names they can run, so callers can submit those
names once a worker has connected. Each running job has a lease
(`leaseSeconds`, default 30) that the worker keeps alive with heartbeats; if
a worker dies, its job goes back in the queue for another worker. A job can
therefore run more than once, so jobs with side effects should be safe to
repeat.

There is no authentication: anyone who can reach the server can submit jobs
or act as a worker. Only expose it on a network you trust.

`examples/queue_server.nim` and `examples/text_worker.nim` show this with
the pipeline example:

```sh
nim r examples/queue_server.nim      # one terminal
nim r examples/text_worker.nim       # one or more terminals or machines
nim r examples/pipeline_client.nim   # submits the jobs
```

## Limiting how many finished jobs are kept

Finished jobs (succeeded or failed) are kept forever by default. Set
`maxFinishedJobs` to keep only the most recent ones; once there are more, the
jobs that finished longest ago are removed:

```nim
var queue = newJobQueue(JobQueueConfig(port: 8080, workerCount: 4, maxFinishedJobs: 10_000))
```

Asking for a removed job gives 404, so keep the limit well above the number
of jobs that finish while a caller is waiting. Queued and running jobs are
never removed.

## Keeping jobs across restarts

By default jobs live in memory and are lost when the server stops. Give a
database path to keep them in a SQLite file instead:

```nim
var queue = newJobQueue(
  JobQueueConfig(port: 8080, workerCount: 4, databasePath: "jobs.sqlite")
)
```

Queued jobs then run after a restart. A job that was running when the server
died is run again, so jobs with side effects should be safe to repeat.

Jobs are always kept with SQLite (in memory unless a path is given), so the
server needs `libsqlite3` on the machine and the `db_connector` nimble package.

## Client

```nim
import std/json
import jobqueue

let client = newJobClient("http://127.0.0.1:8080")
let jobId = client.submitJob("add", %*{"a": 1, "b": 2})
let job = client.waitForJob(jobId, timeoutSeconds = 30.0)

if job.status == jsSucceeded:
  echo job.result # 3
else:
  echo job.errorMessage
```

Or from anything that speaks HTTP:

```sh
curl -X POST localhost:8080/jobs -d '{"name": "add", "args": {"a": 1, "b": 2}}'
curl localhost:8080/jobs/<id>
curl localhost:8080/status
```

From Nim, `echo client.getStatus()` prints
`workers=4 remoteWorkers=2 queued=3 running=4 succeeded=120 failed=2`;
`examples/print_status.nim` does this once a second.

| Request | Reply |
| --- | --- |
| `POST /jobs` | `202 {"id": "...", "status": "queued"}` |
| `GET /jobs/{id}` | `200 {"id", "name", "status", "result", "error", ...}` |
| `GET /job-names` | `200 ["add", ...]` |
| `GET /status` | `200 {"workers": 4, "remoteWorkers": 2, "queued": 3, "running": 4, "succeeded": 120, "failed": 2}` |
| `GET /health` | `200 {"status": "ok", "queued": 0}` |

## Examples and tests

`examples/text_jobs.nim`, `pipeline_server.nim` and `pipeline_client.nim` show
a shared job module and a client that fans jobs out and chains their results.

```sh
nim r examples/pipeline_server.nim   # in one terminal
nim r examples/pipeline_client.nim   # in another
nimble test
```

See `PLAN.md` for the design, its limitations, and what comes next.
