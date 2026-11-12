## Types shared by every module.

import std/json

type
  JobStatus* = enum
    jsQueued = "queued"
    jsRunning = "running"
    jsSucceeded = "succeeded"
    jsFailed = "failed"

  JobRecord* = object
    ## Everything the server knows about one job.
    ##
    ## Arguments and results are kept as JSON *text* rather than `JsonNode`.
    ## `JsonNode` is a ref type and is not safe to share between the HTTP
    ## thread and the worker threads; plain strings are copied, so they are.
    id*: string
    name*: string
    status*: JobStatus
    argsJson*: string
    resultJson*: string ## only set when status is jsSucceeded
    errorMessage*: string ## only set when status is jsFailed
    submittedAt*: float ## unix time in seconds
    startedAt*: float ## 0.0 until a worker picks the job up
    finishedAt*: float ## 0.0 until the job succeeds or fails
    workerId*: string ## who is running the job; "" unless status is jsRunning
    leaseExpiresAt*: float
      ## For a job running on a remote worker: unix time after which the job
      ## is put back in the queue unless the worker sends a heartbeat.
      ## 0.0 for jobs on the server's own threads, which never expire.

  JobHandler* = proc(args: JsonNode): JsonNode {.gcsafe, nimcall.}
    ## The signature every job must have. Raise any exception to fail the job;
    ## the exception message is reported back to the caller.

  JobQueueConfig* = object
    host*: string ## address to bind, "" means all interfaces
    port*: int
    workerCount*: int
      ## Number of worker threads, 0 means one per CPU core. No threads are
      ## started if no jobs are registered on the server itself: it then only
      ## queues jobs for remote workers.
    databasePath*: string
      ## SQLite file to keep jobs in, so they survive a restart.
      ## "" (the default) keeps them in memory only.
    maxFinishedJobs*: int
      ## Keep at most this many succeeded or failed jobs; beyond that, the
      ## ones that finished longest ago are removed and GET /jobs/{id} gives
      ## 404 for them. 0 (the default) keeps every finished job.
    leaseSeconds*: float
      ## How long a remote worker may go without a heartbeat before its job
      ## is put back in the queue for another worker. 0 means 30 seconds.
