## Types shared by every module.

import std/json

const
  defaultRetryDelaySeconds* = 1.0
    ## How long a job waits before its first retry, unless registered with
    ## another `retryDelaySeconds`.
  maxRetryDelaySeconds* = 3600.0
    ## The wait doubles with every retry of a job, but never exceeds this.

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
    retryCount*: int
      ## How many times the job has been put back in the queue because its
      ## handler raised RetryableJobError.
    runAfter*: float
      ## Unix time before which a queued job is not handed out: set when a
      ## retry has to wait. 0.0 for a job that can run straight away.
    leaseExpiresAt*: float
      ## For a job running on a remote worker: unix time after which the job
      ## is put back in the queue unless the worker sends a heartbeat.
      ## 0.0 for jobs on the server's own threads, which never expire.

  JobHandler* = proc(args: JsonNode): JsonNode {.gcsafe, nimcall.}
    ## The signature every job must have. Raise any exception to fail the job;
    ## the exception message is reported back to the caller. Raise
    ## RetryableJobError instead, or an exception named in the job's
    ## `retryOn`, to have the job run again later, if it was registered with
    ## `maxRetries`.

  RetryableJobError* = object of CatchableError
    ## Raised by a handler (directly, or as a type that inherits from it) to
    ## say "this failure may not happen next time": a network timeout, a busy
    ## service. The job goes back in the queue, up to the `maxRetries` it was
    ## registered with; after that, or when `maxRetries` is 0, it fails like
    ## any other error. Typical use:
    ##
    ##   try:
    ##     return download(args["url"].getStr())
    ##   except IOError as error:
    ##     raise newException(RetryableJobError, error.msg)

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
