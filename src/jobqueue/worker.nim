## The server's own worker pool: threads that take jobs from the store and
## run them. `runJob` is also used by remote workers (remote_worker.nim).

import std/[json, math, options, os]
import ./[registry, store, types]

const localWorkerId* = "local"
  ## The worker id of the server's own threads. Remote workers may not use it.

const storeErrorPauseMilliseconds = 1000
  ## How long a worker waits after a store error before trying again, so a
  ## broken database does not turn into a busy loop of error messages.

type
  JobOutcome* = object
    succeeded*: bool
    resultJson*: string ## set when succeeded
    errorMessage*: string ## set when not succeeded
    shouldRetry*: bool
      ## Not succeeded, but the job should go back in the queue rather than
      ## be marked failed: the handler raised RetryableJobError and the job
      ## has retries left.
    retryDelaySeconds*: float ## set when shouldRetry: how long to wait first

  WorkerContext* = object
    ## What each thread is given at start-up. The pointers refer to fields of
    ## the JobQueue, which outlives the threads.
    store*: ptr JobStore
    registry*: ptr JobRegistry

proc failedOutcome(errorMessage: string): JobOutcome =
  return JobOutcome(succeeded: false, errorMessage: errorMessage)

proc retryDelay(registeredJob: RegisteredJob, retryCount: int): float =
  ## The wait before retry number `retryCount + 1`: the registered delay,
  ## doubled for every retry already made, up to maxRetryDelaySeconds.
  let doubled = registeredJob.retryDelaySeconds * pow(2.0, float(retryCount))
  return min(doubled, maxRetryDelaySeconds)

proc isRetryable(registeredJob: RegisteredJob, error: ref Exception): bool =
  ## RetryableJobError and the types that inherit from it always are; other
  ## errors only if the job was registered with their exact name in `retryOn`.
  return error of RetryableJobError or $error.name in registeredJob.retryOn

proc runJob*(registry: JobRegistry, job: JobRecord): JobOutcome {.gcsafe.} =
  ## Look up the handler, parse the arguments, call the handler and catch
  ## anything it raises. Never raises itself. Kept separate from the thread
  ## loop so it can be unit tested without threads.
  ##
  ## Whether a failure is retried is decided here, from the handler's
  ## exception, the job's registration and `job.retryCount`.
  let registered = registry.findJob(job.name)
  if registered.isNone():
    return failedOutcome(unknownJobMessage(job.name))
  let registeredJob = registered.get()

  var args: JsonNode
  try:
    args = parseJson(job.argsJson)
  except CatchableError as error:
    return failedOutcome("job arguments are not valid JSON: " & error.msg)

  try:
    let handlerResult = registeredJob.handler(args)
    if handlerResult.isNil():
      return JobOutcome(succeeded: true, resultJson: "null")
    return JobOutcome(succeeded: true, resultJson: $handlerResult)
  except Exception as error:
    # Exception rather than CatchableError, so that a bug in one job (an
    # index out of range, say) fails that job instead of killing the server.
    var message = $error.name & ": " & error.msg
    if not isRetryable(registeredJob, error):
      return failedOutcome(message)
    if job.retryCount < registeredJob.maxRetries:
      return JobOutcome(
        succeeded: false,
        errorMessage: message,
        shouldRetry: true,
        retryDelaySeconds: retryDelay(registeredJob, job.retryCount),
      )
    if job.retryCount > 0:
      message.add(" (gave up after " & $job.retryCount & " retries)")
    return failedOutcome(message)

proc runNextJob(context: WorkerContext, jobNames: seq[string]): bool =
  ## Take one job, run it and record the outcome. Returns false once the
  ## store is shutting down. Errors from the store itself (a full disk, a
  ## locked database file) are raised.
  let nextJob = context.store[].takeNextJob(jobNames, localWorkerId)
  if nextJob.isNone():
    return false

  # These jobs have no lease, so they stay ours and recording cannot fail.
  let job = nextJob.get()
  let outcome = runJob(context.registry[], job)
  if outcome.succeeded:
    discard context.store[].markSucceeded(job.id, localWorkerId, outcome.resultJson)
  elif outcome.shouldRetry:
    discard context.store[].retryJob(job.id, localWorkerId, outcome.retryDelaySeconds)
  else:
    discard context.store[].markFailed(job.id, localWorkerId, outcome.errorMessage)
  return true

proc workerLoop*(context: WorkerContext) {.thread.} =
  ## Repeat runNextJob until the store shuts down.
  ##
  ## An exception escaping a thread would end the whole process, so a store
  ## error is reported and the loop carries on after a short pause. A job
  ## whose outcome could not be saved stays "running"; with SQLite it is run
  ## again on the next start.
  ##
  ## Only jobs registered on the server are taken; the rest are left for
  ## remote workers.
  let jobNames = context.registry[].jobNames()
  while true:
    try:
      if not runNextJob(context, jobNames):
        break
    except CatchableError as error:
      stderr.writeLine("jobqueue: worker could not use the job store: " & error.msg)
      sleep(storeErrorPauseMilliseconds)

proc startWorkers*(
    threads: var seq[Thread[WorkerContext]],
    store: ptr JobStore,
    registry: ptr JobRegistry,
    workerCount: int,
) =
  # Size the seq once, up front: a running Thread must not be moved.
  threads = newSeq[Thread[WorkerContext]](workerCount)
  let context = WorkerContext(store: store, registry: registry)
  for workerIndex in 0 ..< workerCount:
    createThread(threads[workerIndex], workerLoop, context)

proc stopWorkers*(threads: var seq[Thread[WorkerContext]], store: ptr JobStore) =
  ## Shut the store down, then join every thread. Jobs that are already
  ## running are allowed to finish.
  store[].shutdown()
  joinThreads(threads)
  threads.setLen(0)
