import std/[json, options, os, unittest]
import jobqueue/[registry, store, types, worker]
import ./helpers

proc readsPastTheEnd(args: JsonNode): JsonNode {.gcsafe, nimcall.} =
  let items = args.getElems()
  return items[items.len + 1]

proc makeRegistry(): JobRegistry =
  result.registerJob("add", addNumbers)
  result.registerJob("always-fails", alwaysFails)
  result.registerJob("reads-past-the-end", readsPastTheEnd)

type BusyError = object of RetryableJobError

proc raisesBusyError(args: JsonNode): JsonNode {.gcsafe, nimcall.} =
  raise newException(BusyError, "the service is busy")

proc makeRetryRegistry(): JobRegistry =
  result.registerJob(
    "retryable", alwaysRetryable, maxRetries = 2, retryDelaySeconds = 0.5
  )
  result.registerJob("busy", raisesBusyError, maxRetries = 2)
  result.registerJob("no-retries", alwaysRetryable)
  result.registerJob("always-fails", alwaysFails, maxRetries = 2)
  result.registerJob(
    "named-error", alwaysFails, maxRetries = 2, retryOn = @["IOError", "ValueError"]
  )
  result.registerJob(
    "other-named-error", alwaysFails, maxRetries = 2, retryOn = @["IOError"]
  )
  result.registerJob(
    "named-parent", raisesBusyError, maxRetries = 2, retryOn = @["CatchableError"]
  )
  result.registerJob(
    "named-defect", readsPastTheEnd, maxRetries = 2, retryOn = @["IndexDefect"]
  )
  result.registerJob(
    "slow-retries", alwaysRetryable, maxRetries = 100, retryDelaySeconds = 60.0
  )

suite "registry":
  test "registering the same name twice raises":
    var registry = makeRegistry()
    expect ValueError:
      registry.registerJob("add", addNumbers)

  test "jobNames is sorted":
    check makeRegistry().jobNames() == @["add", "always-fails", "reads-past-the-end"]

suite "worker.runJob":
  let registry = makeRegistry()

  test "a handler's return value becomes resultJson":
    let outcome =
      runJob(registry, JobRecord(name: "add", argsJson: """{"a": 1, "b": 2}"""))
    check outcome.succeeded
    check outcome.resultJson == "3"

  test "an exception in the handler becomes errorMessage":
    let outcome = runJob(registry, JobRecord(name: "always-fails", argsJson: "{}"))
    check not outcome.succeeded
    check outcome.errorMessage == "ValueError: this job always fails"

  test "a defect in the handler fails the job rather than crashing":
    let outcome =
      runJob(registry, JobRecord(name: "reads-past-the-end", argsJson: "[]"))
    check not outcome.succeeded

  test "an unregistered job name fails rather than raising":
    let outcome = runJob(registry, JobRecord(name: "no-such-job", argsJson: "{}"))
    check not outcome.succeeded

  test "arguments that are not valid JSON fail rather than raising":
    let outcome = runJob(registry, JobRecord(name: "add", argsJson: "{not json"))
    check not outcome.succeeded

suite "worker.runJob retries":
  let registry = makeRetryRegistry()

  test "RetryableJobError asks for a retry while the job has retries left":
    let outcome = runJob(registry, JobRecord(name: "retryable", argsJson: "{}"))
    check not outcome.succeeded
    check outcome.shouldRetry
    check outcome.retryDelaySeconds == 0.5
    check outcome.errorMessage == "RetryableJobError: try again later"

  test "the delay doubles with every retry, up to maxRetryDelaySeconds":
    let secondRetry =
      runJob(registry, JobRecord(name: "retryable", argsJson: "{}", retryCount: 1))
    check secondRetry.shouldRetry
    check secondRetry.retryDelaySeconds == 1.0

    let lateRetry =
      runJob(registry, JobRecord(name: "slow-retries", argsJson: "{}", retryCount: 50))
    check lateRetry.shouldRetry
    check lateRetry.retryDelaySeconds == maxRetryDelaySeconds

  test "the job fails once its retries are used up":
    let outcome =
      runJob(registry, JobRecord(name: "retryable", argsJson: "{}", retryCount: 2))
    check not outcome.succeeded
    check not outcome.shouldRetry
    check outcome.errorMessage ==
      "RetryableJobError: try again later (gave up after 2 retries)"

  test "an error type that inherits from RetryableJobError is retried too":
    let outcome = runJob(registry, JobRecord(name: "busy", argsJson: "{}"))
    check outcome.shouldRetry
    check outcome.errorMessage == "BusyError: the service is busy"

  test "a job registered without maxRetries fails at once":
    let outcome = runJob(registry, JobRecord(name: "no-retries", argsJson: "{}"))
    check not outcome.shouldRetry
    check outcome.errorMessage == "RetryableJobError: try again later"

  test "any other exception fails at once, whatever maxRetries is":
    let outcome = runJob(registry, JobRecord(name: "always-fails", argsJson: "{}"))
    check not outcome.succeeded
    check not outcome.shouldRetry

  test "an error named in retryOn is retried, then fails when retries run out":
    let firstRun = runJob(registry, JobRecord(name: "named-error", argsJson: "{}"))
    check firstRun.shouldRetry
    check firstRun.errorMessage == "ValueError: this job always fails"

    let lastRun =
      runJob(registry, JobRecord(name: "named-error", argsJson: "{}", retryCount: 2))
    check not lastRun.shouldRetry
    check lastRun.errorMessage ==
      "ValueError: this job always fails (gave up after 2 retries)"

  test "an error that retryOn does not name fails at once":
    let outcome = runJob(registry, JobRecord(name: "other-named-error", argsJson: "{}"))
    check not outcome.succeeded
    check not outcome.shouldRetry

  test "retryOn matches the exact type, but RetryableJobError still counts":
    # BusyError is a CatchableError, but only by inheritance; it is retried
    # here because it also inherits from RetryableJobError.
    check runJob(registry, JobRecord(name: "named-parent", argsJson: "{}")).shouldRetry
    # ValueError is a CatchableError too, and is not retried.
    var parentOnly: JobRegistry
    parentOnly.registerJob(
      "fails", alwaysFails, maxRetries = 2, retryOn = @["CatchableError"]
    )
    check not runJob(parentOnly, JobRecord(name: "fails", argsJson: "{}")).shouldRetry

  test "a defect named in retryOn is retried":
    let outcome = runJob(registry, JobRecord(name: "named-defect", argsJson: "[]"))
    check outcome.shouldRetry

  test "retryOn without maxRetries, or with an empty name, is rejected":
    var badRegistry: JobRegistry
    expect ValueError:
      badRegistry.registerJob("add", addNumbers, retryOn = @["IOError"])
    expect ValueError:
      badRegistry.registerJob("add", addNumbers, maxRetries = 1, retryOn = @[""])

  test "negative retry settings are rejected":
    var badRegistry: JobRegistry
    expect ValueError:
      badRegistry.registerJob("add", addNumbers, maxRetries = -1)
    expect ValueError:
      badRegistry.registerJob("add", addNumbers, retryDelaySeconds = -1.0)

proc waitForStatus(store: var JobStore, jobId: string, status: JobStatus) =
  ## Wait, at most ten seconds, for the job to reach `status`.
  for attempt in 0 ..< 1000:
    if store.getJob(jobId).get().status == status:
      return
    sleep(10)

suite "worker threads":
  test "a job is retried by the pool until its retries are used up":
    var registry: JobRegistry
    registry.registerJob(
      "retryable", alwaysRetryable, maxRetries = 3, retryDelaySeconds = 0.0
    )
    var store: JobStore
    initJobStore(store)
    var threads: seq[Thread[WorkerContext]]
    startWorkers(threads, addr store, addr registry, workerCount = 2)

    let jobId = store.addJob("retryable", "{}")
    store.waitForStatus(jobId, jsFailed)
    stopWorkers(threads, addr store)

    let job = store.getJob(jobId).get()
    check job.status == jsFailed
    check job.retryCount == 3
    check job.errorMessage ==
      "RetryableJobError: try again later (gave up after 3 retries)"
    closeJobStore(store)

  test "a pool of workers runs every queued job exactly once":
    var registry = makeRegistry()
    var store: JobStore
    initJobStore(store)
    var threads: seq[Thread[WorkerContext]]
    startWorkers(threads, addr store, addr registry, workerCount = 4)

    var jobIds: seq[string] = @[]
    for number in 0 ..< 200:
      jobIds.add(store.addJob("add", $(%*{"a": number, "b": 1000})))

    # Wait (at most ten seconds) for the last job, then stop the pool, which
    # lets any job that is still running finish.
    store.waitForStatus(jobIds[^1], jsSucceeded)
    stopWorkers(threads, addr store)

    for number in 0 ..< 200:
      let job = store.getJob(jobIds[number]).get()
      check job.status == jsSucceeded
      check job.resultJson == $(number + 1000)
