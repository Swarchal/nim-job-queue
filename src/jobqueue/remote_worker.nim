## A worker process that runs jobs for a server on another machine (or in
## another process on the same one).
##
##   var worker = newRemoteWorker(RemoteWorkerConfig(serverUrl: "http://queue-host:8080"))
##   worker.registerJob("add", addNumbers)
##   worker.run() # blocks until ctrl-c
##
## Each of `threadCount` threads repeatedly asks the server for a job whose
## name it knows (POST /workers/claim), runs it, and sends back the outcome
## (POST /jobs/{id}/result). One more thread sends a heartbeat for every
## running job (POST /jobs/{id}/heartbeat) so the server knows the worker is
## alive. If the heartbeats stop, the server gives the job to another worker.
##
## To deploy new or changed jobs without stopping the server: start workers
## running the new code, then press ctrl-c on the old ones. They finish the
## job they are running and exit.

import
  std/
    [httpclient, json, locks, nativesockets, options, os, strutils, sysrand, times, uri]
import ./[client, registry, types, worker]

type
  RemoteWorkerConfig* = object
    serverUrl*: string ## for example "http://queue-host:8080"
    threadCount*: int ## jobs run at the same time; 0 means 1
    workerName*: string
      ## Start of the worker id the server sees, to tell workers apart in logs.
      ## "" means "worker". The host name, process id and a random suffix are
      ## always added, e.g. "text-buildbox-4121-9f3a0c2e".
    pollIntervalSeconds*: float
      ## How long to wait before asking again when there is no job; 0 means 0.5.

  SharedState = object
    ## Shared by the job threads and the heartbeat thread, guarded by `lock`.
    lock: Lock
    stopClaiming: bool ## job threads finish their current job, then exit
    stopHeartbeats: bool ## set once every job thread has exited
    runningJobIds: seq[string] ## by job thread index; "" when idle
    leaseSeconds: float ## from the server's latest reply

  ThreadContext = object
    ## What each thread is given. `shared` and `registry` point into the
    ## RemoteWorker, which outlives the threads.
    shared: ptr SharedState
    registry: ptr JobRegistry
    serverUrl: string
    workerId: string
    jobNames: seq[string]
    threadIndex: int
    pollMilliseconds: int

  RemoteWorker* = ref object
    config: RemoteWorkerConfig
    registry: JobRegistry
    workerId: string
    shared: SharedState
    jobThreads: seq[Thread[ThreadContext]]
    heartbeatThread: Thread[ThreadContext]
    isRunning: bool
    isStopped: bool

const
  reportAttempts = 3
  reconnectPauseMilliseconds = 1000

var interruptRequested: bool ## set by the ctrl-c hook, read by run()

proc handleControlC() {.noconv.} =
  interruptRequested = true

proc log(context: ThreadContext, message: string) =
  stderr.writeLine("jobqueue worker " & context.workerId & ": " & message)

# ---------------------------------------------------------------------------
# Shared state, always read and written under its lock.
# ---------------------------------------------------------------------------

proc shouldStopClaiming(shared: ptr SharedState): bool =
  withLock shared.lock:
    result = shared.stopClaiming

proc setRunningJob(shared: ptr SharedState, threadIndex: int, jobId: string) =
  withLock shared.lock:
    shared.runningJobIds[threadIndex] = jobId

proc isRunningJob(shared: ptr SharedState, jobId: string): bool =
  withLock shared.lock:
    result = jobId in shared.runningJobIds

proc setLeaseSeconds(shared: ptr SharedState, leaseSeconds: float) =
  withLock shared.lock:
    shared.leaseSeconds = leaseSeconds

# ---------------------------------------------------------------------------
# Talking to the server.
# ---------------------------------------------------------------------------

proc jobPath(jobId: string, action: string): string =
  return "/jobs/" & encodeUrl(jobId, usePlus = false) & "/" & action

proc claimJob(
    context: ThreadContext, server: JobClient, httpClient: HttpClient
): Option[JobRecord] =
  let payload = %*{"workerId": context.workerId, "jobNames": context.jobNames}
  let reply = server.sendRequest(httpClient, HttpPost, "/workers/claim", $payload)
  context.shared.setLeaseSeconds(reply["leaseSeconds"].getFloat())
  let jobNode = reply["job"]
  if jobNode.kind == JNull:
    return none(JobRecord)
  return some(
    JobRecord(
      id: jobNode["id"].getStr(),
      name: jobNode["name"].getStr(),
      argsJson: $jobNode["args"],
    )
  )

proc reportOutcome(
    context: ThreadContext,
    server: JobClient,
    httpClient: var HttpClient,
    jobId: string,
    outcome: JobOutcome,
) =
  ## Send the outcome, retrying a few times if the server cannot be reached.
  ## Heartbeats carry on meanwhile, because the job is still marked running.
  var payload = %*{"workerId": context.workerId, "succeeded": outcome.succeeded}
  if outcome.succeeded:
    payload["result"] = parseJson(outcome.resultJson)
  else:
    payload["error"] = %outcome.errorMessage

  for attempt in 1 .. reportAttempts:
    try:
      discard
        server.sendRequest(httpClient, HttpPost, jobPath(jobId, "result"), $payload)
      return
    except JobQueueError as error:
      # The server answered but refused: retrying will not help.
      context.log("the result of job " & jobId & " was not accepted: " & error.msg)
      return
    except CatchableError as error:
      context.log("could not send the result of job " & jobId & ": " & error.msg)
      httpClient.close()
      httpClient = newJsonHttpClient()
      sleep(reconnectPauseMilliseconds)
  context.log(
    "gave up sending the result of job " & jobId &
      "; the server will run it again once its lease expires"
  )

# ---------------------------------------------------------------------------
# The threads.
# ---------------------------------------------------------------------------

proc jobThreadLoop(context: ThreadContext) {.thread.} =
  ## Claim, run, report, until told to stop. Errors talking to the server are
  ## logged and retried: an exception escaping a thread would end the process.
  let server = newJobClient(context.serverUrl)
  var httpClient = newJsonHttpClient()
  while not context.shared.shouldStopClaiming():
    try:
      let claimed = context.claimJob(server, httpClient)
      if claimed.isNone():
        sleep(context.pollMilliseconds)
        continue
      let job = claimed.get()
      context.shared.setRunningJob(context.threadIndex, job.id)
      let outcome = runJob(context.registry[], job)
      context.reportOutcome(server, httpClient, job.id, outcome)
      context.shared.setRunningJob(context.threadIndex, "")
    except CatchableError as error:
      context.log("could not get a job from the server: " & error.msg)
      httpClient.close()
      httpClient = newJsonHttpClient()
      sleep(reconnectPauseMilliseconds)
  httpClient.close()

proc heartbeatLoop(context: ThreadContext) {.thread.} =
  ## Every third of a lease, send a heartbeat for each running job.
  let server = newJobClient(context.serverUrl)
  var httpClient = newJsonHttpClient()
  var nextHeartbeatAt = 0.0
  while true:
    var runningJobIds: seq[string]
    var leaseSeconds: float
    var stop: bool
    withLock context.shared.lock:
      runningJobIds = context.shared.runningJobIds
      leaseSeconds = context.shared.leaseSeconds
      stop = context.shared.stopHeartbeats
    if stop:
      break

    var hasRunningJob = false
    for jobId in runningJobIds:
      if jobId.len > 0:
        hasRunningJob = true

    # With nothing running, leave nextHeartbeatAt alone, so a newly claimed
    # job gets its first heartbeat straight away rather than up to a third of
    # a lease later.
    if hasRunningJob and epochTime() >= nextHeartbeatAt:
      let payload = $(%*{"workerId": context.workerId})
      for jobId in runningJobIds:
        if jobId.len == 0:
          continue
        try:
          discard server.sendRequest(
            httpClient, HttpPost, jobPath(jobId, "heartbeat"), payload
          )
        except JobQueueError as error:
          # Also refused when the job finished just after the copy of
          # runningJobIds above was taken; that is not worth reporting.
          if context.shared.isRunningJob(jobId):
            context.log("lost job " & jobId & ": " & error.msg)
        except CatchableError as error:
          context.log("could not send a heartbeat: " & error.msg)
          httpClient.close()
          httpClient = newJsonHttpClient()
      nextHeartbeatAt = epochTime() + leaseSeconds / 3
    sleep(100)
  httpClient.close()

# ---------------------------------------------------------------------------
# The public API.
# ---------------------------------------------------------------------------

proc newWorkerId(name: string): string =
  ## Unique across machines and processes. (std/oids is not: two processes
  ## started in the same second can get the same oid.)
  var suffix = ""
  for randomByte in urandom(4):
    suffix.add(toHex(randomByte))
  return
    name & "-" & getHostname() & "-" & $getCurrentProcessId() & "-" &
    suffix.toLowerAscii()

proc newRemoteWorker*(config: RemoteWorkerConfig): RemoteWorker =
  result = RemoteWorker(config: config)
  var name = config.workerName
  if name.len == 0:
    name = "worker"
  result.workerId = newWorkerId(name)
  initLock(result.shared.lock)

proc workerId*(worker: RemoteWorker): string =
  ## The id this worker uses with the server.
  return worker.workerId

proc registerJob*(worker: RemoteWorker, name: string, handler: JobHandler) =
  ## Must be called before start() or run().
  if worker.isRunning or worker.isStopped:
    raise newException(ValueError, "jobs must be registered before the worker starts")
  worker.registry.registerJob(name, handler)

proc start*(worker: RemoteWorker) =
  ## Start the threads and return at once. Mainly useful for tests; run()
  ## is the usual way to use a worker.
  if worker.isRunning or worker.isStopped:
    raise newException(ValueError, "a worker can only be started once")
  if worker.registry.jobNames().len == 0:
    raise newException(ValueError, "register at least one job before starting")

  var threadCount = worker.config.threadCount
  if threadCount <= 0:
    threadCount = 1
  var pollSeconds = worker.config.pollIntervalSeconds
  if pollSeconds <= 0:
    pollSeconds = 0.5

  worker.shared.runningJobIds = newSeq[string](threadCount)
  worker.shared.leaseSeconds = 30.0
  var context = ThreadContext(
    shared: addr worker.shared,
    registry: addr worker.registry,
    serverUrl: worker.config.serverUrl,
    workerId: worker.workerId,
    jobNames: worker.registry.jobNames(),
    pollMilliseconds: int(pollSeconds * 1000),
  )

  # Size the seq once, up front: a running Thread must not be moved.
  worker.jobThreads = newSeq[Thread[ThreadContext]](threadCount)
  for threadIndex in 0 ..< threadCount:
    context.threadIndex = threadIndex
    createThread(worker.jobThreads[threadIndex], jobThreadLoop, context)
  createThread(worker.heartbeatThread, heartbeatLoop, context)
  worker.isRunning = true

proc stop*(worker: RemoteWorker) =
  ## Stop taking new jobs, wait for the running ones to finish and be
  ## reported, then stop the heartbeats. The worker cannot be restarted.
  if not worker.isRunning:
    return
  withLock worker.shared.lock:
    worker.shared.stopClaiming = true
  joinThreads(worker.jobThreads)
  withLock worker.shared.lock:
    worker.shared.stopHeartbeats = true
  joinThread(worker.heartbeatThread)
  worker.isRunning = false
  worker.isStopped = true

proc run*(worker: RemoteWorker) =
  ## start(), then wait for ctrl-c, then stop(). A second ctrl-c quits at
  ## once; the server then gives any running jobs to another worker after
  ## their lease expires.
  worker.start()
  interruptRequested = false
  setControlCHook(handleControlC)
  echo "jobqueue worker ",
    worker.workerId, ": running jobs for ", worker.config.serverUrl
  while not interruptRequested:
    sleep(200)
  unsetControlCHook()
  echo "jobqueue worker ",
    worker.workerId,
    ": finishing running jobs (press ctrl-c again to quit without waiting)"
  worker.stop()
