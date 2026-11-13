## The public server object and its HTTP API.
##
##   POST /jobs        {"name": "add", "args": {...}}  -> 202 {"id": ..., "status": "queued"}
##   GET  /jobs/{id}                                   -> 200 job as JSON, or 404
##   GET  /job-names                                   -> 200 ["add", ...]
##   GET  /status                                      -> 200 {"workers": 4, "queued": 3, "running": 4,
##                                                            "succeeded": 120, "failed": 2}
##   GET  /health                                      -> 200 {"status": "ok", "queued": 3}
##
## Used by remote workers (see remote_worker.nim):
##
##   POST /workers/claim     {"workerId": "w1", "jobNames": ["add"]}
##                           -> 200 {"job": {"id", "name", "args"} or null, "leaseSeconds": 30}
##   POST /jobs/{id}/heartbeat  {"workerId": "w1"}  -> 200, or 409 if the job is no longer theirs
##   POST /jobs/{id}/result     {"workerId": "w1", "succeeded": true, "result": ...}
##                           or {"workerId": "w1", "succeeded": false, "error": "..."}
##                           ("error" may be left out)
##                           or {"workerId": "w1", "succeeded": false, "error": "...",
##                               "retry": true, "retryDelaySeconds": 2.0}
##                           to put the job back in the queue
##                           -> 200, or 409 if the job is no longer theirs

import
  std/[
    algorithm, asyncdispatch, asynchttpserver, cpuinfo, json, options, sequtils, sets,
    strutils, tables, times, uri,
  ]
import ./[registry, store, types, worker]

type
  QueueState = enum
    qsNew ## created, jobs can be registered
    qsRunning ## start() has been called
    qsStopped ## stop() has been called; the queue cannot be used again

  JobQueue* = ref object
    config: JobQueueConfig
    registry: JobRegistry
    store: JobStore
    workerThreads: seq[Thread[WorkerContext]]
    httpServer: AsyncHttpServer
    state: QueueState
    remoteJobNames: HashSet[string]
      ## Names announced by remote workers since start-up. Jobs with these
      ## names are accepted even though the server cannot run them itself.
    remoteWorkerLastSeen: Table[string, float]
      ## Worker id -> unix time of its last claim or heartbeat.

  BadRequestError = object of ValueError
    ## Raised while reading a request body; routeRequest turns it into a 400.

  HttpReply* = object
    code*: HttpCode
    body*: string ## always JSON

const jobPathPrefix = "/jobs/"

const defaultLeaseSeconds = 30.0

const maintenanceIntervalMilliseconds = 100
  ## How often expired leases and due retries are looked for.

const noErrorMessage = "the worker reported a failure without an error message"

var interruptRequested: bool ## set by the ctrl-c hook, read by serve()

proc handleControlC() {.noconv.} =
  interruptRequested = true

proc newJobQueue*(config: JobQueueConfig): JobQueue =
  result = JobQueue(config: config)
  initJobStore(result.store, config.databasePath, config.maxFinishedJobs)

proc registerJob*(
    queue: JobQueue,
    name: string,
    handler: JobHandler,
    maxRetries: int = 0,
    retryDelaySeconds: float = defaultRetryDelaySeconds,
    retryOn: seq[string] = @[],
) =
  ## Must be called before start() or serve().
  ##
  ## When the handler raises RetryableJobError, or an exception whose type is
  ## named in `retryOn` (e.g. @["TimeoutError", "OSError"]), the job is run
  ## again, up to `maxRetries` times, waiting `retryDelaySeconds` before the
  ## first retry and twice as long before each later one. Any other exception
  ## fails the job at once.
  ##
  ## `retryOn` names are compared with the exact type raised: "OSError" does
  ## not cover an IOError or a type that inherits from OSError.
  if queue.state != qsNew:
    raise newException(ValueError, "jobs must be registered before the server starts")
  queue.registry.registerJob(name, handler, maxRetries, retryDelaySeconds, retryOn)

proc leaseSeconds(queue: JobQueue): float =
  if queue.config.leaseSeconds > 0:
    return queue.config.leaseSeconds
  return defaultLeaseSeconds

proc isKnownJobName(queue: JobQueue, name: string): bool =
  return queue.registry.hasJob(name) or name in queue.remoteJobNames

proc allJobNames(queue: JobQueue): seq[string] =
  ## Registered on the server or announced by a remote worker, sorted.
  var names = toHashSet(queue.registry.jobNames())
  names.incl(queue.remoteJobNames)
  return names.toSeq().sorted()

proc timestampToJson(unixSeconds: float): JsonNode =
  if unixSeconds == 0.0:
    return newJNull()
  return %unixSeconds

proc jobToJson(job: JobRecord): JsonNode =
  ## The JSON shape returned by GET /jobs/{id} and parsed by the client:
  ## {"id", "name", "status", "result", "error", "retries",
  ##  "submittedAt", "startedAt", "finishedAt"}
  var resultNode = newJNull()
  if job.status == jsSucceeded:
    resultNode = parseJson(job.resultJson)

  var errorNode = newJNull()
  if job.status == jsFailed:
    errorNode = %job.errorMessage

  return %*{
    "id": job.id,
    "name": job.name,
    "status": $job.status,
    "result": resultNode,
    "error": errorNode,
    "retries": job.retryCount,
    "submittedAt": timestampToJson(job.submittedAt),
    "startedAt": timestampToJson(job.startedAt),
    "finishedAt": timestampToJson(job.finishedAt),
  }

proc jsonReply(code: HttpCode, body: JsonNode): HttpReply =
  return HttpReply(code: code, body: $body)

proc errorReply(code: HttpCode, message: string): HttpReply =
  return jsonReply(code, %*{"error": message})

proc wrongMethodReply(expectedMethod: HttpMethod, path: string): HttpReply =
  return errorReply(Http405, "use " & $expectedMethod & " for " & path)

proc parseObjectBody(body: string): JsonNode =
  ## Raises BadRequestError unless `body` is a JSON object.
  try:
    result = parseJson(body)
  except CatchableError:
    raise newException(BadRequestError, "request body is not valid JSON")
  if result.kind != JObject:
    raise newException(BadRequestError, "request body must be a JSON object")

proc requireString(request: JsonNode, key: string): string =
  ## Raises BadRequestError unless `request[key]` is a non-empty string.
  if not request.hasKey(key) or request[key].kind != JString or
      request[key].getStr().len == 0:
    raise newException(BadRequestError, "request body needs a string '" & key & "'")
  return request[key].getStr()

proc requireRemoteWorkerId(request: JsonNode): string =
  result = requireString(request, "workerId")
  if result == localWorkerId:
    raise
      newException(BadRequestError, "the worker id '" & localWorkerId & "' is reserved")

proc notYourJobReply(jobId: string, workerId: string): HttpReply =
  return errorReply(
    Http409,
    "job '" & jobId & "' is not running on worker '" & workerId &
      "' (its lease may have expired)",
  )

proc submitJobReply(queue: JobQueue, body: string): HttpReply =
  let request = parseObjectBody(body)
  let name = requireString(request, "name")
  if not queue.isKnownJobName(name):
    return errorReply(Http400, unknownJobMessage(name))

  var args = newJObject()
  if request.hasKey("args"):
    args = request["args"]

  let jobId = queue.store.addJob(name, $args)
  return jsonReply(Http202, %*{"id": jobId, "status": $jsQueued})

proc getJobReply(queue: JobQueue, jobId: string): HttpReply =
  let job = queue.store.getJob(jobId)
  if job.isNone():
    var message = "no job with id '" & jobId & "'"
    if queue.config.maxFinishedJobs > 0:
      message.add(
        " (only the last " & $queue.config.maxFinishedJobs & " finished jobs are kept)"
      )
    return errorReply(Http404, message)
  return jsonReply(Http200, jobToJson(job.get()))

proc claimJobReply(queue: JobQueue, body: string): HttpReply =
  let request = parseObjectBody(body)
  let workerId = requireRemoteWorkerId(request)
  if not request.hasKey("jobNames") or request["jobNames"].kind != JArray or
      request["jobNames"].len == 0:
    raise
      newException(BadRequestError, "request body needs a non-empty list 'jobNames'")
  var jobNames: seq[string]
  for nameNode in request["jobNames"]:
    if nameNode.kind != JString or nameNode.getStr().len == 0:
      raise newException(BadRequestError, "'jobNames' must hold non-empty strings")
    jobNames.add(nameNode.getStr())
  # Only remember the names once the whole request is known to be valid, so a
  # rejected request cannot make a name submittable.
  for name in jobNames:
    queue.remoteJobNames.incl(name)
  queue.remoteWorkerLastSeen[workerId] = epochTime()

  let job = queue.store.claimJob(jobNames, workerId, queue.leaseSeconds())
  var jobNode = newJNull()
  if job.isSome():
    jobNode = %*{
      "id": job.get().id,
      "name": job.get().name,
      "args": parseJson(job.get().argsJson),
      "retries": job.get().retryCount,
    }
  return jsonReply(Http200, %*{"job": jobNode, "leaseSeconds": queue.leaseSeconds()})

proc heartbeatReply(queue: JobQueue, jobId: string, body: string): HttpReply =
  let request = parseObjectBody(body)
  let workerId = requireRemoteWorkerId(request)
  queue.remoteWorkerLastSeen[workerId] = epochTime()
  if not queue.store.extendLease(jobId, workerId, queue.leaseSeconds()):
    return notYourJobReply(jobId, workerId)
  return jsonReply(Http200, %*{"leaseSeconds": queue.leaseSeconds()})

proc failureMessage(request: JsonNode): string =
  ## The "error" of a failure report. It may be missing or empty: refusing the
  ## report would leave the job running until its lease expired, after which
  ## it would run, and fail, all over again.
  if not request.hasKey("error") or request["error"].kind == JNull:
    return noErrorMessage
  if request["error"].kind != JString:
    raise newException(BadRequestError, "'error' must be a string")
  if request["error"].getStr().len == 0:
    return noErrorMessage
  return request["error"].getStr()

proc retryRequested(request: JsonNode): bool =
  ## The optional "retry" of a failure report.
  if not request.hasKey("retry"):
    return false
  if request["retry"].kind != JBool:
    raise newException(BadRequestError, "'retry' must be a boolean")
  return request["retry"].getBool()

proc retryDelaySeconds(request: JsonNode): float =
  ## The optional "retryDelaySeconds" of a failure report that asks for a retry.
  if not request.hasKey("retryDelaySeconds"):
    return 0.0
  let delayNode = request["retryDelaySeconds"]
  if delayNode.kind notin {JInt, JFloat} or delayNode.getFloat() < 0:
    raise newException(
      BadRequestError, "'retryDelaySeconds' must be a number that is not negative"
    )
  return delayNode.getFloat()

proc resultReply(queue: JobQueue, jobId: string, body: string): HttpReply =
  let request = parseObjectBody(body)
  let workerId = requireRemoteWorkerId(request)
  if not request.hasKey("succeeded") or request["succeeded"].kind != JBool:
    raise newException(BadRequestError, "request body needs a boolean 'succeeded'")
  queue.remoteWorkerLastSeen[workerId] = epochTime()

  var recorded: bool
  if request["succeeded"].getBool():
    var resultNode = newJNull()
    if request.hasKey("result"):
      resultNode = request["result"]
    recorded = queue.store.markSucceeded(jobId, workerId, $resultNode)
  elif retryRequested(request):
    recorded = queue.store.retryJob(jobId, workerId, retryDelaySeconds(request))
  else:
    recorded = queue.store.markFailed(jobId, workerId, failureMessage(request))

  if not recorded:
    return notYourJobReply(jobId, workerId)
  return jsonReply(Http200, %*{"id": jobId})

proc remoteWorkerCount(queue: JobQueue): int =
  ## Remote workers heard from within the last lease period.
  let cutoff = epochTime() - queue.leaseSeconds()
  for lastSeen in queue.remoteWorkerLastSeen.values():
    if lastSeen >= cutoff:
      result += 1

proc statusToJson(queue: JobQueue): JsonNode =
  ## {"workers": 4, "remoteWorkers": 2, "queued": 3, "running": 4,
  ##  "succeeded": 120, "failed": 2}
  ## There is one key per JobStatus, so a new status shows up automatically.
  result =
    %*{"workers": queue.workerThreads.len, "remoteWorkers": queue.remoteWorkerCount()}
  let counts = queue.store.countByStatus()
  for status in JobStatus:
    result[$status] = %counts[status]

proc dispatchRequest(
    queue: JobQueue, httpMethod: HttpMethod, path: string, body: string
): HttpReply =
  if path == "/jobs":
    if httpMethod != HttpPost:
      return wrongMethodReply(HttpPost, path)
    return submitJobReply(queue, body)
  elif path == "/health":
    if httpMethod != HttpGet:
      return wrongMethodReply(HttpGet, path)
    return jsonReply(Http200, %*{"status": "ok", "queued": queue.store.queuedCount()})
  elif path == "/status":
    if httpMethod != HttpGet:
      return wrongMethodReply(HttpGet, path)
    return jsonReply(Http200, statusToJson(queue))
  elif path == "/job-names":
    if httpMethod != HttpGet:
      return wrongMethodReply(HttpGet, path)
    return jsonReply(Http200, %queue.allJobNames())
  elif path == "/workers/claim":
    if httpMethod != HttpPost:
      return wrongMethodReply(HttpPost, path)
    return claimJobReply(queue, body)
  elif path.startsWith(jobPathPrefix) and path.len > jobPathPrefix.len:
    # "/jobs/{id}" or "/jobs/{id}/{action}". The id is percent-encoded by
    # the clients, so it never contains a "/" itself.
    let rest = path[jobPathPrefix.len .. ^1]
    let slashIndex = rest.find('/')
    if slashIndex < 0:
      if httpMethod != HttpGet:
        return wrongMethodReply(HttpGet, path)
      return getJobReply(queue, decodeUrl(rest, decodePlus = false))

    let jobId = decodeUrl(rest[0 ..< slashIndex], decodePlus = false)
    let action = rest[slashIndex + 1 .. ^1]
    if action == "heartbeat":
      if httpMethod != HttpPost:
        return wrongMethodReply(HttpPost, path)
      return heartbeatReply(queue, jobId, body)
    elif action == "result":
      if httpMethod != HttpPost:
        return wrongMethodReply(HttpPost, path)
      return resultReply(queue, jobId, body)
    else:
      return errorReply(Http404, "unknown path '" & path & "'")
  else:
    return errorReply(Http404, "unknown path '" & path & "'")

proc routeRequest*(
    queue: JobQueue, httpMethod: HttpMethod, path: string, body: string
): HttpReply {.gcsafe.} =
  ## All of the HTTP behaviour, with no sockets involved, so the API can be
  ## unit tested by calling this directly. Bad JSON and unknown job names give
  ## 400, unknown paths and ids give 404, a known path with the wrong method
  ## gives 405, a worker reporting on a job that is no longer theirs gets 409,
  ## any request after stop() gives 503, and an unexpected error (from the
  ## database, say) gives 500. Never raises.
  if queue.state == qsStopped:
    return errorReply(Http503, "the server has stopped")
  try:
    return dispatchRequest(queue, httpMethod, path, body)
  except BadRequestError as error:
    return errorReply(Http400, error.msg)
  except CatchableError as error:
    stderr.writeLine("jobqueue: request failed: " & error.msg)
    return errorReply(Http500, "internal error: " & error.msg)

proc acceptLoop(queue: JobQueue) {.async.} =
  # asynchttpserver needs its callback to be a closure; this one only hands
  # the request to routeRequest and sends back whatever it returns.
  proc handleRequest(request: Request): Future[void] {.closure, gcsafe.} =
    let reply = routeRequest(queue, request.reqMethod, request.url.path, request.body)
    let headers = newHttpHeaders({"Content-Type": "application/json"})
    return request.respond(reply.code, reply.body, headers)

  while queue.state == qsRunning:
    try:
      await queue.httpServer.acceptRequest(handleRequest)
    except CatchableError as error:
      if queue.state == qsRunning:
        stderr.writeLine("jobqueue: could not accept a connection: " & error.msg)
        await sleepAsync(100)

proc maintenanceLoop(queue: JobQueue) {.async.} =
  ## Every so often, put jobs whose remote worker has stopped sending
  ## heartbeats back in the queue, and wake the worker threads for jobs whose
  ## wait before a retry is over.
  while queue.state == qsRunning:
    try:
      let requeuedCount = queue.store.requeueExpiredLeases()
      if requeuedCount > 0:
        stderr.writeLine(
          "jobqueue: put " & $requeuedCount &
            " job(s) back in the queue: their remote worker stopped responding"
        )
      queue.store.wakeForDueJobs()
    except CatchableError as error:
      stderr.writeLine("jobqueue: could not check leases and retries: " & error.msg)
    await sleepAsync(maintenanceIntervalMilliseconds)

proc start*(queue: JobQueue) =
  ## Start the worker threads and begin listening, without blocking. The
  ## caller must then drive the async dispatcher (`poll()`), which is what
  ## serve() does. Mainly useful for tests and for embedding in a program
  ## that already has an async loop.
  ##
  ## Raises ValueError if called twice or after stop(), and OSError if the
  ## port cannot be used; in that case nothing has been started.
  case queue.state
  of qsRunning:
    raise newException(ValueError, "the server is already running")
  of qsStopped:
    raise newException(ValueError, "a stopped server cannot be started again")
  of qsNew:
    discard

  # Listen first: it is the step most likely to fail (port already in use),
  # and nothing needs undoing if it does.
  let httpServer = newAsyncHttpServer()
  try:
    httpServer.listen(Port(queue.config.port), queue.config.host)
  except CatchableError:
    httpServer.close()
    raise
  queue.httpServer = httpServer

  # With no jobs registered here, the server only queues jobs for remote
  # workers, and threads would have nothing to run.
  if queue.registry.jobNames().len > 0:
    var workerCount = queue.config.workerCount
    if workerCount <= 0:
      workerCount = max(1, countProcessors())
    startWorkers(
      queue.workerThreads, addr queue.store, addr queue.registry, workerCount
    )

  queue.state = qsRunning
  asyncCheck acceptLoop(queue)
  asyncCheck maintenanceLoop(queue)

proc port*(queue: JobQueue): int =
  ## The port being listened on. Differs from the configured port when that
  ## was 0, which asks the operating system to pick a free one.
  return int(queue.httpServer.getPort())

proc stop*(queue: JobQueue) =
  ## Stop listening and stop the workers. Running jobs are allowed to finish.
  ## Jobs still in the queue are not run: they are lost, unless a
  ## `databasePath` was configured, in which case they run on the next start.
  ## The queue cannot be used again after this: routeRequest answers 503 and
  ## start() raises. Also safe to call on a queue that was never started,
  ## which just closes the store.
  case queue.state
  of qsStopped:
    return
  of qsRunning:
    queue.state = qsStopped
    queue.httpServer.close()
    stopWorkers(queue.workerThreads, addr queue.store)
  of qsNew:
    queue.state = qsStopped
  closeJobStore(queue.store)

proc serve*(queue: JobQueue) =
  ## start(), then handle requests on the calling thread until interrupted
  ## with ctrl-c, then stop().
  queue.start()
  interruptRequested = false
  setControlCHook(handleControlC)
  echo "jobqueue: listening on port ", queue.port()
  let recoveredCount = queue.store.recoveredJobCount()
  if recoveredCount > 0:
    echo "jobqueue: re-running ",
      recoveredCount, " job(s) interrupted by the last shutdown"

  while not interruptRequested:
    try:
      poll(200)
    except CatchableError as error:
      # For example a caller that disconnected before reading its reply.
      stderr.writeLine("jobqueue: request failed: " & error.msg)

  # Give ctrl-c back its default behaviour, so a second press quits at once
  # even if a running job never finishes.
  unsetControlCHook()
  echo "jobqueue: shutting down, waiting for running jobs to finish ",
    "(press ctrl-c again to quit without waiting)"
  queue.stop()
