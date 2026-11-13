import
  std/[
    asyncdispatch, asynchttpserver, httpclient, httpcore, json, net, os, times, unittest
  ]
import jobqueue
import ./helpers

proc makeQueue(): JobQueue =
  # Port 0 asks the operating system for any free port.
  result = newJobQueue(JobQueueConfig(host: "127.0.0.1", port: 0, workerCount: 2))
  result.registerJob("add", addNumbers)
  result.registerJob("always-fails", alwaysFails)

proc post(queue: JobQueue, path: string, body: JsonNode): HttpReply =
  return queue.routeRequest(HttpPost, path, $body)

proc submit(queue: JobQueue, body: JsonNode): string =
  ## POST /jobs; returns the new job's id.
  return parseJson(queue.post("/jobs", body).body)["id"].getStr()

proc getJobJson(queue: JobQueue, jobId: string): JsonNode =
  ## The job as GET /jobs/{id} returns it.
  return parseJson(queue.routeRequest(HttpGet, "/jobs/" & jobId, "").body)

proc submitAndClaim(queue: JobQueue, workerId: string): string =
  ## Have the remote worker `workerId` announce the job name "x", submit one
  ## such job and have the worker claim it. Returns the job's id.
  let claim = %*{"workerId": workerId, "jobNames": ["x"]}
  discard queue.post("/workers/claim", claim)
  result = queue.submit(%*{"name": "x"})
  discard queue.post("/workers/claim", claim)

proc waitUntilSubmittable(queue: JobQueue, jobName: string) =
  ## The server learns a remote worker's job names from its first claim.
  ## Drive the async loop, which the worker needs to reach the server, until
  ## `jobName` is known (at most ten seconds).
  let deadline = epochTime() + 10.0
  while epochTime() < deadline:
    poll(20)
    let names = parseJson(queue.routeRequest(HttpGet, "/job-names", "").body)
    if %jobName in names.getElems():
      return

proc waitUntilFinished(queue: JobQueue, jobId: string): JsonNode =
  ## Drive the async loop until the job has succeeded or failed (at most ten
  ## seconds), and return it as GET /jobs/{id} does.
  let deadline = epochTime() + 10.0
  while true:
    poll(20)
    result = queue.getJobJson(jobId)
    let status = result["status"].getStr()
    if status == "succeeded" or status == "failed" or epochTime() >= deadline:
      return

proc stopRemoteWorker(worker: RemoteWorker) {.thread.} =
  worker.stop()

proc stopWhilePolling(worker: RemoteWorker) =
  ## stop() waits for the worker's threads, which need the server to answer,
  ## so it runs on another thread while this one drives the async loop.
  var stopThread: Thread[RemoteWorker]
  createThread(stopThread, stopRemoteWorker, worker)
  while stopThread.running():
    poll(20)
  joinThread(stopThread)

suite "server.routeRequest":
  # The queue is never started here, so submitted jobs stay queued.
  setup:
    let queue = makeQueue()

  test "POST /jobs with a registered name returns 202 and an id":
    let reply = queue.routeRequest(HttpPost, "/jobs", """{"name": "add", "args": {}}""")
    check reply.code == Http202
    let body = parseJson(reply.body)
    check body["id"].getStr().len > 0
    check body["status"].getStr() == "queued"

  test "POST /jobs with an unknown name returns 400":
    let reply = queue.routeRequest(HttpPost, "/jobs", """{"name": "no-such-job"}""")
    check reply.code == Http400
    check parseJson(reply.body).hasKey("error")

  test "POST /jobs with a body that is not JSON returns 400":
    check queue.routeRequest(HttpPost, "/jobs", "{not json").code == Http400
    check queue.routeRequest(HttpPost, "/jobs", "[1, 2]").code == Http400

  test "GET /jobs/{id} returns the job, or 404 for an unknown id":
    let submitReply =
      queue.routeRequest(HttpPost, "/jobs", """{"name": "add", "args": {"a": 1}}""")
    let jobId = parseJson(submitReply.body)["id"].getStr()

    let reply = queue.routeRequest(HttpGet, "/jobs/" & jobId, "")
    check reply.code == Http200
    let body = parseJson(reply.body)
    check body["id"].getStr() == jobId
    check body["name"].getStr() == "add"
    check body["status"].getStr() == "queued"
    check body["result"].kind == JNull
    check body["error"].kind == JNull

    check queue.routeRequest(HttpGet, "/jobs/no-such-id", "").code == Http404

  test "GET /health reports the number of queued jobs":
    discard queue.routeRequest(HttpPost, "/jobs", """{"name": "add"}""")
    let reply = queue.routeRequest(HttpGet, "/health", "")
    check reply.code == Http200
    check parseJson(reply.body)["queued"].getInt() == 1

  test "GET /status counts jobs in each status":
    discard queue.routeRequest(HttpPost, "/jobs", """{"name": "add"}""")
    let reply = queue.routeRequest(HttpGet, "/status", "")
    check reply.code == Http200
    check parseJson(reply.body) ==
      %*{
        "workers": 0,
        "remoteWorkers": 0,
        "queued": 1,
        "running": 0,
        "succeeded": 0,
        "failed": 0,
      }
    check queue.routeRequest(HttpPost, "/status", "").code == Http405

  test "GET /job-names lists the registered jobs":
    let reply = queue.routeRequest(HttpGet, "/job-names", "")
    check parseJson(reply.body) == %["add", "always-fails"]

  test "unknown paths give 404 and wrong methods give 405":
    check queue.routeRequest(HttpGet, "/nothing-here", "").code == Http404
    check queue.routeRequest(HttpGet, "/jobs", "").code == Http405
    check queue.routeRequest(HttpPost, "/health", "").code == Http405

  test "GET /jobs/{id} decodes a percent-encoded id":
    let reply = queue.routeRequest(HttpGet, "/jobs/a%2Fb%3Fc", "")
    check reply.code == Http404
    check parseJson(reply.body)["error"].getStr() == "no job with id 'a/b?c'"

proc sleepThenReturn(args: JsonNode): JsonNode {.gcsafe, nimcall.} =
  sleep(args["milliseconds"].getInt())
  return %"done"

suite "server.routeRequest for remote workers":
  setup:
    let queue = makeQueue()

  test "a remote worker announces names, claims jobs and reports results":
    # Unknown until a worker that can run it has asked for work.
    check queue.post("/jobs", %*{"name": "remote-add"}).code == Http400
    let emptyClaim =
      queue.post("/workers/claim", %*{"workerId": "w1", "jobNames": ["remote-add"]})
    check emptyClaim.code == Http200
    check parseJson(emptyClaim.body)["job"].kind == JNull
    check parseJson(queue.routeRequest(HttpGet, "/job-names", "").body) ==
      %["add", "always-fails", "remote-add"]

    let submitReply =
      queue.post("/jobs", %*{"name": "remote-add", "args": {"a": 1, "b": 2}})
    check submitReply.code == Http202
    let jobId = parseJson(submitReply.body)["id"].getStr()

    let claimReply =
      queue.post("/workers/claim", %*{"workerId": "w1", "jobNames": ["remote-add"]})
    let claimed = parseJson(claimReply.body)
    check claimed["job"]["id"].getStr() == jobId
    check claimed["job"]["name"].getStr() == "remote-add"
    check claimed["job"]["args"] == %*{"a": 1, "b": 2}
    check claimed["leaseSeconds"].getFloat() > 0

    check queue.post("/jobs/" & jobId & "/heartbeat", %*{"workerId": "w1"}).code ==
      Http200
    let resultReply = queue.post(
      "/jobs/" & jobId & "/result", %*{"workerId": "w1", "succeeded": true, "result": 3}
    )
    check resultReply.code == Http200

    let job = queue.getJobJson(jobId)
    check job["status"].getStr() == "succeeded"
    check job["result"].getInt() == 3
    check parseJson(queue.routeRequest(HttpGet, "/status", "").body)["remoteWorkers"].getInt() ==
      1

  test "a failure report records the error message":
    let jobId = queue.submitAndClaim("w1")
    let reply = queue.post(
      "/jobs/" & jobId & "/result",
      %*{"workerId": "w1", "succeeded": false, "error": "it broke"},
    )
    check reply.code == Http200
    let job = queue.getJobJson(jobId)
    check job["status"].getStr() == "failed"
    check job["error"].getStr() == "it broke"

  test "a failure report asking for a retry puts the job back in the queue":
    discard queue.post("/workers/claim", %*{"workerId": "w1", "jobNames": ["x"]})
    let jobId = queue.submit(%*{"name": "x"})
    let firstClaim =
      queue.post("/workers/claim", %*{"workerId": "w1", "jobNames": ["x"]})
    check parseJson(firstClaim.body)["job"]["retries"].getInt() == 0

    let reply = queue.post(
      "/jobs/" & jobId & "/result",
      %*{"workerId": "w1", "succeeded": false, "error": "busy", "retry": true},
    )
    check reply.code == Http200
    let job = queue.getJobJson(jobId)
    check job["status"].getStr() == "queued"
    check job["retries"].getInt() == 1
    check job["error"].kind == JNull

    # Any worker can pick it up, and is told how often it has been retried.
    let secondClaim =
      queue.post("/workers/claim", %*{"workerId": "w2", "jobNames": ["x"]})
    check parseJson(secondClaim.body)["job"]["id"].getStr() == jobId
    check parseJson(secondClaim.body)["job"]["retries"].getInt() == 1

  test "a retry with a delay is not handed out straight away":
    let jobId = queue.submitAndClaim("w1")
    let reply = queue.post(
      "/jobs/" & jobId & "/result",
      %*{"workerId": "w1", "succeeded": false, "retry": true, "retryDelaySeconds": 60},
    )
    check reply.code == Http200
    let claim = queue.post("/workers/claim", %*{"workerId": "w1", "jobNames": ["x"]})
    check parseJson(claim.body)["job"].kind == JNull

  test "a retry report with a bad 'retry' or 'retryDelaySeconds' gives 400":
    let jobId = queue.submitAndClaim("w1")
    for report in [
      %*{"workerId": "w1", "succeeded": false, "retry": "yes"},
      %*{"workerId": "w1", "succeeded": false, "retry": true, "retryDelaySeconds": -1},
      %*{"workerId": "w1", "succeeded": false, "retry": true, "retryDelaySeconds": "1"},
    ]:
      check queue.post("/jobs/" & jobId & "/result", report).code == Http400
    let job = queue.getJobJson(jobId)
    check job["status"].getStr() == "running"

  test "a failure report with no error message is recorded":
    discard queue.post("/workers/claim", %*{"workerId": "w1", "jobNames": ["x"]})
    for report in [
      %*{"workerId": "w1", "succeeded": false},
      %*{"workerId": "w1", "succeeded": false, "error": ""},
    ]:
      let jobId = queue.submit(%*{"name": "x"})
      discard queue.post("/workers/claim", %*{"workerId": "w1", "jobNames": ["x"]})
      check queue.post("/jobs/" & jobId & "/result", report).code == Http200
      let job = queue.getJobJson(jobId)
      check job["status"].getStr() == "failed"
      check job["error"].getStr().len > 0

  test "a failure report whose error is not a string gives 400":
    let jobId = queue.submitAndClaim("w1")
    check queue.post(
      "/jobs/" & jobId & "/result", %*{"workerId": "w1", "succeeded": false, "error": 5}
    ).code == Http400

  test "a rejected claim does not make its job names submittable":
    check queue.post(
      "/workers/claim", %*{"workerId": "w1", "jobNames": ["typo-name", 5]}
    ).code == Http400
    check queue.post("/jobs", %*{"name": "typo-name"}).code == Http400
    check parseJson(queue.routeRequest(HttpGet, "/job-names", "").body) ==
      %["add", "always-fails"]

  test "another worker's heartbeat or result is refused with 409":
    let jobId = queue.submitAndClaim("w1")

    check queue.post("/jobs/" & jobId & "/heartbeat", %*{"workerId": "w2"}).code ==
      Http409
    check queue.post(
      "/jobs/" & jobId & "/result", %*{"workerId": "w2", "succeeded": true}
    ).code == Http409

  test "bad worker requests give 400, 404 or 405":
    check queue.post("/workers/claim", %*{"jobNames": ["x"]}).code == Http400
    check queue.post("/workers/claim", %*{"workerId": "w1", "jobNames": []}).code ==
      Http400
    check queue.post("/workers/claim", %*{"workerId": "w1", "jobNames": [1]}).code ==
      Http400
    check queue.post("/workers/claim", %*{"workerId": "local", "jobNames": ["x"]}).code ==
      Http400
    check queue.post("/jobs/some-id/result", %*{"workerId": "w1"}).code == Http400
    check queue.post("/jobs/some-id/cancel", %*{"workerId": "w1"}).code == Http404
    check queue.routeRequest(HttpGet, "/jobs/some-id/result", "").code == Http405
    check queue.routeRequest(HttpGet, "/workers/claim", "").code == Http405

suite "server lifecycle":
  test "after stop() requests get 503 and start() raises":
    let queue = makeQueue()
    queue.start()
    queue.stop()
    check queue.routeRequest(HttpGet, "/health", "").code == Http503
    expect ValueError:
      queue.start()
    queue.stop() # a second stop() does nothing

  test "stop() on a queue that was never started closes it":
    let queue = makeQueue()
    queue.stop()
    check queue.routeRequest(HttpGet, "/health", "").code == Http503

  test "start() on a port in use raises and leaves nothing running":
    let firstQueue = makeQueue()
    firstQueue.start()
    let busyPort = firstQueue.port()

    let secondQueue =
      newJobQueue(JobQueueConfig(host: "127.0.0.1", port: busyPort, workerCount: 2))
    expect OSError:
      secondQueue.start()
    # Still new, so it can be stopped cleanly and jobs can still be registered.
    secondQueue.registerJob("add", addNumbers)
    secondQueue.stop()
    firstQueue.stop()

# ---------------------------------------------------------------------------
# End to end: a real server and the real client, over a real socket.
#
# The server needs the main thread to drive its async loop, and the client
# blocks while it waits, so the client runs on a second thread. It reports
# back through these two plain globals.
# ---------------------------------------------------------------------------

var clientIsFinished: bool
var clientChecksPassed: bool

proc runClient(port: int) {.thread.} =
  try:
    let client = newJobClient("http://127.0.0.1:" & $port)

    let addJob = client.waitForJob(
      client.submitJob("add", %*{"a": 2, "b": 3}), timeoutSeconds = 10.0
    )
    doAssert addJob.status == jsSucceeded
    doAssert addJob.result.getInt() == 5

    let failingJob = client.waitForJob(
      client.submitJob("always-fails", newJObject()), timeoutSeconds = 10.0
    )
    doAssert failingJob.status == jsFailed
    doAssert failingJob.errorMessage == "ValueError: this job always fails"

    var unknownNameWasRejected = false
    try:
      discard client.submitJob("no-such-job", newJObject())
    except JobQueueError as error:
      unknownNameWasRejected = error.httpCode == Http400
    doAssert unknownNameWasRejected

    let status = client.getStatus()
    doAssert status.workers == 2
    doAssert status.succeeded == 1
    doAssert status.failed == 1
    doAssert status.queued == 0
    doAssert status.running == 0

    clientChecksPassed = true
  except Exception as error:
    echo "client thread failed: ", error.name, ": ", error.msg
  clientIsFinished = true

suite "end to end":
  test "jobs submitted with the client run and report results and failures":
    let queue = makeQueue()
    queue.start()

    var clientThread: Thread[int]
    createThread(clientThread, runClient, queue.port())

    let deadline = epochTime() + 30.0
    while not clientIsFinished and epochTime() < deadline:
      poll(20)

    check clientIsFinished
    if clientIsFinished:
      joinThread(clientThread)
    queue.stop()
    check clientChecksPassed

proc checkRetriedJobs(queue: JobQueue) =
  ## Submit a "flaky" job (retryableUntil) and a "gives-up" job
  ## (alwaysRetryable, maxRetries = 2) and check how each one ends.
  let flakyId =
    queue.submit(%*{"name": "flaky", "args": {"succeedAt": epochTime() + 0.3}})
  let givesUpId = queue.submit(%*{"name": "gives-up"})

  let flakyJob = queue.waitUntilFinished(flakyId)
  check flakyJob["status"].getStr() == "succeeded"
  check flakyJob["result"].getStr() == "done"
  check flakyJob["retries"].getInt() >= 1

  let givesUpJob = queue.waitUntilFinished(givesUpId)
  check givesUpJob["status"].getStr() == "failed"
  check givesUpJob["retries"].getInt() == 2
  check givesUpJob["error"].getStr() ==
    "RetryableJobError: try again later (gave up after 2 retries)"

suite "retries end to end":
  test "the server's own threads retry a job until it succeeds":
    let queue = newJobQueue(JobQueueConfig(host: "127.0.0.1", port: 0, workerCount: 2))
    queue.registerJob(
      "flaky", retryableUntil, maxRetries = 10, retryDelaySeconds = 0.05
    )
    queue.registerJob(
      "gives-up", alwaysRetryable, maxRetries = 2, retryDelaySeconds = 0.05
    )
    queue.start()
    queue.checkRetriedJobs()
    queue.stop()

  test "a remote worker retries a job until it succeeds":
    let queue = newJobQueue(JobQueueConfig(host: "127.0.0.1", port: 0))
    queue.start()
    let worker = newRemoteWorker(
      RemoteWorkerConfig(
        serverUrl: "http://127.0.0.1:" & $queue.port(), pollIntervalSeconds: 0.05
      )
    )
    worker.registerJob(
      "flaky", retryableUntil, maxRetries = 10, retryDelaySeconds = 0.05
    )
    worker.registerJob(
      "gives-up", alwaysRetryable, maxRetries = 2, retryDelaySeconds = 0.05
    )
    worker.start()
    queue.waitUntilSubmittable("gives-up")
    queue.checkRetriedJobs()
    worker.stopWhilePolling()
    queue.stop()

suite "remote workers end to end":
  test "a remote worker runs jobs for a server with no jobs of its own":
    let queue = newJobQueue(JobQueueConfig(host: "127.0.0.1", port: 0))
    queue.start()
    let worker = newRemoteWorker(
      RemoteWorkerConfig(
        serverUrl: "http://127.0.0.1:" & $queue.port(),
        threadCount: 2,
        pollIntervalSeconds: 0.05,
      )
    )
    worker.registerJob("add", addNumbers)
    worker.registerJob("always-fails", alwaysFails)
    worker.start()

    queue.waitUntilSubmittable("add")
    let addId = queue.submit(%*{"name": "add", "args": {"a": 4, "b": 5}})
    let failId = queue.submit(%*{"name": "always-fails"})
    let addJob = queue.waitUntilFinished(addId)
    let failJob = queue.waitUntilFinished(failId)

    check addJob["status"].getStr() == "succeeded"
    check addJob["result"].getInt() == 9
    check failJob["status"].getStr() == "failed"
    check failJob["error"].getStr() == "ValueError: this job always fails"
    let status = parseJson(queue.routeRequest(HttpGet, "/status", "").body)
    check status["workers"].getInt() == 0
    # One per job thread: each has its own worker id.
    check status["remoteWorkers"].getInt() == 2

    worker.stopWhilePolling()
    queue.stop()

  test "heartbeats keep a job that outlasts its lease from being handed out again":
    let queue =
      newJobQueue(JobQueueConfig(host: "127.0.0.1", port: 0, leaseSeconds: 0.3))
    queue.start()
    let worker = newRemoteWorker(
      RemoteWorkerConfig(
        serverUrl: "http://127.0.0.1:" & $queue.port(), pollIntervalSeconds: 0.05
      )
    )
    worker.registerJob("slow", sleepThenReturn)
    worker.start()

    queue.waitUntilSubmittable("slow")
    let jobId = queue.submit(%*{"name": "slow", "args": {"milliseconds": 1500}})

    # Note when it first starts; if the lease ran out and the job was handed
    # out again, startedAt would change.
    var firstStartedAt = newJNull()
    var job: JsonNode
    let deadline = epochTime() + 10.0
    while epochTime() < deadline:
      poll(20)
      job = queue.getJobJson(jobId)
      if firstStartedAt.kind == JNull and job["startedAt"].kind != JNull:
        firstStartedAt = job["startedAt"]
      if job["status"].getStr() == "succeeded":
        break

    check job["status"].getStr() == "succeeded"
    check job["startedAt"] == firstStartedAt
    check job["finishedAt"].getFloat() - job["startedAt"].getFloat() > 1.0

    worker.stopWhilePolling()
    queue.stop()

  test "a job is given to another worker when its lease expires":
    let queue =
      newJobQueue(JobQueueConfig(host: "127.0.0.1", port: 0, leaseSeconds: 0.2))
    queue.start()
    let jobId = queue.submitAndClaim("w1")

    # No heartbeats from w1, so the lease check puts the job back.
    var job: JsonNode
    let deadline = epochTime() + 5.0
    while epochTime() < deadline:
      poll(20)
      job = queue.getJobJson(jobId)
      if job["status"].getStr() == "queued":
        break
    check job["status"].getStr() == "queued"

    let claimReply =
      queue.post("/workers/claim", %*{"workerId": "w2", "jobNames": ["x"]})
    check parseJson(claimReply.body)["job"]["id"].getStr() == jobId
    check queue.post(
      "/jobs/" & jobId & "/result", %*{"workerId": "w1", "succeeded": true}
    ).code == Http409
    queue.stop()

# ---------------------------------------------------------------------------
# A fake server, for failures the real one does not produce on demand. It
# hands out one "add" job, answers the first result report with a proxy's
# 502 page and accepts the second.
# ---------------------------------------------------------------------------

var fakeServerIsRunning: bool
var fakeJobHandedOut: bool
var fakeResultReports: seq[JsonNode]

proc fakeServerReply(path: string, body: string): (HttpCode, string) =
  if path == "/workers/claim":
    var job = newJNull()
    if not fakeJobHandedOut:
      fakeJobHandedOut = true
      job = %*{"id": "j1", "name": "add", "args": {"a": 1, "b": 2}}
    return (Http200, $(%*{"job": job, "leaseSeconds": 30}))
  if path == "/jobs/j1/result":
    fakeResultReports.add(parseJson(body))
    if fakeResultReports.len == 1:
      return (Http502, "<html>Bad Gateway</html>")
    return (Http200, $(%*{"id": "j1"}))
  return (Http200, "{}")

proc handleFakeRequest(request: Request): Future[void] {.gcsafe.} =
  # The fake server's globals are only touched from the main thread, which
  # runs the async loop, so this is safe even though the compiler cannot
  # tell.
  {.cast(gcsafe).}:
    let (code, body) = fakeServerReply(request.url.path, request.body)
  return request.respond(code, body)

proc runFakeServer(server: AsyncHttpServer) {.async.} =
  while fakeServerIsRunning:
    try:
      await server.acceptRequest(handleFakeRequest)
    except CatchableError:
      discard

suite "remote worker failures":
  test "a result refused with a 5xx error is sent again":
    let fakeServer = newAsyncHttpServer()
    fakeServer.listen(Port(0), "127.0.0.1")
    fakeServerIsRunning = true
    asyncCheck runFakeServer(fakeServer)
    let worker = newRemoteWorker(
      RemoteWorkerConfig(
        serverUrl: "http://127.0.0.1:" & $int(fakeServer.getPort()),
        pollIntervalSeconds: 0.05,
      )
    )
    worker.registerJob("add", addNumbers)
    worker.start()

    let deadline = epochTime() + 10.0
    while fakeResultReports.len < 2 and epochTime() < deadline:
      poll(20)

    check fakeResultReports.len == 2
    if fakeResultReports.len == 2:
      check fakeResultReports[1]["result"].getInt() == 3
      check fakeResultReports[1]["workerId"].getStr() == worker.workerId & "-0"

    worker.stopWhilePolling()
    fakeServerIsRunning = false
    fakeServer.close()

suite "client timeouts":
  # A socket that accepts connections but never answers, like a server that
  # has hung or a network that drops packets.
  setup:
    let silentSocket = newSocket()
    silentSocket.bindAddr(Port(0), "127.0.0.1")
    silentSocket.listen()
    let silentUrl = "http://127.0.0.1:" & $int(silentSocket.getLocalAddr()[1])

  teardown:
    silentSocket.close()

  test "a request to a server that never answers raises TimeoutError":
    let httpClient = newJsonHttpClient(timeoutSeconds = 0.2)
    expect TimeoutError:
      discard newJobClient(silentUrl).sendRequest(httpClient, HttpGet, "/status", "")
    httpClient.close()

  test "waitForJob gives up at its deadline when the server never answers":
    let startedAt = epochTime()
    expect JobTimeoutError:
      discard newJobClient(silentUrl).waitForJob("some-id", timeoutSeconds = 0.3)
    check epochTime() - startedAt < 5.0

suite "sqlite persistence":
  test "jobs queued before a restart run after it":
    let databasePath = newDatabasePath()
    let config = JobQueueConfig(
      host: "127.0.0.1", port: 0, workerCount: 2, databasePath: databasePath
    )

    # The first server accepts a job but stops before running it.
    let firstQueue = newJobQueue(config)
    firstQueue.registerJob("add", addNumbers)
    let submitReply = firstQueue.routeRequest(
      HttpPost, "/jobs", """{"name": "add", "args": {"a": 20, "b": 22}}"""
    )
    let jobId = parseJson(submitReply.body)["id"].getStr()
    # Never started, so no worker can run the job before the "restart".
    firstQueue.stop()

    # A second server on the same file. The workers need no help from the
    # async loop, so sleeping is enough to let the job run.
    let secondQueue = newJobQueue(config)
    secondQueue.registerJob("add", addNumbers)
    secondQueue.start()
    var job = newJObject()
    for attempt in 0 ..< 500:
      job = parseJson(secondQueue.routeRequest(HttpGet, "/jobs/" & jobId, "").body)
      if job["status"].getStr() == "succeeded":
        break
      sleep(10)
    secondQueue.stop()

    check job["status"].getStr() == "succeeded"
    check job["result"].getInt() == 42

    removeDatabase(databasePath)
