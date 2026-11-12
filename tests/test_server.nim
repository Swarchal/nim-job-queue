import std/[asyncdispatch, httpcore, json, os, times, unittest]
import jobqueue
import ./helpers

proc makeQueue(): JobQueue =
  # Port 0 asks the operating system for any free port.
  result = newJobQueue(JobQueueConfig(host: "127.0.0.1", port: 0, workerCount: 2))
  result.registerJob("add", addNumbers)
  result.registerJob("always-fails", alwaysFails)

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

proc post(queue: JobQueue, path: string, body: JsonNode): HttpReply =
  return queue.routeRequest(HttpPost, path, $body)

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

    let job = parseJson(queue.routeRequest(HttpGet, "/jobs/" & jobId, "").body)
    check job["status"].getStr() == "succeeded"
    check job["result"].getInt() == 3
    check parseJson(queue.routeRequest(HttpGet, "/status", "").body)["remoteWorkers"].getInt() ==
      1

  test "a failure report records the error message":
    discard queue.post("/workers/claim", %*{"workerId": "w1", "jobNames": ["x"]})
    let jobId = parseJson(queue.post("/jobs", %*{"name": "x"}).body)["id"].getStr()
    discard queue.post("/workers/claim", %*{"workerId": "w1", "jobNames": ["x"]})
    let reply = queue.post(
      "/jobs/" & jobId & "/result",
      %*{"workerId": "w1", "succeeded": false, "error": "it broke"},
    )
    check reply.code == Http200
    let job = parseJson(queue.routeRequest(HttpGet, "/jobs/" & jobId, "").body)
    check job["status"].getStr() == "failed"
    check job["error"].getStr() == "it broke"

  test "another worker's heartbeat or result is refused with 409":
    discard queue.post("/workers/claim", %*{"workerId": "w1", "jobNames": ["x"]})
    let jobId = parseJson(queue.post("/jobs", %*{"name": "x"}).body)["id"].getStr()
    discard queue.post("/workers/claim", %*{"workerId": "w1", "jobNames": ["x"]})

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
    except JobQueueError:
      unknownNameWasRejected = true
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

    # The server learns the names from the worker's first claim. The worker
    # talks to the server over HTTP, so the async loop must keep running.
    var deadline = epochTime() + 10.0
    while epochTime() < deadline and
        queue.routeRequest(HttpPost, "/jobs", """{"name": "add"}""").code != Http202:
      poll(20)

    let addId = parseJson(
      queue.routeRequest(
        HttpPost, "/jobs", """{"name": "add", "args": {"a": 4, "b": 5}}"""
      ).body
    )["id"].getStr()
    let failId = parseJson(
      queue.routeRequest(HttpPost, "/jobs", """{"name": "always-fails"}""").body
    )["id"].getStr()

    var addJob, failJob: JsonNode
    deadline = epochTime() + 10.0
    while epochTime() < deadline:
      poll(20)
      addJob = parseJson(queue.routeRequest(HttpGet, "/jobs/" & addId, "").body)
      failJob = parseJson(queue.routeRequest(HttpGet, "/jobs/" & failId, "").body)
      if addJob["status"].getStr() != "queued" and addJob["status"].getStr() != "running" and
          failJob["status"].getStr() != "queued" and
          failJob["status"].getStr() != "running":
        break

    check addJob["status"].getStr() == "succeeded"
    check addJob["result"].getInt() == 9
    check failJob["status"].getStr() == "failed"
    check failJob["error"].getStr() == "ValueError: this job always fails"
    let status = parseJson(queue.routeRequest(HttpGet, "/status", "").body)
    check status["workers"].getInt() == 0
    check status["remoteWorkers"].getInt() == 1

    # stop() waits for the worker's threads, which need the server to answer.
    var stopThread: Thread[RemoteWorker]
    proc stopWorker(worker: RemoteWorker) {.thread.} =
      worker.stop()

    createThread(stopThread, stopWorker, worker)
    while stopThread.running():
      poll(20)
    joinThread(stopThread)
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

    var deadline = epochTime() + 10.0
    var submitReply: HttpReply
    while epochTime() < deadline:
      poll(20)
      submitReply = queue.routeRequest(
        HttpPost, "/jobs", """{"name": "slow", "args": {"milliseconds": 1500}}"""
      )
      if submitReply.code == Http202:
        break
    let jobId = parseJson(submitReply.body)["id"].getStr()

    # Note when it first starts; if the lease ran out and the job was handed
    # out again, startedAt would change.
    var firstStartedAt = newJNull()
    var job: JsonNode
    deadline = epochTime() + 10.0
    while epochTime() < deadline:
      poll(20)
      job = parseJson(queue.routeRequest(HttpGet, "/jobs/" & jobId, "").body)
      if firstStartedAt.kind == JNull and job["startedAt"].kind != JNull:
        firstStartedAt = job["startedAt"]
      if job["status"].getStr() == "succeeded":
        break

    check job["status"].getStr() == "succeeded"
    check job["startedAt"] == firstStartedAt
    check job["finishedAt"].getFloat() - job["startedAt"].getFloat() > 1.0

    var stopThread: Thread[RemoteWorker]
    proc stopWorker(worker: RemoteWorker) {.thread.} =
      worker.stop()

    createThread(stopThread, stopWorker, worker)
    while stopThread.running():
      poll(20)
    joinThread(stopThread)
    queue.stop()

  test "a job is given to another worker when its lease expires":
    let queue =
      newJobQueue(JobQueueConfig(host: "127.0.0.1", port: 0, leaseSeconds: 0.2))
    queue.start()
    discard queue.post("/workers/claim", %*{"workerId": "w1", "jobNames": ["x"]})
    let jobId = parseJson(queue.post("/jobs", %*{"name": "x"}).body)["id"].getStr()
    discard queue.post("/workers/claim", %*{"workerId": "w1", "jobNames": ["x"]})

    # No heartbeats from w1, so the lease check puts the job back.
    var job: JsonNode
    let deadline = epochTime() + 5.0
    while epochTime() < deadline:
      poll(20)
      job = parseJson(queue.routeRequest(HttpGet, "/jobs/" & jobId, "").body)
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
