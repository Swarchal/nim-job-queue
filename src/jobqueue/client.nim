## A Nim client for the HTTP API. Anything that can speak HTTP and JSON can
## use the server; this module is just a convenience.

import std/[httpclient, json, net, os, strutils, times, uri]
import ./types

type
  JobClient* = object
    baseUrl: string

  JobInfo* = object ## A job as seen by a caller.
    id*: string
    name*: string
    status*: JobStatus
    result*: JsonNode ## nil unless status is jsSucceeded
    errorMessage*: string ## "" unless status is jsFailed
    retries*: int ## times the job was run again after RetryableJobError

  QueueStatus* = object ## How busy the server is, from GET /status.
    workers*: int ## the server's own worker threads
    remoteWorkers*: int
      ## Remote worker job threads (each has its own worker id) heard from
      ## within the last lease period.
    queued*: int
    running*: int
    succeeded*: int
    failed*: int

  JobQueueError* = object of CatchableError
    ## The server answered with an error status. A 4xx code means it refused
    ## the request (unknown job name, unknown id, ...); a 5xx code means it,
    ## or a proxy in front of it, failed, and trying again later may work.
    httpCode*: HttpCode

  JobTimeoutError* = object of CatchableError
    ## waitForJob gave up before the job finished.

proc newJobClient*(baseUrl: string): JobClient =
  ## `baseUrl` is for example "http://localhost:8080".
  var trimmedUrl = baseUrl
  trimmedUrl.removeSuffix("/")
  return JobClient(baseUrl: trimmedUrl)

const defaultRequestTimeoutSeconds* = 30.0
  ## How long a request may wait for the server to send more of its reply.

proc newJsonHttpClient*(
    timeoutSeconds: float = defaultRequestTimeoutSeconds
): HttpClient =
  ## An HTTP client that sends JSON. Used here and by remote_worker.nim.
  ##
  ## A request raises TimeoutError (from std/net) when the server sends
  ## nothing for `timeoutSeconds`. Without a timeout, a server or network
  ## that stops responding mid-request would block the caller for ever.
  ## (Connecting is not covered: that is limited by the operating system.)
  return newHttpClient(
    headers = newHttpHeaders({"Content-Type": "application/json"}),
    timeout = int(timeoutSeconds * 1000),
  )

proc newJobQueueError(httpCode: HttpCode, message: string): ref JobQueueError =
  result = newException(JobQueueError, message)
  result.httpCode = httpCode

proc sendRequest*(
    client: JobClient,
    httpClient: HttpClient,
    httpMethod: HttpMethod,
    path: string,
    body: string,
): JsonNode =
  ## Send one request over `httpClient` and return the parsed JSON reply.
  ## Used here and by remote_worker.nim.
  ## Raises JobQueueError if the server answers with an error status or a
  ## reply that is not JSON; connection problems raise the usual OSError,
  ## and a server that stops answering raises TimeoutError.
  let response =
    httpClient.request(client.baseUrl & path, httpMethod = httpMethod, body = body)

  var reply: JsonNode
  try:
    reply = parseJson(response.body)
  except JsonParsingError:
    raise newJobQueueError(
      response.code, "the server sent a reply that is not JSON: " & response.status
    )

  if not response.code.is2xx():
    var message = response.status
    if reply.kind == JObject and reply.hasKey("error"):
      message = reply["error"].getStr()
    raise newJobQueueError(response.code, message)
  return reply

proc submitJob*(client: JobClient, name: string, args: JsonNode): string =
  ## POST /jobs. Returns the job id. Raises JobQueueError if rejected.
  let payload = %*{"name": name, "args": args}
  let httpClient = newJsonHttpClient()
  try:
    let reply = client.sendRequest(httpClient, HttpPost, "/jobs", $payload)
    return reply["id"].getStr()
  finally:
    httpClient.close()

proc getJobWith(client: JobClient, httpClient: HttpClient, jobId: string): JobInfo =
  ## getJob over a connection the caller owns, so polling can reuse it.
  let path = "/jobs/" & encodeUrl(jobId, usePlus = false)
  let reply = client.sendRequest(httpClient, HttpGet, path, "")
  result = JobInfo(
    id: reply["id"].getStr(),
    name: reply["name"].getStr(),
    status: parseEnum[JobStatus](reply["status"].getStr()),
    retries: reply{"retries"}.getInt(),
  )
  if result.status == jsSucceeded:
    result.result = reply["result"]
  if result.status == jsFailed:
    result.errorMessage = reply["error"].getStr()

proc getJob*(client: JobClient, jobId: string): JobInfo =
  ## GET /jobs/{id}. Raises JobQueueError if the id is unknown.
  let httpClient = newJsonHttpClient()
  try:
    return client.getJobWith(httpClient, jobId)
  finally:
    httpClient.close()

proc getStatus*(client: JobClient): QueueStatus =
  ## GET /status: the number of workers and of jobs in each status.
  let httpClient = newJsonHttpClient()
  try:
    let reply = client.sendRequest(httpClient, HttpGet, "/status", "")
    return QueueStatus(
      workers: reply["workers"].getInt(),
      remoteWorkers: reply["remoteWorkers"].getInt(),
      queued: reply[$jsQueued].getInt(),
      running: reply[$jsRunning].getInt(),
      succeeded: reply[$jsSucceeded].getInt(),
      failed: reply[$jsFailed].getInt(),
    )
  finally:
    httpClient.close()

proc `$`*(status: QueueStatus): string =
  ## One line for printing, e.g.
  ## "workers=4 remoteWorkers=2 queued=3 running=4 succeeded=120 failed=2"
  return
    "workers=" & $status.workers & " remoteWorkers=" & $status.remoteWorkers & " queued=" &
    $status.queued & " running=" & $status.running & " succeeded=" & $status.succeeded &
    " failed=" & $status.failed

proc waitForJob*(
    client: JobClient,
    jobId: string,
    timeoutSeconds: float = 60.0,
    pollIntervalSeconds: float = 0.2,
): JobInfo =
  ## Poll getJob until the job has succeeded or failed.
  ## Raises JobTimeoutError if that takes longer than timeoutSeconds, even
  ## when the server stops answering. Every poll reuses one keep-alive
  ## connection.
  let deadline = epochTime() + timeoutSeconds
  let httpClient = newJsonHttpClient()
  try:
    while true:
      # Never wait on the server past the deadline.
      let secondsLeft = deadline - epochTime()
      let requestTimeoutSeconds = clamp(secondsLeft, 0.1, defaultRequestTimeoutSeconds)
      httpClient.timeout = int(requestTimeoutSeconds * 1000)
      var job: JobInfo
      try:
        job = client.getJobWith(httpClient, jobId)
      except TimeoutError:
        raise newException(
          JobTimeoutError,
          "gave up waiting for job " & jobId & ": the server stopped answering",
        )
      if job.status == jsSucceeded or job.status == jsFailed:
        return job
      if epochTime() >= deadline:
        raise newException(
          JobTimeoutError,
          "job " & jobId & " was still " & $job.status & " after " & $timeoutSeconds &
            " seconds",
        )
      sleep(int(pollIntervalSeconds * 1000))
  finally:
    httpClient.close()
