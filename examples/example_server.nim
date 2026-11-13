## Run with: nim r examples/example_server.nim
##
## Give a file name to keep jobs in SQLite, so they survive a restart:
##   nim r examples/example_server.nim jobs.sqlite

import std/[json, os, random]
import jobqueue

proc addNumbers(args: JsonNode): JsonNode {.gcsafe, nimcall.} =
  return %(args["a"].getInt() + args["b"].getInt())

proc slowEcho(args: JsonNode): JsonNode {.gcsafe, nimcall.} =
  sleep(args["milliseconds"].getInt())
  return args["message"]

proc alwaysFails(args: JsonNode): JsonNode {.gcsafe, nimcall.} =
  raise newException(ValueError, "this job always fails")

proc flakyGreeting(args: JsonNode): JsonNode {.gcsafe, nimcall.} =
  ## Stands in for a call to an unreliable service: about half the time
  ## it is "busy". Raising RetryableJobError has the job run again later; a
  ## missing name is the caller's mistake, so that fails at once.
  let name = args["name"].getStr()
  if name.len == 0:
    raise newException(ValueError, "name is empty")
  if rand(1) == 0:
    echo "flaky-greeting: the service is busy, asking for a retry"
    raise newException(RetryableJobError, "the greeting service is busy")
  return %("hello, " & name)

var databasePath = "" # "" keeps jobs in memory only
if paramCount() >= 1:
  databasePath = paramStr(1)

var queue = newJobQueue(
  JobQueueConfig(
    host: "127.0.0.1", port: 8080, workerCount: 4, databasePath: databasePath
  )
)
queue.registerJob("add", addNumbers)
queue.registerJob("slow-echo", slowEcho)
queue.registerJob("always-fails", alwaysFails)
# Up to 5 retries, waiting 0.5, 1, 2, 4 and 8 seconds before them.
queue.registerJob(
  "flaky-greeting", flakyGreeting, maxRetries = 5, retryDelaySeconds = 0.5
)
queue.serve()
