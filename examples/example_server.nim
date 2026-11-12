## Run with: nim r examples/example_server.nim
##
## Give a file name to keep jobs in SQLite, so they survive a restart:
##   nim r examples/example_server.nim jobs.sqlite

import std/[json, os]
import jobqueue

proc addNumbers(args: JsonNode): JsonNode {.gcsafe, nimcall.} =
  return %(args["a"].getInt() + args["b"].getInt())

proc slowEcho(args: JsonNode): JsonNode {.gcsafe, nimcall.} =
  sleep(args["milliseconds"].getInt())
  return args["message"]

proc alwaysFails(args: JsonNode): JsonNode {.gcsafe, nimcall.} =
  raise newException(ValueError, "this job always fails")

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
queue.serve()
