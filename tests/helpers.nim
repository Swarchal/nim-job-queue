## Helpers shared by the test files.

import std/[json, oids, os, times]
import jobqueue/types

proc addNumbers*(args: JsonNode): JsonNode {.gcsafe, nimcall.} =
  return %(args["a"].getInt() + args["b"].getInt())

proc alwaysFails*(args: JsonNode): JsonNode {.gcsafe, nimcall.} =
  raise newException(ValueError, "this job always fails")

proc alwaysRetryable*(args: JsonNode): JsonNode {.gcsafe, nimcall.} =
  raise newException(RetryableJobError, "try again later")

proc retryableUntil*(args: JsonNode): JsonNode {.gcsafe, nimcall.} =
  ## Raises RetryableJobError until the unix time args["succeedAt"], then
  ## returns "done". Handlers cannot keep state, so the time does the counting.
  if epochTime() < args["succeedAt"].getFloat():
    raise newException(RetryableJobError, "not ready yet")
  return %"done"

proc newDatabasePath*(): string =
  return getTempDir() / ("jobqueue_test_" & $genOid() & ".sqlite")

proc removeDatabase*(databasePath: string) =
  # SQLite's write-ahead log keeps two extra files next to the database.
  removeFile(databasePath)
  removeFile(databasePath & "-wal")
  removeFile(databasePath & "-shm")
