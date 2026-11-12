## Helpers shared by the test files.

import std/[json, oids, os]

proc addNumbers*(args: JsonNode): JsonNode {.gcsafe, nimcall.} =
  return %(args["a"].getInt() + args["b"].getInt())

proc alwaysFails*(args: JsonNode): JsonNode {.gcsafe, nimcall.} =
  raise newException(ValueError, "this job always fails")

proc newDatabasePath*(): string =
  return getTempDir() / ("jobqueue_test_" & $genOid() & ".sqlite")

proc removeDatabase*(databasePath: string) =
  # SQLite's write-ahead log keeps two extra files next to the database.
  removeFile(databasePath)
  removeFile(databasePath & "-wal")
  removeFile(databasePath & "-shm")
