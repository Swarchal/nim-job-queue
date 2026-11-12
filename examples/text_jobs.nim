## Job definitions shared by pipeline_server.nim and pipeline_client.nim.
##
## This plays the role of celery's `tasks.py`: the functions live here, the
## server compiles them in and runs them, and the client only uses the names
## and the small submit procs at the bottom. No code is sent over the network.

import std/[json, strutils, tables]
import jobqueue

const
  countWordsJobName* = "count-words"
  mergeCountsJobName* = "merge-counts"
  topWordsJobName* = "top-words"

# ---------------------------------------------------------------------------
# The jobs themselves. These run on the server's worker threads.
# ---------------------------------------------------------------------------

proc countsToJson(counts: CountTable[string]): JsonNode =
  ## {"the": 2, "cat": 1, ...}
  result = newJObject()
  for word, count in counts.pairs():
    result[word] = %count

proc addCounts(totals: var CountTable[string], counts: JsonNode) =
  ## Add a {"word": count, ...} object onto `totals`.
  for word, count in counts.pairs():
    totals.inc(word, count.getInt())

proc countWords(args: JsonNode): JsonNode {.gcsafe, nimcall.} =
  ## args: {"text": "..."}  ->  {"the": 2, "cat": 1, ...}
  let text = args["text"].getStr()
  if text.strip().len == 0:
    raise newException(ValueError, "text is empty")

  var counts = initCountTable[string]()
  for word in text.toLowerAscii().splitWhitespace():
    counts.inc(word)

  return countsToJson(counts)

proc mergeCounts(args: JsonNode): JsonNode {.gcsafe, nimcall.} =
  ## args: {"counts": [{"the": 2}, {"the": 1, "dog": 1}]}  ->  {"the": 3, "dog": 1}
  var totals = initCountTable[string]()
  for documentCounts in args["counts"].getElems():
    addCounts(totals, documentCounts)
  return countsToJson(totals)

proc topWords(args: JsonNode): JsonNode {.gcsafe, nimcall.} =
  ## args: {"counts": {"the": 3, ...}, "limit": 5}  ->  [{"word": "the", "count": 3}, ...]
  var totals = initCountTable[string]()
  addCounts(totals, args["counts"])
  totals.sort() # most common first

  result = newJArray()
  let limit = args["limit"].getInt()
  for word, count in totals.pairs():
    if result.len >= limit:
      break
    result.add(%*{"word": word, "count": count})

# ---------------------------------------------------------------------------
# Server or remote worker side: register everything in one call.
# ---------------------------------------------------------------------------

proc registerTextJobs*(queue: JobQueue) =
  queue.registerJob(countWordsJobName, countWords)
  queue.registerJob(mergeCountsJobName, mergeCounts)
  queue.registerJob(topWordsJobName, topWords)

proc registerTextJobs*(worker: RemoteWorker) =
  ## The same jobs, for a remote worker (see text_worker.nim).
  worker.registerJob(countWordsJobName, countWords)
  worker.registerJob(mergeCountsJobName, mergeCounts)
  worker.registerJob(topWordsJobName, topWords)

# ---------------------------------------------------------------------------
# Client side: typed wrappers, so callers don't build JSON by hand.
# Each one returns the id of the submitted job.
# ---------------------------------------------------------------------------

proc submitCountWords*(client: JobClient, text: string): string =
  return client.submitJob(countWordsJobName, %*{"text": text})

proc submitMergeCounts*(client: JobClient, counts: seq[JsonNode]): string =
  return client.submitJob(mergeCountsJobName, %*{"counts": counts})

proc submitTopWords*(client: JobClient, counts: JsonNode, limit: int): string =
  return client.submitJob(topWordsJobName, %*{"counts": counts, "limit": limit})
