## Run with: nim r examples/pipeline_client.nim (while pipeline_server is running)
##
## Counts the words in several documents in parallel, merges the counts, and
## asks for the most common words:
##
##   count-words (one job per document)  ->  merge-counts  ->  top-words

import std/json
import jobqueue
import ./text_jobs

const documents = [
  "the cat sat on the mat",
  "the dog chased the cat",
  "", # count-words will fail on this one
  "a dog and a cat and a mat",
]

proc waitForResult(client: JobClient, jobId: string): JsonNode =
  ## Wait for one job and return its result, or raise if it failed.
  let job = client.waitForJob(jobId, timeoutSeconds = 30.0)
  if job.status == jsFailed:
    raise newException(JobQueueError, job.name & " failed: " & job.errorMessage)
  return job.result

proc main() =
  let client = newJobClient("http://127.0.0.1:8080")

  # Fan out: submit every document before waiting for any of them, so the
  # server's workers can run them at the same time.
  var countJobIds: seq[string] = @[]
  for document in documents:
    countJobIds.add(client.submitCountWords(document))
  echo "submitted ", countJobIds.len, " count-words jobs"

  # Collect the results. One bad document should not stop the others.
  var documentCounts: seq[JsonNode] = @[]
  for jobId in countJobIds:
    let job = client.waitForJob(jobId, timeoutSeconds = 30.0)
    if job.status == jsSucceeded:
      documentCounts.add(job.result)
    else:
      echo "skipping job ", jobId, ": ", job.errorMessage

  # Fan in: each of these jobs uses the result of the one before it.
  let mergeJobId = client.submitMergeCounts(documentCounts)
  let totalCounts = waitForResult(client, mergeJobId)

  let topJobId = client.submitTopWords(totalCounts, limit = 3)
  let mostCommon = waitForResult(client, topJobId)

  echo "most common words across ", documentCounts.len, " documents:"
  for entry in mostCommon.getElems():
    echo "  ", entry["word"].getStr(), ": ", entry["count"].getInt()

main()
