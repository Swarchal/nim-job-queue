## jobqueue: a small celery-like job queue server.
##
## Server side:
##
##   import std/json
##   import jobqueue
##
##   proc addNumbers(args: JsonNode): JsonNode {.gcsafe, nimcall.} =
##     return %(args["a"].getInt() + args["b"].getInt())
##
##   var queue = newJobQueue(JobQueueConfig(port: 8080, workerCount: 4))
##   queue.registerJob("add", addNumbers)
##   queue.serve() # blocks
##
## Client side:
##
##   let client = newJobClient("http://localhost:8080")
##   let jobId = client.submitJob("add", %*{"a": 1, "b": 2})
##   let job = client.waitForJob(jobId, timeoutSeconds = 30.0)

import jobqueue/[types, server, client, remote_worker]
export types, server, client, remote_worker
