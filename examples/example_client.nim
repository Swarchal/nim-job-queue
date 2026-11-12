## Run with: nim r examples/example_client.nim (while example_server is running)
##
## The same thing with curl:
##   curl -X POST localhost:8080/jobs -d '{"name": "add", "args": {"a": 1, "b": 2}}'
##   curl localhost:8080/jobs/<id>

import std/json
import jobqueue

let client = newJobClient("http://127.0.0.1:8080")
let jobId = client.submitJob("add", %*{"a": 1, "b": 2})
let job = client.waitForJob(jobId, timeoutSeconds = 10.0)

if job.status == jsSucceeded:
  echo "result: ", job.result
else:
  echo "job failed: ", job.errorMessage
