## Run with: nim r examples/text_worker.nim [server url]
##
## A remote worker that runs the jobs in text_jobs.nim for queue_server.nim.
## Start as many as you like, on any machine. To deploy changed jobs, start
## workers with the new code and press ctrl-c on the old ones: they finish
## their current job and exit, and the server keeps running throughout.

import std/os
import jobqueue
import ./text_jobs

var serverUrl = "http://127.0.0.1:8080"
if paramCount() >= 1:
  serverUrl = paramStr(1)

var worker = newRemoteWorker(
  RemoteWorkerConfig(serverUrl: serverUrl, threadCount: 4, workerName: "text")
)
worker.registerTextJobs()
worker.run()
