## Run with: nim r examples/pipeline_server.nim

import jobqueue
import ./text_jobs

var queue = newJobQueue(JobQueueConfig(host: "127.0.0.1", port: 8080, workerCount: 4))
queue.registerTextJobs()
queue.serve()
