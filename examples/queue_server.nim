## Run with: nim r examples/queue_server.nim
##
## A server with no jobs of its own: it only queues jobs and hands them to
## remote workers such as text_worker.nim. Start workers on any machine that
## can reach this one, then submit jobs with pipeline_client.nim.
##
## Binds to all interfaces so workers on other machines can connect. There is
## no authentication: only do this on a network you trust.

import jobqueue

var queue = newJobQueue(JobQueueConfig(host: "", port: 8080))
queue.serve()
