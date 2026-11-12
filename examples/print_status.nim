## Run with: nim r examples/print_status.nim (while a server is running)
##
## Prints the queue's status once a second until ctrl-c. The same with curl:
##   curl localhost:8080/status

import std/os
import jobqueue

let client = newJobClient("http://127.0.0.1:8080")
while true:
  echo client.getStatus()
  sleep(1000)
