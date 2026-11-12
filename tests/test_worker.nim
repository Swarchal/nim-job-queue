import std/[json, options, os, unittest]
import jobqueue/[registry, store, types, worker]
import ./helpers

proc readsPastTheEnd(args: JsonNode): JsonNode {.gcsafe, nimcall.} =
  let items = args.getElems()
  return items[items.len + 1]

proc makeRegistry(): JobRegistry =
  result.registerJob("add", addNumbers)
  result.registerJob("always-fails", alwaysFails)
  result.registerJob("reads-past-the-end", readsPastTheEnd)

suite "registry":
  test "registering the same name twice raises":
    var registry = makeRegistry()
    expect ValueError:
      registry.registerJob("add", addNumbers)

  test "jobNames is sorted":
    check makeRegistry().jobNames() == @["add", "always-fails", "reads-past-the-end"]

suite "worker.runJob":
  let registry = makeRegistry()

  test "a handler's return value becomes resultJson":
    let outcome =
      runJob(registry, JobRecord(name: "add", argsJson: """{"a": 1, "b": 2}"""))
    check outcome.succeeded
    check outcome.resultJson == "3"

  test "an exception in the handler becomes errorMessage":
    let outcome = runJob(registry, JobRecord(name: "always-fails", argsJson: "{}"))
    check not outcome.succeeded
    check outcome.errorMessage == "ValueError: this job always fails"

  test "a defect in the handler fails the job rather than crashing":
    let outcome =
      runJob(registry, JobRecord(name: "reads-past-the-end", argsJson: "[]"))
    check not outcome.succeeded

  test "an unregistered job name fails rather than raising":
    let outcome = runJob(registry, JobRecord(name: "no-such-job", argsJson: "{}"))
    check not outcome.succeeded

  test "arguments that are not valid JSON fail rather than raising":
    let outcome = runJob(registry, JobRecord(name: "add", argsJson: "{not json"))
    check not outcome.succeeded

suite "worker threads":
  test "a pool of workers runs every queued job exactly once":
    var registry = makeRegistry()
    var store: JobStore
    initJobStore(store)
    var threads: seq[Thread[WorkerContext]]
    startWorkers(threads, addr store, addr registry, workerCount = 4)

    var jobIds: seq[string] = @[]
    for number in 0 ..< 200:
      jobIds.add(store.addJob("add", $(%*{"a": number, "b": 1000})))

    # Wait (at most ten seconds) for the last job, then stop the pool, which
    # lets any job that is still running finish.
    for attempt in 0 ..< 1000:
      if store.getJob(jobIds[^1]).get().status == jsSucceeded:
        break
      sleep(10)
    stopWorkers(threads, addr store)

    for number in 0 ..< 200:
      let job = store.getJob(jobIds[number]).get()
      check job.status == jsSucceeded
      check job.resultJson == $(number + 1000)
