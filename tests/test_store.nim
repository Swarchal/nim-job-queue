import std/[options, unittest]
import jobqueue/[store, types]
import ./helpers

const testWorker = "test-worker"

proc finishAll(store: var JobStore, count: int) =
  ## Take `count` queued jobs and mark each one succeeded, in that order.
  for i in 0 ..< count:
    let job = store.takeNextJob(@[], testWorker).get()
    doAssert store.markSucceeded(job.id, testWorker, "null")

# Jobs are kept in memory (SQLite ":memory:") or in a SQLite file, and both
# must behave identically, so every test runs against each.
for backendName in ["memory", "sqlite"]:
  suite "store (" & backendName & ")":
    setup:
      var databasePath = ""
      if backendName == "sqlite":
        databasePath = newDatabasePath()
      var store: JobStore
      initJobStore(store, databasePath)

    teardown:
      closeJobStore(store)
      if databasePath.len > 0:
        removeDatabase(databasePath)

    test "addJob returns a unique id and the job is queued":
      let firstId = store.addJob("add", "{}")
      let secondId = store.addJob("add", "{}")
      check firstId != secondId
      check store.queuedCount() == 2

      let job = store.getJob(firstId).get()
      check job.id == firstId
      check job.name == "add"
      check job.status == jsQueued
      check job.argsJson == "{}"
      check job.submittedAt > 0.0
      check job.startedAt == 0.0

    test "countByStatus counts jobs in every status":
      check store.countByStatus() == [0, 0, 0, 0]
      let goodId = store.addJob("good", "{}")
      let badId = store.addJob("bad", "{}")
      discard store.addJob("running", "{}")
      discard store.addJob("queued", "{}")
      discard store.takeNextJob(@[], testWorker)
      discard store.takeNextJob(@[], testWorker)
      discard store.takeNextJob(@[], testWorker)
      check store.markSucceeded(goodId, testWorker, "1")
      check store.markFailed(badId, testWorker, "broke")

      let counts = store.countByStatus()
      check counts[jsQueued] == 1
      check counts[jsRunning] == 1
      check counts[jsSucceeded] == 1
      check counts[jsFailed] == 1

    test "takeNextJob returns jobs oldest first and marks them running":
      let firstId = store.addJob("first", "1")
      let secondId = store.addJob("second", "2")

      let firstJob = store.takeNextJob(@[], testWorker).get()
      check firstJob.id == firstId
      check firstJob.name == "first"
      check firstJob.argsJson == "1"
      check firstJob.status == jsRunning
      check firstJob.startedAt > 0.0
      check store.getJob(firstId).get().status == jsRunning
      # SQLite hands times back rounded to about a microsecond.
      check abs(store.getJob(firstId).get().startedAt - firstJob.startedAt) < 0.001
      check store.getJob(secondId).get().status == jsQueued

      check store.takeNextJob(@[], testWorker).get().id == secondId
      check store.queuedCount() == 0

    test "markSucceeded and markFailed record the outcome and finish time":
      let goodId = store.addJob("good", "{}")
      let badId = store.addJob("bad", "{}")
      discard store.takeNextJob(@[], testWorker)
      discard store.takeNextJob(@[], testWorker)

      check store.markSucceeded(goodId, testWorker, "42")
      check store.markFailed(badId, testWorker, "it broke")

      let goodJob = store.getJob(goodId).get()
      check goodJob.status == jsSucceeded
      check goodJob.resultJson == "42"
      check goodJob.finishedAt > 0.0

      let badJob = store.getJob(badId).get()
      check badJob.status == jsFailed
      check badJob.errorMessage == "it broke"
      check badJob.finishedAt > 0.0

    test "text with quotes and non-ASCII characters is stored unchanged":
      let awkwardText =
        """{"text": "it's a \"test\" — ünïcödé ? ; DROP TABLE jobs"}"""
      let jobId = store.addJob("echo", awkwardText)
      discard store.takeNextJob(@[], testWorker)
      check store.markSucceeded(jobId, testWorker, awkwardText)

      let job = store.getJob(jobId).get()
      check job.argsJson == awkwardText
      check job.resultJson == awkwardText

    test "getJob returns none for an unknown id":
      check store.getJob("no-such-id").isNone()

    test "takeNextJob returns none after shutdown":
      discard store.addJob("add", "{}")
      store.shutdown()
      check store.takeNextJob(@[], testWorker).isNone()

    test "claimJob only returns jobs with the given names, oldest first":
      let addId = store.addJob("add", "{}")
      discard store.addJob("other", "{}")
      let secondAddId = store.addJob("add", "{}")

      check store.claimJob(@["missing"], "w1", 30.0).isNone()
      let claimed = store.claimJob(@["add", "missing"], "w1", 30.0).get()
      check claimed.id == addId
      check claimed.status == jsRunning
      check claimed.workerId == "w1"
      check claimed.leaseExpiresAt > claimed.startedAt
      check store.claimJob(@["add"], "w2", 30.0).get().id == secondAddId
      check store.claimJob(@["add"], "w2", 30.0).isNone()
      check store.queuedCount() == 1

    test "only the worker running a job can report it or extend its lease":
      let jobId = store.addJob("add", "{}")
      discard store.claimJob(@["add"], "w1", 30.0)

      check not store.extendLease(jobId, "w2", 30.0)
      check not store.markSucceeded(jobId, "w2", "1")
      check store.getJob(jobId).get().status == jsRunning

      check store.extendLease(jobId, "w1", 30.0)
      check store.markSucceeded(jobId, "w1", "1")
      check store.getJob(jobId).get().status == jsSucceeded
      check store.getJob(jobId).get().workerId == ""
      # Finished, so nobody can report it again.
      check not store.markFailed(jobId, "w1", "late")

    test "a job whose lease expired is queued again, ahead of newer jobs":
      let expiringId = store.addJob("add", "{}")
      let newerId = store.addJob("add", "{}")
      # A negative lease has already run out.
      discard store.claimJob(@["add"], "w1", -1.0)
      discard store.claimJob(@["add"], "w1", 30.0) # still valid

      check store.requeueExpiredLeases() == 1
      let job = store.getJob(expiringId).get()
      check job.status == jsQueued
      check job.workerId == ""
      check job.startedAt == 0.0
      check store.getJob(newerId).get().status == jsRunning

      # The old worker's late result is refused; the next worker gets it.
      check not store.markSucceeded(expiringId, "w1", "1")
      check store.claimJob(@["add"], "w2", 30.0).get().id == expiringId

    test "jobs on the server's own threads have no lease and never expire":
      let jobId = store.addJob("add", "{}")
      let job = store.takeNextJob(@["add"], testWorker).get()
      check job.leaseExpiresAt == 0.0
      check store.requeueExpiredLeases() == 0
      check store.getJob(jobId).get().status == jsRunning

    test "maxFinishedJobs removes the jobs that finished longest ago":
      var cappedPath = ""
      if backendName == "sqlite":
        cappedPath = newDatabasePath()
      var capped: JobStore
      initJobStore(capped, cappedPath, maxFinishedJobs = 2)

      let firstId = capped.addJob("first", "{}")
      let secondId = capped.addJob("second", "{}")
      let thirdId = capped.addJob("third", "{}")
      let queuedId = capped.addJob("queued", "{}")
      discard capped.takeNextJob(@[], testWorker)
      discard capped.takeNextJob(@[], testWorker)
      discard capped.takeNextJob(@[], testWorker)
      # Finish in a different order from submission: second, first, third.
      check capped.markSucceeded(secondId, testWorker, "2")
      check capped.markFailed(firstId, testWorker, "1")
      check capped.getJob(secondId).isSome()
      check capped.markSucceeded(thirdId, testWorker, "3")

      check capped.getJob(secondId).isNone() # finished first, so removed
      check capped.getJob(firstId).isSome()
      check capped.getJob(thirdId).isSome()
      check capped.getJob(queuedId).get().status == jsQueued # never removed
      check capped.countByStatus() == [1, 0, 1, 1]

      closeJobStore(capped)
      if cappedPath.len > 0:
        removeDatabase(cappedPath)

    test "a negative maxFinishedJobs is rejected":
      var badStore: JobStore
      expect ValueError:
        initJobStore(badStore, "", maxFinishedJobs = -1)

suite "store (sqlite) across restarts":
  setup:
    let databasePath = newDatabasePath()
    var firstStore: JobStore
    initJobStore(firstStore, databasePath)

  teardown:
    removeDatabase(databasePath)

  test "finished and queued jobs are still there after reopening":
    let finishedId = firstStore.addJob("finished", "{}")
    let queuedId = firstStore.addJob("queued", "{}")
    discard firstStore.takeNextJob(@[], testWorker)
    check firstStore.markSucceeded(finishedId, testWorker, "42")
    closeJobStore(firstStore)

    var secondStore: JobStore
    initJobStore(secondStore, databasePath)
    check secondStore.recoveredJobCount() == 0
    check secondStore.getJob(finishedId).get().status == jsSucceeded
    check secondStore.getJob(finishedId).get().resultJson == "42"
    check secondStore.queuedCount() == 1
    check secondStore.takeNextJob(@[], testWorker).get().id == queuedId
    closeJobStore(secondStore)

  test "a job that was running is queued again, ahead of newer jobs":
    let interruptedId = firstStore.addJob("interrupted", "{}")
    let waitingId = firstStore.addJob("waiting", "{}")
    discard firstStore.takeNextJob(@[], testWorker) # now running, and never finishes
    closeJobStore(firstStore)

    var secondStore: JobStore
    initJobStore(secondStore, databasePath)
    check secondStore.recoveredJobCount() == 1
    check secondStore.getJob(interruptedId).get().status == jsQueued
    check secondStore.getJob(interruptedId).get().startedAt == 0.0
    check secondStore.takeNextJob(@[], testWorker).get().id == interruptedId
    check secondStore.takeNextJob(@[], testWorker).get().id == waitingId
    closeJobStore(secondStore)

  test "a lower maxFinishedJobs is applied when the file is reopened":
    for i in 0 ..< 5:
      discard firstStore.addJob("job" & $i, "{}")
    firstStore.finishAll(5)
    closeJobStore(firstStore)

    var secondStore: JobStore
    initJobStore(secondStore, databasePath, maxFinishedJobs = 2)
    check secondStore.countByStatus()[jsSucceeded] == 2

    # The finished count was read back correctly, so the limit still holds.
    discard secondStore.addJob("one-more", "{}")
    secondStore.finishAll(1)
    check secondStore.countByStatus()[jsSucceeded] == 2
    closeJobStore(secondStore)
