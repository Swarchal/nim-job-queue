## Storage for queued jobs and their results, shared by the HTTP thread and
## the worker threads.
##
## This module owns the lock and the "a job is available" signal, and every
## public proc takes the lock itself, so callers never touch it. The jobs
## themselves are kept by sqlite_backend.nim, in a file or in memory.
##
## Every running job belongs to a worker id. Only that worker may report its
## outcome or extend its lease, so a worker whose lease ran out (and whose job
## was handed to someone else) cannot overwrite the other worker's result.

import std/[locks, oids, options, times]
import ./[sqlite_backend, types]

type JobStore* = object
  lock: Lock
  jobIsAvailable: Cond ## signalled when a job is queued or on shutdown
  isShuttingDown: bool
  backend: SqliteBackend

proc initJobStore*(
    store: var JobStore, databasePath: string = "", maxFinishedJobs: int = 0
) =
  ## An empty `databasePath` keeps jobs in memory. Otherwise it names a SQLite
  ## file, which is created if it does not exist.
  ##
  ## Once more than `maxFinishedJobs` jobs have succeeded or failed, the ones
  ## that finished longest ago are removed. 0 keeps every finished job.
  if maxFinishedJobs < 0:
    raise newException(ValueError, "maxFinishedJobs must not be negative")
  var path = databasePath
  if path.len == 0:
    path = ":memory:"
  initLock(store.lock)
  initCond(store.jobIsAvailable)
  store.backend = openSqliteBackend(path, maxFinishedJobs)

proc recoveredJobCount*(store: var JobStore): int =
  ## How many jobs were found interrupted by a previous shutdown or crash and
  ## were put back in the queue when the store was opened.
  withLock store.lock:
    result = store.backend.recoveredJobCount

proc addJob*(store: var JobStore, name: string, argsJson: string): string =
  ## Queue a new job and wake one worker thread. Returns the new job's id.
  withLock store.lock:
    let job = JobRecord(
      id: $genOid(),
      name: name,
      status: jsQueued,
      argsJson: argsJson,
      submittedAt: epochTime(),
    )
    store.backend.insertJob(job)
    signal(store.jobIsAvailable)
    result = job.id

proc takeNextJob*(
    store: var JobStore, jobNames: seq[string], workerId: string
): Option[JobRecord] =
  ## Called by the server's own worker threads. Blocks until a job whose name
  ## is in `jobNames` is queued (an empty list matches any name), marks it as
  ## running on `workerId` and returns it. These jobs have no lease. Returns
  ## none() once the store is shutting down.
  withLock store.lock:
    result = none(JobRecord)
    while not store.isShuttingDown:
      result = store.backend.claimNextJob(
        jobNames, workerId, startedAt = epochTime(), leaseExpiresAt = 0.0
      )
      if result.isSome():
        break
      wait(store.jobIsAvailable, store.lock)

proc claimJob*(
    store: var JobStore, jobNames: seq[string], workerId: string, leaseSeconds: float
): Option[JobRecord] =
  ## Called for remote workers. Like takeNextJob, but returns none() at once
  ## if nothing matches, and the job is put back in the queue unless the
  ## worker extends its lease within `leaseSeconds`.
  withLock store.lock:
    let now = epochTime()
    result = store.backend.claimNextJob(
      jobNames, workerId, startedAt = now, leaseExpiresAt = now + leaseSeconds
    )

proc extendLease*(
    store: var JobStore, jobId: string, workerId: string, leaseSeconds: float
): bool =
  ## A remote worker's heartbeat. Returns false if the job is no longer
  ## running on `workerId`.
  withLock store.lock:
    result = store.backend.extendLease(jobId, workerId, epochTime() + leaseSeconds)

proc requeueExpiredLeases*(store: var JobStore): int =
  ## Put jobs whose remote worker stopped sending heartbeats back in the
  ## queue, and wake the worker threads. Returns how many were put back.
  withLock store.lock:
    result = store.backend.requeueExpiredLeases(epochTime())
    if result > 0:
      broadcast(store.jobIsAvailable)

proc finishJob(
    store: var JobStore,
    jobId: string,
    workerId: string,
    status: JobStatus,
    resultJson: string,
    errorMessage: string,
): bool =
  withLock store.lock:
    result = store.backend.finishJob(
      jobId, workerId, status, resultJson, errorMessage, epochTime()
    )

proc markSucceeded*(
    store: var JobStore, jobId: string, workerId: string, resultJson: string
): bool =
  ## Returns false, and changes nothing, if the job is not running on
  ## `workerId`.
  return store.finishJob(jobId, workerId, jsSucceeded, resultJson, "")

proc markFailed*(
    store: var JobStore, jobId: string, workerId: string, errorMessage: string
): bool =
  ## Returns false, and changes nothing, if the job is not running on
  ## `workerId`.
  return store.finishJob(jobId, workerId, jsFailed, "", errorMessage)

proc getJob*(store: var JobStore, jobId: string): Option[JobRecord] =
  ## Returns a copy of the job, or none() if the id is unknown.
  withLock store.lock:
    result = store.backend.findJob(jobId)

proc queuedCount*(store: var JobStore): int =
  withLock store.lock:
    result = store.backend.countQueued()

proc countByStatus*(store: var JobStore): array[JobStatus, int] =
  ## How many jobs are in each status, for GET /status.
  withLock store.lock:
    result = store.backend.countByStatus()

proc shutdown*(store: var JobStore) =
  ## Wake every blocked worker thread so that takeNextJob returns none().
  ## Jobs that are still queued are not run: in memory they are lost, in a
  ## SQLite file they stay queued for the next start.
  withLock store.lock:
    store.isShuttingDown = true
    broadcast(store.jobIsAvailable)

proc closeJobStore*(store: var JobStore) =
  ## Close the database. Call this after the worker threads have stopped; the
  ## store must not be used afterwards.
  withLock store.lock:
    closeSqliteBackend(store.backend)
