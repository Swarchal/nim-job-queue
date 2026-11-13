## Keeps jobs in SQLite: in a file, so they survive a restart, or in memory
## (":memory:") when no file is given.
##
## Not thread-safe by itself: store.nim holds its lock around every call, so
## the single connection is only ever used by one thread at a time.
##
## Only one server process may use a database file at a time.

import std/[options, strutils]
import db_connector/db_sqlite
import ./types

type SqliteBackend* = object
  database: DbConn
  recoveredJobCount*: int ## interrupted jobs put back in the queue on opening
  maxFinishedJobs: int ## 0 means keep every finished job
  finishedJobCount: int ## kept up to date here, to avoid a COUNT(*) per finish

const jobColumns =
  "id, name, status, args_json, result_json, error_message, " &
  "submitted_at, started_at, finished_at, worker_id, lease_expires_at, " &
  "retry_count, run_after"

const selectJobByIdStatement = sql("SELECT " & jobColumns & " FROM jobs WHERE id = ?")

const createTableStatement =
  sql"""
  CREATE TABLE IF NOT EXISTS jobs (
    sequence         INTEGER PRIMARY KEY AUTOINCREMENT,
    id               TEXT NOT NULL UNIQUE,
    name             TEXT NOT NULL,
    status           TEXT NOT NULL,
    args_json        TEXT NOT NULL,
    result_json      TEXT NOT NULL DEFAULT '',
    error_message    TEXT NOT NULL DEFAULT '',
    submitted_at     REAL NOT NULL,
    started_at       REAL NOT NULL DEFAULT 0,
    finished_at      REAL NOT NULL DEFAULT 0,
    worker_id        TEXT NOT NULL DEFAULT '',
    lease_expires_at REAL NOT NULL DEFAULT 0,
    retry_count      INTEGER NOT NULL DEFAULT 0,
    run_after        REAL NOT NULL DEFAULT 0
  )"""

const createIndexStatement =
  sql"""
  CREATE INDEX IF NOT EXISTS jobs_by_status_and_sequence
  ON jobs (status, sequence)"""

# A job is finished exactly when finished_at > 0. This index covers only
# finished jobs, in the order they finished, so the oldest are cheap to find.
const createFinishedIndexStatement =
  sql"""
  CREATE INDEX IF NOT EXISTS finished_jobs_by_finish_order
  ON jobs (finished_at, sequence) WHERE finished_at > 0"""

# Lets earliestDelayedJob find the next job waiting for a retry without
# reading every queued job.
const createRunAfterIndexStatement =
  sql"""
  CREATE INDEX IF NOT EXISTS jobs_by_status_and_run_after
  ON jobs (status, run_after)"""

const deleteOldestFinishedStatement =
  sql"""DELETE FROM jobs WHERE sequence IN (
          SELECT sequence FROM jobs WHERE finished_at > 0
          ORDER BY finished_at, sequence LIMIT ?)"""

proc removeOldestFinishedJobs(backend: var SqliteBackend) =
  ## Delete the jobs that finished longest ago, beyond maxFinishedJobs.
  if backend.maxFinishedJobs <= 0:
    return
  let excess = backend.finishedJobCount - backend.maxFinishedJobs
  if excess <= 0:
    return
  let removed = backend.database.execAffectedRows(deleteOldestFinishedStatement, excess)
  backend.finishedJobCount -= int(removed)

proc addColumnIfMissing(database: DbConn, columnName: string, definition: string) =
  ## Bring a database file written by an older version up to date.
  for row in database.getAllRows(sql"PRAGMA table_info(jobs)"):
    if row[1] == columnName:
      return
  database.exec(sql("ALTER TABLE jobs ADD COLUMN " & columnName & " " & definition))

proc openSqliteBackend*(databasePath: string, maxFinishedJobs: int): SqliteBackend =
  ## Open (creating if needed) the database. `databasePath` ":memory:" keeps
  ## everything in memory.
  ##
  ## Jobs that were running when the previous server stopped never reported a
  ## result, so they are put back in the queue, in their original order, to
  ## be run again. If the file holds more finished jobs than
  ## `maxFinishedJobs` (the limit may have been lowered since the last run),
  ## the oldest are removed now.
  result.maxFinishedJobs = maxFinishedJobs
  result.database = open(databasePath, "", "", "")
  # Write-ahead logging makes each small write much cheaper.
  discard result.database.getValue(sql"PRAGMA journal_mode = WAL")
  result.database.exec(sql"PRAGMA synchronous = NORMAL")
  result.database.exec(createTableStatement)
  result.database.addColumnIfMissing("retry_count", "INTEGER NOT NULL DEFAULT 0")
  result.database.addColumnIfMissing("run_after", "REAL NOT NULL DEFAULT 0")
  result.database.exec(createRunAfterIndexStatement)
  result.database.exec(createIndexStatement)
  result.database.exec(createFinishedIndexStatement)
  result.recoveredJobCount = int(
    result.database.execAffectedRows(
      sql"""UPDATE jobs SET status = ?, started_at = 0, worker_id = '',
            lease_expires_at = 0 WHERE status = ?""",
      $jsQueued,
      $jsRunning,
    )
  )
  result.finishedJobCount = parseInt(
    result.database.getValue(sql"SELECT COUNT(*) FROM jobs WHERE finished_at > 0")
  )
  result.removeOldestFinishedJobs()

proc closeSqliteBackend*(backend: var SqliteBackend) =
  if backend.database != nil:
    backend.database.close()
    backend.database = nil

proc rowToJob(row: Row): JobRecord =
  ## `row` must have been selected with `jobColumns`.
  ## SQLite returns numbers as text with 15 significant digits, so times come
  ## back rounded to about a microsecond.
  return JobRecord(
    id: row[0],
    name: row[1],
    status: parseEnum[JobStatus](row[2]),
    argsJson: row[3],
    resultJson: row[4],
    errorMessage: row[5],
    submittedAt: parseFloat(row[6]),
    startedAt: parseFloat(row[7]),
    finishedAt: parseFloat(row[8]),
    workerId: row[9],
    leaseExpiresAt: parseFloat(row[10]),
    retryCount: parseInt(row[11]),
    runAfter: parseFloat(row[12]),
  )

proc insertJob*(backend: var SqliteBackend, job: JobRecord) =
  backend.database.exec(
    sql"""INSERT INTO jobs (id, name, status, args_json, submitted_at)
          VALUES (?, ?, ?, ?, ?)""",
    job.id,
    job.name,
    $job.status,
    job.argsJson,
    $job.submittedAt,
  )

proc claimNextJob*(
    backend: var SqliteBackend,
    jobNames: seq[string],
    workerId: string,
    startedAt: float,
    leaseExpiresAt: float,
): Option[JobRecord] =
  ## Mark the oldest queued job whose name is in `jobNames` as running on
  ## `workerId` and return it, or none() if there is no such job. An empty
  ## `jobNames` matches every name. Jobs waiting for a retry are skipped
  ## until their `run_after` time; `startedAt` is taken as the time now.
  var query = "SELECT " & jobColumns & " FROM jobs WHERE status = ? AND run_after <= ?"
  var arguments = @[$jsQueued, $startedAt]
  if jobNames.len > 0:
    var placeholders = newSeq[string](jobNames.len)
    for i in 0 ..< jobNames.len:
      placeholders[i] = "?"
    query.add(" AND name IN (" & placeholders.join(", ") & ")")
    arguments.add(jobNames)
  query.add(" ORDER BY sequence LIMIT 1")

  let row = backend.database.getRow(sql(query), arguments)
  if row[0].len == 0:
    return none(JobRecord)

  var job = rowToJob(row)
  job.status = jsRunning
  job.startedAt = startedAt
  job.workerId = workerId
  job.leaseExpiresAt = leaseExpiresAt
  backend.database.exec(
    sql"""UPDATE jobs SET status = ?, started_at = ?, worker_id = ?,
          lease_expires_at = ? WHERE id = ?""",
    $job.status,
    $job.startedAt,
    job.workerId,
    $job.leaseExpiresAt,
    job.id,
  )
  return some(job)

proc extendLease*(
    backend: var SqliteBackend, jobId: string, workerId: string, leaseExpiresAt: float
): bool =
  ## Returns false if the job is no longer running on `workerId`.
  let updated = backend.database.execAffectedRows(
    sql"""UPDATE jobs SET lease_expires_at = ?
          WHERE id = ? AND status = ? AND worker_id = ?""",
    $leaseExpiresAt,
    jobId,
    $jsRunning,
    workerId,
  )
  return updated == 1

proc requeueExpiredLeases*(backend: var SqliteBackend, now: float): int =
  ## Put running jobs whose lease ran out back in the queue, in their
  ## original order (the queue is ordered by `sequence`). Returns how many.
  let updated = backend.database.execAffectedRows(
    sql"""UPDATE jobs SET status = ?, started_at = 0, worker_id = '',
          lease_expires_at = 0
          WHERE status = ? AND lease_expires_at > 0 AND lease_expires_at < ?""",
    $jsQueued,
    $jsRunning,
    $now,
  )
  return int(updated)

proc retryJob*(
    backend: var SqliteBackend, jobId: string, workerId: string, runAfter: float
): bool =
  ## Put a running job back in the queue, in its original order, to be run
  ## again once `runAfter` has passed, and count the retry. Returns false,
  ## and changes nothing, if the job is no longer running on `workerId`.
  let updated = backend.database.execAffectedRows(
    sql"""UPDATE jobs SET status = ?, started_at = 0, worker_id = '',
          lease_expires_at = 0, retry_count = retry_count + 1, run_after = ?
          WHERE id = ? AND status = ? AND worker_id = ?""",
    $jsQueued,
    $runAfter,
    jobId,
    $jsRunning,
    workerId,
  )
  return updated == 1

proc earliestDelayedJob*(backend: var SqliteBackend, now: float): float =
  ## The soonest `run_after` among queued jobs that are still waiting for
  ## it, or 0.0 if no job is waiting.
  let earliest = backend.database.getValue(
    sql"SELECT MIN(run_after) FROM jobs WHERE status = ? AND run_after > ?",
    $jsQueued,
    $now,
  )
  if earliest.len == 0:
    return 0.0
  return parseFloat(earliest)

proc finishJob*(
    backend: var SqliteBackend,
    jobId: string,
    workerId: string,
    status: JobStatus,
    resultJson: string,
    errorMessage: string,
    finishedAt: float,
): bool =
  ## Record the outcome. Returns false, and changes nothing, if the job is no
  ## longer running on `workerId` (its lease expired, say).
  let updated = backend.database.execAffectedRows(
    sql"""UPDATE jobs
          SET status = ?, result_json = ?, error_message = ?, finished_at = ?,
              worker_id = '', lease_expires_at = 0
          WHERE id = ? AND status = ? AND worker_id = ?""",
    $status,
    resultJson,
    errorMessage,
    $finishedAt,
    jobId,
    $jsRunning,
    workerId,
  )
  backend.finishedJobCount += int(updated)
  backend.removeOldestFinishedJobs()
  return updated == 1

proc findJob*(backend: var SqliteBackend, jobId: string): Option[JobRecord] =
  let row = backend.database.getRow(selectJobByIdStatement, jobId)
  if row[0].len == 0:
    return none(JobRecord)
  return some(rowToJob(row))

proc countQueued*(backend: var SqliteBackend): int =
  let count = backend.database.getValue(
    sql"SELECT COUNT(*) FROM jobs WHERE status = ?", $jsQueued
  )
  return parseInt(count)

proc countByStatus*(backend: var SqliteBackend): array[JobStatus, int] =
  for row in backend.database.fastRows(
    sql"SELECT status, COUNT(*) FROM jobs GROUP BY status"
  ):
    result[parseEnum[JobStatus](row[0])] = parseInt(row[1])
