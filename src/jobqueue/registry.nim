## Maps job names to the procs that run them.
##
## The registry is filled in before the server starts and is read-only
## afterwards, so worker threads can read it without a lock.

import std/[algorithm, options, sequtils, tables]
import ./types

type
  RegisteredJob* = object
    handler*: JobHandler
    maxRetries*: int
      ## How many times the job is run again after raising RetryableJobError.
    retryDelaySeconds*: float
      ## The wait before the first retry; it doubles with each later one.
    retryOn*: seq[string]
      ## Names of other exception types that are retried like
      ## RetryableJobError, e.g. "TimeoutError". Compared with the exact type
      ## of the exception raised, so types that inherit from one are not
      ## included.

  JobRegistry* = object
    jobs: Table[string, RegisteredJob]

proc registerJob*(
    registry: var JobRegistry,
    name: string,
    handler: JobHandler,
    maxRetries: int = 0,
    retryDelaySeconds: float = defaultRetryDelaySeconds,
    retryOn: seq[string] = @[],
) =
  ## Raises ValueError if `name` is empty or already registered, if
  ## `maxRetries` or `retryDelaySeconds` is negative, or if `retryOn` holds an
  ## empty name or is given without `maxRetries` (it would never retry).
  if name.len == 0:
    raise newException(ValueError, "job name must not be empty")
  if name in registry.jobs:
    raise newException(ValueError, "a job named '" & name & "' is already registered")
  if maxRetries < 0:
    raise newException(ValueError, "maxRetries must not be negative")
  if retryDelaySeconds < 0:
    raise newException(ValueError, "retryDelaySeconds must not be negative")
  if retryOn.len > 0 and maxRetries == 0:
    raise newException(ValueError, "retryOn has no effect unless maxRetries is set")
  for errorName in retryOn:
    if errorName.len == 0:
      raise newException(ValueError, "retryOn must not hold an empty name")
  registry.jobs[name] = RegisteredJob(
    handler: handler,
    maxRetries: maxRetries,
    retryDelaySeconds: retryDelaySeconds,
    retryOn: retryOn,
  )

proc hasJob*(registry: JobRegistry, name: string): bool =
  return name in registry.jobs

proc findJob*(registry: JobRegistry, name: string): Option[RegisteredJob] =
  if name in registry.jobs:
    return some(registry.jobs[name])
  return none(RegisteredJob)

proc jobNames*(registry: JobRegistry): seq[string] =
  ## Sorted list of registered names, for GET /job-names.
  return toSeq(registry.jobs.keys()).sorted()

proc unknownJobMessage*(name: string): string =
  ## The error given when a job name is not registered.
  return "no job is registered with the name '" & name & "'"
