## Maps job names to the procs that run them.
##
## The registry is filled in before the server starts and is read-only
## afterwards, so worker threads can read it without a lock.

import std/[algorithm, options, sequtils, tables]
import ./types

type JobRegistry* = object
  handlers: Table[string, JobHandler]

proc registerJob*(registry: var JobRegistry, name: string, handler: JobHandler) =
  ## Raises ValueError if `name` is empty or already registered.
  if name.len == 0:
    raise newException(ValueError, "job name must not be empty")
  if name in registry.handlers:
    raise newException(ValueError, "a job named '" & name & "' is already registered")
  registry.handlers[name] = handler

proc hasJob*(registry: JobRegistry, name: string): bool =
  return name in registry.handlers

proc findHandler*(registry: JobRegistry, name: string): Option[JobHandler] =
  if name in registry.handlers:
    return some(registry.handlers[name])
  return none(JobHandler)

proc jobNames*(registry: JobRegistry): seq[string] =
  ## Sorted list of registered names, for GET /job-names.
  return toSeq(registry.handlers.keys()).sorted()

proc unknownJobMessage*(name: string): string =
  ## The error given when a job name is not registered.
  return "no job is registered with the name '" & name & "'"
