# Package
version = "0.1.0"
author = "Scott Warchal"
description =
  "A celery-like job queue server for Nim: submit jobs over HTTP, run them on a worker pool, fetch the results."
license = "MIT"
srcDir = "src"

# Dependencies
requires "nim >= 2.0.0"
requires "db_connector >= 0.1.0"
