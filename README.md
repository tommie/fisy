# Fisy - File Synchronizer

[![Build and Test](https://github.com/tommie/fisy/actions/workflows/makefile.yml/badge.svg)](https://github.com/tommie/fisy/actions/workflows/makefile.yml)
[![GoDoc](https://godoc.org/github.com/tommie/fisy?status.svg)](https://godoc.org/github.com/tommie/fisy)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Fisy is a bidirectional file synchronizer for multiple hosts sharing a
single server. It can be used as a "private cloud" while allowing you
to work offline. It is useful for backups where you also want the
benefit of being able to access the files directly on the server.

The server-side storage uses flat files, so inspecting the state of a
backup can be done with normal file tools. Hardlinks are used to
deduplicate data. Uploads are copy-on-write. Collisions between
multiple hosts are handled at download time (or through a web
interface). I.e. uploads will still succeed, and there is no ambiguity
of where a file came from.

## Current state

Don't use this software for data you care about. It has seen very
little testing.

## License

Distributed under the [MIT License](LICENSE).
