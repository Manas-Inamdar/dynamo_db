# Changelog

## 1.0.0: rewrite

A ground-up rework of the original course-project implementation (commit
`eebab5b`). [docs/BUG_AUDIT.md](docs/BUG_AUDIT.md) lists every problem
found in the original and how each was fixed.

### Added
- Dotted version vectors: no lost updates or clock collisions when one
  coordinator handles concurrent writes.
- Sloppy quorum: writes for dead replicas go to healthy stand-ins (the
  previously unused `PutHint` RPC), which hand them back on recovery.
- Persistent, reconciled hint store; durable write counter.
- Asynchronous read repair that waits for all replicas off the request path.
- Heartbeat-based failure detection on the local monotonic clock, partition
  healing, leave/rejoin with process generations.
- `DynamoClient` library with discovery and failover; `dynamo-cli` with
  `put / get / resolve / members / leave` and meaningful exit codes.
- Installable package (`pyproject.toml`), `dynamo-server` / `dynamo-cli`
  entry points, Makefile, CI.
- pytest suite: unit, in-process integration, multi-process end-to-end.
- Benchmark harness and six reproducible experiments in `experiments/`.
- Documentation: design, bug audit, testing, experiments.

### Fixed
- A single node crash deadlocked every surviving node permanently
  (re-acquiring a non-reentrant lock on the first write after a failure).
- False failure detections under load (suspect, probe, then judge).
- Unbounded growth of stored versions (memory, disk and response size).
- ~50 ms latency floor from polling quorum waits; writes waited for every
  replica.
- New gRPC channel per RPC (never closed); reconnect backoff of up to 120 s.
- Hinted handoff blocking the gossip handler; hints lost on restart.
- Nodes unable to rejoin after leaving; permanent mutual-DOWN partitions.
- Port sharing via gRPC's default `SO_REUSEPORT`.
- Benchmark and test tooling that reported wrong results (see the bug audit,
  items 27-30).

### Removed
- `prev_server.py`, `tools.bak/`, generated artifacts and logs committed to
  the repository; shell "tests" without assertions; `join_node.py` and
  `leave_node.py` (now `dynamo-cli` commands).
