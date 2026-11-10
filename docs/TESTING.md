# Testing

```bash
make test               # ruff + every test
make test-unit          # pure logic, < 2 s
make test-integration   # in-process clusters, ~20 s
make test-e2e           # real server processes via the CLI, ~10 s
```

The latest full run is recorded in [TEST_REPORT.md](TEST_REPORT.md).

## Layers

| Layer | Location | What it exercises | How |
|---|---|---|---|
| Unit | `tests/unit/` | Vector clocks, dotted-version-vector reconciliation, the hash ring, storage/hints/counter durability, membership merge rules and failure detection, config validation | Plain objects. Membership uses injected fake clocks, so timeouts are tested deterministically without sleeping. |
| Integration | `tests/integration/` | PUT/GET semantics, replica placement, quorum failures, sloppy quorum + hinted handoff, read repair, conflicts and resolution, gossip/join/leave/partition healing, persistence across restarts | Real gRPC servers started in-process on free ports (`conftest.py::Cluster`), each with a temp data dir and aggressive timings (100 ms gossip, 0.8 s failure timeout). A "crash" stops a server without warning; a restart reuses its port and data dir. |
| End-to-end | `tests/e2e/` | The shipped entry points: `python -m dynamo.server`, `python -m dynamo.client`, exit codes, SIGKILL + restart, SIGTERM shutdown, port conflicts | Subprocesses |

## What the integration suite proves

| Dynamo technique | Tests |
|---|---|
| Consistent hashing / placement | `test_replication.py::test_value_stored_on_exactly_the_preference_list` (5 nodes, data on exactly the 3 preference-list nodes) |
| Quorums | `test_one_replica_down_is_tolerated`, `test_quorum_failure_reported_when_majority_down`, `test_quorum_read_sees_latest_acknowledged_write`, `test_r1_w1_stays_available_with_one_node` |
| Vector clocks / siblings | `test_conflicts.py` (siblings from concurrent writes, resolution with merged context, stale-context writes, 8-way concurrent writers on one coordinator) |
| Sloppy quorum + hinted handoff | `test_hinted_handoff.py` (stand-in holds and returns the hint, coordinator-held hints when no stand-in exists, RPC failure before the detector notices, hints surviving a stand-in restart, newer writes winning after handoff) |
| Read repair | `test_read_repair.py` (stale, missing and sibling replicas repaired) |
| Membership | `test_membership.py` (discovery through seeds, crash detection and recovery, client failover, join, leave + rejoin, leaving a crashed node, healing a full mutual-DOWN partition, duplicate ports) |
| Durability | `test_persistence.py` (full-cluster restart, no dot reuse across a coordinator restart) |

## Writing reliable distributed tests

The suite was run 20 times in a row to shake out flakiness. The problems
it and the multi-process demo uncovered, and the rules that came out of
them:

1. **Never assert right after a quorum operation on state outside the
   quorum.** `Put` returns after W acks; the third replica (or a stand-in's
   hint) may still be in flight. Use `wait_until(...)`.
2. **Bump counters before the effect they count**, so an observer that sees
   the effect also sees the count.
3. **Do not pick test ports by binding port 0.** That returns a port from the
   kernel's ephemeral range, which any outgoing connection can take as its
   source port before the server binds it. `free_port()` picks from
   20000–32000 (below the ephemeral range), never hands out the same port
   twice in a session (two nodes drawing the same random number was a ~1 %
   per-run collision), and the fixture retries a bind.
   This was invisible before `SO_REUSEPORT` was disabled ([bug audit
   #19](BUG_AUDIT.md)), because two sockets could share a port silently.
4. **Know what your RPC library shares.** gRPC Python shares TCP
   connections between channels with the same target and options across the
   whole process. In-process tests therefore hid a real startup bug: node A's
   channel to node B was backing off after an early failed attempt, but A's
   requests succeeded through a connection *another in-process node* had
   opened. Each node now owns its connections, which is how it behaves in
   production, where every node is its own process.
5. **Make sure stray processes are gone before trusting a result.** A
   leftover cluster kept answering on the expected ports and made a broken
   start script look healthy (see the port-conflict checks in `scripts/`).

## Baseline

The original `test_server.py` (58 tests) never completes without per-test
timeouts. With a 30 s timeout it ends at 21 failed / 37 passed in 212 s.
See [BUG_AUDIT.md](BUG_AUDIT.md).
