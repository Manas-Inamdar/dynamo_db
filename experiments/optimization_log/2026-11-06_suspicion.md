# Fix: false failure detections under load (suspect, probe, then judge)

**Date:** 2026-11-06
**Setup:** 4 nodes, N=3 R=2 W=2, `--fail-timeout 2` (as in E3/E4), 16
closed-loop clients, 20 s, **no node is actually down**. Metric: number of
"marked DEAD" events in the node logs, i.e. false positives. A/B run from
two checkouts with `benchmarks.cluster` + `benchmarks.bench`.

**Observation that triggered it.** E3's healthy phase showed a few failed
requests (0.02-0.04 %). The logs showed live nodes being marked dead and
recovering ~20 ms later. Each of those episodes sends writes down the
hinted-handoff path and can fail a request whose quorum needed that node.

**Cause.** Heartbeats reach a node only through gossip with one random peer
per second, so under CPU saturation a live peer's heartbeat can be more
than 2 s old. The gossip round also *judged* liveness before doing its
exchanges, so a gossip thread that was itself delayed declared peers dead a
few milliseconds before hearing from them.

**Changes.**
1. Suspicion (as in SWIM): peers whose heartbeat is older than half the
   failure timeout are contacted directly in the next round. A live peer
   refreshes its heartbeat; a dead one fails the RPC and is marked dead
   immediately, i.e. *sooner* than before.
2. Run the failure check after the round's exchanges, not before.

| Version | Run 1 | Run 2 |
|---|---:|---:|
| before | 14 false DEAD | 9 false DEAD |
| + suspect probing | 7 | 4 |
| + judge after probing | **0** | **0** |

Throughput was 236-331 ops/s before and 373-405 ops/s after (fewer
spurious hinted writes), all requests succeeded in every run.
