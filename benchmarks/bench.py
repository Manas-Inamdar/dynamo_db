"""Closed-loop load generator.

Each of ``clients`` workers issues one request at a time (closed loop) to a
coordinator chosen round-robin across the cluster, like a client-side load
balancer: a coordinator that fails is skipped by the whole process for
``AVOID_SECONDS``, and a request rejected as unavailable is retried once on
the next coordinator. Workers are spread over several processes so the generator itself
is not limited by Python's GIL.

Works against both this implementation and the original one: the wire
protocol is compatible, and the original's in-band quorum failures
(``success=False`` / ``"quorum"`` messages) are mapped to the same outcome
as this implementation's gRPC UNAVAILABLE status.

Usage::

    python -m benchmarks.bench --nodes localhost:50051,localhost:50052,localhost:50053 \\
        --clients 16 --duration 20 --put-ratio 0.5 --out bench_out/run1
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
import multiprocessing as mp
import os
import random
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path

OK, QUORUM_FAIL, UNAVAILABLE, TIMEOUT, ERROR = "ok", "quorum_fail", "unavailable", "timeout", "error"
AVOID_SECONDS = 3.0      # how long a process avoids a coordinator that just failed


@dataclass
class LoadSpec:
    nodes: list[str]
    clients: int = 16
    duration: float = 20.0
    put_ratio: float = 0.5
    keyspace: int = 1000
    value_size: int = 100
    timeout: float = 2.0
    processes: int = 0           # 0 => min(clients, cpu count)
    seed: int = 42


# --------------------------------------------------------------------- worker
def _classify(exc: Exception) -> str:
    import grpc

    if isinstance(exc, grpc.RpcError):
        details = exc.details() or ""
        if "quorum" in details:
            return QUORUM_FAIL
        if exc.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
            return TIMEOUT
        if exc.code() == grpc.StatusCode.UNAVAILABLE:
            return UNAVAILABLE
    return ERROR


def _worker_process(spec: dict, worker_ids: list[int], start_at: float, queue) -> None:
    """Run a group of client threads; send their records back via ``queue``."""
    import grpc

    from dynamo.proto import dynamo_pb2 as pb
    from dynamo.proto import dynamo_pb2_grpc
    from dynamo.rpc import CHANNEL_OPTIONS

    nodes = spec["nodes"]
    # Same channel options as DynamoClient: in particular a short minimum
    # reconnect backoff, which also bounds how long a connection attempt to
    # a dead node can hang (gRPC's default is 20 s).
    channels = [grpc.insecure_channel(a, options=CHANNEL_OPTIONS) for a in nodes]
    stubs = [dynamo_pb2_grpc.DynamoServiceStub(c) for c in channels]
    records: list[tuple] = []
    lock = threading.Lock()
    end_at = start_at + spec["duration"]
    # Coordinators that recently failed are avoided by every thread of this
    # process for a few seconds, like a client-side load balancer would.
    avoid_until = [0.0] * len(nodes)

    def pick(idx: int) -> int:
        now = time.time()
        for step in range(len(nodes)):
            if avoid_until[(idx + step) % len(nodes)] <= now:
                return idx + step
        return idx

    def client(wid: int) -> None:
        rng = random.Random(spec["seed"] * 1000 + wid)
        value = os.urandom(spec["value_size"])
        local: list[tuple] = []
        idx = wid
        while time.time() < start_at:
            time.sleep(0.001)
        while (now := time.time()) < end_at:
            key = f"key-{rng.randrange(spec['keyspace'])}"
            is_put = rng.random() < spec["put_ratio"]
            status, retried = ERROR, 0
            for attempt in range(2):             # one failover retry, like DynamoClient
                idx = pick(idx)
                node = idx % len(stubs)
                stub = stubs[node]
                idx += 1
                try:
                    if is_put:
                        reply = stub.Put(pb.PutRequest(key=key, data=pb.ValueWithContext(value=value)),
                                         timeout=spec["timeout"])
                        status = OK if reply.success else QUORUM_FAIL
                    else:
                        reply = stub.Get(pb.GetRequest(key=key), timeout=spec["timeout"])
                        status = QUORUM_FAIL if "quorum" in reply.message.lower() else OK
                except Exception as exc:  # noqa: BLE001
                    status = _classify(exc)
                if status in (UNAVAILABLE, TIMEOUT):
                    avoid_until[node] = time.time() + AVOID_SECONDS
                if status != UNAVAILABLE:
                    break
                retried = attempt + 1
            latency_ms = (time.time() - now) * 1000.0
            local.append((round(now - start_at, 4), round(latency_ms, 3),
                          "put" if is_put else "get", status, retried, node))
        with lock:
            records.extend(local)

    threads = [threading.Thread(target=client, args=(w,)) for w in worker_ids]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    for c in channels:
        c.close()
    queue.put(records)


def preload(nodes: list[str], keyspace: int, value_size: int) -> None:
    """Write every key once so GETs measure real reads, not misses."""
    import grpc

    from dynamo.proto import dynamo_pb2 as pb
    from dynamo.proto import dynamo_pb2_grpc

    stubs = [dynamo_pb2_grpc.DynamoServiceStub(grpc.insecure_channel(a)) for a in nodes]
    value = b"x" * value_size

    def load(part: range) -> None:
        for i in part:
            for attempt in range(5):
                try:
                    stubs[(i + attempt) % len(stubs)].Put(
                        pb.PutRequest(key=f"key-{i}", data=pb.ValueWithContext(value=value)), timeout=5)
                    break
                except Exception:  # noqa: BLE001
                    time.sleep(0.2)

    step = max(1, keyspace // 8)
    threads = [threading.Thread(target=load, args=(range(s, min(s + step, keyspace)),))
               for s in range(0, keyspace, step)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def run(spec: LoadSpec) -> list[tuple]:
    """Run the load; records are ``(t, latency_ms, op, status, retried, coordinator_index)``."""
    procs = spec.processes or min(spec.clients, os.cpu_count() or 1)
    groups = [list(range(i, spec.clients, procs)) for i in range(procs)]
    ctx = mp.get_context("spawn")          # never fork a process that holds gRPC state
    queue = ctx.Queue()
    start_at = time.time() + 2.0           # leave time for interpreters to start
    workers = [ctx.Process(target=_worker_process, args=(asdict(spec), g, start_at, queue))
               for g in groups if g]
    for w in workers:
        w.start()
    records: list[tuple] = []
    for _ in workers:
        records.extend(queue.get())
    for w in workers:
        w.join()
    records.sort()
    return records


# -------------------------------------------------------------------- analysis
def percentile(sorted_values: list[float], p: float) -> float:
    """Nearest-rank percentile of an already sorted list."""
    if not sorted_values:
        return float("nan")
    rank = max(1, min(len(sorted_values), math.ceil(p / 100 * len(sorted_values))))
    return sorted_values[rank - 1]


def latency_stats(latencies: list[float]) -> dict:
    s = sorted(latencies)
    return {
        "count": len(s),
        "mean_ms": round(sum(s) / len(s), 3) if s else None,
        "p50_ms": round(percentile(s, 50), 3) if s else None,
        "p90_ms": round(percentile(s, 90), 3) if s else None,
        "p99_ms": round(percentile(s, 99), 3) if s else None,
        "p999_ms": round(percentile(s, 99.9), 3) if s else None,
        "max_ms": round(s[-1], 3) if s else None,
    }


def summarize(records: list[tuple], spec: LoadSpec) -> dict:
    ok = [r for r in records if r[3] == OK]
    statuses: dict[str, int] = {}
    for r in records:
        statuses[r[3]] = statuses.get(r[3], 0) + 1
    return {
        "spec": asdict(spec),
        "total_requests": len(records),
        "successful": len(ok),
        "throughput_ops_s": round(len(ok) / spec.duration, 1),
        "error_rate": round(1 - len(ok) / len(records), 5) if records else None,
        "statuses": statuses,
        "retried": sum(1 for r in records if r[4]),
        "latency_all": latency_stats([r[1] for r in ok]),
        "latency_put": latency_stats([r[1] for r in ok if r[2] == "put"]),
        "latency_get": latency_stats([r[1] for r in ok if r[2] == "get"]),
    }


def timeline(records: list[tuple], duration: float, bucket: float = 1.0) -> list[dict]:
    """Per-bucket throughput, error count and latency percentiles."""
    n = int(duration / bucket + 0.999)
    buckets: list[list[tuple]] = [[] for _ in range(n)]
    for r in records:
        i = int(r[0] / bucket)
        if 0 <= i < n:
            buckets[i].append(r)
    rows = []
    for i, rs in enumerate(buckets):
        lat = sorted(r[1] for r in rs if r[3] == OK)
        rows.append({
            "t": round((i + 1) * bucket, 3),
            "ok_per_s": round(len(lat) / bucket, 1),
            "errors_per_s": round(sum(1 for r in rs if r[3] != OK) / bucket, 1),
            "p50_ms": round(percentile(lat, 50), 3) if lat else "",
            "p99_ms": round(percentile(lat, 99), 3) if lat else "",
        })
    return rows


def write_results(out: Path, records: list[tuple], spec: LoadSpec, extra: dict | None = None) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    summary = summarize(records, spec)
    if extra:
        summary.update(extra)
    (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    with open(out / "timeline.csv", "w", newline="") as fh:
        rows = timeline(records, spec.duration)
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    with gzip.open(out / "requests.csv.gz", "wt", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["t_s", "latency_ms", "op", "status", "retried", "coordinator"])
        writer.writerows(records)
    return summary


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--nodes", required=True, help="comma-separated host:port of every coordinator")
    p.add_argument("--clients", type=int, default=16)
    p.add_argument("--duration", type=float, default=20.0)
    p.add_argument("--put-ratio", type=float, default=0.5)
    p.add_argument("--keyspace", type=int, default=1000)
    p.add_argument("--value-size", type=int, default=100)
    p.add_argument("--timeout", type=float, default=2.0)
    p.add_argument("--processes", type=int, default=0)
    p.add_argument("--no-preload", action="store_true")
    p.add_argument("--out", default="bench_out/latest")
    a = p.parse_args(argv)
    spec = LoadSpec(nodes=[n.strip() for n in a.nodes.split(",")], clients=a.clients,
                    duration=a.duration, put_ratio=a.put_ratio, keyspace=a.keyspace,
                    value_size=a.value_size, timeout=a.timeout, processes=a.processes)
    if not a.no_preload:
        preload(spec.nodes, spec.keyspace, spec.value_size)
    summary = write_results(Path(a.out), run(spec), spec)
    print(json.dumps({k: summary[k] for k in ("throughput_ops_s", "error_rate", "latency_all")}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
