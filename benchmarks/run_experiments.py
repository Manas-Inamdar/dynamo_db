"""Reproducible experiments. Results are written to ``experiments/<id>/``.

    python -m benchmarks.run_experiments all          # everything (~20 min)
    python -m benchmarks.run_experiments e1 e5        # a subset

| id | question |
|----|----------|
| e1 | How does the rewrite compare with the original implementation? |
| e2 | How do throughput and latency scale with concurrent clients? |
| e3 | What do different (R, W) quorums cost, and what stays available with a node down? |
| e4 | What happens to clients while a node crashes and recovers, and how fast do replicas converge? |
| e5 | How many virtual nodes are needed for an even key distribution? |
| e6 | What does fsync-per-write cost? |
"""

from __future__ import annotations

import argparse
import collections
import csv
import json
import platform
import random
import re
import shutil
import signal
import statistics
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import grpc

from benchmarks.bench import LoadSpec, preload, run, summarize, write_results
from benchmarks.cluster import ROOT, ProcessCluster, ensure_original_worktree
from dynamo.proto import dynamo_pb2 as pb
from dynamo.proto import dynamo_pb2_grpc
from dynamo.ring import HashRing, hash_key
from dynamo.storage import Storage
from dynamo.versioning import reconcile, version_key

OUT = ROOT / "experiments"
WORK = ROOT / "bench_out"          # scratch: logs and data dirs (gitignored)
ORIGINAL = WORK / "original-worktree"


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def save_json(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2) + "\n")


def cluster(impl: str, size: int, name: str, server_args: list[str] | None = None) -> ProcessCluster:
    kwargs = {"original_dir": ensure_original_worktree(ORIGINAL)} if impl == "original" else {}
    return ProcessCluster(size, WORK / name, impl=impl, server_args=server_args, **kwargs).start_all()


KEY_EVENTS = re.compile(r"DEAD|RECOVER|JOINED|LEFT|handoff|holding hinted|SKIP PUT|HINT STORED|"
                        r"Delivered|WARN|ERROR|Traceback|Server started|serving on|stopped")


def keep_logs(c: ProcessCluster, dest: Path) -> None:
    """Copy node logs next to the results: the first and last 100 lines plus
    every membership/handoff event in between (full logs can be huge)."""
    dest.mkdir(parents=True, exist_ok=True)
    for f in sorted(c.workdir.glob("node-*.log")):
        lines = f.read_text(errors="replace").splitlines()
        if len(lines) > 200:
            middle = [ln for ln in lines[100:-100] if KEY_EVENTS.search(ln)]
            lines = (lines[:100] + [f"... {len(lines) - 200} lines omitted except key events ..."]
                     + middle + ["... end of key events ..."] + lines[-100:])
        (dest / f.name).write_text("\n".join(lines) + "\n")


# ============================================================ environment
def environment() -> dict:
    def sh(cmd: str) -> str:
        try:
            return subprocess.run(cmd, shell=True, capture_output=True, text=True).stdout.strip()
        except OSError:
            return ""

    cpu = sh("grep -m1 'model name' /proc/cpuinfo | cut -d: -f2").strip()
    return {
        "date_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
        "git_commit": sh(f"git -C {ROOT} rev-parse --short HEAD"),
        "cpu": cpu or platform.processor(),
        "logical_cpus": int(sh("nproc") or 0),
        "memory_gb": round(int(sh("grep MemTotal /proc/meminfo | awk '{print $2}'") or 0) / 1e6, 1),
        "os": f"{platform.system()} {platform.release()}",
        "python": platform.python_version(),
        "grpcio": grpc.__version__,
        "note": "client and all server processes share this one machine",
    }


# ===================================================================== E1
def e1() -> None:
    """Original vs rewrite: same machine, same load, same 3-node N=3/R=2/W=2 setup."""
    out = OUT / "E1_baseline_vs_rewrite"
    results = {}
    for impl in ("original", "new"):
        log(f"E1: {impl}")
        with cluster(impl, 3, f"e1_{impl}") as c:
            spec = LoadSpec(nodes=c.addresses, clients=16, duration=30)
            preload(c.addresses, spec.keyspace, spec.value_size)
            results[impl] = write_results(out / impl, run(spec), spec, {"impl": impl})
            keep_logs(c, out / impl / "logs")
    save_json(out / "summary.json", {k: _brief(v) for k, v in results.items()})


def _brief(s: dict) -> dict:
    return {
        "throughput_ops_s": s["throughput_ops_s"],
        "requests": s["total_requests"],
        "failed": s["total_requests"] - s["successful"],
        "error_rate": s["error_rate"],
        **{k: s["latency_all"][k] for k in ("p50_ms", "p90_ms", "p99_ms", "max_ms")},
        "put_p50_ms": s["latency_put"]["p50_ms"],
        "get_p50_ms": s["latency_get"]["p50_ms"],
    }


# ===================================================================== E2
def e2() -> None:
    """Closed-loop concurrency sweep for both implementations."""
    out = OUT / "E2_concurrency_scaling"
    levels = [1, 2, 4, 8, 16, 32, 64]
    rows = []
    for impl in ("original", "new"):
        with cluster(impl, 3, f"e2_{impl}") as c:
            preload(c.addresses, 1000, 100)
            for clients in levels:
                log(f"E2: {impl} clients={clients}")
                spec = LoadSpec(nodes=c.addresses, clients=clients, duration=15)
                s = summarize(run(spec), spec)
                rows.append({"impl": impl, "clients": clients, **_brief(s)})
    _write_csv(out / "results.csv", rows)


# ===================================================================== E3
def e3() -> None:
    """Quorum trade-offs on 4 nodes (N=3), healthy and with one node killed.

    2 clients keep the 4-core machine below saturation, so latency reflects
    the cost of waiting for R/W replicas rather than CPU queueing.
    """
    out = OUT / "E3_quorum_tradeoffs"
    configs = [(1, 1), (2, 2), (1, 3), (3, 1), (3, 3)]
    rows = []
    for r, w in configs:
        args = ["-n", "3", "-r", str(r), "-w", str(w), "--fail-timeout", "2"]
        with cluster("new", 4, f"e3_r{r}w{w}", args) as c:
            preload(c.addresses, 1000, 100)
            for phase in ("healthy", "one_node_down"):
                if phase == "one_node_down":
                    c.kill(3)
                    time.sleep(3)          # > fail-timeout: the failure is detected
                nodes = c.addresses[:3] if phase == "one_node_down" else c.addresses
                log(f"E3: R={r} W={w} {phase}")
                spec = LoadSpec(nodes=nodes, clients=2, duration=15)
                s = summarize(run(spec), spec)
                rows.append({"r": r, "w": w, "phase": phase, **_brief(s)})
    _write_csv(out / "results.csv", rows)


# ===================================================================== E4
def e4() -> None:
    """Crash and recovery under load, for both implementations, then convergence."""
    out = OUT / "E4_failure_recovery"
    kill_at, restart_at, duration = 10.0, 25.0, 45.0
    for impl in ("original", "new"):
        log(f"E4: {impl} timeline (kill node-3 at {kill_at}s, restart at {restart_at}s)")
        with cluster(impl, 3, f"e4_{impl}", ["--fail-timeout", "2"] if impl == "new" else None) as c:
            spec = LoadSpec(nodes=c.addresses, clients=16, duration=duration)
            preload(c.addresses, spec.keyspace, spec.value_size)

            def chaos(c: ProcessCluster = c) -> None:
                time.sleep(2.0 + kill_at)  # the load starts ~2 s after run() is called
                c.kill(2, signal.SIGKILL)
                time.sleep(restart_at - kill_at)
                c.start(2)

            t = threading.Thread(target=chaos)
            t.start()
            records = run(spec)
            t.join()
            write_results(out / impl, records, spec,
                          {"impl": impl, "kill_at_s": kill_at, "restart_at_s": restart_at})
            keep_logs(c, out / impl / "logs")
    save_json(out / "convergence.json", _e4_convergence())


def _replica_divergence(addresses: list[str], keys: list[str], vnodes: int, n: int = 3) -> int:
    """Number of keys whose N replicas do not hold identical version sets."""
    channels = [grpc.insecure_channel(a) for a in addresses]
    stubs = {a: dynamo_pb2_grpc.DynamoServiceStub(ch) for a, ch in zip(addresses, channels, strict=True)}
    try:
        members = next(iter(stubs.values())).Gossip(pb.MembershipList(), timeout=5).members
        ring = HashRing(vnodes)
        by_id = {m.node_id: m.address for m in members if m.up}
        for node_id, address in by_id.items():
            ring.add_node(node_id, address)
        divergent = 0
        for key in keys:
            sets = []
            for node_id in ring.preference_list(key, n):
                try:
                    reply = stubs[by_id[node_id]].ForwardGet(pb.GetRequest(key=key), timeout=5)
                except grpc.RpcError:
                    sets.append(None)       # unreachable replica counts as divergent
                    continue
                sets.append(frozenset(version_key(v) for v in reconcile(reply.data)))
            divergent += len(set(sets)) > 1 or None in sets
        return divergent
    finally:
        for ch in channels:
            ch.close()


def _log_time(line: str) -> float | None:
    """Seconds since midnight of a node log line ("HH:MM:SS.mmm ...")."""
    try:
        h, m, rest = line[:12].split(":")
        return int(h) * 3600 + int(m) * 60 + float(rest)
    except ValueError:
        return None


def _e4_convergence() -> dict:
    """Write while node-3 is down (no reads, so no read repair), restart it and
    measure how long hinted handoff alone takes to make every replica identical.

    Handoff timing comes from the nodes' own "delivered" log lines (ms
    resolution). A full scan of every key on every replica then verifies
    that no replica is left divergent.
    """
    log("E4: convergence via hinted handoff")
    keys = [f"key-{i}" for i in range(1000)]
    args = ["--fail-timeout", "2", "--vnodes", "64"]
    with cluster("new", 3, "e4_convergence", args) as c:
        preload(c.addresses, 1000, 100)
        c.kill(2)
        time.sleep(3)
        spec = LoadSpec(nodes=c.addresses[:2], clients=8, duration=10, put_ratio=1.0)
        down = summarize(run(spec), spec)
        c.start(2)
        lt = time.localtime()
        restart = lt.tm_hour * 3600 + lt.tm_min * 60 + lt.tm_sec + (time.time() % 1)
        restarted = time.monotonic()
        scans = []
        while True:
            divergent = _replica_divergence(c.addresses, keys, 64)
            scans.append({"finished_at_s": round(time.monotonic() - restarted, 2),
                          "divergent_keys": divergent})
            log(f"E4: scan {scans[-1]}")
            if divergent == 0 or time.monotonic() - restarted > 60:
                break
        deliveries = []
        for f in c.workdir.glob("node-*.log"):
            for line in f.read_text().splitlines():
                if "hinted handoff: delivered" in line and (t := _log_time(line)) is not None:
                    deliveries.append((t - restart, int(line.split("delivered ")[1].split(" ")[0])))
        deliveries = sorted(d for d in deliveries if d[0] >= 0)
    return {
        "writes_while_down": down["successful"],
        "write_error_rate_while_down": down["error_rate"],
        "hinted_keys_delivered": sum(n for _, n in deliveries),
        "first_delivery_after_restart_s": round(deliveries[0][0], 3) if deliveries else None,
        "last_delivery_after_restart_s": round(deliveries[-1][0], 3) if deliveries else None,
        "deliveries": [{"t_s": round(t, 3), "keys": n} for t, n in deliveries],
        "verification_scans": scans,
        "converged": scans[-1]["divergent_keys"] == 0,
    }


# ===================================================================== E5
def e5() -> None:
    """Key-distribution balance vs vnodes, and data movement on join (pure computation).

    One ring layout is a single random sample (token positions depend on the
    node names), so each setting is averaged over ``trials`` independent
    layouts and the spread is reported too.
    """
    out = OUT / "E5_vnode_balance"
    nodes, trials = 8, 20
    keys = [f"key-{i}" for i in range(50_000)]
    rows = []
    for vnodes in (1, 2, 4, 8, 16, 32, 64, 128, 256):
        cvs, peaks, moved = [], [], []
        for trial in range(trials):
            ring = HashRing(vnodes)
            for i in range(nodes):
                ring.add_node(f"t{trial}-node-{i}", "x")
            owners = [ring.primary(k) for k in keys]
            counts = list(collections.Counter(owners).values())
            counts += [0] * (nodes - len(counts))
            mean = statistics.mean(counts)
            cvs.append(100 * statistics.pstdev(counts) / mean)
            peaks.append(max(counts) / mean)
            ring.add_node(f"t{trial}-node-{nodes}", "x")
            moved.append(100 * sum(o != ring.primary(k)
                                   for o, k in zip(owners, keys, strict=True)) / len(keys))
        rows.append({
            "vnodes": vnodes,
            "load_cv_pct_mean": round(statistics.mean(cvs), 2),
            "load_cv_pct_min": round(min(cvs), 2),
            "load_cv_pct_max": round(max(cvs), 2),
            "max_over_mean_mean": round(statistics.mean(peaks), 3),
            "max_over_mean_worst": round(max(peaks), 3),
            "moved_on_join_pct_mean": round(statistics.mean(moved), 2),
        })
        log(f"E5: vnodes={vnodes} {rows[-1]}")
    # Baseline: naive hash(key) mod N placement moves almost everything on a join.
    modulo_moved = sum((h := hash_key(k)) % nodes != h % (nodes + 1) for k in keys) / len(keys)
    _write_csv(out / "results.csv", rows)
    save_json(out / "summary.json", {
        "nodes": nodes, "keys": len(keys), "trials_per_setting": trials,
        "ideal_moved_on_join_pct": round(100 / (nodes + 1), 2),
        "modulo_hashing_moved_on_join_pct": round(100 * modulo_moved, 2),
    })


# ===================================================================== E6
def e6() -> None:
    """Write latency with and without fsync: end-to-end, and the storage layer alone."""
    out = OUT / "E6_fsync"
    micro = []
    for fsync in (False, True):
        store_dir = WORK / f"e6_micro_{fsync}"
        shutil.rmtree(store_dir, ignore_errors=True)
        store = Storage(store_dir, fsync=fsync)
        lat = []
        for i in range(2000):
            v = pb.ValueWithContext(value=b"x" * 100,
                                    dot=pb.VectorClockItem(node_id="bench", counter=i + 1))
            t0 = time.perf_counter()
            store.put(f"key-{i % 500}", v)
            lat.append((time.perf_counter() - t0) * 1000)
        lat.sort()
        micro.append({"fsync": fsync, "puts": len(lat),
                      "p50_ms": round(lat[len(lat) // 2], 4),
                      "p99_ms": round(lat[int(len(lat) * 0.99)], 4),
                      "mean_ms": round(sum(lat) / len(lat), 4)})
        log(f"E6 storage micro-benchmark: {micro[-1]}")
    _write_csv(out / "storage_put.csv", micro)
    rows = []
    for fsync in (False, True):
        args = ["--fsync"] if fsync else []
        with cluster("new", 3, f"e6_fsync_{fsync}", args) as c:
            log(f"E6: fsync={fsync}")
            spec = LoadSpec(nodes=c.addresses, clients=8, duration=15, put_ratio=1.0)
            s = summarize(run(spec), spec)
            rows.append({"fsync": fsync, **_brief(s)})
    _write_csv(out / "results.csv", rows)


# ================================================================== driver
def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


EXPERIMENTS = {"e1": e1, "e2": e2, "e3": e3, "e4": e4, "e5": e5, "e6": e6}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("which", nargs="+", choices=[*EXPERIMENTS, "all"])
    a = p.parse_args(argv)
    chosen = list(EXPERIMENTS) if "all" in a.which else a.which
    random.seed(42)
    env = environment()
    for name in chosen:
        started = time.monotonic()
        EXPERIMENTS[name]()
        for d in OUT.glob(f"{name.upper()}_*"):
            save_json(d / "environment.json", {**env, "duration_s": round(time.monotonic() - started)})
        log(f"{name} done in {time.monotonic() - started:.0f}s")
    from benchmarks import plots
    plots.main()
    return 0


if __name__ == "__main__":
    sys.exit(main())
