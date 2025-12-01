#!/usr/bin/env python3
import csv
import os
import matplotlib.pyplot as plt

HEALTHY = "tests/bench_output/throughput_results.csv"
FAILURE = "tests/bench_output/throughput_with_node_failure.csv"

def compute_stats(path):
    timestamps = []
    latencies = []

    with open(path, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts = int(row["ts"])
            lat = float(row["latency_ms"])
            timestamps.append(ts)
            latencies.append(lat)

    if len(timestamps) < 2:
        raise RuntimeError(f"Not enough rows in {path}")

    timestamps.sort()
    total_time_ms = timestamps[-1] - timestamps[0]
    total_ops = len(timestamps)

    throughput_ops_sec = total_ops / (total_time_ms / 1000)
    avg_latency = sum(latencies) / len(latencies)

    return throughput_ops_sec, avg_latency


# Compute statistics
healthy_tput, healthy_lat = compute_stats(HEALTHY)
failure_tput, failure_lat = compute_stats(FAILURE)

print("=== HEALTHY CLUSTER ===")
print("Throughput:", healthy_tput)
print("Avg Latency:", healthy_lat)

print("=== FAILURE (NODE DOWN) ===")
print("Throughput:", failure_tput)
print("Avg Latency:", failure_lat)

# Plot Throughput Comparison
plt.figure(figsize=(8,5))
plt.bar(["Healthy (3 nodes)", "Failure (2 nodes)"],
        [healthy_tput, failure_tput], color=["green", "red"])
plt.ylabel("Throughput (ops/sec)")
plt.title("Throughput Before vs After Node Failure")
plt.grid(axis="y", linestyle="--", alpha=0.5)

os.makedirs("tests/bench_output", exist_ok=True)
plt.savefig("tests/bench_output/plot_throughput_node_failure.png", dpi=150)
print("[OK] Saved plot: tests/bench_output/plot_throughput_node_failure.png")

# Plot Latency Comparison
plt.figure(figsize=(8,5))
plt.bar(["Healthy (3 nodes)", "Failure (2 nodes)"],
        [healthy_lat, failure_lat], color=["blue", "orange"])
plt.ylabel("Average Latency (ms)")
plt.title("Latency Before vs After Node Failure")
plt.grid(axis="y", linestyle="--", alpha=0.5)

plt.savefig("tests/bench_output/plot_latency_node_failure.png", dpi=150)
print("[OK] Saved plot: tests/bench_output/plot_latency_node_failure.png")
    