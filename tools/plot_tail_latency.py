#!/usr/bin/env python3
import csv
import matplotlib.pyplot as plt
import numpy as np

def load_latencies(csv_file):
    latencies = []
    with open(csv_file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            latencies.append(float(row["latency_ms"]))
    return latencies

def compute_tail(latencies):
    latencies_sorted = sorted(latencies)

    avg = sum(latencies_sorted) / len(latencies_sorted)
    p95 = latencies_sorted[int(0.95 * len(latencies_sorted)) - 1]
    p99 = latencies_sorted[int(0.99 * len(latencies_sorted)) - 1]

    return avg, p95, p99

def plot_tail(avg, p95, p99, title="Tail Latency (avg / p95 / p99)", out="tail_latency.png"):
    labels = ["Average", "p95", "p99"]
    values = [avg, p95, p99]

    plt.figure(figsize=(7,5))
    bars = plt.bar(labels, values, color=["#4CAF50","#FFC107","#F44336"])

    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, height + 1, f"{height:.2f} ms",
                 ha='center', va='bottom', fontsize=10)

    plt.ylabel("Latency (ms)")
    plt.title(title)
    plt.grid(axis="y")
    plt.tight_layout()
    plt.savefig(out)
    print(f"[OK] Tail latency plot saved to: {out}")

if __name__ == "__main__":
    csv_path = "tests/bench_output/throughput_results.csv"
    lat = load_latencies(csv_path)
    avg, p95, p99 = compute_tail(lat)
    plot_tail(avg, p95, p99)
