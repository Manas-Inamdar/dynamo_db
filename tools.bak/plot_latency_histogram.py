#!/usr/bin/env python3
import csv
import matplotlib.pyplot as plt

def load_latencies(csv_file):
    latencies = []
    with open(csv_file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            latencies.append(float(row["latency_ms"]))
    return latencies

def plot_latency_histogram(latencies, title="Latency Histogram", out="latency_histogram.png"):
    plt.figure(figsize=(8,5))
    plt.hist(latencies, bins=30, edgecolor='black')
    
    plt.xlabel("Latency (ms)")
    plt.ylabel("Number of Requests")
    plt.title(title)
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(out)
    print(f"[OK] Histogram saved to: {out}")

if __name__ == "__main__":
    csv_path = "tests/bench_output/throughput_results.csv"
    lat = load_latencies(csv_path)
    plot_latency_histogram(lat)
