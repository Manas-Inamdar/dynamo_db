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

def plot_latency_cdf(latencies, title="Latency CDF", out="latency_cdf.png"):
    latencies = sorted(latencies)
    n = len(latencies)
    probs = [(i+1)/n for i in range(n)]

    plt.figure(figsize=(8,5))
    plt.plot(latencies, probs)
    
    plt.xlabel("Latency (ms)")
    plt.ylabel("Cumulative Probability")
    plt.title(title)
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(out)
    print(f"[OK] CDF plot saved to: {out}")

if __name__ == "__main__":
    csv_path = "tests/bench_output/throughput_results.csv"
    lat = load_latencies(csv_path)
    plot_latency_cdf(lat)
