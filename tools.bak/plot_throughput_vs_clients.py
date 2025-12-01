#!/usr/bin/env python3
import csv
import matplotlib.pyplot as plt

def load_data(csv_file):
    clients, tput = [], []
    with open(csv_file, "r") as f:
        for row in csv.DictReader(f):
            clients.append(int(row["clients"]))
            tput.append(float(row["ops_per_sec"]))
    return clients, tput

def plot_scaling(clients, tput):
    plt.figure(figsize=(8,5))
    plt.plot(clients, tput, marker="o", linewidth=2)
    plt.xlabel("Logical Clients")
    plt.ylabel("Throughput (ops/sec)")
    plt.title("Throughput Scaling vs Number of Logical Clients (3-node Dynamo Cluster)")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("throughput_vs_clients.png")
    print("[OK] Plot saved: throughput_vs_clients.png")

if __name__ == "__main__":
    c, t = load_data("tests/bench_output/throughput_vs_clients.csv")
    plot_scaling(c, t)
