"""Render the experiment charts from the CSV/JSON results in experiments/.

    python -m benchmarks.plots

Visual conventions: one categorical colour per *entity* used consistently
in every chart (rewrite = blue, original = orange), thin 2 px lines, a
recessive grid, direct labels instead of numbers on every mark, and no dual
axes - two measures get two panels. Every chart's numbers are also
tabulated in the experiment's README.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
EXP = ROOT / "experiments"

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_2 = "#52514e"
GRID = "#e4e3df"
BAND = "#ecebe7"
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
IMPL = {"new": ("Rewrite", BLUE), "original": ("Original", ORANGE)}


def style() -> None:
    plt.rcParams.update({
        "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
        "axes.edgecolor": GRID, "axes.labelcolor": INK_2, "axes.titlecolor": INK,
        "axes.titlesize": 12, "axes.titleweight": "bold", "axes.titlelocation": "left",
        "axes.labelsize": 10, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
        "axes.axisbelow": True, "axes.spines.top": False, "axes.spines.right": False,
        "xtick.color": INK_2, "ytick.color": INK_2, "xtick.labelsize": 9, "ytick.labelsize": 9,
        "text.color": INK, "font.size": 10, "legend.frameon": False, "legend.fontsize": 9,
        "lines.linewidth": 2.0, "lines.markersize": 6,
    })


def save(fig: plt.Figure, path: Path) -> None:
    fig.tight_layout()
    fig.savefig(path, dpi=160)
    plt.close(fig)
    print(f"[plots] {path.relative_to(ROOT)}")


def read_csv(path: Path) -> list[dict]:
    with open(path) as fh:
        return list(csv.DictReader(fh))


def ms(v: float, _pos: object = None) -> str:
    return f"{v:g} ms"


# ---------------------------------------------------------------------- E1
def plot_e1() -> None:
    d = EXP / "E1_baseline_vs_rewrite"
    if not (d / "summary.json").exists():
        return
    s = json.loads((d / "summary.json").read_text())
    fig, (a, b) = plt.subplots(1, 2, figsize=(10, 4), gridspec_kw={"width_ratios": [1, 1.6]})

    impls = ["original", "new"]
    vals = [s[i]["throughput_ops_s"] for i in impls]
    bars = a.bar([IMPL[i][0] for i in impls], vals, color=[IMPL[i][1] for i in impls], width=0.55)
    for bar, v in zip(bars, vals, strict=True):
        a.annotate(f"{v:,.0f}", (bar.get_x() + bar.get_width() / 2, v), xytext=(0, 4),
                   textcoords="offset points", ha="center", color=INK, fontsize=10)
    a.set_title("Throughput (successful ops/s)")
    a.grid(axis="x", visible=False)

    pcts = ["p50_ms", "p90_ms", "p99_ms"]
    width = 0.38
    for k, impl in enumerate(impls):
        xs = [i + (k - 0.5) * width for i in range(len(pcts))]
        ys = [s[impl][p] for p in pcts]
        b.bar(xs, ys, width=width - 0.04, color=IMPL[impl][1], label=IMPL[impl][0])
        for x, y in zip(xs, ys, strict=True):
            b.annotate(f"{y:.1f}", (x, y), xytext=(0, 3), textcoords="offset points",
                       ha="center", fontsize=8, color=INK_2)
    b.set_xticks(range(len(pcts)), ["p50", "p90", "p99"])
    b.yaxis.set_major_formatter(FuncFormatter(ms))
    b.set_title("Request latency (lower is better)")
    b.legend(loc="upper left")
    b.grid(axis="x", visible=False)
    fig.suptitle("3 nodes, N=3 R=2 W=2, 16 closed-loop clients, 50% writes, 30 s",
                 x=0.01, ha="left", fontsize=9, color=INK_2)
    save(fig, d / "comparison.png")


# ---------------------------------------------------------------------- E2
def plot_e2() -> None:
    d = EXP / "E2_concurrency_scaling"
    if not (d / "results.csv").exists():
        return
    rows = read_csv(d / "results.csv")
    fig, (a, b) = plt.subplots(1, 2, figsize=(10, 4))
    for impl in ("original", "new"):
        rs = [r for r in rows if r["impl"] == impl]
        xs = [int(r["clients"]) for r in rs]
        label, color = IMPL[impl]
        a.plot(xs, [float(r["throughput_ops_s"]) for r in rs], marker="o", color=color, label=label)
        b.plot(xs, [float(r["p99_ms"]) for r in rs], marker="o", color=color, label=label)
        b.plot(xs, [float(r["p50_ms"]) for r in rs], marker="o", color=color, linestyle="--",
               alpha=0.8)
    for ax in (a, b):
        ax.set_xscale("log", base=2)
        ax.set_xticks([1, 2, 4, 8, 16, 32, 64], ["1", "2", "4", "8", "16", "32", "64"])
        ax.set_xlabel("concurrent clients")
    a.set_title("Throughput (successful ops/s)")
    a.set_ylim(bottom=0)
    a.legend(loc="upper left")
    b.set_yscale("log")
    b.yaxis.set_major_formatter(FuncFormatter(ms))
    b.set_title("Latency: p99 (solid) and p50 (dashed)")
    b.legend(loc="upper left")
    save(fig, d / "scaling.png")


# ---------------------------------------------------------------------- E3
def plot_e3() -> None:
    d = EXP / "E3_quorum_tradeoffs"
    if not (d / "results.csv").exists():
        return
    rows = read_csv(d / "results.csv")
    labels = [f"R={r['r']} W={r['w']}" for r in rows if r["phase"] == "healthy"]
    healthy = [r for r in rows if r["phase"] == "healthy"]
    down = [r for r in rows if r["phase"] == "one_node_down"]
    fig, (a, b) = plt.subplots(1, 2, figsize=(10, 4))
    width = 0.38
    for k, (col, label, color) in enumerate((("get_p50_ms", "GET", BLUE), ("put_p50_ms", "PUT", AQUA))):
        xs = [i + (k - 0.5) * width for i in range(len(labels))]
        a.bar(xs, [float(r[col]) for r in healthy], width=width - 0.04, color=color, label=label)
    a.set_xticks(range(len(labels)), labels)
    a.set_title("Healthy cluster: median latency")
    a.legend(loc="upper left")
    a.yaxis.set_major_formatter(FuncFormatter(ms))
    avail = [100 * (1 - float(r["error_rate"])) for r in down]
    bars = b.bar(labels, avail, color=BLUE, width=0.55)
    for bar, v in zip(bars, avail, strict=True):
        b.annotate(f"{v:.1f}%", (bar.get_x() + bar.get_width() / 2, v), xytext=(0, 3),
                   textcoords="offset points", ha="center", fontsize=9)
    b.set_ylim(0, 110)
    b.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}%" if v <= 100 else ""))
    b.set_title("One of 4 nodes killed: requests succeeding")
    for ax in (a, b):
        ax.grid(axis="x", visible=False)
        ax.tick_params(axis="x", labelsize=8)
    fig.suptitle("4 nodes, N=3, 2 clients (below saturation), 50% writes, 15 s per phase", x=0.01, ha="left",
                 fontsize=9, color=INK_2)
    save(fig, d / "quorums.png")


# ---------------------------------------------------------------------- E4
def plot_e4() -> None:
    d = EXP / "E4_failure_recovery"
    impls = [i for i in ("original", "new") if (d / i / "timeline.csv").exists()]
    if not impls:
        return
    meta = json.loads((d / impls[-1] / "summary.json").read_text())
    kill, restart = meta["kill_at_s"], meta["restart_at_s"]
    fig, (a, b) = plt.subplots(2, 1, figsize=(10, 6.2), sharex=True)
    for impl in impls:
        rows = read_csv(d / impl / "timeline.csv")
        ts = [float(r["t"]) for r in rows]
        label, color = IMPL[impl]
        a.plot(ts, [float(r["ok_per_s"]) for r in rows], color=color, label=label)
        b.plot(ts, [float(r["errors_per_s"]) for r in rows], color=color, label=label)
    for ax in (a, b):
        ax.axvspan(kill, restart, color=BAND, zorder=0)
        ax.legend(loc="upper right")
    top = a.get_ylim()[1]
    a.annotate("node-3 killed (SIGKILL)", (kill, top), xytext=(4, -12), textcoords="offset points",
               fontsize=8, color=INK_2)
    a.annotate("restarted", (restart, top), xytext=(4, -12), textcoords="offset points",
               fontsize=8, color=INK_2)
    a.set_title("Successful requests per second")
    b.set_title("Failed requests per second")
    b.set_xlabel("time (s)")
    save(fig, d / "timeline.png")

    conv = d / "convergence.json"
    if conv.exists():
        c = json.loads(conv.read_text())
        if c.get("deliveries"):
            fig, ax = plt.subplots(figsize=(7, 3.4))
            total, xs, ys = c["hinted_keys_delivered"], [0.0], [c["hinted_keys_delivered"]]
            remaining = total
            for d_ in c["deliveries"]:
                xs += [d_["t_s"], d_["t_s"]]
                ys += [remaining, remaining - d_["keys"]]
                remaining -= d_["keys"]
            ax.plot(xs, ys, color=BLUE)
            ax.set_xlabel("seconds since node-3 restarted")
            ax.set_title("Hinted writes still waiting for node-3")
            ax.set_ylim(bottom=0)
            last = c["last_delivery_after_restart_s"]
            ax.annotate(f"all {total:,} hinted keys handed back\n{last:.2f} s after restart",
                        (c["last_delivery_after_restart_s"], 0), xytext=(-8, 24), ha="right",
                        textcoords="offset points", fontsize=8, color=INK_2)
            save(fig, d / "convergence.png")


# ---------------------------------------------------------------------- E5
def plot_e5() -> None:
    d = EXP / "E5_vnode_balance"
    if not (d / "results.csv").exists():
        return
    rows = read_csv(d / "results.csv")
    meta = json.loads((d / "summary.json").read_text())
    xs = [int(r["vnodes"]) for r in rows]
    fig, (a, b) = plt.subplots(1, 2, figsize=(10, 4))
    a.fill_between(xs, [float(r["load_cv_pct_min"]) for r in rows],
                   [float(r["load_cv_pct_max"]) for r in rows], color=BLUE, alpha=0.15, linewidth=0)
    a.plot(xs, [float(r["load_cv_pct_mean"]) for r in rows], color=BLUE, marker="o")
    a.set_title("Load imbalance: std/mean of keys per node")
    a.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}%"))
    b.plot(xs, [float(r["max_over_mean_mean"]) for r in rows], color=BLUE, marker="o", label="mean")
    b.plot(xs, [float(r["max_over_mean_worst"]) for r in rows], color=ORANGE, marker="o",
           label="worst of 20")
    b.axhline(1.0, color=INK_2, linewidth=1, linestyle=":")
    b.set_title("Most-loaded node vs. average")
    b.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:.2g}\u00d7"))
    b.legend(loc="upper right")
    for ax in (a, b):
        ax.set_xscale("log", base=2)
        ax.set_xticks(xs, [str(x) for x in xs])
        ax.set_xlabel("virtual nodes per physical node")
        ax.axvline(64, color=INK_2, linewidth=1, linestyle="--")
        ax.set_ylim(bottom=0)
    b.set_ylim(bottom=0.8)
    for ax in (a, b):
        ax.annotate("default (64)", (64, ax.get_ylim()[1]), xytext=(-4, -12), ha="right",
                    textcoords="offset points", fontsize=8, color=INK_2)
    fig.suptitle(f"{meta['nodes']} nodes, {meta['keys']:,} keys; each point is the mean of "
                 f"{meta['trials_per_setting']} random ring layouts (shaded: best to worst)",
                 x=0.01, ha="left", fontsize=9, color=INK_2)
    save(fig, d / "balance.png")


# ---------------------------------------------------------------------- E6
def plot_e6() -> None:
    d = EXP / "E6_fsync"
    if not (d / "results.csv").exists():
        return
    fig, (a, b) = plt.subplots(1, 2, figsize=(10, 3.8))
    for ax, rows, title in (
        (a, read_csv(d / "storage_put.csv"), "Storage.put alone (1 thread)"),
        (b, read_csv(d / "results.csv"), "End-to-end PUT (3 nodes, 8 clients)"),
    ):
        pcts = ["p50_ms", "p99_ms"]
        width = 0.38
        for k, (row, color) in enumerate(zip(rows, (BLUE, AQUA), strict=False)):
            label = "fsync on" if row["fsync"] == "True" else "fsync off"
            xs = [i + (k - 0.5) * width for i in range(len(pcts))]
            ys = [float(row[p]) for p in pcts]
            ax.bar(xs, ys, width=width - 0.04, color=color, label=label)
            for x, y in zip(xs, ys, strict=True):
                ax.annotate(f"{y:.2f}" if y < 10 else f"{y:.0f}", (x, y), xytext=(0, 3),
                            textcoords="offset points", ha="center", fontsize=8, color=INK_2)
        ax.set_xticks(range(len(pcts)), ["p50", "p99"])
        ax.yaxis.set_major_formatter(FuncFormatter(ms))
        ax.set_title(title)
        ax.legend(loc="upper left")
        ax.grid(axis="x", visible=False)
    save(fig, d / "fsync.png")


def main() -> None:
    style()
    for fn in (plot_e1, plot_e2, plot_e3, plot_e4, plot_e5, plot_e6):
        fn()


if __name__ == "__main__":
    main()
