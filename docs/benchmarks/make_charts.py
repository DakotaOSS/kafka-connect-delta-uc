"""Generate benchmark charts (PNG) from the bench CSVs. Run: uv run --with matplotlib python make_charts.py"""
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

BASE = Path(__file__).parent
ACCENT = "#2f7ed8"
ACCENT2 = "#f28e2b"
plt.rcParams.update({"font.size": 11, "axes.grid": True, "grid.alpha": 0.25, "figure.dpi": 150})


def read(name):
    with open(BASE / name, newline="", encoding="utf-8-sig") as f:
        return [r for r in csv.DictReader(f) if "PARSE_FAIL" not in ",".join(r.values())]


def human(n):
    n = float(n)
    for unit in ["", "k", "M", "B"]:
        if abs(n) < 1000:
            return f"{n:.0f}{unit}" if n == int(n) else f"{n:.1f}{unit}"
        n /= 1000
    return f"{n:.0f}T"


def bar(ax, labels, vals, color, fmt=human):
    bars = ax.bar(labels, vals, color=color, width=0.6)
    for b, v in zip(bars, vals):
        ax.annotate(fmt(v), (b.get_x() + b.get_width() / 2, v), ha="center", va="bottom",
                    xytext=(0, 3), textcoords="offset points", fontsize=9)
    ax.margins(y=0.15)


# --- Throughput vs batch size (5M rows) ---
sweep = read("batch-sweep.csv")
labels = [human(r["batch"]) for r in sweep]
tput = [int(r["rows_per_sec"]) for r in sweep]
fig, ax = plt.subplots(figsize=(7, 4))
bar(ax, labels, tput, ACCENT)
ax.set_title("Throughput vs batch size (5M rows, Debezium envelope)")
ax.set_xlabel("batch size (rows per commit)")
ax.set_ylabel("rows / sec")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: human(x)))
fig.tight_layout()
fig.savefig(BASE / "throughput-vs-batch.png")
plt.close(fig)

# --- Commit latency vs batch size (p50 / p99) ---
import numpy as np

p50 = [float(r["p50_ms"]) / 1000 for r in sweep]
p99 = [float(r["p99_ms"]) / 1000 for r in sweep]
x = np.arange(len(labels))
fig, ax = plt.subplots(figsize=(7, 4))
b1 = ax.bar(x - 0.2, p50, 0.38, label="p50", color=ACCENT)
b2 = ax.bar(x + 0.2, p99, 0.38, label="p99", color=ACCENT2)
for bars in (b1, b2):
    for b in bars:
        ax.annotate(f"{b.get_height():.1f}s", (b.get_x() + b.get_width() / 2, b.get_height()),
                    ha="center", va="bottom", xytext=(0, 2), textcoords="offset points", fontsize=8)
ax.axhline(5, color="#cc4444", ls="--", lw=1, label="5s target")
ax.set_xticks(x, labels)
ax.set_title("Synchronous commit latency vs batch size")
ax.set_xlabel("batch size (rows per commit)")
ax.set_ylabel("seconds")
ax.legend()
ax.margins(y=0.18)
fig.tight_layout()
fig.savefig(BASE / "latency-vs-batch.png")
plt.close(fig)

# --- Throughput vs volume ---
vol = read("volume-scaling.csv")
vlabels = [human(r["volume"]) for r in vol]
vtput = [int(r["rows_per_sec"]) for r in vol]
fig, ax = plt.subplots(figsize=(7, 4))
bar(ax, vlabels, vtput, ACCENT)
ax.set_title("Throughput vs volume (1M-row batches)")
ax.set_xlabel("total rows written")
ax.set_ylabel("rows / sec")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: human(x)))
fig.tight_layout()
fig.savefig(BASE / "throughput-vs-volume.png")
plt.close(fig)

# --- Per-commit latency timeline at 100M (proves flatness / no growth) ---
import statistics

commits100 = read("latency-timeline-100m.csv")
cx = [int(r["commit"]) for r in commits100]
cy = [float(r["sync_s"]) for r in commits100]
fig, ax = plt.subplots(figsize=(7, 4))
ax.plot(cx, cy, marker="o", ms=3, color=ACCENT, lw=1.5, label="sync commit")
mean = statistics.mean(cy)
ax.axhline(mean, color="#888", ls=":", lw=1.2, label=f"mean {mean:.1f}s")
ax.set_ylim(0, max(cy) * 1.25)
ax.set_title(f"Per-commit latency across 100M rows ({len(cx)} commits) — flat, no growth")
ax.set_xlabel("commit #")
ax.set_ylabel("sync commit (s)")
ax.legend()
fig.tight_layout()
fig.savefig(BASE / "latency-timeline-100m.png")
plt.close(fig)

# --- Multi-table scaling: aggregate throughput + resource use vs concurrent table count ---
mt_path = BASE / "multi-table-scaling.csv"
if mt_path.exists():
    mt = read("multi-table-scaling.csv")
    tlabels = [r["tables"] for r in mt]
    agg = [int(r["aggregate_rows_s"]) for r in mt]
    fig, ax = plt.subplots(figsize=(7, 4))
    bar(ax, tlabels, agg, ACCENT)
    ax.set_title("Aggregate throughput vs concurrent tables (one JVM, shared FS cache)")
    ax.set_xlabel("concurrent tables")
    ax.set_ylabel("aggregate rows / sec")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: human(x)))
    fig.tight_layout()
    fig.savefig(BASE / "throughput-vs-tables.png")
    plt.close(fig)

    heap = [float(r["peak_heap_mb"]) for r in mt]
    cpu = [float(r["avg_cpu_pct"]) for r in mt]
    x = np.arange(len(tlabels))
    fig, ax = plt.subplots(figsize=(7, 4))
    l1 = ax.plot(x, heap, marker="o", color=ACCENT, lw=1.5, label="peak heap (MB)")
    ax.set_xlabel("concurrent tables")
    ax.set_ylabel("peak heap (MB)", color=ACCENT)
    ax.set_xticks(x, tlabels)
    ax2 = ax.twinx()
    ax2.grid(False)
    l2 = ax2.plot(x, cpu, marker="s", color=ACCENT2, lw=1.5, label="avg process CPU (%)")
    ax2.set_ylabel("avg process CPU (%)", color=ACCENT2)
    ax.set_title("Resource use vs concurrent tables (in-JVM)")
    ax.legend(l1 + l2, [h.get_label() for h in l1 + l2], loc="upper left")
    fig.tight_layout()
    fig.savefig(BASE / "resource-vs-tables.png")
    plt.close(fig)

print("charts written:", [p.name for p in sorted(BASE.glob("*.png"))])
