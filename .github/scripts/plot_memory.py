#!/usr/bin/env python3
"""
Turns the CSV produced by memory-monitor.sh into a time-series chart (PNG) and a
markdown PR comment body summarizing peak memory per process/container.
"""
import argparse
import csv
from collections import defaultdict
from datetime import datetime

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt

# Substrings looked up in a process' full command line, in priority order, to turn
# "java (pid 12345)" into a label that actually means something in the chart legend.
FRIENDLY_LABELS = [
    ("booter", "test fork"),
    ("classworlds.launcher.Launcher", "maven"),
    ("axonserver.jar", "axon server"),
    ("postgres", "postgres"),
]


def friendly_name(raw_name, detail):
    for needle, label in FRIENDLY_LABELS:
        if needle in detail:
            pid = raw_name.rsplit("(", 1)[-1].rstrip(")")
            return f"{label} ({pid})"
    return raw_name


def parse_ts(value):
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")


def load_rows(csv_path):
    with open(csv_path, newline="") as f:
        return list(csv.DictReader(f))


def build_series(rows):
    system = defaultdict(list)  # name -> [(ts, mib)]
    system_limit_mib = None
    processes = defaultdict(list)  # friendly label -> [(ts, mib)]
    process_peak = {}
    containers = defaultdict(list)  # container name -> [(ts, mib)]
    container_limit = {}

    for row in rows:
        ts = parse_ts(row["timestamp"])
        value = float(row["value_mib"])
        metric = row["metric"]

        if metric == "system":
            system[row["name"]].append((ts, value))
            if row["name"] == "used" and row["limit_mib"]:
                system_limit_mib = float(row["limit_mib"])
        elif metric == "process":
            label = friendly_name(row["name"], row.get("detail", ""))
            processes[label].append((ts, value))
            process_peak[label] = max(process_peak.get(label, 0), value)
        elif metric == "container":
            containers[row["name"]].append((ts, value))
            if row["limit_mib"]:
                container_limit[row["name"]] = float(row["limit_mib"])

    return system, system_limit_mib, processes, process_peak, containers, container_limit


def top_n(peaks, n):
    return sorted(peaks, key=peaks.get, reverse=True)[:n]


def make_chart(system, system_limit_mib, processes, process_peak, containers, container_limit, out_path):
    fig, (ax_top, ax_bottom) = plt.subplots(2, 1, figsize=(11, 8), sharex=True)

    if "used" in system:
        xs, ys = zip(*sorted(system["used"]))
        ax_top.plot(xs, ys, label="system used", color="black", linewidth=2)
    if system_limit_mib:
        ax_top.axhline(system_limit_mib, color="black", linestyle=":", linewidth=1, label="system total")

    for label in top_n(process_peak, 8):
        xs, ys = zip(*sorted(processes[label]))
        ax_top.plot(xs, ys, label=label, linewidth=1.3)

    ax_top.set_ylabel("MiB")
    ax_top.set_title("System + process memory")
    ax_top.legend(loc="upper left", fontsize=8, ncol=2)
    ax_top.grid(alpha=0.3)
    ax_top.label_outer()

    for name, points in containers.items():
        xs, ys = zip(*sorted(points))
        (line,) = ax_bottom.plot(xs, ys, label=name, linewidth=1.5)
        if name in container_limit:
            ax_bottom.axhline(container_limit[name], color=line.get_color(), linestyle=":", linewidth=1)

    ax_bottom.set_ylabel("MiB")
    ax_bottom.set_xlabel("time (UTC)")
    ax_bottom.set_title("Container memory (dotted line = cgroup limit, if set)")
    ax_bottom.legend(loc="upper left", fontsize=8)
    ax_bottom.grid(alpha=0.3)
    ax_bottom.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    fig.autofmt_xdate()

    fig.tight_layout()
    fig.savefig(out_path, dpi=130)


def make_comment(jdk, run_url, system, system_limit_mib, process_peak, containers, container_limit, out_path):
    marker = f"<!-- memory-report:jdk-{jdk} -->"
    lines = [marker, f"### Memory usage - JDK {jdk} build", ""]

    peak_used = max((v for _, v in system.get("used", [])), default=None)
    if peak_used is not None:
        total_str = f" / {system_limit_mib:.0f} MiB total" if system_limit_mib else ""
        lines.append(f"Peak system memory used: **{peak_used:.0f} MiB**{total_str}")
        lines.append("")

    if process_peak:
        lines.append("| Process | Peak RSS (MiB) |")
        lines.append("|---|---|")
        for label in top_n(process_peak, 10):
            lines.append(f"| {label} | {process_peak[label]:.0f} |")
        lines.append("")

    if containers:
        peaks = {name: max(v for _, v in pts) for name, pts in containers.items()}
        lines.append("| Container | Peak used (MiB) | cgroup limit (MiB) |")
        lines.append("|---|---|---|")
        for name in top_n(peaks, 10):
            limit = container_limit.get(name)
            limit_str = f"{limit:.0f}" if limit else "-"
            lines.append(f"| {name} | {peaks[name]:.0f} | {limit_str} |")
        lines.append("")

    lines.append(f"Full time-series chart is attached to this workflow run's artifacts: {run_url}")

    with open(out_path, "w") as f:
        f.write("\n".join(lines))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True)
    parser.add_argument("--out-chart", required=True)
    parser.add_argument("--out-comment", required=True)
    parser.add_argument("--jdk", required=True)
    parser.add_argument("--run-url", required=True)
    args = parser.parse_args()

    rows = load_rows(args.csv)
    if not rows:
        with open(args.out_comment, "w") as f:
            f.write(
                f"<!-- memory-report:jdk-{args.jdk} -->\n"
                f"### Memory usage - JDK {args.jdk} build\n\n"
                f"No memory samples were collected.\n"
            )
        return

    system, system_limit_mib, processes, process_peak, containers, container_limit = build_series(rows)
    make_chart(system, system_limit_mib, processes, process_peak, containers, container_limit, args.out_chart)
    make_comment(args.jdk, args.run_url, system, system_limit_mib, process_peak, containers, container_limit, args.out_comment)


if __name__ == "__main__":
    main()
