#!/usr/bin/env python3
"""Prints one Markdown table per event store from results/results.csv (rows: arm and size)."""
import csv
import sys
from collections import defaultdict

COLUMNS = [
    ("all parked (s)", "start_all_parked_ms", 1000),
    ("all hibernated (s)", "hibernate_all_ms", 1000),
    ("heap/workflow (KB)", "heap_per_workflow_bytes", 1024),
    ("wake p50 (ms)", "wake_latency_p50_ms", 1),
    ("wake p99 (ms)", "wake_latency_p99_ms", 1),
    ("events read/wake", "wake_sourced_events_per_workflow_x100", 100),
    ("restore all (s)", "restart_restore_all_ms", 1000),
    ("re-hibernate (s)", "restart_rehibernate_ms", 1000),
    ("restart peak old gen (MB)", "restart_peak_old_gen_after_gc_bytes", 1024 * 1024),
    ("restart events read", "restart_sourced_events", 1),
    ("deadline late p50/p99 (ms)", ("accuracy_lateness_p50_ms", "accuracy_lateness_p99_ms"), 1),
    ("storm size", "storm_count", 1),
    ("storm late p50/p99/max (ms)", ("storm_lateness_p50_ms", "storm_lateness_p99_ms", "storm_lateness_max_ms"), 1),
    ("errors", "log_error_lines", 1),
]


def fmt(value, scale):
    if value is None:
        return "-"
    if value == -1:
        return "WALL"
    return f"{value / scale:.1f}" if scale != 1 else str(value)


def main(path):
    rows = defaultdict(dict)
    for row in csv.DictReader(open(path)):
        rows[(row["store"], row["arm"], int(row["size"]))][row["metric"]] = int(row["value"])
    for store in sorted({key[0] for key in rows}):
        print(f"\n### {store}\n")
        print("| arm | size | " + " | ".join(name for name, _, _ in COLUMNS) + " |")
        print("|---" * (len(COLUMNS) + 2) + "|")
        for key in sorted(k for k in rows if k[0] == store):
            metrics = rows[key]
            cells = []
            for _, metric, scale in COLUMNS:
                names = metric if isinstance(metric, tuple) else (metric,)
                cells.append("/".join(fmt(metrics.get(name), scale) for name in names))
            print(f"| {key[1]} | {key[2]} | " + " | ".join(cells) + " |")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "results/results.csv")
