#!/usr/bin/env python3
"""Turn perf-results.csv rows (sweep,size,metric,value,events) into a self-contained HTML report."""
import csv, json, sys
from collections import defaultdict

src = sys.argv[1]
dst = sys.argv[2]
meta_json = sys.argv[3] if len(sys.argv) > 3 else "{}"
meta = json.loads(meta_json)

data = defaultdict(lambda: defaultdict(dict))   # sweep -> size -> metric -> value
events = defaultdict(dict)                       # sweep -> size -> events
with open(src) as f:
    for row in csv.DictReader(f):
        s, n, m, v, e = row["sweep"], int(row["size"]), row["metric"], int(row["value"]), int(row["events"])
        data[s][n][m] = v
        events[s][n] = max(events[s].get(n, 0), e)

def series(sweep, metric, per=None):
    out = []
    for n in sorted(data[sweep]):
        v = data[sweep][n].get(metric)
        if v is None:
            continue
        if per == "size" and n > 0:
            v = v / n
        elif per == "events":
            ev = events[sweep][n] or 1
            v = v / ev
        out.append({"x": n, "y": round(v, 3)})
    return out

def table(sweep, cols):
    rows = []
    for n in sorted(data[sweep]):
        rows.append([n] + [data[sweep][n].get(c) for c, _ in cols])
    return rows

payload = {
    "running": {
        "start": series("running", "start_all_ms"),
        "start_per": series("running", "start_all_ms", "size"),
        "restore": series("running", "failover_restore_ms"),
        "restore_per": series("running", "failover_restore_ms", "size"),
        "release": series("running", "release_all_ms"),
        "release_per": series("running", "release_all_ms", "size"),
        "heap": series("running", "heap_per_instance_bytes"),
        "raw_list": series("running", "raw_source_running_list_ms"),
        "raw_one": series("running", "raw_source_one_instance_ms"),
        "table": table("running", [("start_all_ms", ""), ("failover_restore_ms", ""), ("release_all_ms", ""),
                                   ("heap_per_instance_bytes", ""), ("raw_source_running_list_ms", ""),
                                   ("raw_source_one_instance_ms", "")]),
    },
    "history": {
        "restore": series("history", "failover_restore_ms"),
        "raw_list": series("history", "raw_source_running_list_ms"),
        "seed": series("history", "seed_ms"),
        "table": table("history", [("seed_ms", ""), ("raw_source_running_list_ms", ""), ("failover_restore_ms", "")]),
        "events": {n: events["history"][n] for n in sorted(data["history"])},
    },
    "steps": {
        "events": series("steps", "events_in_instance"),
        "execute": series("steps", "execute_steps_ms"),
        "execute_per": series("steps", "execute_steps_ms", "size"),
        "restore": series("steps", "failover_restore_ms"),
        "raw_one": series("steps", "raw_source_one_instance_ms"),
        "release": series("steps", "release_after_restore_ms"),
        "table": table("steps", [("events_in_instance", ""), ("execute_steps_ms", ""), ("raw_source_one_instance_ms", ""),
                                 ("failover_restore_ms", ""), ("release_after_restore_ms", "")]),
    },
    "meta": meta,
}

html = open(sys.argv[4]).read() if len(sys.argv) > 4 else ""
html = html.replace("/*__DATA__*/", "const DATA = " + json.dumps(payload) + ";")
html = html.replace("/*__NARRATIVE__*/", open(sys.argv[5]).read())
open(dst, "w").write(html)
print("wrote", dst)
