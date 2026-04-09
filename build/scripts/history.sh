#!/usr/bin/env bash
set -euo pipefail

DEFAULT_JOBS=8

usage() {
  cat <<'EOF'
Usage:
  ./history.sh [--tree] [--tree-only CSV_FILE] [--exclude=path1,path2,...] [output-file] [jobs]

Modes:
  default             Create CSV report
  --tree              Create CSV report and then render tree output from that CSV
  --tree-only FILE    Render tree output from an existing CSV file only

Options:
  --exclude=LIST      Comma-separated path prefixes to exclude, e.g.
                      --exclude=docs/,build/,target/

Defaults:
  CSV output:   {repo-name}-initial-commits.csv
  Tree output:  {repo-name}-tree.txt
  Jobs:         8

Examples:
  ./history.sh
  ./history.sh --tree
  ./history.sh --exclude=docs/,build/
  ./history.sh custom.csv 12
  ./history.sh --tree custom.csv 12
  ./history.sh --tree-only my-repo-initial-commits.csv
EOF
}

repo_name() {
  basename "$(pwd)"
}

build_csv() {
  local output_file="$1"
  local jobs="$2"
  local excludes_str="$3"

  local tmp_dir
  tmp_dir="$(mktemp -d)"
  trap 'rm -rf "$tmp_dir"' RETURN

  local worker_script="$tmp_dir/worker.sh"

  cat > "$worker_script" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

file="$1"

format_ts() {
  local raw_ts="$1"
  if date -d "@0" '+%Y-%m-%dT%H:%M' >/dev/null 2>&1; then
    TZ=Europe/Berlin date -d "@$raw_ts" '+%Y-%m-%dT%H:%M'
  else
    TZ=Europe/Berlin date -r "$raw_ts" '+%Y-%m-%dT%H:%M'
  fi
}

csv_escape() {
  local s="$1"
  s=${s//\"/\"\"}
  printf '"%s"' "$s"
}

is_text_file() {
  local f="$1"
  [[ -f "$f" ]] || return 1
  LC_ALL=C grep -Iq . "$f"
}

is_excluded() {
  local f="$1"
  local rest="${EXCLUDES_STR-}"
  local prefix

  [[ -z "$rest" ]] && return 1

  while [[ -n "$rest" ]]; do
    if [[ "$rest" == *,* ]]; then
      prefix="${rest%%,*}"
      rest="${rest#*,}"
    else
      prefix="$rest"
      rest=""
    fi

    [[ -z "$prefix" ]] && continue

    if [[ "$f" == "$prefix"* ]]; then
      return 0
    fi
  done

  return 1
}

if is_excluded "$file"; then
  exit 0
fi

if ! is_text_file "$file"; then
  exit 0
fi

line="$(
  git log --follow --format='%at%x1f%H%x1f%s' -- "$file" \
  | LC_ALL=C sort -n -t $'\x1f' -k1,1 \
  | head -n 1
)"

first_timestamp=""
first_hash=""
first_message=""

if [[ -n "$line" ]]; then
  raw_ts="$(printf '%s' "$line" | awk -F '\037' '{print $1}')"
  first_hash="$(printf '%s' "$line" | awk -F '\037' '{print $2}')"
  first_message="$(printf '%s' "$line" | awk -F '\037' '{print $3}')"
  first_timestamp="$(format_ts "$raw_ts")"
fi

csv_escape "$file"
printf ','
csv_escape "$first_timestamp"
printf ','
csv_escape "$first_hash"
printf ','
csv_escape "$first_message"
printf '\n'
EOF

  chmod +x "$worker_script"

  printf 'source-file-path,first commit timestamp,first commit hash,first commit message\n' > "$output_file"

  git ls-files -z \
    | xargs -0 -n 1 -P "$jobs" env EXCLUDES_STR="$excludes_str" "$worker_script" \
    | awk 'NF > 0' \
    | LC_ALL=C sort \
    >> "$output_file"
}

render_tree_from_csv() {
  local csv_file="$1"
  local tree_file="$2"

  python3 - "$csv_file" "$tree_file" <<'PY'
import csv
import sys
from pathlib import PurePosixPath

csv_file = sys.argv[1]
tree_file = sys.argv[2]

class Node:
    __slots__ = ("name", "children", "is_file", "meta")
    def __init__(self, name):
        self.name = name
        self.children = {}
        self.is_file = False
        self.meta = None

root = Node("")
rows_total = 0
rows_used = 0
rows_skipped = 0

try:
    with open(csv_file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows_total += 1

            if not row:
                rows_skipped += 1
                continue

            path = (row.get("source-file-path") or "").strip()
            if not path:
                rows_skipped += 1
                continue

            ts = row.get("first commit timestamp", "")
            full_hash = row.get("first commit hash", "")
            short_hash = full_hash[:12] if full_hash else ""
            msg = row.get("first commit message", "")

            parts = [p for p in PurePosixPath(path).parts if p not in ("", ".")]
            if not parts:
                rows_skipped += 1
                continue

            cur = root
            for part in parts[:-1]:
                cur = cur.children.setdefault(part, Node(part))

            leaf_name = parts[-1]
            leaf = cur.children.setdefault(leaf_name, Node(leaf_name))
            leaf.is_file = True
            leaf.meta = (ts, short_hash, msg)
            rows_used += 1

    def sort_key(node):
        return (1 if node.is_file else 0, node.name.lower(), node.name)

    def render(node, prefix="", out=None):
        children = sorted(node.children.values(), key=sort_key)
        for idx, child in enumerate(children):
            last = idx == len(children) - 1
            branch = "└── " if last else "├── "
            if child.is_file:
                ts, short_hash, msg = child.meta
                line = f"{prefix}{branch}{child.name} ({ts} | {short_hash} | {msg})"
            else:
                line = f"{prefix}{branch}{child.name}"
            out.append(line)
            if not child.is_file:
                extension = "    " if last else "│   "
                render(child, prefix + extension, out)

    lines = []
    render(root, "", lines)

    with open(tree_file, "w", encoding="utf-8", newline="\n") as f:
        for line in lines:
            f.write(line + "\n")

    print(f"Tree rows total: {rows_total}", file=sys.stderr)
    print(f"Tree rows used: {rows_used}", file=sys.stderr)
    print(f"Tree rows skipped: {rows_skipped}", file=sys.stderr)

except Exception as e:
    print(f"ERROR while rendering tree from {csv_file}: {e}", file=sys.stderr)
    raise
PY
}

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "Error: current directory is not inside a git repository." >&2
  exit 1
fi

MODE="csv"
EXCLUDES_STR=""
TREE_ONLY_FILE=""

if [[ $# -eq 0 ]]; then
  :
else
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --tree)
        MODE="tree"
        shift
        ;;
      --tree-only)
        MODE="tree-only"
        if [[ $# -lt 2 ]]; then
          echo "Error: --tree-only requires a CSV file argument." >&2
          exit 1
        fi
        TREE_ONLY_FILE="$2"
        shift 2
        ;;
      --exclude=*)
        EXCLUDES_STR="${1#--exclude=}"
        shift
        ;;
      --help|-h)
        usage
        exit 0
        ;;
      *)
        break
        ;;
    esac
  done
fi

REPO_NAME="$(repo_name)"
DEFAULT_CSV_FILE="${REPO_NAME}-initial-commits.csv"
DEFAULT_TREE_FILE="${REPO_NAME}-tree.txt"

if [[ "$MODE" == "tree-only" ]]; then
  CSV_FILE="$TREE_ONLY_FILE"

  if [[ ! -f "$CSV_FILE" ]]; then
    echo "Error: CSV file not found: $CSV_FILE" >&2
    exit 1
  fi

  TREE_FILE="$DEFAULT_TREE_FILE"
  if [[ "$CSV_FILE" == *-initial-commits.csv ]]; then
    TREE_FILE="${CSV_FILE%-initial-commits.csv}-tree.txt"
  elif [[ "$CSV_FILE" == *.csv ]]; then
    TREE_FILE="${CSV_FILE%.csv}-tree.txt"
  fi

  render_tree_from_csv "$CSV_FILE" "$TREE_FILE"
  echo "Wrote tree report to: $TREE_FILE"
  exit 0
fi

CSV_FILE="${1:-$DEFAULT_CSV_FILE}"
JOBS="${2:-$DEFAULT_JOBS}"

build_csv "$CSV_FILE" "$JOBS" "$EXCLUDES_STR"
echo "Wrote CSV report to: $CSV_FILE"
echo "Parallel jobs used: $JOBS"

if [[ -n "$EXCLUDES_STR" ]]; then
  echo "Excluded prefixes: $EXCLUDES_STR"
fi

if [[ "$MODE" == "tree" ]]; then
  TREE_FILE="$DEFAULT_TREE_FILE"

  if [[ "$CSV_FILE" == *-initial-commits.csv ]]; then
    TREE_FILE="${CSV_FILE%-initial-commits.csv}-tree.txt"
  elif [[ "$CSV_FILE" == *.csv ]]; then
    TREE_FILE="${CSV_FILE%.csv}-tree.txt"
  fi

  render_tree_from_csv "$CSV_FILE" "$TREE_FILE"
  echo "Wrote tree report to: $TREE_FILE"
fi
