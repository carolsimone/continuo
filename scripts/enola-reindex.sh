#!/usr/bin/env bash
#
# Rebuild the enola knowledge-graph for this polyglot monorepo.
#
# Why this exists: enola models each snapshotted repo_path as ONE service node in
# the cross-repo graph. If you snapshot the repo root, every service under it
# (Go, Python, Node) collapses into a single bogus "continuo" node. So instead we
# snapshot each service directory on its own — discovered by its build marker
# (go.mod / pyproject.toml / package.json) — giving one honest node per service
# plus the cross-repo dependency edges between them. Go also *requires* a per-dir
# snapshot: enola detects Go via go.mod, and the repo root carries only go.work.
#
# The accumulated multi-repo store is written under whichever service is
# snapshotted last (enola resolves output.dir relative to each repo_path), so we
# consolidate it into the root .enola/ the MCP server loads at startup.
#
# Assumes the enola config's output.dir is ".enola" (relative to each repo_path).
#
# Usage:   scripts/enola-reindex.sh
# Env:     ENOLA_CONFIG   (default ~/.config/enola/mcp-arch.yaml)
#          ENOLA_BIN      (default: enola on PATH)
#          STEP_SLEEP     (default 5; seconds to wait per snapshot call)
#          EXCLUDE_DIRS   (space-separated service dirs to skip, e.g. "dbt")
#
# After it finishes: restart the enola MCP server (Claude Code: /mcp reconnect,
# or restart the app) so it reloads the freshly built .enola/facts.jsonl.
set -euo pipefail

CONFIG="${ENOLA_CONFIG:-$HOME/.config/enola/mcp-arch.yaml}"
ENOLA_BIN="${ENOLA_BIN:-enola}"
STEP_SLEEP="${STEP_SLEEP:-5}"
EXCLUDE_DIRS="${EXCLUDE_DIRS:-}"

command -v "$ENOLA_BIN" >/dev/null 2>&1 || { echo "error: '$ENOLA_BIN' not on PATH" >&2; exit 1; }
[ -f "$CONFIG" ] || { echo "error: enola config not found: $CONFIG" >&2; exit 1; }

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

# Discover service roots: any top-level dir carrying a build marker. maxdepth 2
# catches ./<svc>/<marker> but skips nested markers (tests/e2e/go.mod at depth 3,
# dbt/services/*/, .venv, node_modules). Deduped so a dir with two markers counts
# once. Run inside the script so it hits real `find`, not a shell wrapper.
is_excluded() { case " $EXCLUDE_DIRS " in *" $1 "*) return 0 ;; *) return 1 ;; esac }

SERVICES=()
while IFS= read -r d; do
  is_excluded "$d" && continue
  SERVICES+=("$d")
done < <(
  {
    find . -maxdepth 2 -name go.mod
    find . -maxdepth 2 -name pyproject.toml
    find . -maxdepth 2 -name package.json
  } 2>/dev/null \
    | grep -vE '/(node_modules|\.venv|\.opencode|vendor)/' \
    | while IFS= read -r m; do d="$(dirname "$m")"; echo "${d#./}"; done \
    | grep -vE '^\.$' \
    | sort -u
)

if [ "${#SERVICES[@]}" -eq 0 ]; then
  echo "error: no service dirs (go.mod/pyproject.toml/package.json) found under $ROOT" >&2
  exit 1
fi

echo "Reindexing enola knowledge-graph"
echo "  root     : $ROOT"
echo "  config   : $CONFIG"
echo "  services : ${SERVICES[*]}"
[ -n "$EXCLUDE_DIRS" ] && echo "  excluded : $EXCLUDE_DIRS"
echo

# Clean slate: enola bootstrap-loads root/.enola at startup, so a stale store
# would merge into the new one. Remove root and any per-service scratch stores.
rm -rf "$ROOT/.enola"
for s in "${SERVICES[@]}"; do rm -rf "$s/.enola"; done

LOG="$(mktemp -t enola-reindex)"

# Emit the MCP/JSON-RPC driver stream: initialize handshake, then one snapshot
# per service. The first is the base; every following one appends, which is what
# builds the cross-repo service graph. Sleeps keep stdin open long enough for the
# server to process each call before the next arrives.
emit_driver() {
  printf '%s\n' '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"enola-reindex","version":"0"}}}'
  sleep 1
  printf '%s\n' '{"jsonrpc":"2.0","method":"notifications/initialized"}'
  id=2
  first=1
  for s in "${SERVICES[@]}"; do
    if [ "$first" = 1 ]; then
      echo "  snapshot: $s (base)" >&2
      printf '{"jsonrpc":"2.0","id":%d,"method":"tools/call","params":{"name":"generate_snapshot","arguments":{"repo_path":"%s/%s"}}}\n' "$id" "$ROOT" "$s"
      first=0
    else
      echo "  snapshot: $s (append)" >&2
      printf '{"jsonrpc":"2.0","id":%d,"method":"tools/call","params":{"name":"generate_snapshot","arguments":{"repo_path":"%s/%s","append":true}}}\n' "$id" "$ROOT" "$s"
    fi
    sleep "$STEP_SLEEP"
    id=$((id + 1))
  done
  sleep 2
}

emit_driver | "$ENOLA_BIN" "$CONFIG" >"$LOG" 2>&1 || true

# The fully accumulated store lives under the LAST appended service (output.dir is
# relative to repo_path). Pick the largest facts.jsonl to be order-independent.
BIGGEST=""; BIGGEST_N=-1
for s in "${SERVICES[@]}"; do
  f="$s/.enola/facts.jsonl"
  if [ -f "$f" ]; then
    n="$(wc -l < "$f" | tr -d ' ')"
    if [ "$n" -gt "$BIGGEST_N" ]; then BIGGEST_N="$n"; BIGGEST="$s"; fi
  fi
done

if [ -z "$BIGGEST" ]; then
  echo "error: no per-service .enola store was produced; extraction likely failed." >&2
  echo "       see log: $LOG" >&2
  exit 1
fi

# Consolidate into the root store the MCP server boots from, then drop the
# per-service scratch stores so root .enola/ is the single source of truth.
mkdir -p "$ROOT/.enola"
cp "$BIGGEST/.enola/facts.jsonl" \
   "$BIGGEST/.enola/insights.json" \
   "$BIGGEST/.enola/snapshot.meta.json" \
   "$BIGGEST/.enola/llm_context.md" \
   "$ROOT/.enola/"
for s in "${SERVICES[@]}"; do rm -rf "$s/.enola"; done

echo
echo "Consolidated -> $ROOT/.enola/  (from service '$BIGGEST')"
if command -v python3 >/dev/null 2>&1; then
  python3 - <<'PY'
import json, collections
kinds = collections.Counter(); repos = collections.Counter(); cross = 0; junk = 0
for line in open('.enola/facts.jsonl'):
    line = line.strip()
    if not line: continue
    o = json.loads(line); kinds[o.get('kind')] += 1
    f = o.get('file') or ''
    if 'node_modules' in f or '/.venv' in f or '.opencode' in f: junk += 1
    p = o.get('props') or {}
    if p.get('type') == 'cross_repo': cross += 1
    r = p.get('repo') or o.get('repo')
    if r: repos[r] += 1
print(f"  facts         : {sum(kinds.values())}")
print(f"  service nodes : {kinds.get('service')}")
print(f"  cross_repo    : {cross}")
print(f"  junk facts    : {junk}")
print(f"  repos ({len(repos)})   : {', '.join(sorted(repos))}")
PY
fi

echo
echo "Done. Restart the enola MCP server so it reloads the new snapshot:"
echo "  Claude Code: /mcp -> reconnect 'enola'  (or restart the app)"
echo "Driver log: $LOG"
