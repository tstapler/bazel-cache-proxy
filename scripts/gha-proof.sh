#!/usr/bin/env bash
# One phase ("cold" or "warm") of the GitHub Actions proof run. Requires ACTIONS_RESULTS_URL and
# ACTIONS_RUNTIME_TOKEN in the environment (exported by a prior step) and never prints them.
# Usage: scripts/gha-proof.sh <phase> <proxy-binary> <key-prefix>
set -euo pipefail

phase=$1
proxy_bin=$(cd "$(dirname "$2")" && pwd)/$(basename "$2")
prefix=$3
repo=$(cd "$(dirname "$0")/.." && pwd)
work=${RUNNER_TEMP:-/tmp}/proof-$phase
rm -rf "$work"; mkdir -p "$work"
summary=${GITHUB_STEP_SUMMARY:-/dev/null}

cat >"$work/config.toml" <<TOML
[backend]
type = "gha"
prefix = "$prefix"
root = "$work/store"
TOML

"$proxy_bin" serve --config "$work/config.toml" >"$work/proxy.log" 2>&1 &
proxy_pid=$!
for _ in $(seq 300); do curl -fsS http://127.0.0.1:9090/healthz >/dev/null 2>&1 && break; sleep 0.2; done
grep -E "restored|no cache manifest|starting cold|restore failed" "$work/proxy.log" || true

cd "$repo/tests/bazel-smoke"
start=$(date +%s.%N)
bazel build --remote_cache=http://127.0.0.1:9090/cache --disk_cache= \
  --profile="$work/profile.gz" //:combined 2>&1 | tee "$work/bazel.log" | tail -4
elapsed=$(echo "$(date +%s.%N) - $start" | bc)
hits=$(grep -Eo '[0-9]+ remote cache hit' "$work/bazel.log" | grep -Eo '^[0-9]+' || echo 0)

# Persist what this build produced, then shut down (SIGTERM also flushes; the endpoint is the
# explicit, observable path).
code=$(curl -sS -o "$work/flush.json" -w '%{http_code}' -X POST http://127.0.0.1:9090/admin/flush)
if [[ $code != 200 ]]; then
  echo "flush failed: HTTP $code: $(cat "$work/flush.json")" >&2
  echo "--- proxy log ---" >&2; cat "$work/proxy.log" >&2
  exit 1
fi
flush=$(cat "$work/flush.json")
if grep -q '"skipped_reason":"restore failed' <<<"$flush"; then
  echo "::warning::cache restore failed earlier in this run; nothing was published"
fi
metrics=$(curl -fsS http://127.0.0.1:9090/metrics | grep -E '^bazel_cache_(lookups|put)' || true)
kill "$proxy_pid"; wait "$proxy_pid" || true

{
  echo "### $phase build"
  echo "- wall time: ${elapsed}s (bazel invocation incl. server start)"
  echo "- remote cache hits: $hits"
  echo "- flush report: \`$flush\`"
  echo '```'; echo "$metrics"; echo '```'
} | tee -a "$summary"

if [[ $phase == warm && $hits -lt 20 ]]; then
  echo "FAIL: warm build expected >=20 remote cache hits, got $hits" >&2
  cat "$work/proxy.log" >&2
  exit 1
fi
if [[ $phase == cold && $hits -ne 0 ]]; then
  echo "FAIL: cold build unexpectedly hit the cache ($hits hits); prefix not unique?" >&2
  exit 1
fi
