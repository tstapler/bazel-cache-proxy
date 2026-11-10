#!/usr/bin/env bash
# End-to-end smoke test: build twice with a clean local state in between and assert the
# second build is served from the proxy. Usage: scripts/bazel-smoke.sh <proxy-binary> [config.toml]
set -euo pipefail

proxy_bin=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
repo=$(cd "$(dirname "$0")/.." && pwd)
work=$(mktemp -d)
trap 'kill "${proxy_pid:-0}" 2>/dev/null || true; rm -rf "$work"' EXIT

config=${2:-$work/config.toml}
if [[ $# -lt 2 ]]; then
  cat >"$config" <<TOML
[backend]
type = "disk"
root = "$work/store"
TOML
fi

"$proxy_bin" serve --config "$config" >"$work/proxy.log" 2>&1 &
proxy_pid=$!
for _ in $(seq 300); do curl -fsS http://127.0.0.1:9090/healthz >/dev/null 2>&1 && break; sleep 0.2; done
kill -0 "$proxy_pid" 2>/dev/null || { echo "proxy exited at startup (port in use?):" >&2; cat "$work/proxy.log" >&2; exit 1; }

cd "$repo/tests/bazel-smoke"
bazel clean >/dev/null 2>&1 # outputs left by an earlier run would skip the uploads under test
flags=(${SMOKE_EXTRA_FLAGS:-} --remote_cache=http://127.0.0.1:9090/cache --disk_cache= \
       --execution_log_json_file="$work/exec.json" --profile="$work/profile.gz")

bazel build "${flags[@]}" //:combined 2>&1 | tail -3
bazel clean >/dev/null 2>&1
bazel shutdown >/dev/null 2>&1
out=$(bazel build "${flags[@]}" //:combined 2>&1)
echo "$out" | tail -4

hits=$(echo "$out" | grep -Eo '[0-9]+ remote cache hit' | grep -Eo '^[0-9]+' || echo 0)
echo "second build remote cache hits: $hits"
curl -fsS http://127.0.0.1:9090/metrics | grep -E '^bazel_cache_(lookups|put)' || true
[[ "$hits" -ge 20 ]] || { echo "FAIL: expected >=20 remote cache hits" >&2; cat "$work/proxy.log" >&2; exit 1; }
echo "OK"
