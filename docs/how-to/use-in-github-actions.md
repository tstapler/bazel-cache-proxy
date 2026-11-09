# Use the proxy in GitHub Actions

1. Export the cache credentials with an action (`crazy-max/ghaction-github-runtime`, pinned to a
   SHA). Plain `run:` steps do not get `ACTIONS_RESULTS_URL` / `ACTIONS_RUNTIME_TOKEN`.
2. Start `bazel-cache-proxy serve --config proxy.toml` in the background and wait for `/healthz`.
3. Pass `--remote_cache=http://127.0.0.1:9090/cache` to Bazel.
4. After the build, with `if: always()`, `curl -X POST http://127.0.0.1:9090/admin/flush`.
   Killing the proxy with SIGTERM also flushes, but the endpoint reports what was uploaded.

Choose `prefix` per job type; hits are keyed by Bazel's action digests, so sharing across
unrelated workflows wastes budget rather than causing wrong results.

Keep pull requests from fork or untrusted branches read-only (`BAZEL_CACHE_PROXY_UPLOAD=false`).
A working end-to-end example is [`scripts/gha-proof.sh`](../../scripts/gha-proof.sh) driven by
[`.github/workflows/gha-cache-proof.yml`](../../.github/workflows/gha-cache-proof.yml).
