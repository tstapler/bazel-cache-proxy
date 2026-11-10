# Configuration reference

TOML file passed with `serve --config <file>`; environment variables prefixed `BAZEL_CACHE_`
override top-level keys (figment). `BAZEL_CACHE_PROXY_UPLOAD` also starts with that prefix and is
ignored by the config loader; it is read separately. Validate with `validate-config --config <file>`.

| Key | Default | Meaning |
|---|---|---|
| `http_addr` | `127.0.0.1:9090` | HTTP listener (`/cache/{ac,cas}/<sha256>`, `/healthz`, `/readyz`, `/metrics`, `POST /admin/flush`) |
| `grpc_addr` | `127.0.0.1:9091` | gRPC listener (REAPI cache + ByteStream) |
| `metrics_path` | `/metrics` | Prometheus scrape path |
| `backend.type` | required | `disk`, `s3`, `gcs`, `sqlite`, `gha`, `layered` |

## `backend.type = "gha"`

| Key | Default | Meaning |
|---|---|---|
| `prefix` | `bazel` | Key prefix. Include OS/arch/workflow so unrelated caches stay apart |
| `root` | `$RUNNER_TEMP/bazel-cache-proxy` | Local working cache |
| `max_local_bytes` | `4 GiB` | LRU bound for the local cache |
| `max_pack_bytes` | `200 MiB` | Uncompressed size per pack (held in memory while uploading) |
| `max_packs` | `24` | When a manifest would list more packs, the whole cache is rewritten as fresh packs |
| `upload` | `true` | Publish on flush. `BAZEL_CACHE_PROXY_UPLOAD` set to `false`, `0`, `no` or `off` forces `false` |

Credentials: `ACTIONS_RESULTS_URL` and `ACTIONS_RUNTIME_TOKEN`. When absent the proxy logs a
warning and serves a local-only disk cache.

Cache entries written per flush: `<prefix>-pack-<run>-<attempt>-<nonce>-f<flush>-<n>` (one or
more) and `<prefix>-manifest-<run>-<attempt>-<nonce>-f<flush>`. The nonce is random per process,
so concurrent jobs never collide on a key.

Validation: `prefix` must match `[A-Za-z0-9._-]{1,128}`; `max_packs >= 1`; `max_pack_bytes >= 1 MiB`.
A blob larger than `max_pack_bytes` is served locally but never persisted (a warning is logged).
