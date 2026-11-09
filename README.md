# bazel-cache-proxy

A Bazel remote cache (HTTP and gRPC) whose storage can be the **GitHub Actions cache**, so a CI
job gets a persistent `--remote_cache` with no cache server, cloud account or paid runner.

[![CI](https://github.com/tstapler/bazel-cache-proxy/actions/workflows/ci.yml/badge.svg)](https://github.com/tstapler/bazel-cache-proxy/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue)](#license)

> Status: pre-1.0. The GitHub Actions backend is exercised by a real two-job run on every
> relevant PR ([proof workflow](.github/workflows/gha-cache-proof.yml)), but it relies on an
> undocumented GitHub protocol (see [Limitations](#limitations)).

## How it works

Inside a job the proxy serves Bazel from a local disk cache on loopback. At startup it restores
the newest *manifest* from the GitHub Actions cache and unpacks the *packs* it lists; on
shutdown (or `POST /admin/flush`) it uploads only the blobs written during this job as one
new pack plus a new manifest. GitHub cache entries are immutable and the service allows
[200 uploads per minute per repository](https://docs.github.com/en/actions/reference/workflows-and-actions/dependency-caching),
so one entry per Bazel blob does not scale; a few packs per job do.
Design rationale: [docs/explanation/design.md](docs/explanation/design.md).

Other backends: local disk, S3, GCS, SQLite, and a layered L1/L2 combination.

## Quickstart (GitHub Actions)

`run:` steps do not receive the cache credentials, so export them with a small action first.

```yaml
steps:
  - uses: actions/checkout@v5
  - uses: crazy-max/ghaction-github-runtime@v3   # pin to a SHA in real use
  - name: Start cache proxy
    run: |
      # download a release binary (or cargo install --git ... --locked) and write the config
      cat > proxy.toml <<'TOML'
      [backend]
      type = "gha"
      prefix = "bazel-${{ runner.os }}-${{ github.workflow }}"
      TOML
      ./bazel-cache-proxy serve --config proxy.toml > proxy.log 2>&1 &
      for _ in $(seq 50); do curl -fsS localhost:9090/healthz && break; sleep 0.2; done
  - run: bazel build --remote_cache=http://127.0.0.1:9090/cache //...
  - name: Publish cache
    if: always()
    run: curl -fsS -X POST localhost:9090/admin/flush
```

Equivalent `.bazelrc` line: `build:ci --remote_cache=http://127.0.0.1:9090/cache`.
Set `BAZEL_CACHE_PROXY_UPLOAD=false` (or `upload = false`) on pull requests that should read
the cache but not write it. More: [docs/how-to/use-in-github-actions.md](docs/how-to/use-in-github-actions.md).

## Observability

`GET /metrics` (Prometheus): `bazel_cache_lookups_total{op,kind,result}`,
`bazel_cache_put_total{kind}`, `bazel_cache_put_bytes_total{kind}`. `POST /admin/flush` returns
a JSON report (packs, blobs, bytes uploaded, whether the cache was compacted). Startup logs
how many packs and blobs were restored.

## Security defaults

Loopback bind only (`127.0.0.1:9090` HTTP, `:9091` gRPC), no authentication. The runtime token
and signed storage URLs are never logged. See [SECURITY.md](SECURITY.md).

## Limitations

- The GitHub cache upload/download protocol is not a documented public API
  ([toolkit source](https://github.com/actions/toolkit/tree/main/packages/cache) is the
  reference); it can change.
- Repository budget is 10 GB by default and shared with every other cache; entries unused for
  7 days are evicted. A pack is held in memory while built and uploaded (default cap 200 MiB
  uncompressed per pack).
- Cache scoping follows GitHub's rules: pull-request runs write to their own merge-ref scope
  and read the base branch's.
- **Last writer wins per prefix.** Two jobs that start from the same manifest each publish a
  manifest listing the old packs plus their own; the newer manifest hides the other's new
  packs (they become garbage until evicted). Give concurrent jobs (matrix legs) distinct
  `prefix` values, or let only one of them upload.
- Memory: a pack is built and uploaded from memory, and up to four are downloaded at once on
  restore. Budget a few times `max_pack_bytes`; one blob larger than that becomes its own pack.
- Each compaction (every `max_packs` packs) leaves the old packs and manifests in the
  repository budget until GitHub evicts them.
- Bazel compression flags (`--remote_cache_compression`) are not tested with the HTTP endpoint.
- The whole local cache of a run is bounded by `max_local_bytes` (default 4 GiB).

## Documentation

- [Use it in GitHub Actions](docs/how-to/use-in-github-actions.md)
- [Configuration reference](docs/reference/configuration.md)
- [Design and prior art](docs/explanation/design.md)

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
See [CONTRIBUTING.md](CONTRIBUTING.md) to contribute.
