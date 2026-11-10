# Design and prior art

## Why packs, not one entry per blob

Facts from GitHub's [caching docs](https://docs.github.com/en/actions/reference/workflows-and-actions/dependency-caching):
entries are immutable per key; 10 GB per repository by default; least-recently-accessed
eviction when over the limit ("cache thrashing" is called out); 200 uploads and 1500 downloads
per minute per repository; entries unused for 7 days are removed.

A Bazel build issues hundreds to thousands of blob writes. One entry per blob would exhaust the
upload limit in seconds (the first design in this repo did exactly that, throttled to 200/min),
and each lookup would cost a Twirp round-trip. Packs make a job cost a handful of entries.

## Manifest chain

Restore: look up the newest `<prefix>-manifest-*` (restore-key prefix match returns the most
recently created), download the packs it lists, unpack into the disk cache. Flush: upload
new blobs as new packs, then the new manifest (last, so it never points at a missing pack).
Past `max_packs`, or when a listed pack was evicted, the whole local cache is republished.
Unpacking accepts only `ac/<64 hex>` and `cas/<64 hex>` regular files and verifies CAS content
against its name.

## Prior art

- [bazel-contrib/setup-bazel](https://github.com/bazel-contrib/setup-bazel): caches Bazel's
  disk/repository caches as whole `actions/cache` archives; no remote cache protocol.
- [tsawada/bazel-github-actions-cache](https://github.com/tsawada/bazel-github-actions-cache):
  HTTP cache on the legacy API (README lists compression and gRPC as TODO).
- [Silic0nS0ldier/bazel-github-actions-cache-v2](https://github.com/Silic0nS0ldier/bazel-github-actions-cache-v2):
  loopback HTTP cache on cache v2 with an optional packs mode; same conclusion about entry rates.
- [buchgr/bazel-remote](https://github.com/buchgr/bazel-remote): self-hosted, S3/GCS proxy
  backends, no GitHub backend.

## Why not just `actions/cache` on a disk cache

That is simpler and needs no proxy. The proxy adds: Bazel serving only what a build asks for,
incremental publish (new blobs only, instead of re-archiving the whole disk cache), and
hit/miss counters. It costs one more process per job and an undocumented-protocol dependency.
