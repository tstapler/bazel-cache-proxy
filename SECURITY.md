# Security policy

## Reporting a vulnerability

Please report vulnerabilities privately through
[GitHub private vulnerability reporting](https://github.com/tstapler/bazel-cache-proxy/security/advisories/new)
rather than a public issue. Expect an acknowledgement within a week.

## Threat model in brief

- The proxy binds to **loopback** (`127.0.0.1`) by default and has **no authentication**.
  Binding it to a non-loopback address exposes every cached build output to that network;
  do so only behind your own access control.
- Anything a client can write can later be served to every build that reads the same cache.
  Keep untrusted pull requests from writing to the cache scope that trusted builds read
  (see the README on `upload` and GitHub cache scoping).
- `POST /admin/flush` shares the cache listener and is unauthenticated; it can only trigger an
  upload of data already in the local cache. AC entries are trusted as written (only CAS content
  is verified against its hash); packs are only as trustworthy as the cache scope they came from.
- The GitHub backend never logs the runtime token or signed storage URLs.
