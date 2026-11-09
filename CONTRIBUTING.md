# Contributing

Pull requests are welcome. Use [Conventional Commits](https://www.conventionalcommits.org/)
(`feat:`, `fix:`, `docs:`, `chore:` ...); release-please derives versions and the changelog
from them.

Before opening a PR, run what CI runs:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Pin any new GitHub Action to a full commit SHA with a `# vX` comment.
