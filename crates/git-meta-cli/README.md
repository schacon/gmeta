# git-meta-cli

Command-line tool for structured Git metadata: get/set/remove values, serialize and materialize metadata trees, push/pull metadata refs, and more.

Install from [crates.io](https://crates.io/crates/git-meta-cli):

```sh
cargo install git-meta-cli
```

The installed binary is named `git-meta`.

This crate depends on [`git-meta-lib`](https://crates.io/crates/git-meta-lib) for all core logic.

### Publishing to crates.io

Publish the library first, then the CLI (the CLI manifest pins `git-meta-lib` to the same version):

1. `cargo publish -p git-meta-lib`
2. `cargo publish -p git-meta-cli`
