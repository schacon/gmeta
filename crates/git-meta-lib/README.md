# git-meta-lib

Rust library for attaching and exchanging structured metadata in Git repositories: local SQLite storage, serialize/materialize to Git trees and refs, and merge semantics aligned with the [git metadata specification](https://git-meta.com/).

Add to your `Cargo.toml`:

```toml
[dependencies]
git-meta-lib = "0.1.0"
```

The public API is under the `git_meta_lib` crate name (hyphens in the package name become underscores in Rust).

For the command-line interface, see the [`git-meta-rs`](https://crates.io/crates/git-meta-rs) crate (binary `git-meta`).

### Publishing

`cargo publish -p git-meta-lib` must succeed on crates.io before `cargo publish -p git-meta-rs`, because the CLI crate depends on that exact version.
