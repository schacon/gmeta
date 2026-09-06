---
description: "git-meta reference implementation engineering guide for Claude/Codex agents: Rust architecture, testing, documentation"
alwaysApply: true
---

# git-meta Engineering Guide

This is a reference implementation of the git-meta spec ([docs](https://git-meta.com/))

## Rust Style and Idioms

- Use traits only for real behaviour boundaries: multiple implementations, dependency inversion, or a clear test seam. Do not add traits just to make code look extensible.
- Derive `Default` when all fields have sensible defaults.
- Use concrete types (`struct`/`enum`) over `serde_json::Value` wherever shape is known.
- **Match on types, never strings.** Only convert to strings at serialization/display boundaries.
- Prefer `From`/`Into`/`TryFrom`/`TryInto` over manual conversions. Ask before adding manual conversion paths.
- **Forbidden:** `Mutex<()>` / `Arc<Mutex<()>>` — mutex must guard actual state.
- Use `anyhow::Result` for app errors, `thiserror` for library errors. Propagate with `?`.
- **Never `.unwrap()`/`.expect()` in production.** Workspace lints deny these. Use `?`, `ok_or_else`, `unwrap_or_default`, `unwrap_or_else(|e| e.into_inner())` for locks.
- Prefer `Option<T>` over sentinel values.
- Use `time` crate (workspace dep) for date/time — no manual epoch math or magic constants like `86400`.
- Prefer guard clauses (early returns) over nested `if` blocks.
- Prefer the clearest Rust. Iterators are good when they improve clarity; simple `for` loops are fine when they are easier to read. Use `Cow<'_, str>` when allocation is conditional and the added type complexity is worth it.
- **No banner/separator comments.** Do not use decorative divider comments like `// ── Section ───`. Use normal `//` comments or doc comments to explain *why*, not to visually partition files.

## Dependencies

- **Use `gix` (gitoxide), not `git2` (libgit2).** `gix` is the preferred Git library for this project. Do not introduce `git2` dependencies or suggest `git2`-based solutions. If existing code uses `git2`, prefer migrating it to `gix` when touching that code.

## Architecture and Design

- For public API code, include useful doc comments explaining purpose, invariants, errors, and examples when helpful. Avoid boilerplate docs that merely repeat names or types.
- Documentation and comments **must** be kept up-to-date with code changes.
- Do not re-discover Git repositories, instead take them as inputs to functions and methods.
- Avoid implicitly using the current time like `std::time::SystemTime::now()`, instead pass the current time as argument.
- Keep public API surfaces small. Use `#[must_use]` where return values matter.

## AI Code Review Pass

Before finishing generated Rust, review it as if it came from an overeager AI:

- Delete fake extensibility.
- Collapse one-use traits/types/functions.
- Rename generic concepts to domain concepts.
- Prefer passing explicit inputs over rediscovering state.
- Remove comments that describe what the next line already says.
- Keep error handling intentional: typed errors for library boundaries, `anyhow` for app/CLI boundaries.
- Preserve behavior; do not turn cleanup into a refactor.

## Testing

- All code must have an appropriate test coverage (proportionate to its complexity and criticality).
- Do not test external dependencies or trivial getters/setters.
- Create a mix of unit tests and end-to-end tests.
- Do not use `anyhow::ensure!` in tests; use panicking assertions (`assert!`, `assert_eq!`, `assert_ne!`) so failures are test panics.

## Committing and Version Control

- Prefer repository/library APIs over shelling out to `git` in production code. In tests, use project test helpers where available. Use the `git` CLI only when testing CLI interoperability or behavior that specifically depends on Git’s command-line semantics.
- When you are done making changes, always run `cargo fmt` and `cargo clippy --fix --allow-dirty` and ensure no warnings remain.

## Benchmark fixtures

`~/git-meta-fixtures/6m-history` is a generated metadata repository holding
**6,426,841 key writes across 158,182 metadata commits** (~3.8 GB). It took
about four hours to build and is the fixture the scale benchmarks measure
against.

**Do not delete it, and do not run `git meta teardown` or `git gc --prune` in
it, unless explicitly asked.** Regenerating it is hours of work.

It carries three refs marking equivalent histories at different depths, so
measurements can compare scale without rebuilding anything:

| ref | commits | key writes | tip keys |
| --- | --- | --- | --- |
| `refs/meta/sized/250k` | 6,252 | 250,000 | 5,832 |
| `refs/meta/sized/1m` | 25,002 | 1,000,000 | 4,881 |
| `refs/meta/sized/6m` | 158,184 | 6,426,841 | 6,023 |
| `refs/meta/local/main` | 158,182 | 6,426,841 | 6,023 |

The `sized/*` refs each end with a commit that publishes 1,000 keys on a single
commit target (`commit:0000…424242`) followed by a prune commit that removes
them from the tip, so the promisor fetch path can be exercised identically at
every depth. The 250k and 1M tips are truncated to roughly the size of the real
auto-pruned 6M tip: those cut points predate auto-pruning, and without
truncation their tips would carry every key ever written, confounding tip size
with history depth.

Generate a replacement with `crates/git-meta-bench` if it is ever lost — see
that crate's README.
