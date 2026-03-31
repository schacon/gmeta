---
description: "gmeta reference implementation engineering guide for Claude/Codex agents: Rust architecture, testing, documentation"
alwaysApply: true
---

# Gmeta Engineering Guide

This is a reference implementation of the gmeta spec ([docs](https://schacon.github.io/gmeta/))

## Rust Style and Idioms

- Use traits for behaviour boundaries.
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
- Prefer iterators/combinators over manual loops. Use `Cow<'_, str>` when allocation is conditional.

## Architecture and Design

- For code that you create, **always** include doc comments for all public functions, structs, enums, and methods and also document function parameters, return values, and errors.
- Documentation and comments **must** be kept up-to-date with code changes.
- Do not re-discover Git repositories, instead take them as inputs to functions and methods.
- Avoid implicitly using the current time like `std::time::SystemTime::now()`, instead pass the current time as argument.
- Keep public API surfaces small. Use `#[must_use]` where return values matter.

## Testing

- All code must have an appropriate test coverage (proportionate to its complexity and criticality).
- Do not test external dependencies or trivial getters/setters.
- Create a mix of unit tests and end-to-end tests.
- Do not use `anyhow::ensure!` in tests; use panicking assertions (`assert!`, `assert_eq!`, `assert_ne!`) so failures are test panics.

## Committing and Version Control

- If available, always prefer the `but` (GitButler) cli over `git`. Always load the respective skill.
- Before committing, always run `cargo fmt` and `cargo clippy --fix --allow-dirty` and ensure no warnings remain.
