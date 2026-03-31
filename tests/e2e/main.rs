//! End-to-end tests for gmeta.
//!
//! These tests exercise the `gmeta` CLI binary against real git repositories,
//! verifying the full round-trip of metadata operations. The test harness
//! provides environment isolation and fixture-based repo setup inspired by
//! GitButler's `but-testsupport` crate.

#[allow(dead_code)]
mod harness;

mod list;
mod materialize;
mod promisor;
mod push_pull;
mod remote;
mod serialize;
mod set_get;
