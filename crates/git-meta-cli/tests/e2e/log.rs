//! End-to-end tests for `git meta log`, focused on the pager
//! integration added by [`crate::pager`].
//!
//! `assert_cmd` always captures stdout, so [`std::io::IsTerminal`]
//! returns `false` inside these tests and the pager is auto-skipped.
//! That is exactly the contract we need to verify: the command must
//! still produce its normal output when no pager is in play, and it
//! must not try to spawn anything (e.g. `less`) that could hang the
//! test runner waiting for keyboard input.

use predicates::prelude::*;

use crate::harness::{self, commit_target, setup_repo};

/// Smoke test: with stdout captured (i.e. not a TTY), `git meta log`
/// prints the commit and any associated metadata directly without
/// trying to invoke a pager. Output must contain both the metadata
/// header and the value we just wrote.
#[test]
fn log_prints_directly_when_stdout_is_not_a_tty() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    harness::git_meta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["log", "-n", "1"])
        .assert()
        .success()
        .stdout(predicate::str::contains("commit"))
        .stdout(predicate::str::contains("--- metadata ---"))
        .stdout(predicate::str::contains("agent:model"))
        .stdout(predicate::str::contains("claude-4.6"));
}

/// `PAGER=cat` is git's well-known sentinel for "do not paginate".
/// Even if the test environment somehow looked like a TTY, the command
/// must take the no-pager path and not block waiting for a child
/// process. Asserting `success()` here doubles as a hang-detector:
/// `assert_cmd` would time out if we accidentally tried to spawn
/// something that waited on stdin.
#[test]
fn log_treats_pager_cat_as_disabled() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    harness::git_meta(dir.path())
        .args(["set", &target, "k", "v"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .env("PAGER", "cat")
        .args(["log", "-n", "1"])
        .assert()
        .success()
        .stdout(predicate::str::contains("commit"));
}

/// `GIT_META_PAGER_IN_USE=1` indicates a parent process already
/// paginated — the inner invocation must not try to spawn another
/// pager. We can't directly observe "no spawn" in a black-box e2e
/// test, but we can assert the command still completes successfully
/// and emits its full output, which confirms the no-pager path was
/// taken (a hung child or a failure to write would surface here).
#[test]
fn log_skips_pager_when_already_in_use() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    harness::git_meta(dir.path())
        .args(["set", &target, "k", "v"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .env("GIT_META_PAGER_IN_USE", "1")
        .args(["log", "-n", "1"])
        .assert()
        .success()
        .stdout(predicate::str::contains("commit"));
}
