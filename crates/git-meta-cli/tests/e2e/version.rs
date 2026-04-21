use predicates::prelude::*;
use tempfile::TempDir;

use crate::harness;

/// `--version` prints the package version (sourced from `CARGO_PKG_VERSION`)
/// and exits successfully without requiring a Git repository.
#[test]
fn version_flag_prints_package_version() {
    let dir = TempDir::new().unwrap();

    harness::git_meta(dir.path())
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains(env!("CARGO_PKG_VERSION")))
        .stdout(predicate::str::contains("git-meta"));
}

/// `-V` is the conventional short form of `--version` and must behave the
/// same way.
#[test]
fn version_short_flag_prints_package_version() {
    let dir = TempDir::new().unwrap();

    harness::git_meta(dir.path())
        .arg("-V")
        .assert()
        .success()
        .stdout(predicate::str::contains(env!("CARGO_PKG_VERSION")));
}
