use predicates::prelude::*;

use crate::harness::{self, commit_target, setup_repo};

#[test]
fn list_push() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    harness::git_meta(dir.path())
        .args(["list:push", &target, "tags", "first"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["list:push", &target, "tags", "second"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["get", &target, "tags"])
        .assert()
        .success()
        .stdout(predicate::str::contains("first"))
        .stdout(predicate::str::contains("second"));
}

#[test]
fn list_push_converts_string_to_list() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    harness::git_meta(dir.path())
        .args(["set", &target, "note", "original"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["list:push", &target, "note", "appended"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["get", &target, "note"])
        .assert()
        .success()
        .stdout(predicate::str::contains("original"))
        .stdout(predicate::str::contains("appended"));
}

#[test]
fn list_pop() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    harness::git_meta(dir.path())
        .args(["list:push", &target, "tags", "a"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["list:push", &target, "tags", "b"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["list:pop", &target, "tags", "b"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["get", &target, "tags"])
        .assert()
        .success()
        .stdout(predicate::str::contains("a"))
        .stdout(predicate::str::contains("b").not());
}

#[test]
fn list_push_creates_list_from_scratch() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    for value in ["hello", "world"] {
        harness::git_meta(dir.path())
            .args(["list:push", &target, "items", value])
            .assert()
            .success();
    }

    harness::git_meta(dir.path())
        .args(["get", &target, "items"])
        .assert()
        .success()
        .stdout(predicate::str::contains("hello"))
        .stdout(predicate::str::contains("world"));
}
