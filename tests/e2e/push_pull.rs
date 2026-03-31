use predicates::prelude::*;

use crate::harness::{self, commit_target, setup_bare_with_meta, setup_repo};

#[test]
fn push_simple() {
    let (dir, sha) = setup_repo();
    let bare_dir = setup_bare_with_meta("meta");
    let bare_path = bare_dir.path().to_str().unwrap();

    harness::gmeta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();
    harness::gmeta(dir.path()).args(["pull"]).assert().success();

    let target = commit_target(&sha);
    harness::gmeta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["push"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Pushed metadata to meta"));

    let bare = git2::Repository::open_bare(bare_dir.path()).unwrap();
    let commit = bare
        .find_reference("refs/meta/main")
        .unwrap()
        .peel_to_commit()
        .unwrap();
    assert_eq!(
        commit.parent_count(),
        1,
        "pushed commit should have exactly 1 parent (no merge commits)"
    );
}

#[test]
fn push_up_to_date() {
    let (dir, sha) = setup_repo();
    let bare_dir = setup_bare_with_meta("meta");
    let bare_path = bare_dir.path().to_str().unwrap();

    harness::gmeta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();
    harness::gmeta(dir.path()).args(["pull"]).assert().success();

    let target = commit_target(&sha);
    harness::gmeta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    harness::gmeta(dir.path()).args(["push"]).assert().success();

    harness::gmeta(dir.path())
        .args(["push"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Everything up-to-date"));
}

#[test]
fn push_commit_message_format() {
    let (dir, sha) = setup_repo();
    let bare_dir = setup_bare_with_meta("meta");
    let bare_path = bare_dir.path().to_str().unwrap();

    harness::gmeta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();
    harness::gmeta(dir.path()).args(["pull"]).assert().success();

    let target = commit_target(&sha);
    harness::gmeta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();
    harness::gmeta(dir.path())
        .args(["set", &target, "agent:cost", "0.05"])
        .assert()
        .success();
    harness::gmeta(dir.path()).args(["push"]).assert().success();

    let bare = git2::Repository::open_bare(bare_dir.path()).unwrap();
    let commit = bare
        .find_reference("refs/meta/main")
        .unwrap()
        .peel_to_commit()
        .unwrap();
    let msg = commit.message().unwrap();
    assert!(
        msg.contains("gmeta: serialize"),
        "commit message should start with 'gmeta: serialize', got: {}",
        msg
    );
    assert!(
        msg.contains("agent:model"),
        "commit message should contain changed key, got: {}",
        msg
    );
}

#[test]
fn push_conflict_produces_no_merge_commits() {
    let bare_dir = setup_bare_with_meta("meta");
    let bare_path = bare_dir.path().to_str().unwrap();

    let (dir_a, sha_a) = setup_repo();
    let (dir_b, sha_b) = setup_repo();

    harness::gmeta(dir_a.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();
    harness::gmeta(dir_b.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();

    harness::gmeta(dir_a.path())
        .args(["pull"])
        .assert()
        .success();
    harness::gmeta(dir_b.path())
        .args(["pull"])
        .assert()
        .success();

    let target_a = commit_target(&sha_a);
    harness::gmeta(dir_a.path())
        .args(["set", &target_a, "from:a", "value-a"])
        .assert()
        .success();
    harness::gmeta(dir_a.path())
        .args(["push"])
        .assert()
        .success();

    let target_b = commit_target(&sha_b);
    harness::gmeta(dir_b.path())
        .args(["set", &target_b, "from:b", "value-b"])
        .assert()
        .success();
    harness::gmeta(dir_b.path())
        .args(["push"])
        .assert()
        .success();

    let bare = git2::Repository::open_bare(bare_dir.path()).unwrap();
    let tip = bare
        .find_reference("refs/meta/main")
        .unwrap()
        .peel_to_commit()
        .unwrap();

    let mut revwalk = bare.revwalk().unwrap();
    revwalk.push(tip.id()).unwrap();
    for oid in revwalk {
        let oid = oid.unwrap();
        let commit = bare.find_commit(oid).unwrap();
        assert!(
            commit.parent_count() <= 1,
            "commit {} has {} parents — merge commits are not allowed in pushed history",
            &commit.id().to_string()[..8],
            commit.parent_count()
        );
    }
}

#[test]
fn pull_simple() {
    let (dir, _sha) = setup_repo();
    let bare_dir = setup_bare_with_meta("meta");
    let bare_path = bare_dir.path().to_str().unwrap();

    harness::gmeta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();

    harness::gmeta(dir.path()).args(["pull"]).assert().success();

    harness::gmeta(dir.path())
        .args(["get", "project", "testing"])
        .assert()
        .success()
        .stdout(predicate::str::contains("hello"));
}

#[test]
fn pull_up_to_date() {
    let (dir, _sha) = setup_repo();
    let bare_dir = setup_bare_with_meta("meta");
    let bare_path = bare_dir.path().to_str().unwrap();

    harness::gmeta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();

    harness::gmeta(dir.path()).args(["pull"]).assert().success();

    harness::gmeta(dir.path())
        .args(["pull"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Already up-to-date"));
}

#[test]
fn pull_merges_with_local_data() {
    let (dir, sha) = setup_repo();
    let bare_dir = setup_bare_with_meta("meta");
    let bare_path = bare_dir.path().to_str().unwrap();

    let target = commit_target(&sha);
    harness::gmeta(dir.path())
        .args(["set", &target, "local:key", "local-value"])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();
    harness::gmeta(dir.path()).args(["pull"]).assert().success();

    harness::gmeta(dir.path())
        .args(["get", &target, "local:key"])
        .assert()
        .success()
        .stdout(predicate::str::contains("local-value"));

    harness::gmeta(dir.path())
        .args(["get", "project", "testing"])
        .assert()
        .success()
        .stdout(predicate::str::contains("hello"));
}
