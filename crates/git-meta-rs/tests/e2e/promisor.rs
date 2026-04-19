use gix::prelude::ObjectIdExt;
use predicates::prelude::*;

use crate::harness::{
    self, open_repo, ref_to_commit_oid, setup_bare_with_history, setup_bare_with_history_retained,
    setup_bare_with_meta, setup_repo,
};

#[test]
fn pull_inserts_promisor_entries() {
    let (dir, _sha) = setup_repo();
    let bare_dir = setup_bare_with_history();
    let bare_path = bare_dir.path().to_str().unwrap();

    harness::git_meta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success()
        .stderr(predicate::str::contains("Indexed 1 keys from history"));

    harness::git_meta(dir.path())
        .args(["get", "project", "testing"])
        .assert()
        .success()
        .stdout(predicate::str::contains("hello"));

    // The historical key was pruned from the tip tree; get should not crash.
    harness::git_meta(dir.path())
        .args(["get", "project", "old_key"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["get", "project", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("testing"))
        .stdout(predicate::str::contains("hello"));
}

#[test]
fn promisor_hydration_from_tip_tree() {
    let (dir, _sha) = setup_repo();
    let bare_dir = setup_bare_with_history_retained();
    let bare_path = bare_dir.path().to_str().unwrap();

    harness::git_meta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();
    harness::git_meta(dir.path())
        .args(["pull"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["get", "project", "old_key"])
        .assert()
        .success()
        .stdout(predicate::str::contains("old_value"));

    harness::git_meta(dir.path())
        .args(["get", "project", "testing"])
        .assert()
        .success()
        .stdout(predicate::str::contains("hello"));
}

#[test]
fn promisor_entry_not_serialized() {
    let (dir, _sha) = setup_repo();
    let bare_dir = setup_bare_with_history();
    let bare_path = bare_dir.path().to_str().unwrap();

    harness::git_meta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();
    harness::git_meta(dir.path())
        .args(["pull"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["serialize"])
        .assert()
        .success();

    let repo = open_repo(dir.path());
    let commit_oid = ref_to_commit_oid(&repo, "refs/meta/local/main");
    let commit_obj = commit_oid.attach(&repo).object().unwrap().into_commit();
    let tree = commit_obj.tree().unwrap();

    let project_entry = tree.find_entry("project").unwrap();
    let project_tree = project_entry.object().unwrap().into_tree();

    assert!(
        project_tree.find_entry("testing").is_some(),
        "tip key 'testing' should be in serialized tree"
    );
    assert!(
        project_tree.find_entry("old_key").is_none(),
        "promised key 'old_key' should NOT be in serialized tree"
    );
}

#[test]
fn pull_tip_only_no_promisor_entries() {
    let (dir, _sha) = setup_repo();
    let bare_dir = setup_bare_with_meta("meta");
    let bare_path = bare_dir.path().to_str().unwrap();

    harness::git_meta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();
    harness::git_meta(dir.path())
        .args(["pull"])
        .assert()
        .success()
        .stderr(predicate::str::contains("Indexed").not());
}
