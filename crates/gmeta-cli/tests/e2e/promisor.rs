use predicates::prelude::*;

use crate::harness::{
    self, setup_bare_with_history, setup_bare_with_history_retained, setup_bare_with_meta,
    setup_repo,
};

#[test]
fn pull_inserts_promisor_entries() {
    let (dir, _sha) = setup_repo();
    let bare_dir = setup_bare_with_history();
    let bare_path = bare_dir.path().to_str().unwrap();

    harness::gmeta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success()
        .stderr(predicate::str::contains("Indexed 1 keys from history"));

    harness::gmeta(dir.path())
        .args(["get", "project", "testing"])
        .assert()
        .success()
        .stdout(predicate::str::contains("hello"));

    // The historical key was pruned from the tip tree; get should not crash.
    harness::gmeta(dir.path())
        .args(["get", "project", "old_key"])
        .assert()
        .success();

    harness::gmeta(dir.path())
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

    harness::gmeta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();
    harness::gmeta(dir.path()).args(["pull"]).assert().success();

    harness::gmeta(dir.path())
        .args(["get", "project", "old_key"])
        .assert()
        .success()
        .stdout(predicate::str::contains("old_value"));

    harness::gmeta(dir.path())
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

    harness::gmeta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();
    harness::gmeta(dir.path()).args(["pull"]).assert().success();

    harness::gmeta(dir.path())
        .args(["serialize"])
        .assert()
        .success();

    let repo = git2::Repository::open(dir.path()).unwrap();
    let local_ref = repo.find_reference("refs/meta/local/main").unwrap();
    let commit = local_ref.peel_to_commit().unwrap();
    let tree = commit.tree().unwrap();

    let project_entry = tree.get_name("project").unwrap();
    let project_tree = repo.find_tree(project_entry.id()).unwrap();

    assert!(
        project_tree.get_name("testing").is_some(),
        "tip key 'testing' should be in serialized tree"
    );
    assert!(
        project_tree.get_name("old_key").is_none(),
        "promised key 'old_key' should NOT be in serialized tree"
    );
}

#[test]
fn pull_tip_only_no_promisor_entries() {
    let (dir, _sha) = setup_repo();
    let bare_dir = setup_bare_with_meta("meta");
    let bare_path = bare_dir.path().to_str().unwrap();

    harness::gmeta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();
    harness::gmeta(dir.path())
        .args(["pull"])
        .assert()
        .success()
        .stderr(predicate::str::contains("Indexed").not());
}
