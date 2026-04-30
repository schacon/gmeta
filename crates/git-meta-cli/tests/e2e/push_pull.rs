use gix::bstr::ByteSlice;
use gix::prelude::ObjectIdExt;
use predicates::prelude::*;

use crate::harness::{
    self, commit_target, open_repo, ref_to_commit_oid, setup_bare_with_meta, setup_repo,
    target_fanout,
};

#[test]
fn push_simple() {
    let (dir, sha) = setup_repo();
    let bare_dir = setup_bare_with_meta("meta");
    let bare_path = bare_dir.path().to_str().unwrap();

    harness::git_meta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();
    harness::git_meta(dir.path())
        .args(["pull"])
        .assert()
        .success();

    let target = commit_target(&sha);
    harness::git_meta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["push"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Pushed metadata to meta"))
        .stderr(predicate::str::contains(
            "checking local and remote metadata refs",
        ))
        .stderr(predicate::str::contains("serializing local metadata"))
        .stderr(predicate::str::contains(
            "pushing refs/meta/local/main to meta:refs/meta/main",
        ));

    let bare = open_repo(bare_dir.path());
    let tip_oid = ref_to_commit_oid(&bare, "refs/meta/main");
    let commit = tip_oid.attach(&bare).object().unwrap().into_commit();
    assert_eq!(
        commit.parent_ids().count(),
        1,
        "pushed commit should have exactly 1 parent (no merge commits)"
    );
}

#[test]
fn push_set_add() {
    let (dir, sha) = setup_repo();
    let bare_dir = setup_bare_with_meta("meta");
    let bare_path = bare_dir.path().to_str().unwrap();

    harness::git_meta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();
    harness::git_meta(dir.path())
        .args(["pull"])
        .assert()
        .success();

    let target = commit_target(&sha);
    harness::git_meta(dir.path())
        .args(["set:add", &target, "reviewer", "alice@example.com"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["push"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Pushed metadata to meta"));
}

#[test]
fn push_up_to_date() {
    let (dir, sha) = setup_repo();
    let bare_dir = setup_bare_with_meta("meta");
    let bare_path = bare_dir.path().to_str().unwrap();

    harness::git_meta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();
    harness::git_meta(dir.path())
        .args(["pull"])
        .assert()
        .success();

    let target = commit_target(&sha);
    harness::git_meta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["push"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["push"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Everything up-to-date"));
}

#[test]
fn push_preserves_pre_serialized_force_full_tree() {
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
        .success();

    harness::git_meta(dir.path())
        .args([
            "set",
            "--timestamp",
            "1000",
            "branch:legacy",
            "historical:key",
            "beta",
        ])
        .assert()
        .success();
    harness::git_meta(dir.path())
        .args(["config", "meta:prune:since", "14d"])
        .assert()
        .success();
    harness::git_meta(dir.path())
        .args(["serialize", "--force-full"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["push"])
        .assert()
        .success();

    let bare = open_repo(bare_dir.path());
    let tip_oid = ref_to_commit_oid(&bare, "refs/meta/main");
    let commit = tip_oid.attach(&bare).object().unwrap().into_commit();
    let tree = commit.tree().unwrap();
    let fanout = target_fanout("legacy");
    let expected_path = format!("branch/{fanout}/legacy/historical/key/__value");
    let mut paths = Vec::new();
    collect_tree_paths(&bare, tree.id, "", &mut paths);
    assert!(
        paths.iter().any(|path| path == &expected_path),
        "push should preserve the force-full serialized historical key"
    );
}

#[test]
fn push_commit_message_format() {
    let (dir, sha) = setup_repo();
    let bare_dir = setup_bare_with_meta("meta");
    let bare_path = bare_dir.path().to_str().unwrap();

    harness::git_meta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();
    harness::git_meta(dir.path())
        .args(["pull"])
        .assert()
        .success();

    let target = commit_target(&sha);
    harness::git_meta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();
    harness::git_meta(dir.path())
        .args(["set", &target, "agent:cost", "0.05"])
        .assert()
        .success();
    harness::git_meta(dir.path())
        .args(["push"])
        .assert()
        .success();

    let bare = open_repo(bare_dir.path());
    let tip_oid = ref_to_commit_oid(&bare, "refs/meta/main");
    let commit = tip_oid.attach(&bare).object().unwrap().into_commit();
    let msg = commit.message_raw_sloppy().to_string();
    assert!(
        msg.contains("git-meta: serialize"),
        "commit message should contain 'git-meta: serialize', got: {msg}"
    );
    assert!(
        msg.contains("agent:model"),
        "commit message should contain changed key, got: {msg}"
    );
}

#[test]
fn push_conflict_produces_no_merge_commits() {
    let bare_dir = setup_bare_with_meta("meta");
    let bare_path = bare_dir.path().to_str().unwrap();

    let (dir_a, sha_a) = setup_repo();
    let (dir_b, sha_b) = setup_repo();

    harness::git_meta(dir_a.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();
    harness::git_meta(dir_b.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();

    harness::git_meta(dir_a.path())
        .args(["pull"])
        .assert()
        .success();
    harness::git_meta(dir_b.path())
        .args(["pull"])
        .assert()
        .success();

    let target_a = commit_target(&sha_a);
    harness::git_meta(dir_a.path())
        .args(["set", &target_a, "from:a", "value-a"])
        .assert()
        .success();
    harness::git_meta(dir_a.path())
        .args(["push"])
        .assert()
        .success();

    let target_b = commit_target(&sha_b);
    harness::git_meta(dir_b.path())
        .args(["set", &target_b, "from:b", "value-b"])
        .assert()
        .success();
    harness::git_meta(dir_b.path())
        .args(["push"])
        .assert()
        .success();

    let bare = open_repo(bare_dir.path());
    let tip_oid = ref_to_commit_oid(&bare, "refs/meta/main");

    let walk = bare.rev_walk(Some(tip_oid));
    let iter = walk.all().unwrap();
    for info_result in iter {
        let info = info_result.unwrap();
        let oid = info.id;
        let commit = oid.attach(&bare).object().unwrap().into_commit();
        let parent_count = commit.parent_ids().count();
        assert!(
            parent_count <= 1,
            "commit {} has {} parents — merge commits are not allowed in pushed history",
            &oid.to_string()[..8],
            parent_count
        );
    }
}

fn collect_tree_paths(
    repo: &gix::Repository,
    tree_id: gix::ObjectId,
    prefix: &str,
    paths: &mut Vec<String>,
) {
    let tree = tree_id.attach(repo).object().unwrap().into_tree();
    for entry_result in tree.iter() {
        let entry = entry_result.unwrap();
        let name = entry.filename().to_str().unwrap();
        let path = if prefix.is_empty() {
            name.to_string()
        } else {
            format!("{prefix}/{name}")
        };
        if entry.mode().is_tree() {
            collect_tree_paths(repo, entry.object_id(), &path, paths);
        } else {
            paths.push(path);
        }
    }
}

#[test]
fn pull_simple() {
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
        .success();

    harness::git_meta(dir.path())
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

    harness::git_meta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["pull"])
        .assert()
        .success();

    harness::git_meta(dir.path())
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
    harness::git_meta(dir.path())
        .args(["set", &target, "local:key", "local-value"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();
    harness::git_meta(dir.path())
        .args(["pull"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["get", &target, "local:key"])
        .assert()
        .success()
        .stdout(predicate::str::contains("local-value"));

    harness::git_meta(dir.path())
        .args(["get", "project", "testing"])
        .assert()
        .success()
        .stdout(predicate::str::contains("hello"));
}

#[test]
fn sync_pulls_then_pushes() {
    let (dir, sha) = setup_repo();
    let bare_dir = setup_bare_with_meta("meta");
    let bare_path = bare_dir.path().to_str().unwrap();

    let target = commit_target(&sha);
    harness::git_meta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["sync"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Already up-to-date from meta."))
        .stdout(predicate::str::contains("Pushed metadata to meta"));

    harness::git_meta(dir.path())
        .args(["get", "project", "testing"])
        .assert()
        .success()
        .stdout(predicate::str::contains("hello"));

    let (reader_dir, _reader_sha) = setup_repo();
    harness::git_meta(reader_dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();
    harness::git_meta(reader_dir.path())
        .args(["pull"])
        .assert()
        .success();
    harness::git_meta(reader_dir.path())
        .args(["get", &target, "agent:model"])
        .assert()
        .success()
        .stdout(predicate::str::contains("claude-4.6"));
}

#[test]
fn sync_merges_remote_and_local_metadata() {
    let bare_dir = setup_bare_with_meta("meta");
    let bare_path = bare_dir.path().to_str().unwrap();

    let (dir_a, sha_a) = setup_repo();
    let (dir_b, sha_b) = setup_repo();

    harness::git_meta(dir_a.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();
    harness::git_meta(dir_b.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();

    let target_a = commit_target(&sha_a);
    harness::git_meta(dir_a.path())
        .args(["set", &target_a, "from:a", "value-a"])
        .assert()
        .success();
    harness::git_meta(dir_a.path())
        .args(["sync"])
        .assert()
        .success();

    let target_b = commit_target(&sha_b);
    harness::git_meta(dir_b.path())
        .args(["set", &target_b, "from:b", "value-b"])
        .assert()
        .success();
    harness::git_meta(dir_b.path())
        .args(["sync"])
        .assert()
        .success();

    harness::git_meta(dir_a.path())
        .args(["sync"])
        .assert()
        .success();

    harness::git_meta(dir_a.path())
        .args(["get", &target_b, "from:b"])
        .assert()
        .success()
        .stdout(predicate::str::contains("value-b"));
    harness::git_meta(dir_b.path())
        .args(["get", &target_a, "from:a"])
        .assert()
        .success()
        .stdout(predicate::str::contains("value-a"));

    let bare = open_repo(bare_dir.path());
    let tip_oid = ref_to_commit_oid(&bare, "refs/meta/main");
    let walk = bare.rev_walk(Some(tip_oid));
    let iter = walk.all().unwrap();
    for info_result in iter {
        let info = info_result.unwrap();
        let oid = info.id;
        let commit = oid.attach(&bare).object().unwrap().into_commit();
        let parent_count = commit.parent_ids().count();
        assert!(
            parent_count <= 1,
            "commit {} has {} parents, merge commits are not allowed in synced history",
            &oid.to_string()[..8],
            parent_count
        );
    }
}
