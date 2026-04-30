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

fn setup_bare_with_promised_blame_metadata(commit_sha: &str) -> TempDir {
    let bare_dir = TempDir::new().expect("should be able to create temp dir");
    let _init = gix::init_bare(bare_dir.path()).expect("should be able to init bare repo");
    let bare = gix::open_opts(bare_dir.path(), harness::test_open_opts())
        .expect("should reopen bare repo");
    let sig = gix::actor::Signature {
        name: "Test User".into(),
        email: "test@example.com".into(),
        time: gix::date::Time::new(946684800, 0),
    };

    let branch_id = "feature#1";
    let entries = [
        (
            Target::from_parts(TargetType::Commit, Some(commit_sha.to_string())),
            "branch-id",
            branch_id,
        ),
        (
            Target::branch(branch_id),
            "title",
            "Hydrated blame metadata",
        ),
        (Target::branch(branch_id), "review:number", "1"),
    ];
    let tree1 = write_meta_tree(&bare, &entries, false);
    let commit1 = gix::objs::Commit {
        message: format!(
            "git-meta: serialize (3 changes)\n\n\
             A\tcommit:{commit_sha}\tbranch-id\n\
             A\tbranch:{branch_id}\ttitle\n\
             A\tbranch:{branch_id}\treview:number"
        )
        .into(),
        tree: tree1,
        author: sig.clone(),
        committer: sig.clone(),
        encoding: None,
        parents: Default::default(),
        extra_headers: Default::default(),
    };
    let commit1_oid = bare
        .write_object(&commit1)
        .expect("should create metadata commit")
        .detach();

    let tree2 = write_meta_tree(&bare, &entries, true);
    let commit2 = gix::objs::Commit {
        message: "git-meta: serialize (1 changes)\n\nA\tproject\ttesting".into(),
        tree: tree2,
        author: sig.clone(),
        committer: sig,
        encoding: None,
        parents: vec![commit1_oid].into(),
        extra_headers: Default::default(),
    };
    let commit2_oid = bare
        .write_object(&commit2)
        .expect("should create tip metadata commit")
        .detach();
    bare.reference(
        "refs/meta/main",
        commit2_oid,
        PreviousValue::Any,
        "metadata tip",
    )
    .expect("should create metadata ref");

    bare_dir
}

fn write_meta_tree(
    repo: &gix::Repository,
    entries: &[(Target, &str, &str)],
    include_tip_key: bool,
) -> gix::ObjectId {
    let mut editor = repo.empty_tree().edit().expect("should create tree editor");
    for (target, key, value) in entries {
        let blob = repo
            .write_blob(serde_json::to_string(value).unwrap().as_bytes())
            .expect("should write metadata blob")
            .detach();
        let path = git_meta_lib::tree_paths::tree_path(target, key).unwrap();
        editor
            .upsert(path, gix::objs::tree::EntryKind::Blob, blob)
            .expect("should insert metadata entry");
    }
    if include_tip_key {
        let blob = repo
            .write_blob(b"\"hello\"")
            .expect("should write tip blob")
            .detach();
        editor
            .upsert(
                "project/testing/__value",
                gix::objs::tree::EntryKind::Blob,
                blob,
            )
            .expect("should insert tip entry");
    }
    editor.write().expect("should write metadata tree").detach()
}

fn mark_blame_metadata_promised(repo_path: &std::path::Path, commit_sha: &str) {
    let db_path = repo_path.join(".git/git-meta.sqlite");
    let conn = rusqlite::Connection::open(db_path).unwrap();
    for (target_type, target_value, key) in [
        ("commit", commit_sha, "branch-id"),
        ("branch", "feature#1", "title"),
        ("branch", "feature#1", "review:number"),
    ] {
        conn.execute(
            "UPDATE metadata
             SET value = '', value_type = 'string', is_git_ref = 0, is_promised = 1
             WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
            params![target_type, target_value, key],
        )
        .unwrap();
    }
}

fn git(repo: &std::path::Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .args(["-C", &repo.to_string_lossy()])
        .args(args)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env(
            "GIT_CONFIG_GLOBAL",
            if cfg!(windows) { "NUL" } else { "/dev/null" },
        )
        .env("GIT_AUTHOR_DATE", "2000-01-01 00:00:00 +0000")
        .env("GIT_AUTHOR_EMAIL", "test@example.com")
        .env("GIT_AUTHOR_NAME", "Test User")
        .env("GIT_COMMITTER_DATE", "2000-01-02 00:00:00 +0000")
        .env("GIT_COMMITTER_EMAIL", "test@example.com")
        .env("GIT_COMMITTER_NAME", "Test User")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}
