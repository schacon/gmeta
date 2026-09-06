use git_meta_lib::types::{Target, TargetType};
use gix::prelude::ObjectIdExt;
use gix::refs::transaction::PreviousValue;
use predicates::prelude::*;
use rusqlite::params;
use tempfile::TempDir;

use crate::harness::{
    self, open_repo, ref_to_commit_oid, setup_bare_with_history, setup_bare_with_history_retained,
    setup_bare_with_meta, setup_bare_with_omitted_history, setup_repo,
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
        .stderr(predicate::str::contains(
            "Indexing history in the background",
        ));

    // Indexing is backgrounded, so wait for it rather than racing it.
    wait_for_index(dir.path());
    assert_eq!(count_promised(dir.path()), 1);

    harness::git_meta(dir.path())
        .args(["get", "project", "testing"])
        .assert()
        .success()
        .stdout(predicate::str::contains("hello"));

    // `old_key` was written in an earlier commit and is no longer in the tip
    // tree — which is exactly what a promisor is for. Indexing recorded the
    // commit that last wrote it, so fetching it is a lookup in that tree.
    harness::git_meta(dir.path())
        .args(["get", "project", "old_key"])
        .assert()
        .success()
        .stdout(predicate::str::contains("old_value"));

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
fn blame_hydrates_promised_commit_and_branch_metadata() {
    let (dir, _sha) = setup_repo();
    std::fs::write(dir.path().join("file.txt"), "first\n").unwrap();
    git(dir.path(), &["add", "file.txt"]);
    git(dir.path(), &["commit", "-m", "add file"]);
    let blamed_commit = git(dir.path(), &["rev-parse", "HEAD"]);

    let bare_dir = setup_bare_with_promised_blame_metadata(blamed_commit.trim());
    let bare_path = bare_dir.path().to_str().unwrap();
    harness::git_meta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();
    mark_blame_metadata_promised(dir.path(), blamed_commit.trim());

    harness::git_meta(dir.path())
        .args(["blame", "--json", "file.txt"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"branch_id\": \"feature#1\""))
        .stdout(predicate::str::contains("\"number\": \"1\""))
        .stdout(predicate::str::contains(
            "\"title\": \"Hydrated blame metadata\"",
        ));
}

#[test]
fn pull_indexes_omitted_change_commit_tree() {
    let (dir, _sha) = setup_repo();
    let bare_dir = setup_bare_with_omitted_history();
    let bare_path = bare_dir.path().to_str().unwrap();

    harness::git_meta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success()
        .stderr(predicate::str::contains(
            "Indexing history in the background",
        ));

    wait_for_index(dir.path());
    assert_eq!(count_promised(dir.path()), 2);

    harness::git_meta(dir.path())
        .args(["inspect", "--promisor", "project"])
        .assert()
        .success()
        .stdout(predicate::str::contains("old_key"))
        .stdout(predicate::str::contains("omitted_key"));
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
        // Serialized trees hold the raw string, not a JSON-encoded one.
        let blob = repo
            .write_blob(value.as_bytes())
            .expect("should write metadata blob")
            .detach();
        let path = git_meta_lib::tree_paths::tree_path(target, key).unwrap();
        editor
            .upsert(path, gix::objs::tree::EntryKind::Blob, blob)
            .expect("should insert metadata entry");
    }
    if include_tip_key {
        let blob = repo
            .write_blob(b"hello")
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

/// A promised entry records that a key exists somewhere in history. Failing to
/// locate its value must not discard that record: the commit holding it may
/// simply not have been fetched yet, and forgetting the key loses the only
/// evidence it ever existed.
#[test]
fn a_value_that_cannot_be_located_keeps_its_promisor_entry() {
    let (dir, _sha) = setup_repo();
    let bare_dir = setup_bare_with_history();
    let bare_path = bare_dir.path().to_str().unwrap();

    harness::git_meta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();

    // Promise a key that was never published anywhere.
    let db_path = dir.path().join(".git/git-meta.sqlite");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute(
        "INSERT INTO metadata
            (target_type, target_value, key, value, value_type, last_timestamp,
             is_git_ref, is_promised, promised_commit)
         VALUES ('project', '', 'never:published', '', 'string', 0, 0, 1, NULL)",
        params![],
    )
    .unwrap();
    drop(conn);

    harness::git_meta(dir.path())
        .args(["get", "project", "never:published"])
        .assert()
        .success();

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    let still_promised: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM metadata
             WHERE key = 'never:published' AND is_promised = 1",
            params![],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        still_promised, 1,
        "the promisor entry was discarded when its value could not be found"
    );
}

/// Indexing a long history is minutes of work, so it must survive being
/// interrupted: the next `git meta` command picks it up from the checkpoint
/// rather than walking the whole history again.
#[test]
fn interrupted_indexing_resumes_from_its_checkpoint() {
    let (dir, _sha) = setup_repo();
    let bare_dir = setup_bare_with_history();
    let bare_path = bare_dir.path().to_str().unwrap();

    harness::git_meta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();

    wait_for_index(dir.path());

    let state_path = dir.path().join(".git/git-meta-index.json");
    let complete: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&state_path).unwrap()).unwrap();
    assert_eq!(
        complete["complete"], true,
        "indexing should have finished: {complete}"
    );
    let indexed_keys: i64 = count_promised(dir.path());
    assert!(indexed_keys > 0, "nothing was indexed");

    // Simulate a kill mid-walk: an unfinished checkpoint whose heartbeat is old
    // enough that the process that wrote it is presumed gone.
    let db_path = dir.path().join(".git/git-meta.sqlite");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute("DELETE FROM metadata WHERE is_promised = 1", params![])
        .unwrap();
    drop(conn);
    assert_eq!(count_promised(dir.path()), 0);

    let mut interrupted = complete;
    interrupted["complete"] = serde_json::Value::Bool(false);
    interrupted["resume_from"] = serde_json::Value::Null;
    interrupted["commits_indexed"] = serde_json::json!(0);
    interrupted["keys_indexed"] = serde_json::json!(0);
    interrupted["heartbeat_ms"] = serde_json::json!(0);
    std::fs::write(&state_path, interrupted.to_string()).unwrap();

    // Any command should notice the unfinished work and restart it.
    harness::git_meta(dir.path())
        .args(["index-history"])
        .assert()
        .success();

    let resumed: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&state_path).unwrap()).unwrap();
    assert_eq!(resumed["complete"], true, "resumed pass did not finish");
    assert_eq!(
        count_promised(dir.path()),
        indexed_keys,
        "resuming did not restore the same index"
    );
}

/// A checkpoint whose heartbeat is fresh means another process is working; a
/// second indexer must not duplicate the effort.
#[test]
fn indexing_does_not_start_while_another_indexer_is_live() {
    let (dir, _sha) = setup_repo();
    let bare_dir = setup_bare_with_history();
    let bare_path = bare_dir.path().to_str().unwrap();

    harness::git_meta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();
    wait_for_index(dir.path());

    let state_path = dir.path().join(".git/git-meta-index.json");
    let mut state: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&state_path).unwrap()).unwrap();
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    state["complete"] = serde_json::Value::Bool(false);
    state["heartbeat_ms"] = serde_json::json!(now_ms);
    state["pid"] = serde_json::json!(999_999);
    std::fs::write(&state_path, state.to_string()).unwrap();

    harness::git_meta(dir.path())
        .args(["index-history"])
        .assert()
        .success()
        .stderr(predicate::str::contains(
            "Another indexer is already running",
        ));
}

/// Block until background indexing reports itself finished.
///
/// Indexing is spawned detached, so a test that reads its results has to wait
/// for it rather than assume it has run.
fn wait_for_index(dir: &std::path::Path) {
    let state_path = dir.join(".git/git-meta-index.json");
    for _ in 0..200 {
        if let Ok(contents) = std::fs::read_to_string(&state_path) {
            if let Ok(state) = serde_json::from_str::<serde_json::Value>(&contents) {
                if state["complete"] == true {
                    return;
                }
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    panic!("background indexing did not finish within 10s");
}

/// Promisor entries currently recorded.
fn count_promised(dir: &std::path::Path) -> i64 {
    let conn = rusqlite::Connection::open(dir.join(".git/git-meta.sqlite")).unwrap();
    conn.query_row(
        "SELECT COUNT(*) FROM metadata WHERE is_promised = 1",
        params![],
        |row| row.get(0),
    )
    .unwrap()
}
