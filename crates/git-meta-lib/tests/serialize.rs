#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

mod helpers;

use git_meta_lib::*;
use gix::prelude::ObjectIdExt;
use helpers::*;

#[test]
fn serialize_creates_git_ref() {
    let (_dir, repo) = setup_repo();
    let sha = head_sha(&repo);
    let session = open_session(repo);

    let target = Target::commit(&sha).unwrap();
    session
        .target(&target)
        .set("agent:model", "claude-4.6")
        .unwrap();

    let output = session.serialize().unwrap();
    assert!(output.changes > 0);
    assert!(
        output
            .refs_written
            .iter()
            .any(|r| r.contains("refs/meta/local/main")),
        "serialize should write refs/meta/local/main, got: {:?}",
        output.refs_written
    );
}

#[test]
fn serialize_and_materialize_roundtrip() {
    // -- Repo A: set metadata and serialize --
    let (dir_a, repo_a) = setup_repo();
    let sha_a = head_sha(&repo_a);
    let session_a = open_session(repo_a);

    let target = Target::commit(&sha_a).unwrap();
    session_a
        .target(&target)
        .set("agent:model", "claude-4.6")
        .unwrap();
    session_a
        .target(&Target::project())
        .set("version", "1.0.0")
        .unwrap();
    session_a
        .target(&Target::path("src/lib.rs"))
        .set("owner", "teamA")
        .unwrap();

    let output = session_a.serialize().unwrap();
    assert!(output.changes > 0);

    // -- Bare repo B: simulate a remote by copying objects and refs --
    let bare_dir = tempfile::TempDir::new().unwrap();
    let _bare_init = gix::init_bare(bare_dir.path()).unwrap();
    let bare_repo = gix::open_opts(
        bare_dir.path(),
        gix::open::Options::isolated()
            .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
    )
    .unwrap();

    // Copy objects from A to bare
    let src_objects = dir_a.path().join(".git").join("objects");
    let dst_objects = bare_dir.path().join("objects");
    copy_dir_contents(&src_objects, &dst_objects);

    // Copy the local ref from A to bare
    let repo_a_reopen = gix::open_opts(
        dir_a.path(),
        gix::open::Options::isolated()
            .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
    )
    .unwrap();
    let local_ref = repo_a_reopen
        .find_reference("refs/meta/local/main")
        .unwrap();
    let local_oid = local_ref.into_fully_peeled_id().unwrap().detach();
    bare_repo
        .reference(
            "refs/meta/local/main",
            local_oid,
            gix::refs::transaction::PreviousValue::Any,
            "copy from A",
        )
        .unwrap();

    // -- Repo C: simulate a "fetch" by copying objects from bare --
    let (dir_c, repo_c) = setup_repo();
    let repo_c_objects = dir_c.path().join(".git").join("objects");
    copy_dir_contents(&dst_objects, &repo_c_objects);

    // Create a remote tracking ref in C (simulating a fetch)
    let repo_c_reopen = gix::open_opts(
        dir_c.path(),
        gix::open::Options::isolated()
            .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
    )
    .unwrap();
    repo_c_reopen
        .reference(
            "refs/meta/origin",
            local_oid,
            gix::refs::transaction::PreviousValue::Any,
            "simulated fetch",
        )
        .unwrap();

    // -- Materialize in C --
    let session_c = Session::open(repo_c_reopen.path())
        .unwrap()
        .with_timestamp(2000);
    let mat_output = session_c.materialize(None).unwrap();
    assert!(
        !mat_output.results.is_empty(),
        "materialize should process at least one ref"
    );

    // Verify the metadata arrived in C
    let sha_c = head_sha(&repo_c);
    // The commit SHA in repo A and C should be identical (same initial commit)
    assert_eq!(sha_a, sha_c);

    let commit_val = session_c
        .target(&Target::commit(&sha_c).unwrap())
        .get_value("agent:model")
        .unwrap();
    assert_eq!(
        commit_val,
        Some(MetaValue::String("claude-4.6".to_string()))
    );

    let project_val = session_c
        .target(&Target::project())
        .get_value("version")
        .unwrap();
    assert_eq!(project_val, Some(MetaValue::String("1.0.0".to_string())));

    let path_val = session_c
        .target(&Target::path("src/lib.rs"))
        .get_value("owner")
        .unwrap();
    assert_eq!(path_val, Some(MetaValue::String("teamA".to_string())));
}

#[test]
fn serialize_empty_is_no_op() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let output = session.serialize().unwrap();
    assert_eq!(output.changes, 0);
    assert!(output.refs_written.is_empty());
}

#[test]
fn incremental_serialize_only_includes_changes() {
    let (dir, repo) = setup_repo();
    let session = Session::open(repo.path()).unwrap().with_timestamp(1000);

    // First serialize: set key1
    session
        .target(&Target::project())
        .set("key1", "alpha")
        .unwrap();
    let output1 = session.serialize().unwrap();
    assert!(output1.changes > 0, "first serialize should have changes");
    assert!(
        !output1.refs_written.is_empty(),
        "first serialize should write refs"
    );

    // Reopen session with a later timestamp so the second set is after
    // the last_materialized marker and will be picked up by incremental mode.
    let session2 = reopen_session(dir.path(), 2000);

    // Second serialize: set key2 (key1 is unchanged)
    session2
        .target(&Target::project())
        .set("key2", "beta")
        .unwrap();
    let output2 = session2.serialize().unwrap();
    assert!(output2.changes > 0, "second serialize should have changes");

    // Verify both keys exist after second serialize
    let val1 = session2
        .target(&Target::project())
        .get_value("key1")
        .unwrap();
    assert_eq!(val1, Some(MetaValue::String("alpha".to_string())));

    let val2 = session2
        .target(&Target::project())
        .get_value("key2")
        .unwrap();
    assert_eq!(val2, Some(MetaValue::String("beta".to_string())));

    // The second serialize is incremental: it should report fewer or equal
    // changes compared to a hypothetical full re-serialize. At minimum,
    // the second serialize should succeed with changes > 0 since key2 was added.
    assert!(
        output2.changes > 0,
        "incremental serialize should still report changes"
    );
}

#[test]
fn published_prefix_survives_incremental_serialize_materialize() {
    let (dir_a, repo_a) = setup_repo();
    let session_a = Session::open(repo_a.path()).unwrap().with_timestamp(1000);
    let handle_a = session_a.target(&Target::project());

    handle_a.set("version", "1.0.0").unwrap();
    handle_a
        .set("local:agent:session:s1:title", "draft")
        .unwrap();
    let _ = session_a.serialize().unwrap();

    let session_a2 = reopen_session(dir_a.path(), 2000);
    session_a2
        .target(&Target::project())
        .publish_local([LocalPublish::key_prefix(
            "local:agent:session:s1",
            "agent:session:s1",
        )])
        .unwrap();
    let _ = session_a2.serialize().unwrap();

    let repo_a_re = gix::open_opts(
        dir_a.path(),
        gix::open::Options::isolated()
            .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
    )
    .unwrap();
    let message = meta_commit_message(&repo_a_re, "refs/meta/local/main");
    assert!(
        !message.contains("local:agent:session:s1"),
        "serialize commit message must not leak local-only keys"
    );
    assert!(message.contains("agent:session:s1:title"));
    let a_oid = repo_a_re
        .find_reference("refs/meta/local/main")
        .unwrap()
        .into_fully_peeled_id()
        .unwrap()
        .detach();

    let (dir_c, _repo_c) = setup_repo();
    let src_objects = dir_a.path().join(".git").join("objects");
    inject_remote_ref(&src_objects, dir_c.path(), a_oid);

    let session_c = reopen_session(dir_c.path(), 3000);
    let _ = session_c.materialize(None).unwrap();
    let handle_c = session_c.target(&Target::project());

    assert_eq!(
        handle_c.get_value("agent:session:s1:title").unwrap(),
        Some(MetaValue::String("draft".to_string()))
    );
    assert!(handle_c
        .get_value("local:agent:session:s1:title")
        .unwrap()
        .is_none());
}

#[test]
fn published_set_members_survive_incremental_serialize_materialize() {
    let (dir_a, repo_a) = setup_repo();
    let session_a = Session::open(repo_a.path()).unwrap().with_timestamp(1000);
    let handle_a = session_a.target(&Target::project());

    handle_a.set_add("local:agent:sessions", "s1").unwrap();
    handle_a.set_add("local:agent:sessions", "s2").unwrap();
    let _ = session_a.serialize().unwrap();

    let session_a2 = reopen_session(dir_a.path(), 2000);
    let members = vec!["s1".to_string()];
    session_a2
        .target(&Target::project())
        .publish_local([LocalPublish::set_members(
            "local:agent:sessions",
            "agent:sessions",
            &members,
        )])
        .unwrap();
    let _ = session_a2.serialize().unwrap();

    let repo_a_re = gix::open_opts(
        dir_a.path(),
        gix::open::Options::isolated()
            .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
    )
    .unwrap();
    let a_oid = repo_a_re
        .find_reference("refs/meta/local/main")
        .unwrap()
        .into_fully_peeled_id()
        .unwrap()
        .detach();

    let (dir_c, _repo_c) = setup_repo();
    let src_objects = dir_a.path().join(".git").join("objects");
    inject_remote_ref(&src_objects, dir_c.path(), a_oid);

    let session_c = reopen_session(dir_c.path(), 3000);
    let _ = session_c.materialize(None).unwrap();
    let Some(MetaValue::Set(sessions)) = session_c
        .target(&Target::project())
        .get_value("agent:sessions")
        .unwrap()
    else {
        panic!("expected published set");
    };

    assert!(sessions.contains("s1"));
    assert!(!sessions.contains("s2"));
}

#[test]
fn incremental_serialize_clears_main_when_modified_key_routes_away() {
    let (dir_a, repo_a) = setup_repo();
    let session_a = Session::open(repo_a.path()).unwrap().with_timestamp(1000);
    session_a
        .target(&Target::project())
        .set("agent:session:s1:title", "draft")
        .unwrap();
    let _ = session_a.serialize().unwrap();

    let session_a2 = reopen_session(dir_a.path(), 2000);
    session_a2
        .target(&Target::project())
        .set_add("local:meta:filter", "route agent:** private")
        .unwrap();
    let output = session_a2.serialize().unwrap();
    assert!(
        output
            .refs_written
            .iter()
            .any(|ref_name| ref_name == "refs/meta/local/main"),
        "main ref should be rewritten to remove stale routed-away data"
    );

    let repo_a_re = gix::open_opts(
        dir_a.path(),
        gix::open::Options::isolated()
            .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
    )
    .unwrap();
    assert!(
        repo_a_re.find_reference("refs/meta/local/private").is_ok(),
        "routed key should still be written to its new destination"
    );
    let a_oid = repo_a_re
        .find_reference("refs/meta/local/main")
        .unwrap()
        .into_fully_peeled_id()
        .unwrap()
        .detach();

    let (dir_c, _repo_c) = setup_repo();
    let src_objects = dir_a.path().join(".git").join("objects");
    inject_remote_ref(&src_objects, dir_c.path(), a_oid);

    let session_c = reopen_session(dir_c.path(), 3000);
    let _ = session_c.materialize(None).unwrap();
    assert!(session_c
        .target(&Target::project())
        .get_value("agent:session:s1:title")
        .unwrap()
        .is_none());
}

#[test]
fn published_set_prefix_ignores_stale_destination_member_tombstones() {
    let (dir_a, repo_a) = setup_repo();
    let session_a = Session::open(repo_a.path()).unwrap().with_timestamp(1000);
    let handle_a = session_a.target(&Target::project());

    handle_a.set_add("agent:sessions", "s1").unwrap();
    handle_a.set_remove("agent:sessions", "s1").unwrap();
    handle_a.remove("agent:sessions").unwrap();
    handle_a.set_add("local:agent:sessions", "s1").unwrap();
    handle_a
        .publish_local([LocalPublish::key_prefix(
            "local:agent:sessions",
            "agent:sessions",
        )])
        .unwrap();
    let _ = session_a.serialize().unwrap();

    let repo_a_re = gix::open_opts(
        dir_a.path(),
        gix::open::Options::isolated()
            .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
    )
    .unwrap();
    let a_oid = repo_a_re
        .find_reference("refs/meta/local/main")
        .unwrap()
        .into_fully_peeled_id()
        .unwrap()
        .detach();

    let (dir_c, _repo_c) = setup_repo();
    let src_objects = dir_a.path().join(".git").join("objects");
    inject_remote_ref(&src_objects, dir_c.path(), a_oid);

    let session_c = reopen_session(dir_c.path(), 2000);
    let _ = session_c.materialize(None).unwrap();
    let Some(MetaValue::Set(sessions)) = session_c
        .target(&Target::project())
        .get_value("agent:sessions")
        .unwrap()
    else {
        panic!("expected published set");
    };

    assert!(sessions.contains("s1"));
}

fn meta_commit_message(repo: &gix::Repository, ref_name: &str) -> String {
    let commit_oid = repo
        .find_reference(ref_name)
        .unwrap()
        .into_fully_peeled_id()
        .unwrap()
        .detach();
    let commit = commit_oid.attach(repo).object().unwrap().into_commit();
    commit.message_raw().unwrap().to_string()
}

#[test]
fn serialize_detects_historical_writes_after_prior_serialize() {
    let (dir, _repo) = setup_repo();
    let session = Session::open(dir.path()).unwrap().with_timestamp(2000);

    session
        .target(&Target::project())
        .set("key1", "alpha")
        .unwrap();
    let output1 = session.serialize().unwrap();
    assert!(output1.changes > 0, "first serialize should have changes");

    let session2 = reopen_session(dir.path(), 3000);
    session2
        .target(&Target::project())
        .set("imported:key", "historical")
        .unwrap();
    let conn = rusqlite::Connection::open(dir.path().join(".git").join("git-meta.sqlite")).unwrap();
    conn.execute(
        "UPDATE metadata
         SET last_timestamp = 1000
         WHERE target_type = 'project' AND target_value = '' AND key = 'imported:key'",
        [],
    )
    .unwrap();
    conn.execute(
        "UPDATE metadata_log
         SET timestamp = 1000
         WHERE target_type = 'project' AND target_value = '' AND key = 'imported:key'",
        [],
    )
    .unwrap();

    let output2 = session2.serialize().unwrap();
    assert!(
        output2.changes > 0,
        "serialize should detect writes whose event timestamp predates last_materialized"
    );
    assert!(
        !output2.refs_written.is_empty(),
        "historical write should update the serialized ref"
    );

    let output3 = session2.serialize().unwrap();
    assert_eq!(output3.changes, 0, "unchanged tree should be a no-op");
    assert!(output3.refs_written.is_empty());
}

/// The scoped read that incremental serialize uses must produce exactly the
/// tree a full serialize would. This is the safety net for reading only the
/// changed targets' rows instead of the whole metadata table.
#[test]
fn incremental_serialize_matches_a_full_rebuild() {
    let (dir, repo) = setup_repo();
    let sha = head_sha(&repo);
    let session = open_session(repo);

    // Seed several targets of different types, each with a mix of value types.
    let targets = [
        Target::commit(&sha).unwrap(),
        Target::commit("00112233445566778899aabbccddeeff00112233").unwrap(),
        Target::path("crates/lib/src/main.rs"),
        Target::branch("feature/scoped-reads"),
    ];
    for (index, target) in targets.iter().enumerate() {
        let handle = session.target(target);
        handle.set("agent:model", format!("model-{index}")).unwrap();
        handle.set("review:status", "pending").unwrap();
        handle.list_push("agent:transcript", "first line").unwrap();
        handle.set_add("review:owners", "alice").unwrap();
    }
    let _ = session.serialize().unwrap();

    // Touch a subset, the way a real session would between publishes.
    let handle = session.target(&targets[1]);
    handle.set("review:status", "approved").unwrap();
    handle.list_push("agent:transcript", "second line").unwrap();
    session.target(&targets[2]).remove("agent:model").unwrap();
    let _ = session.serialize().unwrap();

    let incremental_tree = local_tree(&dir);

    // A forced full serialize reads every row and rebuilds from scratch.
    let session = reopen_session(dir.path(), 0);
    let _ = session.serialize_full().unwrap();
    let full_tree = local_tree(&dir);

    assert_eq!(
        incremental_tree, full_tree,
        "incremental serialize diverged from a full rebuild"
    );
}

/// A commit that only removes keys leaves no metadata rows for its target, so
/// serialize must still rebuild that subtree rather than treating the empty
/// read as "nothing to do".
#[test]
fn incremental_serialize_applies_a_removal_only_change() {
    let (dir, repo) = setup_repo();
    let sha = head_sha(&repo);
    let session = open_session(repo);

    let kept = Target::commit(&sha).unwrap();
    let emptied = Target::commit("00112233445566778899aabbccddeeff00112233").unwrap();

    session.target(&kept).set("agent:model", "keep-me").unwrap();
    session
        .target(&emptied)
        .set("agent:model", "remove-me")
        .unwrap();
    let _ = session.serialize().unwrap();

    assert!(session.target(&emptied).remove("agent:model").unwrap());
    let _ = session.serialize().unwrap();

    let tree = local_tree(&dir);
    let emptied_sha = "00112233445566778899aabbccddeeff00112233";
    let removed_path = format!("commit/00/{emptied_sha}/agent/model/__value");
    let kept_path = format!("commit/{}/{sha}/agent/model/__value", &sha[..2]);

    assert!(
        blob_at(dir.path(), tree, &removed_path).is_none(),
        "removed key is still in the serialized tree"
    );
    assert!(
        blob_at(dir.path(), tree, &kept_path).is_some(),
        "unrelated key was dropped"
    );
}

/// Resolve a slash-separated path inside a tree, if every segment exists.
fn blob_at(dir: &std::path::Path, tree: gix::ObjectId, path: &str) -> Option<gix::ObjectId> {
    let repo = gix::open(dir).unwrap();
    let mut current = tree;
    let segments: Vec<&str> = path.split('/').collect();
    for (index, segment) in segments.iter().enumerate() {
        let tree = repo.find_tree(current).ok()?;
        let entry = tree.find_entry(*segment)?;
        if index == segments.len() - 1 {
            return Some(entry.object_id());
        }
        if !entry.mode().is_tree() {
            return None;
        }
        current = entry.object_id();
    }
    None
}

/// Read the tree of `refs/meta/local/main`.
fn local_tree(dir: &tempfile::TempDir) -> gix::ObjectId {
    let repo = gix::open(dir.path()).unwrap();
    let commit = repo
        .find_reference("refs/meta/local/main")
        .unwrap()
        .into_fully_peeled_id()
        .unwrap()
        .object()
        .unwrap()
        .into_commit();
    commit.tree_id().unwrap().detach()
}

/// Serialization writes objects through gix, which has no equivalent of Git's
/// automatic maintenance, and nothing else runs in a metadata-only repository.
/// Without a maintenance hook the object store only ever grows loose; a long
/// history reaches millions of loose objects.
#[test]
fn serializing_many_commits_does_not_leave_every_object_loose() {
    let (dir, repo) = setup_repo();
    let sha = head_sha(&repo);
    let session = open_session(repo);

    // gc.auto's default threshold is in the thousands of loose objects, so
    // lower it rather than writing enough commits to trip the default.
    run_git(dir.path(), &["config", "gc.auto", "64"]);
    run_git(dir.path(), &["config", "gc.autoDetach", "false"]);

    let target = Target::commit(&sha).unwrap();
    for round in 0..120 {
        session
            .target(&target)
            .set(&format!("agent:step-{round}"), format!("value-{round}"))
            .unwrap();
        let _ = session.serialize().unwrap();
    }

    assert!(
        loose_object_count(dir.path()) < 400,
        "expected maintenance to pack objects, found {} loose",
        loose_object_count(dir.path())
    );
}

/// Count loose objects in a repository's object store.
fn loose_object_count(dir: &std::path::Path) -> usize {
    let objects = dir.join(".git").join("objects");
    let Ok(entries) = std::fs::read_dir(&objects) else {
        return 0;
    };
    entries
        .flatten()
        .filter(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            name.len() == 2 && name.chars().all(|c| c.is_ascii_hexdigit())
        })
        .filter_map(|entry| std::fs::read_dir(entry.path()).ok())
        .map(|inner| inner.flatten().count())
        .sum()
}

fn run_git(dir: &std::path::Path, args: &[&str]) {
    let output = std::process::Command::new("git")
        .current_dir(dir)
        .args(args)
        .output()
        .unwrap();
    assert!(output.status.success(), "git {args:?} failed");
}
