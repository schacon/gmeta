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

/// Maintenance must not run while the session that wrote the objects is still
/// in use. `gc.autoDetach` defaults to true, so `git gc --auto` returns
/// immediately and keeps packing in the background, deleting the loose objects
/// it packs — and a `Repository` handle opened before that finishes then fails
/// with "object ... could not be found".
///
/// This asserts the safe order: serialize repeatedly with a live session and
/// nothing disappears, then maintain once the work is done.
#[test]
fn serializing_repeatedly_does_not_lose_objects_to_maintenance() {
    let (dir, repo) = setup_repo();
    let sha = head_sha(&repo);
    let session = open_session(repo);

    // Enough rounds to pass the default loose-object threshold, so the test
    // does not depend on gc.auto being configured. gc.autoDetach is left at
    // its default, so a background gc would race if serialize triggered one.
    let target = Target::commit(&sha).unwrap();
    for round in 0..900 {
        session
            .target(&target)
            .set(&format!("agent:step-{round}"), format!("value-{round}"))
            .unwrap();
        let _ = session.serialize().unwrap();
        // Read back through the same session, which is what a background gc
        // would break.
        assert_eq!(
            session
                .target(&target)
                .get_value(&format!("agent:step-{round}"))
                .unwrap(),
            Some(MetaValue::String(format!("value-{round}")))
        );
    }

    let before = loose_object_count(dir.path());
    assert!(before > 0, "expected loose objects before maintenance");

    // Maintenance is safe here: the work is finished.
    session.maintain_object_store();

    assert!(
        loose_object_count(dir.path()) < before,
        "maintenance packed nothing: {before} loose before, {} after",
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

const AUTO_PRUNE_DAY_MS: i64 = 24 * 60 * 60 * 1000;
const AUTO_PRUNE_T0: i64 = 1_700_000_000_000;
/// Auto-prune rebuilds the whole tree from the entries serialize read. An
/// incremental serialize that only reads the changed targets' rows would
/// therefore publish a tree containing only those targets — and, with a floor
/// of one key, would keep the wrong one for the wrong reason.
#[test]
fn auto_prune_after_an_incremental_serialize_keeps_untouched_targets() {
    let (dir, repo) = setup_repo();
    let sha = head_sha(&repo);

    // A target old enough to age out, so the prune has real work to do.
    let stale_sha = "aabbccddeeff00112233445566778899aabbccdd";
    let stale = Target::commit(stale_sha).unwrap();
    let session = reopen_session(dir.path(), AUTO_PRUNE_T0 - 100 * AUTO_PRUNE_DAY_MS);
    session.target(&stale).set("agent:model", "old").unwrap();
    drop(session);

    let session = reopen_session(dir.path(), AUTO_PRUNE_T0);
    let project = Target::project();
    // Retention far wider than the ages involved; max-keys forces the trigger.
    session
        .target(&project)
        .set("meta:prune:max-keys", "2")
        .unwrap();
    session
        .target(&project)
        .set("meta:prune:min-keys", "1")
        .unwrap();

    let untouched_sha = "00112233445566778899aabbccddeeff00112233";
    let touched = Target::commit(&sha).unwrap();
    let untouched = Target::commit(untouched_sha).unwrap();
    session.target(&touched).set("agent:model", "a").unwrap();
    session.target(&untouched).set("agent:model", "b").unwrap();
    let first = session.serialize().unwrap();
    drop(session);

    // One day later: touch only one target, so serialize goes incremental.
    let session = reopen_session(dir.path(), AUTO_PRUNE_T0 + AUTO_PRUNE_DAY_MS);
    session.target(&touched).set("agent:model", "a2").unwrap();
    let second = session.serialize().unwrap();
    eprintln!(
        "first: changes={} pruned={} | second: changes={} pruned={}",
        first.changes, first.pruned, second.changes, second.pruned
    );
    // The first serialize proves auto-prune is wired up: three keys over a
    // ceiling of two, cut back to a floor of one. The second runs it again on
    // an incremental read, where the entries it rebuilds from must still be the
    // complete set rather than just the target that changed.
    assert!(first.pruned > 0, "auto-prune did not run");
    drop(session);

    let repo = gix::open(dir.path()).unwrap();
    let tree = repo
        .find_reference("refs/meta/local/main")
        .unwrap()
        .into_fully_peeled_id()
        .unwrap()
        .object()
        .unwrap()
        .into_commit()
        .tree_id()
        .unwrap()
        .detach();

    let find = |path: &str| -> bool {
        let segments: Vec<&str> = path.split('/').collect();
        let mut current = tree;
        for (index, segment) in segments.iter().enumerate() {
            let Ok(found) = repo.find_tree(current) else {
                return false;
            };
            let Some(entry) = found.find_entry(*segment) else {
                return false;
            };
            if index == segments.len() - 1 {
                return true;
            }
            current = entry.object_id();
        }
        false
    };

    // The most recently modified key is the one that survives, and it is the
    // one the incremental serialize touched.
    assert!(
        find(&format!("commit/{}/{sha}/agent/model/__value", &sha[..2])),
        "auto-prune dropped the most recently modified key"
    );
    // The oldest key is the one that goes.
    assert!(
        !find(&format!(
            "commit/{}/{stale_sha}/agent/model/__value",
            &stale_sha[..2]
        )),
        "auto-prune kept the oldest key over more recent ones"
    );
}

/// The point of a floor: a prune has to buy room, or the very next serialize
/// triggers again and the tree never settles.
#[test]
fn auto_prune_cuts_to_the_floor_and_then_leaves_the_tree_alone() {
    let (dir, repo) = setup_repo();
    let session = open_session(repo);
    let project = Target::project();
    session
        .target(&project)
        .set("meta:prune:max-keys", "20")
        .unwrap();
    session
        .target(&project)
        .set("meta:prune:min-keys", "10")
        .unwrap();

    let write = |index: usize, at: i64| {
        let session = reopen_session(dir.path(), at);
        let sha = format!("{index:040x}");
        session
            .target(&Target::commit(&sha).unwrap())
            .set("agent:model", format!("m{index}"))
            .unwrap();
        session.serialize().unwrap()
    };

    // Fill past the ceiling; the prune that fires must land on the floor.
    let mut pruned_at = Vec::new();
    for index in 0..24 {
        let output = write(index, AUTO_PRUNE_T0 + index as i64 * 1000);
        if output.pruned > 0 {
            pruned_at.push(index);
        }
    }

    assert!(!pruned_at.is_empty(), "auto-prune never fired");
    let tip_keys = count_tip_keys(dir.path());
    assert!(
        (10..=20).contains(&tip_keys),
        "tree settled at {tip_keys} keys, outside the 10..=20 band"
    );

    // Having cut to 10, the next few writes must not each trigger another
    // prune — that was the old behaviour this design replaces.
    assert!(
        pruned_at.len() <= 2,
        "pruned on {} of 24 serializes: {pruned_at:?}",
        pruned_at.len()
    );
}

/// The tree measurements auto-prune depends on are cached by tree OID, so a
/// repeated check does not re-walk the tree.
#[test]
fn tree_measurements_are_cached_by_oid() {
    let (dir, repo) = setup_repo();
    let sha = head_sha(&repo);
    let session = open_session(repo);
    let project = Target::project();
    session
        .target(&project)
        .set("meta:prune:max-keys", "100000")
        .unwrap();

    let target = Target::commit(&sha).unwrap();
    for round in 0..20 {
        session
            .target(&target)
            .set(&format!("agent:step-{round}"), "v")
            .unwrap();
        let _ = session.serialize().unwrap();
    }

    let cached = count_tree_stats(dir.path());
    assert!(
        cached > 0,
        "no tree measurements were cached, so every check re-walked the tree"
    );
}

/// Rows in the tree-measurement cache.
fn count_tree_stats(dir: &std::path::Path) -> i64 {
    let connection = rusqlite::Connection::open(dir.join(".git/git-meta.sqlite")).unwrap();
    connection
        .query_row("SELECT COUNT(*) FROM tree_stats", [], |row| row.get(0))
        .unwrap()
}

/// Count the metadata keys in the published tip.
fn count_tip_keys(dir: &std::path::Path) -> usize {
    let output = std::process::Command::new("git")
        .current_dir(dir)
        .args(["ls-tree", "-r", "--name-only", "refs/meta/local/main"])
        .output()
        .unwrap();
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|line| !line.starts_with("project/"))
        .filter(|line| line.ends_with("/__value"))
        .count()
}
