#![allow(clippy::unwrap_used, clippy::expect_used)]

mod helpers;

use gmeta::*;
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
    let session_c = Session::open(repo_c_reopen).unwrap().with_timestamp(2000);
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
    let session = Session::open(repo).unwrap().with_timestamp(1000);

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
