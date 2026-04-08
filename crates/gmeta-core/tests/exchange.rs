#![allow(clippy::unwrap_used, clippy::expect_used)]

mod helpers;

use gmeta_core::*;
use helpers::*;

#[test]
fn tombstone_survives_serialize_materialize() {
    // Repo A: set key, serialize
    let (dir_a, repo_a) = setup_repo();
    let session_a = Session::open(repo_a).unwrap().with_timestamp(1000);
    session_a
        .target(&Target::project())
        .set("ephemeral", "temp-value")
        .unwrap();
    let _ = session_a.serialize().unwrap();

    // Repo A: remove key (creates tombstone), serialize again
    let session_a2 = reopen_session(dir_a.path(), 2000);
    let removed = session_a2
        .target(&Target::project())
        .remove("ephemeral")
        .unwrap();
    assert!(removed, "remove should return true for existing key");
    let _ = session_a2.serialize().unwrap();

    // Get A's commit OID after the tombstone serialize
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

    // Repo C: materialize A's state (which includes the tombstone)
    let (dir_c, _repo_c) = setup_repo();
    let src_objects = dir_a.path().join(".git").join("objects");
    inject_remote_ref(&src_objects, dir_c.path(), a_oid);

    let session_c = reopen_session(dir_c.path(), 3000);
    let mat_output = session_c.materialize(None).unwrap();

    assert!(
        !mat_output.results.is_empty(),
        "materialize should process at least one ref"
    );

    // The key should NOT exist in C (tombstone was applied)
    let val = session_c
        .target(&Target::project())
        .get_value("ephemeral")
        .unwrap();
    assert!(
        val.is_none(),
        "tombstoned key should not be visible after materialize"
    );
}

#[test]
fn filter_routes_keys_to_destinations() {
    let (dir, repo) = setup_repo();
    let session = Session::open(repo).unwrap().with_timestamp(1000);

    // Set a filter rule: route "private:**" keys to "private" destination
    session
        .target(&Target::project())
        .set_add("meta:local:filter", "route private:** private")
        .unwrap();

    // Set a regular key and a private key
    session
        .target(&Target::project())
        .set("public:info", "everyone-sees-this")
        .unwrap();
    session
        .target(&Target::project())
        .set("private:secret", "only-private-dest")
        .unwrap();

    // Serialize
    let output = session.serialize().unwrap();
    assert!(output.changes > 0, "should have serialized something");

    // Verify refs: main should exist, and private destination ref should exist
    let repo_re = gix::open_opts(
        dir.path(),
        gix::open::Options::isolated()
            .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
    )
    .unwrap();

    let main_ref = repo_re.find_reference("refs/meta/local/main");
    assert!(main_ref.is_ok(), "refs/meta/local/main should exist");

    let private_ref = repo_re.find_reference("refs/meta/local/private");
    assert!(
        private_ref.is_ok(),
        "refs/meta/local/private should exist for routed keys"
    );

    // Verify refs_written includes both destinations
    assert!(
        output
            .refs_written
            .iter()
            .any(|r| r.contains("refs/meta/local/main")),
        "should write main ref, got: {:?}",
        output.refs_written
    );
    assert!(
        output
            .refs_written
            .iter()
            .any(|r| r.contains("refs/meta/local/private")),
        "should write private ref, got: {:?}",
        output.refs_written
    );
}

#[test]
fn push_once_with_no_remote_returns_error() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    // Set some metadata so we have something to push
    session
        .target(&Target::project())
        .set("key", "value")
        .unwrap();

    // push_once with no remote configured should return an error
    let result = session.push_once(None);
    assert!(
        result.is_err(),
        "push_once should fail when no remote is configured"
    );
}

#[test]
fn pull_with_no_remote_returns_error() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    // pull with no remote configured should return an error
    let result = session.pull(None);
    assert!(
        result.is_err(),
        "pull should fail when no remote is configured"
    );
}
