#![allow(clippy::unwrap_used, clippy::expect_used)]

mod helpers;

use std::collections::BTreeSet;

use git_meta_lib::*;
use helpers::*;

#[test]
fn merge_string_conflict_local_wins() {
    // Set up the common base: both repos share "status" = "draft"
    let (dir_a, dir_c, _base_oid) = setup_three_way_base(|s| {
        s.target(&Target::project()).set("status", "draft").unwrap();
    });

    // Repo A: change status to "published", serialize
    let session_a = reopen_session(dir_a.path(), 2000);
    session_a
        .target(&Target::project())
        .set("status", "published")
        .unwrap();
    let _ = session_a.serialize().unwrap();

    // Repo C: change status to "archived", serialize
    let session_c = reopen_session(dir_c.path(), 2500);
    session_c
        .target(&Target::project())
        .set("status", "archived")
        .unwrap();
    let _ = session_c.serialize().unwrap();

    // Get A's new commit OID for the remote tracking ref
    let repo_a = gix::open_opts(
        dir_a.path(),
        gix::open::Options::isolated()
            .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
    )
    .unwrap();
    let a_new_oid = repo_a
        .find_reference("refs/meta/local/main")
        .unwrap()
        .into_fully_peeled_id()
        .unwrap()
        .detach();

    // Copy A's objects to C and update remote tracking ref
    let src_objects = dir_a.path().join(".git").join("objects");
    inject_remote_ref(&src_objects, dir_c.path(), a_new_oid);

    // Materialize in C: three-way merge -- local (C) wins for strings
    let session_c2 = reopen_session(dir_c.path(), 3000);
    let mat_output = session_c2.materialize(None).unwrap();

    assert!(
        !mat_output.results.is_empty(),
        "materialize should process at least one ref"
    );
    assert_eq!(
        mat_output.results[0].strategy,
        MaterializeStrategy::ThreeWayMerge,
    );

    // The local value "archived" should win
    let val = session_c2
        .target(&Target::project())
        .get_value("status")
        .unwrap();
    assert_eq!(val, Some(MetaValue::String("archived".to_string())));
}

#[test]
fn merge_list_union() {
    // Set up the common base: both repos share a list with "base-entry"
    let (dir_a, dir_c, _base_oid) = setup_three_way_base(|s| {
        s.target(&Target::project())
            .list_push("comments", "base-entry")
            .unwrap();
    });

    // Repo A: append "from-a", serialize
    let session_a = reopen_session(dir_a.path(), 2000);
    session_a
        .target(&Target::project())
        .list_push("comments", "from-a")
        .unwrap();
    let _ = session_a.serialize().unwrap();

    // Repo C: append "from-c", serialize
    let session_c = reopen_session(dir_c.path(), 2500);
    session_c
        .target(&Target::project())
        .list_push("comments", "from-c")
        .unwrap();
    let _ = session_c.serialize().unwrap();

    // Get A's new commit OID
    let repo_a = gix::open_opts(
        dir_a.path(),
        gix::open::Options::isolated()
            .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
    )
    .unwrap();
    let a_new_oid = repo_a
        .find_reference("refs/meta/local/main")
        .unwrap()
        .into_fully_peeled_id()
        .unwrap()
        .detach();

    // Copy A's objects to C and update remote tracking ref
    let src_objects = dir_a.path().join(".git").join("objects");
    inject_remote_ref(&src_objects, dir_c.path(), a_new_oid);

    // Materialize in C: three-way merge -- lists use union
    let session_c2 = reopen_session(dir_c.path(), 3000);
    let mat_output = session_c2.materialize(None).unwrap();

    assert!(
        !mat_output.results.is_empty(),
        "materialize should process at least one ref"
    );
    assert_eq!(
        mat_output.results[0].strategy,
        MaterializeStrategy::ThreeWayMerge,
    );

    // Verify the list contains entries from both sides (union)
    let entries = session_c2
        .target(&Target::project())
        .list_entries("comments")
        .unwrap();
    let values: Vec<&str> = entries.iter().map(|e| e.value.as_str()).collect();
    assert!(
        values.contains(&"base-entry"),
        "should still have base entry, got: {values:?}"
    );
    assert!(
        values.contains(&"from-a"),
        "should have A's entry, got: {values:?}"
    );
    assert!(
        values.contains(&"from-c"),
        "should have C's entry, got: {values:?}"
    );
}

#[test]
fn merge_set_union() {
    // Set up the common base: both repos share a set with "shared"
    let (dir_a, dir_c, _base_oid) = setup_three_way_base(|s| {
        s.target(&Target::project())
            .set_add("owners", "shared")
            .unwrap();
    });

    // Repo A: set owners to {"shared", "alice"} using set() to build the
    // full value (avoids set:add log entry which triggers a known incremental
    // serialization parsing issue)
    let session_a = reopen_session(dir_a.path(), 2000);
    let mut owners_a = BTreeSet::new();
    owners_a.insert("shared".to_string());
    owners_a.insert("alice".to_string());
    session_a
        .target(&Target::project())
        .set("owners", MetaValue::Set(owners_a))
        .unwrap();
    let _ = session_a.serialize().unwrap();

    // Repo C: set owners to {"shared", "bob"}
    let session_c = reopen_session(dir_c.path(), 2500);
    let mut owners_c = BTreeSet::new();
    owners_c.insert("shared".to_string());
    owners_c.insert("bob".to_string());
    session_c
        .target(&Target::project())
        .set("owners", MetaValue::Set(owners_c))
        .unwrap();
    let _ = session_c.serialize().unwrap();

    // Get A's new commit OID
    let repo_a = gix::open_opts(
        dir_a.path(),
        gix::open::Options::isolated()
            .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
    )
    .unwrap();
    let a_new_oid = repo_a
        .find_reference("refs/meta/local/main")
        .unwrap()
        .into_fully_peeled_id()
        .unwrap()
        .detach();

    // Copy A's objects to C and update remote tracking ref
    let src_objects = dir_a.path().join(".git").join("objects");
    inject_remote_ref(&src_objects, dir_c.path(), a_new_oid);

    // Materialize in C: three-way merge -- sets use union
    let session_c2 = reopen_session(dir_c.path(), 3000);
    let mat_output = session_c2.materialize(None).unwrap();

    assert!(
        !mat_output.results.is_empty(),
        "materialize should process at least one ref"
    );
    assert_eq!(
        mat_output.results[0].strategy,
        MaterializeStrategy::ThreeWayMerge,
    );

    // Verify the set contains members from both sides (union)
    let val = session_c2
        .target(&Target::project())
        .get_value("owners")
        .unwrap();
    assert!(val.is_some(), "owners should exist");
    if let Some(MetaValue::Set(members)) = val {
        assert!(
            members.contains("shared"),
            "should still have shared, got: {members:?}"
        );
        assert!(
            members.contains("alice"),
            "should have alice from A, got: {members:?}"
        );
        assert!(
            members.contains("bob"),
            "should have bob from C, got: {members:?}"
        );
    } else {
        panic!("expected MetaValue::Set, got: {val:?}");
    }
}
