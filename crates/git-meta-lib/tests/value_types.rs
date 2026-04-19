#![allow(clippy::unwrap_used, clippy::expect_used)]

mod helpers;

use git_meta_lib::*;
use helpers::*;

#[test]
fn string_upsert_overwrites() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::project();
    let handle = session.target(&target);

    handle.set("config", "first").unwrap();
    assert_eq!(
        handle.get_value("config").unwrap(),
        Some(MetaValue::String("first".to_string()))
    );

    handle.set("config", "second").unwrap();
    assert_eq!(
        handle.get_value("config").unwrap(),
        Some(MetaValue::String("second".to_string()))
    );
}

#[test]
fn list_preserves_order() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::project();
    let handle = session.target(&target);

    let items = ["alpha", "beta", "gamma", "delta", "epsilon"];
    for item in &items {
        handle.list_push("sequence", item).unwrap();
    }

    let entries = handle.list_entries("sequence").unwrap();
    assert_eq!(entries.len(), items.len());
    for (i, item) in items.iter().enumerate() {
        assert_eq!(
            entries[i].value, *item,
            "entry at index {i} should be {item}, got {}",
            entries[i].value
        );
    }

    // Timestamps should be monotonically non-decreasing
    for i in 1..entries.len() {
        assert!(
            entries[i].timestamp >= entries[i - 1].timestamp,
            "timestamps should be non-decreasing: {} >= {}",
            entries[i].timestamp,
            entries[i - 1].timestamp
        );
    }
}

#[test]
fn set_deduplicates_members() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::path("src");
    let handle = session.target(&target);

    handle.set_add("owners", "alice").unwrap();
    handle.set_add("owners", "alice").unwrap();
    handle.set_add("owners", "bob").unwrap();
    handle.set_add("owners", "alice").unwrap();

    let value = handle.get_value("owners").unwrap().unwrap();
    if let MetaValue::Set(members) = value {
        assert_eq!(members.len(), 2, "set should deduplicate: got {members:?}");
        assert!(members.contains("alice"));
        assert!(members.contains("bob"));
    } else {
        panic!("expected MetaValue::Set");
    }
}

#[test]
fn large_string_value_roundtrips() {
    // Create a string > 1024 bytes (the GIT_REF_THRESHOLD)
    let large_value: String = "x".repeat(2048);

    // Set it in repo A and serialize
    let (dir_a, repo_a) = setup_repo();
    let session_a = Session::open(repo_a).unwrap().with_timestamp(1000);
    session_a
        .target(&Target::project())
        .set("large:payload", large_value.as_str())
        .unwrap();
    let output = session_a.serialize().unwrap();
    assert!(output.changes > 0);

    // Verify it reads back correctly in A
    let val_a = session_a
        .target(&Target::project())
        .get_value("large:payload")
        .unwrap();
    assert_eq!(
        val_a,
        Some(MetaValue::String(large_value.clone())),
        "large value should roundtrip in the same repo"
    );

    // Materialize to repo C and verify the full value is recovered
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

    let val_c = session_c
        .target(&Target::project())
        .get_value("large:payload")
        .unwrap();
    assert_eq!(
        val_c,
        Some(MetaValue::String(large_value)),
        "large value should survive serialize + materialize roundtrip"
    );
}
