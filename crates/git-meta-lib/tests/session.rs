#![allow(clippy::unwrap_used, clippy::expect_used)]

mod helpers;

use git_meta_lib::*;
use helpers::*;

#[test]
fn session_with_timestamp_is_deterministic() {
    // Use two separate repos so each has its own SQLite database
    let (_dir1, repo1) = setup_repo();
    let (_dir2, repo2) = setup_repo();

    let session1 = Session::open(repo1).unwrap().with_timestamp(42_000);
    let session2 = Session::open(repo2).unwrap().with_timestamp(42_000);

    // Set identical data in both
    session1
        .target(&Target::project())
        .set("key", "value")
        .unwrap();
    session2
        .target(&Target::project())
        .set("key", "value")
        .unwrap();

    // Serialize both
    let output1 = session1.serialize().unwrap();
    let output2 = session2.serialize().unwrap();

    // Both should write the same number of changes and refs
    assert_eq!(output1.changes, output2.changes);
    assert_eq!(output1.refs_written.len(), output2.refs_written.len());
}

#[test]
fn target_named_constructors() {
    let (_dir, repo) = setup_repo();
    let sha = head_sha(&repo);
    let session = open_session(repo);

    // Verify each named constructor produces a usable target
    let targets = [
        Target::commit(&sha).unwrap(),
        Target::path("src/lib.rs"),
        Target::project(),
        Target::branch("main"),
        Target::change_id("change-abc"),
    ];

    let expected_types = [
        TargetType::Commit,
        TargetType::Path,
        TargetType::Project,
        TargetType::Branch,
        TargetType::ChangeId,
    ];

    for (target, expected_type) in targets.iter().zip(expected_types.iter()) {
        assert_eq!(target.target_type(), expected_type);

        // Each target should be usable with session.target()
        let handle = session.target(target);
        handle.set("test:key", "test-value").unwrap();
        let val = handle.get_value("test:key").unwrap();
        assert_eq!(
            val,
            Some(MetaValue::String("test-value".to_string())),
            "target {target} should support set/get"
        );
    }
}

#[test]
fn session_provides_config_values() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    assert_eq!(session.email(), "test@example.com");
    assert_eq!(session.name(), "Test User");
    assert_eq!(session.namespace(), "meta");
}
