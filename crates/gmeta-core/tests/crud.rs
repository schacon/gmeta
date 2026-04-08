#![allow(clippy::unwrap_used, clippy::expect_used)]

mod helpers;

use gmeta_core::*;
use helpers::*;

#[test]
fn set_and_get_string_value() {
    let (_dir, repo) = setup_repo();
    let sha = head_sha(&repo);
    let session = open_session(repo);

    let target = Target::commit(&sha).unwrap();
    let handle = session.target(&target);

    handle.set("agent:model", "claude-4.6").unwrap();

    let value = handle.get_value("agent:model").unwrap();
    assert!(value.is_some(), "expected a value for agent:model");
    let value = value.unwrap();
    assert_eq!(value, MetaValue::String("claude-4.6".to_string()));
    assert_eq!(value.value_type(), ValueType::String);
}

#[test]
fn set_and_get_list_value() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::branch("feature-x");
    let handle = session.target(&target);

    handle.list_push("review:comments", "first").unwrap();
    handle.list_push("review:comments", "second").unwrap();
    handle.list_push("review:comments", "third").unwrap();

    let value = handle.get_value("review:comments").unwrap();
    assert!(value.is_some());
    let value = value.unwrap();
    assert_eq!(value.value_type(), ValueType::List);

    if let MetaValue::List(entries) = &value {
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].value, "first");
        assert_eq!(entries[1].value, "second");
        assert_eq!(entries[2].value, "third");
    } else {
        panic!("expected MetaValue::List, got {value:?}");
    }
}

#[test]
fn set_and_get_set_value() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::path("src/metrics");
    let handle = session.target(&target);

    handle.set_add("owners", "alice").unwrap();
    handle.set_add("owners", "bob").unwrap();
    handle.set_add("owners", "charlie").unwrap();
    // Duplicate -- should not increase count
    handle.set_add("owners", "alice").unwrap();

    let value = handle.get_value("owners").unwrap();
    assert!(value.is_some());
    let value = value.unwrap();
    assert_eq!(value.value_type(), ValueType::Set);

    if let MetaValue::Set(members) = &value {
        assert_eq!(members.len(), 3);
        assert!(members.contains("alice"));
        assert!(members.contains("bob"));
        assert!(members.contains("charlie"));
    } else {
        panic!("expected MetaValue::Set, got {value:?}");
    }
}

#[test]
fn remove_key() {
    let (_dir, repo) = setup_repo();
    let sha = head_sha(&repo);
    let session = open_session(repo);

    let target = Target::commit(&sha).unwrap();
    let handle = session.target(&target);

    handle.set("agent:model", "claude-4.6").unwrap();
    assert!(handle.get_value("agent:model").unwrap().is_some());

    let removed = handle.remove("agent:model").unwrap();
    assert!(removed, "remove should return true for existing key");

    let value = handle.get_value("agent:model").unwrap();
    assert!(value.is_none(), "value should be gone after remove");
}

#[test]
fn all_target_types() {
    let (_dir, repo) = setup_repo();
    let sha = head_sha(&repo);
    let session = open_session(repo);

    // Commit target
    let commit_target = Target::commit(&sha).unwrap();
    session
        .target(&commit_target)
        .set("provenance", "ai-generated")
        .unwrap();

    // Path target
    let path_target = Target::path("src/main.rs");
    session.target(&path_target).set("owner", "teamA").unwrap();

    // Branch target
    let branch_target = Target::branch("feature-branch");
    session
        .target(&branch_target)
        .set("ci:status", "green")
        .unwrap();

    // Project target
    let project_target = Target::project();
    session
        .target(&project_target)
        .set("version", "1.0.0")
        .unwrap();

    // Change-id target
    let change_target = Target::change_id("jj-change-abc123");
    session
        .target(&change_target)
        .set("review:status", "approved")
        .unwrap();

    // Verify each independently
    assert_eq!(
        session
            .target(&commit_target)
            .get_value("provenance")
            .unwrap(),
        Some(MetaValue::String("ai-generated".to_string()))
    );
    assert_eq!(
        session.target(&path_target).get_value("owner").unwrap(),
        Some(MetaValue::String("teamA".to_string()))
    );
    assert_eq!(
        session
            .target(&branch_target)
            .get_value("ci:status")
            .unwrap(),
        Some(MetaValue::String("green".to_string()))
    );
    assert_eq!(
        session
            .target(&project_target)
            .get_value("version")
            .unwrap(),
        Some(MetaValue::String("1.0.0".to_string()))
    );
    assert_eq!(
        session
            .target(&change_target)
            .get_value("review:status")
            .unwrap(),
        Some(MetaValue::String("approved".to_string()))
    );
}
