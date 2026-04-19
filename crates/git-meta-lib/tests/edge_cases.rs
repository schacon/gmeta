#![allow(clippy::unwrap_used, clippy::expect_used)]

mod helpers;

use std::collections::BTreeSet;

use git_meta_lib::*;
use helpers::*;

#[test]
fn get_nonexistent_key_returns_none() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::project();
    let handle = session.target(&target);

    let value = handle.get_value("does:not:exist").unwrap();
    assert!(value.is_none());
}

#[test]
fn remove_nonexistent_key_returns_false() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::project();
    let handle = session.target(&target);

    let removed = handle.remove("nonexistent").unwrap();
    assert!(!removed, "removing a nonexistent key should return false");
}

#[test]
fn set_remove_then_re_add() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::path("src/metrics");
    let handle = session.target(&target);

    handle.set_add("owners", "alice").unwrap();
    handle.set_add("owners", "bob").unwrap();

    handle.set_remove("owners", "alice").unwrap();

    let value = handle.get_value("owners").unwrap().unwrap();
    if let MetaValue::Set(members) = &value {
        assert_eq!(members.len(), 1);
        assert!(members.contains("bob"));
    } else {
        panic!("expected Set");
    }

    // Re-add alice
    handle.set_add("owners", "alice").unwrap();
    let value = handle.get_value("owners").unwrap().unwrap();
    if let MetaValue::Set(members) = &value {
        assert_eq!(members.len(), 2);
        assert!(members.contains("alice"));
        assert!(members.contains("bob"));
    } else {
        panic!("expected Set");
    }
}

#[test]
fn authorship_tracks_writer() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::project();
    let handle = session.target(&target);

    handle.set("tracked:key", "some-value").unwrap();

    let authorship = handle.get_authorship("tracked:key").unwrap();
    assert!(authorship.is_some(), "authorship should exist after set");
    let authorship = authorship.unwrap();
    assert_eq!(authorship.email, "test@example.com");
}

#[test]
fn key_prefix_matching_returns_subkeys() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::project();
    let handle = session.target(&target);

    handle.set("agent:model", "claude").unwrap();
    handle.set("agent:provider", "anthropic").unwrap();
    handle.set("other:key", "unrelated").unwrap();

    // Filter by "agent" prefix
    let agent_values = handle.get_all_values(Some("agent")).unwrap();
    assert_eq!(
        agent_values.len(),
        2,
        "should return only agent:* keys, got: {:?}",
        agent_values
            .iter()
            .map(|(k, _)| k.as_str())
            .collect::<Vec<_>>()
    );

    let keys: BTreeSet<String> = agent_values.iter().map(|(k, _)| k.clone()).collect();
    assert!(keys.contains("agent:model"));
    assert!(keys.contains("agent:provider"));
    assert!(!keys.contains("other:key"));

    // Filter by more specific prefix "agent:model"
    let model_values = handle.get_all_values(Some("agent:model")).unwrap();
    assert_eq!(model_values.len(), 1);
    assert_eq!(model_values[0].0, "agent:model");

    // No filter returns everything
    let all_values = handle.get_all_values(None).unwrap();
    assert_eq!(all_values.len(), 3);
}

#[test]
fn namespaced_keys_work_correctly() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::project();
    let handle = session.target(&target);

    // Deeply namespaced keys
    handle.set("agent:claude:session:id", "sess-123").unwrap();
    handle.set("agent:claude:session:model", "opus").unwrap();
    handle
        .set("agent:claude:prompt", "make a sandwich")
        .unwrap();

    // Get all under "agent:claude:session" prefix
    let session_values = handle.get_all_values(Some("agent:claude:session")).unwrap();
    assert_eq!(session_values.len(), 2);
    let keys: BTreeSet<String> = session_values.iter().map(|(k, _)| k.clone()).collect();
    assert!(keys.contains("agent:claude:session:id"));
    assert!(keys.contains("agent:claude:session:model"));

    // Get all under "agent" prefix
    let agent_values = handle.get_all_values(Some("agent")).unwrap();
    assert_eq!(agent_values.len(), 3);
}

#[test]
fn multiple_targets_are_independent() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target_a = Target::branch("branch-a");
    let target_b = Target::branch("branch-b");

    session.target(&target_a).set("status", "draft").unwrap();
    session.target(&target_b).set("status", "ready").unwrap();

    assert_eq!(
        session.target(&target_a).get_value("status").unwrap(),
        Some(MetaValue::String("draft".to_string()))
    );
    assert_eq!(
        session.target(&target_b).get_value("status").unwrap(),
        Some(MetaValue::String("ready".to_string()))
    );
}

#[test]
fn list_remove_by_index() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::project();
    let handle = session.target(&target);

    handle.list_push("items", "a").unwrap();
    handle.list_push("items", "b").unwrap();
    handle.list_push("items", "c").unwrap();

    // Remove the middle element by index
    handle.list_remove("items", 1).unwrap();

    let entries = handle.list_entries("items").unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].value, "a");
    assert_eq!(entries[1].value, "c");
}
