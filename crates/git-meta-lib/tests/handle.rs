#![allow(clippy::unwrap_used, clippy::expect_used)]

mod helpers;

use git_meta_lib::*;
use helpers::*;

#[test]
fn handle_set_convenience() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::project();
    let handle = session.target(&target);

    // From<&str> conversion
    handle.set("config:key", "value-from-str").unwrap();
    assert_eq!(
        handle.get_value("config:key").unwrap(),
        Some(MetaValue::String("value-from-str".to_string()))
    );

    // From<String> conversion
    handle
        .set("config:key2", String::from("value-from-string"))
        .unwrap();
    assert_eq!(
        handle.get_value("config:key2").unwrap(),
        Some(MetaValue::String("value-from-string".to_string()))
    );
}

#[test]
fn handle_get_all_values() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::project();
    let handle = session.target(&target);

    handle.set("agent:model", "claude").unwrap();
    handle.set("agent:provider", "anthropic").unwrap();
    handle.set("review:status", "approved").unwrap();

    // Filter by "agent" prefix
    let agent_values = handle.get_all_values(Some("agent")).unwrap();
    assert_eq!(agent_values.len(), 2);
    let keys: Vec<&str> = agent_values.iter().map(|(k, _)| k.as_str()).collect();
    assert!(keys.contains(&"agent:model"));
    assert!(keys.contains(&"agent:provider"));

    // No filter returns everything
    let all_values = handle.get_all_values(None).unwrap();
    assert_eq!(all_values.len(), 3);
}

#[test]
fn handle_list_operations() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::branch("main");
    let handle = session.target(&target);

    // Push entries
    handle.list_push("comments", "hello").unwrap();
    handle.list_push("comments", "world").unwrap();
    handle.list_push("comments", "goodbye").unwrap();

    // Read entries
    let entries = handle.list_entries("comments").unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].value, "hello");
    assert_eq!(entries[1].value, "world");
    assert_eq!(entries[2].value, "goodbye");

    // Pop a specific value
    handle.list_pop("comments", "world").unwrap();
    let entries = handle.list_entries("comments").unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].value, "hello");
    assert_eq!(entries[1].value, "goodbye");
}

#[test]
fn handle_set_operations() {
    let (_dir, repo) = setup_repo();
    let session = open_session(repo);

    let target = Target::path("src/metrics");
    let handle = session.target(&target);

    handle.set_add("owners", "alice").unwrap();
    handle.set_add("owners", "bob").unwrap();
    handle.set_add("owners", "charlie").unwrap();

    let value = handle.get_value("owners").unwrap().unwrap();
    if let MetaValue::Set(members) = &value {
        assert_eq!(members.len(), 3);
    } else {
        panic!("expected Set");
    }

    handle.set_remove("owners", "bob").unwrap();
    let value = handle.get_value("owners").unwrap().unwrap();
    if let MetaValue::Set(members) = &value {
        assert_eq!(members.len(), 2);
        assert!(members.contains("alice"));
        assert!(members.contains("charlie"));
        assert!(!members.contains("bob"));
    } else {
        panic!("expected Set");
    }
}
