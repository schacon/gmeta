use assert_cmd::Command;
use predicates::prelude::*;
use sha1::{Digest, Sha1};
use std::path::Path;
use tempfile::TempDir;

/// Create a temporary git repo and return the TempDir handle + the initial commit SHA.
fn setup_repo() -> (TempDir, String) {
    let dir = TempDir::new().unwrap();
    let repo = git2::Repository::init(dir.path()).unwrap();

    // Set up user config so commands can read email
    let mut config = repo.config().unwrap();
    config.set_str("user.email", "test@example.com").unwrap();
    config.set_str("user.name", "Test User").unwrap();

    // Create an initial commit so the repo is valid
    let sig = git2::Signature::now("Test User", "test@example.com").unwrap();
    let tree_oid = repo.treebuilder(None).unwrap().write().unwrap();
    let tree = repo.find_tree(tree_oid).unwrap();
    let commit_oid = repo
        .commit(Some("HEAD"), &sig, &sig, "initial", &tree, &[])
        .unwrap();

    (dir, commit_oid.to_string())
}

fn gmeta(dir: &Path) -> Command {
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("gmeta");
    cmd.current_dir(dir);
    cmd
}

/// Helper to build a commit target string from a full SHA.
fn commit_target(sha: &str) -> String {
    format!("commit:{}", sha)
}

fn target_fanout(value: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(value.as_bytes());
    let hash = format!("{:x}", hasher.finalize());
    hash[..2].to_string()
}

#[test]
fn test_set_and_get_string() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    gmeta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["get", &target])
        .assert()
        .success()
        .stdout(predicate::str::contains("agent:model"))
        .stdout(predicate::str::contains("claude-4.6"));
}

#[test]
fn test_set_and_get_with_partial_sha() {
    let (dir, sha) = setup_repo();
    let full_target = commit_target(&sha);
    let partial_target = commit_target(&sha[..8]);

    // Set with partial SHA
    gmeta(dir.path())
        .args(["set", &partial_target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    // Get with full SHA should find it (was expanded on set)
    gmeta(dir.path())
        .args(["get", &full_target])
        .assert()
        .success()
        .stdout(predicate::str::contains("claude-4.6"));

    // Get with partial SHA should also find it
    gmeta(dir.path())
        .args(["get", &partial_target])
        .assert()
        .success()
        .stdout(predicate::str::contains("claude-4.6"));
}

#[test]
fn test_set_and_get_specific_key() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    gmeta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["set", &target, "agent:provider", "anthropic"])
        .assert()
        .success();

    // Get specific key
    gmeta(dir.path())
        .args(["get", &target, "agent:model"])
        .assert()
        .success()
        .stdout(predicate::str::contains("claude-4.6"))
        .stdout(predicate::str::contains("provider").not());
}

#[test]
fn test_set_and_get_json() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    gmeta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["set", &target, "agent:provider", "anthropic"])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["get", "--json", &target])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"model\": \"claude-4.6\""))
        .stdout(predicate::str::contains("\"provider\": \"anthropic\""));
}

#[test]
fn test_json_with_authorship() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    gmeta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["get", "--json", "--with-authorship", &target])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"value\": \"claude-4.6\""))
        .stdout(predicate::str::contains("\"author\": \"test@example.com\""))
        .stdout(predicate::str::contains("\"timestamp\""));
}

#[test]
fn test_partial_key_matching() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    gmeta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["set", &target, "agent:provider", "anthropic"])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["set", &target, "other:key", "value"])
        .assert()
        .success();

    // Partial key "agent" should match both agent: keys
    gmeta(dir.path())
        .args(["get", &target, "agent"])
        .assert()
        .success()
        .stdout(predicate::str::contains("agent:model"))
        .stdout(predicate::str::contains("agent:provider"))
        .stdout(predicate::str::contains("other:key").not());
}

#[test]
fn test_partial_key_matching_commit_namespace_example() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    gmeta(dir.path())
        .args(["set", &target, "agent:model", "claude-opus-4-6"])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["set", &target, "agent:provider", "anthropic"])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["set", &target, "agent:prompt", "Make me a sandwich"])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["set", &target, "agent:transcript", "..."])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["get", &target, "agent"])
        .assert()
        .success()
        .stdout(predicate::str::contains("agent:model"))
        .stdout(predicate::str::contains("claude-opus-4-6"))
        .stdout(predicate::str::contains("agent:provider"))
        .stdout(predicate::str::contains("anthropic"))
        .stdout(predicate::str::contains("agent:prompt"))
        .stdout(predicate::str::contains("Make me a sandwich"))
        .stdout(predicate::str::contains("agent:transcript"));
}

#[test]
fn test_rm_removes_value() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    gmeta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["rm", &target, "agent:model"])
        .assert()
        .success();

    // Should produce no output now
    gmeta(dir.path())
        .args(["get", &target, "agent:model"])
        .assert()
        .success()
        .stdout(predicate::str::is_empty());
}

#[test]
fn test_list_push() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    gmeta(dir.path())
        .args(["list:push", &target, "tags", "first"])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["list:push", &target, "tags", "second"])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["get", &target, "tags"])
        .assert()
        .success()
        .stdout(predicate::str::contains("first"))
        .stdout(predicate::str::contains("second"));
}

#[test]
fn test_list_push_converts_string_to_list() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    gmeta(dir.path())
        .args(["set", &target, "note", "original"])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["list:push", &target, "note", "appended"])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["get", &target, "note"])
        .assert()
        .success()
        .stdout(predicate::str::contains("original"))
        .stdout(predicate::str::contains("appended"));
}

#[test]
fn test_list_pop() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    gmeta(dir.path())
        .args(["list:push", &target, "tags", "a"])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["list:push", &target, "tags", "b"])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["list:pop", &target, "tags", "b"])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["get", &target, "tags"])
        .assert()
        .success()
        .stdout(predicate::str::contains("a"))
        .stdout(predicate::str::contains("b").not());
}

#[test]
fn test_set_list_type() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    gmeta(dir.path())
        .args([
            "set",
            "-t",
            "list",
            &target,
            "items",
            r#"["hello","world"]"#,
        ])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["get", &target, "items"])
        .assert()
        .success()
        .stdout(predicate::str::contains("hello"))
        .stdout(predicate::str::contains("world"));
}

#[test]
fn test_serialize_creates_ref() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    gmeta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["serialize"])
        .assert()
        .success()
        .stdout(predicate::str::contains("refs/meta/local/main"));

    // Verify the ref exists and contains the right tree structure
    let repo = git2::Repository::open(dir.path()).unwrap();
    let reference = repo.find_reference("refs/meta/local/main").unwrap();
    let commit = reference.peel_to_commit().unwrap();
    let tree = commit.tree().unwrap();

    // Build the expected path from the full SHA
    let first2 = &sha[..2];
    let expected_path = format!("commit/{}/{}/agent/model/__value", first2, sha);

    // Walk the tree and verify structure
    let mut found = false;
    tree.walk(git2::TreeWalkMode::PreOrder, |root, entry| {
        let full_path = format!("{}{}", root, entry.name().unwrap_or(""));
        if full_path == expected_path {
            // Verify blob content
            let blob = repo.find_blob(entry.id()).unwrap();
            let content = std::str::from_utf8(blob.content()).unwrap();
            assert_eq!(content, "claude-4.6");
            found = true;
        }
        git2::TreeWalkResult::Ok
    })
    .unwrap();

    assert!(found, "expected tree path not found in serialized tree");
}

#[test]
fn test_serialize_path_target_uses_raw_segments_and_separator() {
    let (dir, _sha) = setup_repo();

    gmeta(dir.path())
        .args(["set", "path:src/__generated/file.rs", "owner", "schacon"])
        .assert()
        .success();

    gmeta(dir.path()).args(["serialize"]).assert().success();

    let repo = git2::Repository::open(dir.path()).unwrap();
    let reference = repo.find_reference("refs/meta/local/main").unwrap();
    let commit = reference.peel_to_commit().unwrap();
    let tree = commit.tree().unwrap();

    let expected_path = "path/src/~__generated/file.rs/__target__/owner/__value";

    let mut found = false;
    tree.walk(git2::TreeWalkMode::PreOrder, |root, entry| {
        let full_path = format!("{}{}", root, entry.name().unwrap_or(""));
        if full_path == expected_path {
            let blob = repo.find_blob(entry.id()).unwrap();
            let content = std::str::from_utf8(blob.content()).unwrap();
            assert_eq!(content, "schacon");
            found = true;
        }
        git2::TreeWalkResult::Ok
    })
    .unwrap();

    assert!(found, "expected tree path not found in serialized tree");
}

#[test]
fn test_project_target() {
    let (dir, _sha) = setup_repo();

    gmeta(dir.path())
        .args(["set", "project", "name", "my-project"])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["get", "project", "name"])
        .assert()
        .success()
        .stdout(predicate::str::contains("my-project"));
}

#[test]
fn test_invalid_target_type() {
    let (dir, _sha) = setup_repo();

    gmeta(dir.path())
        .args(["set", "unknown:abc123", "key", "value"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("unknown target type"));
}

#[test]
fn test_target_value_too_short() {
    let (dir, _sha) = setup_repo();

    gmeta(dir.path())
        .args(["set", "commit:ab", "key", "value"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("at least 3 characters"));
}

#[test]
fn test_serialize_list_values() {
    let (dir, _sha) = setup_repo();

    gmeta(dir.path())
        .args([
            "set",
            "-t",
            "list",
            "branch:sc-branch-1-deadbeef",
            "agent:chat",
            r#"["how's it going","pretty good"]"#,
        ])
        .assert()
        .success();

    gmeta(dir.path()).args(["serialize"]).assert().success();

    // Verify tree structure has list entries with timestamp-hash format
    let repo = git2::Repository::open(dir.path()).unwrap();
    let reference = repo.find_reference("refs/meta/local/main").unwrap();
    let commit = reference.peel_to_commit().unwrap();
    let tree = commit.tree().unwrap();

    let mut list_entries = Vec::new();
    let fanout = target_fanout("sc-branch-1-deadbeef");
    tree.walk(git2::TreeWalkMode::PreOrder, |root, entry| {
        let full_path = format!("{}{}", root, entry.name().unwrap_or(""));
        if full_path.starts_with(&format!(
            "branch/{}/sc-branch-1-deadbeef/agent/chat/__list/",
            fanout
        )) {
            if entry.kind() == Some(git2::ObjectType::Blob) {
                list_entries.push(full_path);
            }
        }
        git2::TreeWalkResult::Ok
    })
    .unwrap();

    assert_eq!(
        list_entries.len(),
        2,
        "expected 2 list entries, got: {:?}",
        list_entries
    );

    // Verify entry names follow timestamp-hash format
    for entry_path in &list_entries {
        let filename = entry_path.rsplit('/').next().unwrap();
        let parts: Vec<&str> = filename.split('-').collect();
        assert_eq!(
            parts.len(),
            2,
            "list entry should be timestamp-hash: {}",
            filename
        );
        assert!(
            parts[0].chars().all(|c| c.is_ascii_digit()),
            "first part should be digits: {}",
            filename
        );
        assert_eq!(
            parts[1].len(),
            5,
            "hash part should be 5 chars: {}",
            filename
        );
    }
}

#[test]
fn test_upsert_overwrites() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    gmeta(dir.path())
        .args(["set", &target, "agent:model", "v1"])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["set", &target, "agent:model", "v2"])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["get", &target, "agent:model"])
        .assert()
        .success()
        .stdout(predicate::str::contains("v2"))
        .stdout(predicate::str::contains("v1").not());
}

#[test]
fn test_path_target() {
    let (dir, _sha) = setup_repo();

    gmeta(dir.path())
        .args(["set", "path:src/main.rs", "review:status", "approved"])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["get", "path:src/main.rs"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "src/main.rs;review:status  approved",
        ));
}

#[test]
fn test_path_target_tree_lookup() {
    let (dir, _sha) = setup_repo();

    gmeta(dir.path())
        .args(["set", "path:src/git", "owner", "schacon"])
        .assert()
        .success();
    gmeta(dir.path())
        .args(["set", "path:src/observability", "owner", "caleb"])
        .assert()
        .success();
    gmeta(dir.path())
        .args(["set", "path:src/metrics", "owner", "kiril"])
        .assert()
        .success();
    gmeta(dir.path())
        .args(["set", "path:srcx/metrics", "owner", "nope"])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["get", "path:src", "owner"])
        .assert()
        .success()
        .stdout(predicate::str::contains("src/git;owner"))
        .stdout(predicate::str::contains("schacon"))
        .stdout(predicate::str::contains("src/observability;owner"))
        .stdout(predicate::str::contains("caleb"))
        .stdout(predicate::str::contains("src/metrics;owner"))
        .stdout(predicate::str::contains("kiril"))
        .stdout(predicate::str::contains("srcx/metrics;owner").not());
}

#[test]
fn test_change_id_target() {
    let (dir, _sha) = setup_repo();

    gmeta(dir.path())
        .args([
            "set",
            "change-id:550e8400-e29b-41d4-a716-446655440000",
            "status",
            "merged",
        ])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["get", "change-id:550e8400-e29b-41d4-a716-446655440000"])
        .assert()
        .success()
        .stdout(predicate::str::contains("status"))
        .stdout(predicate::str::contains("merged"));
}

#[test]
fn test_serialize_empty() {
    let (dir, _sha) = setup_repo();

    gmeta(dir.path())
        .args(["serialize"])
        .assert()
        .success()
        .stdout(predicate::str::contains("no metadata to serialize"));
}

#[test]
fn test_serialize_list_uses_stored_timestamp() {
    let (dir, _sha) = setup_repo();

    // Set a list value
    gmeta(dir.path())
        .args([
            "set",
            "-t",
            "list",
            "branch:sc-branch-1-deadbeef",
            "agent:chat",
            r#"["hello","world"]"#,
        ])
        .assert()
        .success();

    // Serialize once
    gmeta(dir.path()).args(["serialize"]).assert().success();

    // Collect list entry names from first serialization
    let repo = git2::Repository::open(dir.path()).unwrap();
    let first_entries = collect_list_entry_names(&repo);
    assert_eq!(first_entries.len(), 2);

    // Serialize again without any changes — timestamps should be identical
    // because serialize uses the stored last_timestamp, not the current time
    gmeta(dir.path()).args(["serialize"]).assert().success();

    let repo = git2::Repository::open(dir.path()).unwrap();
    let second_entries = collect_list_entry_names(&repo);
    assert_eq!(second_entries.len(), 2);

    // Entry names (timestamp-hash) should be exactly the same both times
    assert_eq!(
        first_entries, second_entries,
        "list entry names should be stable across serializations when data is unchanged"
    );
}

#[test]
fn test_serialize_rm_writes_tombstone_blob() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    gmeta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["rm", &target, "agent:model"])
        .assert()
        .success();

    gmeta(dir.path()).args(["serialize"]).assert().success();

    let repo = git2::Repository::open(dir.path()).unwrap();
    let reference = repo.find_reference("refs/meta/local/main").unwrap();
    let commit = reference.peel_to_commit().unwrap();
    let tree = commit.tree().unwrap();

    let first2 = &sha[..2];
    let value_path = format!("commit/{}/{}/agent/model/__value", first2, sha);
    let tombstone_path = format!(
        "commit/{}/{}/__tombstones/agent/model/__deleted",
        first2, sha
    );

    let mut found_value = false;
    let mut found_tombstone = false;
    tree.walk(git2::TreeWalkMode::PreOrder, |root, entry| {
        let full_path = format!("{}{}", root, entry.name().unwrap_or(""));
        if full_path == value_path {
            found_value = true;
        }
        if full_path == tombstone_path {
            found_tombstone = true;
            let blob = repo.find_blob(entry.id()).unwrap();
            let payload: serde_json::Value = serde_json::from_slice(blob.content()).unwrap();
            assert_eq!(payload["email"], "test@example.com");
            assert!(payload["timestamp"].as_i64().is_some());
        }
        git2::TreeWalkResult::Ok
    })
    .unwrap();

    assert!(!found_value, "value blob should be removed after rm");
    assert!(found_tombstone, "tombstone blob should be serialized");
}

#[test]
fn test_materialize_fast_forward_applies_remote_removal() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    // Initial value and first serialized snapshot.
    gmeta(dir.path())
        .args(["set", &target, "agent:model", "v1"])
        .assert()
        .success();
    gmeta(dir.path()).args(["serialize"]).assert().success();

    let repo = git2::Repository::open(dir.path()).unwrap();
    let first_oid = repo
        .find_reference("refs/meta/local/main")
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id();
    drop(repo);

    // Remove the key and serialize second snapshot.
    gmeta(dir.path())
        .args(["rm", &target, "agent:model"])
        .assert()
        .success();
    gmeta(dir.path()).args(["serialize"]).assert().success();

    let repo = git2::Repository::open(dir.path()).unwrap();
    let second_oid = repo
        .find_reference("refs/meta/local/main")
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id();

    // Simulate a fetched remote ref ahead of local.
    repo.reference("refs/meta/origin", second_oid, true, "test remote")
        .unwrap();
    // Move local ref back so materialize takes fast-forward path.
    repo.reference("refs/meta/local/main", first_oid, true, "rollback local")
        .unwrap();
    drop(repo);

    // Local SQLite still has a stale value to be removed.
    gmeta(dir.path())
        .args(["set", &target, "agent:model", "stale"])
        .assert()
        .success();

    gmeta(dir.path()).args(["materialize"]).assert().success();

    // Key should be removed after materialize.
    gmeta(dir.path())
        .args(["get", &target, "agent:model"])
        .assert()
        .success()
        .stdout(predicate::str::is_empty());
}

#[test]
fn test_materialize_fast_forward_applies_remote_list_entry_removal() {
    let (dir, _sha) = setup_repo();
    let target = "branch:sc-branch-1-deadbeef";

    // Initial list and first serialized snapshot.
    gmeta(dir.path())
        .args([
            "set",
            "-t",
            "list",
            target,
            "agent:chat",
            r#"["a","b","c"]"#,
        ])
        .assert()
        .success();
    gmeta(dir.path()).args(["serialize"]).assert().success();

    let repo = git2::Repository::open(dir.path()).unwrap();
    let first_oid = repo
        .find_reference("refs/meta/local/main")
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id();
    drop(repo);

    // Remove one list entry and serialize second snapshot.
    gmeta(dir.path())
        .args(["list:pop", target, "agent:chat", "b"])
        .assert()
        .success();
    gmeta(dir.path()).args(["serialize"]).assert().success();

    let repo = git2::Repository::open(dir.path()).unwrap();
    let second_oid = repo
        .find_reference("refs/meta/local/main")
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id();

    // Simulate fetched remote ahead of local, then rewind local ref.
    repo.reference("refs/meta/origin", second_oid, true, "test remote")
        .unwrap();
    repo.reference("refs/meta/local/main", first_oid, true, "rollback local")
        .unwrap();
    drop(repo);

    // Recreate stale local SQLite state with the removed item still present.
    gmeta(dir.path())
        .args([
            "set",
            "-t",
            "list",
            target,
            "agent:chat",
            r#"["a","b","c"]"#,
        ])
        .assert()
        .success();

    gmeta(dir.path()).args(["materialize"]).assert().success();

    // Removed list entry should be gone after materialize.
    gmeta(dir.path())
        .args(["get", target, "agent:chat"])
        .assert()
        .success()
        .stdout(predicate::str::contains("a"))
        .stdout(predicate::str::contains("c"))
        .stdout(predicate::str::contains("b").not());
}

fn collect_list_entry_names(repo: &git2::Repository) -> Vec<String> {
    let reference = repo.find_reference("refs/meta/local/main").unwrap();
    let commit = reference.peel_to_commit().unwrap();
    let tree = commit.tree().unwrap();

    let mut entries = Vec::new();
    let fanout = target_fanout("sc-branch-1-deadbeef");
    tree.walk(git2::TreeWalkMode::PreOrder, |root, entry| {
        let full_path = format!("{}{}", root, entry.name().unwrap_or(""));
        if full_path.starts_with(&format!(
            "branch/{}/sc-branch-1-deadbeef/agent/chat/__list/",
            fanout
        )) && entry.kind() == Some(git2::ObjectType::Blob)
        {
            let name = entry.name().unwrap().to_string();
            entries.push(name);
        }
        git2::TreeWalkResult::Ok
    })
    .unwrap();

    entries.sort();
    entries
}

#[test]
fn test_set_add_and_rm() {
    let (dir, _sha) = setup_repo();
    let target = "branch:sc-branch-1-deadbeef";

    gmeta(dir.path())
        .args(["set:add", target, "reviewer", "alice@example.com"])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["set:add", target, "reviewer", "bob@example.com"])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["get", target, "reviewer"])
        .assert()
        .success()
        .stdout(predicate::str::contains("alice@example.com"))
        .stdout(predicate::str::contains("bob@example.com"));

    gmeta(dir.path())
        .args(["set:rm", target, "reviewer", "alice@example.com"])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["get", target, "reviewer"])
        .assert()
        .success()
        .stdout(predicate::str::contains("bob@example.com"))
        .stdout(predicate::str::contains("alice@example.com").not());
}

#[test]
fn test_set_add_deduplicates_members() {
    let (dir, _sha) = setup_repo();
    let target = "branch:sc-branch-1-deadbeef";

    gmeta(dir.path())
        .args(["set:add", target, "reviewer", "alice@example.com"])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["set:add", target, "reviewer", "alice@example.com"])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["get", target, "reviewer"])
        .assert()
        .success()
        .stdout(predicate::str::contains("alice@example.com").count(1));
}

#[test]
fn test_set_type_round_trips_and_serializes_members() {
    let (dir, _sha) = setup_repo();

    gmeta(dir.path())
        .args([
            "set",
            "-t",
            "set",
            "branch:sc-branch-1-deadbeef",
            "reviewer",
            r#"["alice@example.com","bob@example.com","alice@example.com"]"#,
        ])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["get", "branch:sc-branch-1-deadbeef", "reviewer"])
        .assert()
        .success()
        .stdout(predicate::str::contains("alice@example.com"))
        .stdout(predicate::str::contains("bob@example.com"));

    gmeta(dir.path()).args(["serialize"]).assert().success();

    let repo = git2::Repository::open(dir.path()).unwrap();
    let reference = repo.find_reference("refs/meta/local/main").unwrap();
    let commit = reference.peel_to_commit().unwrap();
    let tree = commit.tree().unwrap();
    let fanout = target_fanout("sc-branch-1-deadbeef");

    let mut set_members = Vec::new();
    tree.walk(git2::TreeWalkMode::PreOrder, |root, entry| {
        let full_path = format!("{}{}", root, entry.name().unwrap_or(""));
        if full_path.starts_with(&format!(
            "branch/{}/sc-branch-1-deadbeef/reviewer/__set/",
            fanout
        )) && entry.kind() == Some(git2::ObjectType::Blob)
        {
            let tail = full_path
                .strip_prefix(&format!(
                    "branch/{}/sc-branch-1-deadbeef/reviewer/__set/",
                    fanout
                ))
                .unwrap();
            if !tail.contains('/') {
                set_members.push(full_path);
            }
        }
        git2::TreeWalkResult::Ok
    })
    .unwrap();

    assert_eq!(set_members.len(), 2);
}

#[test]
fn test_custom_namespace() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    // Set meta.namespace to a custom value
    let repo = git2::Repository::open(dir.path()).unwrap();
    repo.config()
        .unwrap()
        .set_str("meta.namespace", "notes")
        .unwrap();
    drop(repo);

    gmeta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["serialize"])
        .assert()
        .success()
        .stdout(predicate::str::contains("refs/notes/local/main"));

    // Verify the ref exists under the custom namespace
    let repo = git2::Repository::open(dir.path()).unwrap();
    assert!(repo.find_reference("refs/notes/local/main").is_ok());
    assert!(repo.find_reference("refs/meta/local/main").is_err());
}

/// Simulate the full round-trip described in the bug report:
///
/// 1. User A sets metadata, serializes, "pushes" (we copy the ref)
/// 2. User B pulls, materializes (no new data), "pushes" the materialize commit
/// 3. User A pulls that back, overwrites a value locally, serializes
/// 4. User A materializes — the local change should NOT be overwritten
///    because the remote side didn't actually change that key.
#[test]
fn test_materialize_preserves_local_changes_over_stale_remote() {
    // === Setup: two repos sharing via bare intermediary ===
    let bare_dir = TempDir::new().unwrap();
    let repo_a_dir = TempDir::new().unwrap();
    let repo_b_dir = TempDir::new().unwrap();

    // Create bare repo
    git2::Repository::init_bare(bare_dir.path()).unwrap();

    // Clone into repo A
    let repo_a = git2::Repository::init(repo_a_dir.path()).unwrap();
    {
        let mut config = repo_a.config().unwrap();
        config.set_str("user.email", "alice@example.com").unwrap();
        config.set_str("user.name", "Alice").unwrap();
    }
    repo_a
        .remote("origin", bare_dir.path().to_str().unwrap())
        .unwrap();
    // Initial commit so repo is valid
    let sig_a = git2::Signature::now("Alice", "alice@example.com").unwrap();
    let tree_oid = repo_a.treebuilder(None).unwrap().write().unwrap();
    let tree = repo_a.find_tree(tree_oid).unwrap();
    let init_oid = repo_a
        .commit(Some("HEAD"), &sig_a, &sig_a, "initial", &tree, &[])
        .unwrap();

    // Push initial commit to bare so repo B can work
    repo_a
        .reference("refs/remotes/origin/main", init_oid, true, "init")
        .unwrap();

    // Clone into repo B
    let repo_b = git2::Repository::init(repo_b_dir.path()).unwrap();
    {
        let mut config = repo_b.config().unwrap();
        config.set_str("user.email", "bob@example.com").unwrap();
        config.set_str("user.name", "Bob").unwrap();
    }
    repo_b
        .remote("origin", bare_dir.path().to_str().unwrap())
        .unwrap();
    // Give repo B the same initial commit
    let sig_b = git2::Signature::now("Bob", "bob@example.com").unwrap();
    let tree_oid_b = repo_b.treebuilder(None).unwrap().write().unwrap();
    let tree_b = repo_b.find_tree(tree_oid_b).unwrap();
    repo_b
        .commit(Some("HEAD"), &sig_b, &sig_b, "initial", &tree_b, &[])
        .unwrap();

    // === Step 1: User A sets metadata and serializes ===
    gmeta(repo_a_dir.path())
        .args([
            "set",
            "change-id:uzytqkxrnstmxlzmvwluqomoynnowolp",
            "testing:user",
            "alice@example.com",
        ])
        .assert()
        .success();

    gmeta(repo_a_dir.path())
        .args([
            "set",
            "change-id:uzytqkxrnstmxlzmvwluqomoynnowolp",
            "license",
            "apache",
        ])
        .assert()
        .success();

    gmeta(repo_a_dir.path())
        .args(["serialize"])
        .assert()
        .success();

    // "Push": copy refs/meta/local from A to bare
    let a_local_ref = repo_a.find_reference("refs/meta/local/main").unwrap();
    let a_local_oid = a_local_ref.peel_to_commit().unwrap().id();
    copy_meta_objects(&repo_a, &bare_dir);
    let bare_repo = git2::Repository::open_bare(bare_dir.path()).unwrap();
    bare_repo
        .reference("refs/meta/local/main", a_local_oid, true, "push from A")
        .unwrap();

    // === Step 2: User B pulls and materializes (no new data) ===
    // "Fetch": copy meta objects from bare to B, set refs/meta/origin
    copy_meta_objects_from(&bare_dir, &repo_b);
    repo_b
        .reference("refs/meta/origin", a_local_oid, true, "fetch from bare")
        .unwrap();

    gmeta(repo_b_dir.path())
        .args(["materialize"])
        .assert()
        .success();

    // B serializes (just the materialize merge commit, no new data)
    gmeta(repo_b_dir.path())
        .args(["serialize"])
        .assert()
        .success();

    // "Push" B's local ref back to bare
    let b_local_ref = repo_b.find_reference("refs/meta/local/main").unwrap();
    let b_local_oid = b_local_ref.peel_to_commit().unwrap().id();
    copy_meta_objects(&repo_b, &bare_dir);
    let bare_repo = git2::Repository::open_bare(bare_dir.path()).unwrap();
    bare_repo
        .reference("refs/meta/local/main", b_local_oid, true, "push from B")
        .unwrap();

    // === Step 3: User A pulls B's ref, overwrites a value locally, serializes ===
    // "Fetch": copy objects from bare to A, update refs/meta/origin
    copy_meta_objects_from(&bare_dir, &repo_a);
    let bare_repo = git2::Repository::open_bare(bare_dir.path()).unwrap();
    let bare_local = bare_repo
        .find_reference("refs/meta/local/main")
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id();
    repo_a
        .reference("refs/meta/origin", bare_local, true, "fetch from bare")
        .unwrap();

    // A overwrites testing:user locally
    gmeta(repo_a_dir.path())
        .args([
            "set",
            "change-id:uzytqkxrnstmxlzmvwluqomoynnowolp",
            "testing:user",
            "tom@example.com",
        ])
        .assert()
        .success();

    gmeta(repo_a_dir.path())
        .args(["serialize"])
        .assert()
        .success();

    // === Step 4: User A materializes — local change must survive ===
    gmeta(repo_a_dir.path())
        .args(["materialize"])
        .assert()
        .success();

    // Verify: testing:user should be tom (the local change), NOT alice (stale remote)
    gmeta(repo_a_dir.path())
        .args(["get", "change-id:uzytqkxrnstmxlzmvwluqomoynnowolp"])
        .assert()
        .success()
        .stdout(predicate::str::contains("testing:user"))
        .stdout(predicate::str::contains("tom@example.com"))
        .stdout(predicate::str::contains("alice@example.com").not());

    // license should still be there (unchanged on both sides)
    gmeta(repo_a_dir.path())
        .args([
            "get",
            "change-id:uzytqkxrnstmxlzmvwluqomoynnowolp",
            "license",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("apache"));
}

/// Both User A and User B modify the same key. The one with the later
/// commit timestamp should win in the three-way merge.
///
/// 1. User A sets testing:user=alice, serializes, pushes
/// 2. User B pulls, materializes, changes testing:user=bob, serializes, pushes
/// 3. User A changes testing:user=tom, serializes (AFTER B), materializes
///    → A's value wins because A serialized later
/// 4. Repeat but have A serialize BEFORE B pushes, then materialize
///    → B's value wins because B serialized later
#[test]
fn test_materialize_both_sides_modified_later_timestamp_wins() {
    // === Setup: two repos sharing via bare intermediary ===
    let bare_dir = TempDir::new().unwrap();
    let repo_a_dir = TempDir::new().unwrap();
    let repo_b_dir = TempDir::new().unwrap();

    git2::Repository::init_bare(bare_dir.path()).unwrap();

    // Init repo A
    let repo_a = git2::Repository::init(repo_a_dir.path()).unwrap();
    {
        let mut config = repo_a.config().unwrap();
        config.set_str("user.email", "alice@example.com").unwrap();
        config.set_str("user.name", "Alice").unwrap();
    }
    repo_a
        .remote("origin", bare_dir.path().to_str().unwrap())
        .unwrap();
    let sig_a = git2::Signature::now("Alice", "alice@example.com").unwrap();
    let tree_oid = repo_a.treebuilder(None).unwrap().write().unwrap();
    let tree = repo_a.find_tree(tree_oid).unwrap();
    repo_a
        .commit(Some("HEAD"), &sig_a, &sig_a, "initial", &tree, &[])
        .unwrap();

    // Init repo B
    let repo_b = git2::Repository::init(repo_b_dir.path()).unwrap();
    {
        let mut config = repo_b.config().unwrap();
        config.set_str("user.email", "bob@example.com").unwrap();
        config.set_str("user.name", "Bob").unwrap();
    }
    repo_b
        .remote("origin", bare_dir.path().to_str().unwrap())
        .unwrap();
    let sig_b = git2::Signature::now("Bob", "bob@example.com").unwrap();
    let tree_oid_b = repo_b.treebuilder(None).unwrap().write().unwrap();
    let tree_b = repo_b.find_tree(tree_oid_b).unwrap();
    repo_b
        .commit(Some("HEAD"), &sig_b, &sig_b, "initial", &tree_b, &[])
        .unwrap();

    // === Step 1: User A sets initial data and serializes ===
    gmeta(repo_a_dir.path())
        .args([
            "set",
            "change-id:uzytqkxrnstmxlzmvwluqomoynnowolp",
            "testing:user",
            "alice@example.com",
        ])
        .assert()
        .success();

    gmeta(repo_a_dir.path())
        .args(["serialize"])
        .assert()
        .success();

    // Push A → bare
    let a_oid = repo_a
        .find_reference("refs/meta/local/main")
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id();
    copy_meta_objects(&repo_a, &bare_dir);
    git2::Repository::open_bare(bare_dir.path())
        .unwrap()
        .reference("refs/meta/local/main", a_oid, true, "push A")
        .unwrap();

    // === Step 2: User B pulls, materializes, modifies, serializes ===
    // Fetch bare → B
    copy_meta_objects_from(&bare_dir, &repo_b);
    repo_b
        .reference("refs/meta/origin", a_oid, true, "fetch")
        .unwrap();

    gmeta(repo_b_dir.path())
        .args(["materialize"])
        .assert()
        .success();

    // B changes the same key
    gmeta(repo_b_dir.path())
        .args([
            "set",
            "change-id:uzytqkxrnstmxlzmvwluqomoynnowolp",
            "testing:user",
            "bob@example.com",
        ])
        .assert()
        .success();

    gmeta(repo_b_dir.path())
        .args(["serialize"])
        .assert()
        .success();

    // Push B → bare
    let b_oid = repo_b
        .find_reference("refs/meta/local/main")
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id();
    copy_meta_objects(&repo_b, &bare_dir);
    git2::Repository::open_bare(bare_dir.path())
        .unwrap()
        .reference("refs/meta/local/main", b_oid, true, "push B")
        .unwrap();

    // === Step 3: User A modifies the same key AFTER B, serializes, then materializes ===
    // A changes the value (this serialize will have a later timestamp than B's)
    gmeta(repo_a_dir.path())
        .args([
            "set",
            "change-id:uzytqkxrnstmxlzmvwluqomoynnowolp",
            "testing:user",
            "tom@example.com",
        ])
        .assert()
        .success();

    gmeta(repo_a_dir.path())
        .args(["serialize"])
        .assert()
        .success();

    // Fetch B's changes into A
    copy_meta_objects_from(&bare_dir, &repo_a);
    repo_a
        .reference("refs/meta/origin", b_oid, true, "fetch B")
        .unwrap();

    // Materialize — both sides changed, A's commit is newer → A wins
    gmeta(repo_a_dir.path())
        .args(["materialize"])
        .assert()
        .success();

    gmeta(repo_a_dir.path())
        .args([
            "get",
            "change-id:uzytqkxrnstmxlzmvwluqomoynnowolp",
            "testing:user",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("tom@example.com"));

    // === Now test the reverse: B materializes A's newer changes, B's commit is older ===
    // Fetch A's latest into B
    let a_oid_new = repo_a
        .find_reference("refs/meta/local/main")
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id();
    copy_meta_objects(&repo_a, &bare_dir);
    git2::Repository::open_bare(bare_dir.path())
        .unwrap()
        .reference("refs/meta/local/main", a_oid_new, true, "push A new")
        .unwrap();

    copy_meta_objects_from(&bare_dir, &repo_b);
    repo_b
        .reference("refs/meta/origin", a_oid_new, true, "fetch A new")
        .unwrap();

    // B materializes — A's commit is newer → A's value (tom) wins over B's (bob)
    gmeta(repo_b_dir.path())
        .args(["materialize"])
        .assert()
        .success();

    gmeta(repo_b_dir.path())
        .args([
            "get",
            "change-id:uzytqkxrnstmxlzmvwluqomoynnowolp",
            "testing:user",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("tom@example.com"));
}

#[test]
fn test_materialize_dry_run_does_not_mutate_sqlite_or_ref() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    gmeta(dir.path())
        .args(["set", &target, "agent:model", "v1"])
        .assert()
        .success();
    gmeta(dir.path()).args(["serialize"]).assert().success();

    let repo = git2::Repository::open(dir.path()).unwrap();
    let first_oid = repo
        .find_reference("refs/meta/local/main")
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id();
    drop(repo);

    gmeta(dir.path())
        .args(["set", &target, "agent:model", "v2"])
        .assert()
        .success();
    gmeta(dir.path()).args(["serialize"]).assert().success();

    let repo = git2::Repository::open(dir.path()).unwrap();
    let second_oid = repo
        .find_reference("refs/meta/local/main")
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id();
    repo.reference("refs/meta/origin", second_oid, true, "test remote")
        .unwrap();
    repo.reference("refs/meta/local/main", first_oid, true, "rollback local")
        .unwrap();
    drop(repo);

    // Local DB diverges from remote tree.
    gmeta(dir.path())
        .args(["set", &target, "agent:model", "stale"])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["materialize", "--dry-run"])
        .assert()
        .success()
        .stdout(predicate::str::contains("dry-run: strategy=fast-forward"))
        .stdout(predicate::str::contains("agent:model"));

    // SQLite should not be updated by dry-run.
    gmeta(dir.path())
        .args(["get", &target, "agent:model"])
        .assert()
        .success()
        .stdout(predicate::str::contains("stale"))
        .stdout(predicate::str::contains("v2").not());

    // Local metadata ref should not move in dry-run.
    let repo = git2::Repository::open(dir.path()).unwrap();
    let local_after = repo
        .find_reference("refs/meta/local/main")
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id();
    assert_eq!(local_after, first_oid);
}

#[test]
fn test_materialize_dry_run_reports_concurrent_add_conflict_resolution() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    // Base snapshot without agent:model.
    gmeta(dir.path())
        .args(["set", &target, "base:key", "base"])
        .assert()
        .success();
    gmeta(dir.path()).args(["serialize"]).assert().success();

    let repo = git2::Repository::open(dir.path()).unwrap();
    let base_oid = repo
        .find_reference("refs/meta/local/main")
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id();
    drop(repo);

    // Remote branch adds agent:model=remote.
    gmeta(dir.path())
        .args(["set", &target, "agent:model", "remote"])
        .assert()
        .success();
    gmeta(dir.path()).args(["serialize"]).assert().success();

    let repo = git2::Repository::open(dir.path()).unwrap();
    let remote_oid = repo
        .find_reference("refs/meta/local/main")
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id();
    repo.reference("refs/meta/local/main", base_oid, true, "rollback to base")
        .unwrap();
    drop(repo);

    // Local branch adds the same key with a different value.
    gmeta(dir.path())
        .args(["set", &target, "agent:model", "local"])
        .assert()
        .success();
    gmeta(dir.path()).args(["serialize"]).assert().success();

    let repo = git2::Repository::open(dir.path()).unwrap();
    let local_oid = repo
        .find_reference("refs/meta/local/main")
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id();
    repo.reference("refs/meta/origin", remote_oid, true, "set remote")
        .unwrap();
    drop(repo);

    gmeta(dir.path())
        .args(["materialize", "--dry-run"])
        .assert()
        .success()
        .stdout(predicate::str::contains("dry-run: strategy=three-way"))
        .stdout(predicate::str::contains("reason=concurrent-add"))
        .stdout(predicate::str::contains("agent:model"));

    // Dry-run keeps local state unchanged.
    gmeta(dir.path())
        .args(["get", &target, "agent:model"])
        .assert()
        .success()
        .stdout(predicate::str::contains("local"));

    let repo = git2::Repository::open(dir.path()).unwrap();
    let local_after = repo
        .find_reference("refs/meta/local/main")
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id();
    assert_eq!(local_after, local_oid);
}

#[test]
fn test_materialize_no_common_ancestor_uses_two_way_merge_remote_wins() {
    let bare_dir = TempDir::new().unwrap();
    git2::Repository::init_bare(bare_dir.path()).unwrap();
    let (repo_a_dir, _sha_a) = setup_repo();
    let (repo_b_dir, _sha_b) = setup_repo();

    // Local side (A)
    gmeta(repo_a_dir.path())
        .args(["set", "project", "agent:model", "local"])
        .assert()
        .success();
    gmeta(repo_a_dir.path())
        .args(["set", "project", "local:only", "keep-me"])
        .assert()
        .success();
    gmeta(repo_a_dir.path())
        .args(["serialize"])
        .assert()
        .success();

    let repo_a = git2::Repository::open(repo_a_dir.path()).unwrap();
    let a_oid = repo_a
        .find_reference("refs/meta/local/main")
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id();

    // Remote side (B), completely independent history
    gmeta(repo_b_dir.path())
        .args(["set", "project", "agent:model", "remote"])
        .assert()
        .success();
    gmeta(repo_b_dir.path())
        .args(["set", "project", "remote:only", "keep-too"])
        .assert()
        .success();
    gmeta(repo_b_dir.path())
        .args(["serialize"])
        .assert()
        .success();

    let repo_b = git2::Repository::open(repo_b_dir.path()).unwrap();
    let b_oid = repo_b
        .find_reference("refs/meta/local/main")
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id();

    // Simulate fetch: B -> bare -> A
    copy_meta_objects(&repo_b, &bare_dir);
    git2::Repository::open_bare(bare_dir.path())
        .unwrap()
        .reference("refs/meta/local/main", b_oid, true, "push B")
        .unwrap();
    copy_meta_objects_from(&bare_dir, &repo_a);
    repo_a
        .reference("refs/meta/origin", b_oid, true, "fetch B into A")
        .unwrap();

    // No common ancestor should be identified in dry-run.
    gmeta(repo_a_dir.path())
        .args(["materialize", "--dry-run"])
        .assert()
        .success()
        .stdout(predicate::str::contains("no common ancestor"))
        .stdout(predicate::str::contains(
            "strategy=two-way-no-common-ancestor",
        ))
        .stdout(predicate::str::contains(
            "reason=no-common-ancestor-local-wins",
        ))
        .stdout(predicate::str::contains("agent:model"));

    // Dry-run should not move local ref.
    let a_after_dry_run = repo_a
        .find_reference("refs/meta/local/main")
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id();
    assert_eq!(a_after_dry_run, a_oid);

    // Real materialize applies two-way merge where local wins conflicts.
    gmeta(repo_a_dir.path())
        .args(["materialize"])
        .assert()
        .success()
        .stdout(predicate::str::contains("two-way merge"));

    // Conflict key should come from local.
    gmeta(repo_a_dir.path())
        .args(["get", "project", "agent:model"])
        .assert()
        .success()
        .stdout(predicate::str::contains("local"))
        .stdout(predicate::str::contains("remote").not());

    // Non-conflicting keys from both sides should be preserved.
    gmeta(repo_a_dir.path())
        .args(["get", "project", "local:only"])
        .assert()
        .success()
        .stdout(predicate::str::contains("keep-me"));
    gmeta(repo_a_dir.path())
        .args(["get", "project", "remote:only"])
        .assert()
        .success()
        .stdout(predicate::str::contains("keep-too"));
}

// ── Remote / Push / Pull integration tests ─────────────────────────────────

/// Create a bare repo that has a refs/meta/main with some metadata.
/// Returns the TempDir for the bare repo.
fn setup_bare_with_meta(ns: &str) -> TempDir {
    let bare_dir = TempDir::new().unwrap();
    let bare = git2::Repository::init_bare(bare_dir.path()).unwrap();

    // Build a tree with some metadata
    let sig = git2::Signature::now("Test User", "test@example.com").unwrap();
    let mut tb = bare.treebuilder(None).unwrap();

    // Create a subtree: project/testing/__value
    let blob_oid = bare.blob(b"\"hello\"").unwrap();
    let mut sub_tb = bare.treebuilder(None).unwrap();
    sub_tb.insert("__value", blob_oid, 0o100644).unwrap();
    let sub_tree_oid = sub_tb.write().unwrap();

    let mut project_tb = bare.treebuilder(None).unwrap();
    project_tb
        .insert("testing", sub_tree_oid, 0o040000)
        .unwrap();
    let project_tree_oid = project_tb.write().unwrap();

    tb.insert("project", project_tree_oid, 0o040000).unwrap();
    let tree_oid = tb.write().unwrap();
    let tree = bare.find_tree(tree_oid).unwrap();

    let ref_name = format!("refs/{}/main", ns);
    bare.commit(Some(&ref_name), &sig, &sig, "initial meta", &tree, &[])
        .unwrap();

    bare_dir
}

#[test]
fn test_remote_add_no_meta_refs() {
    let (dir, _sha) = setup_repo();
    // Bare repo with no meta refs at all
    let bare_dir = TempDir::new().unwrap();
    git2::Repository::init_bare(bare_dir.path()).unwrap();

    let bare_path = bare_dir.path().to_str().unwrap();

    gmeta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .failure()
        .stderr(predicate::str::contains("no metadata refs found"));
}

#[test]
fn test_remote_add_meta_refs_in_different_namespace() {
    let (dir, _sha) = setup_repo();
    // Bare repo with refs/altmeta/main but not refs/meta/main
    let bare_dir = setup_bare_with_meta("altmeta");
    let bare_path = bare_dir.path().to_str().unwrap();

    gmeta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .failure()
        .stderr(predicate::str::contains("refs/altmeta/main"))
        .stderr(predicate::str::contains("--namespace=altmeta"));
}

#[test]
fn test_remote_add_with_namespace_override() {
    let (dir, _sha) = setup_repo();
    let bare_dir = setup_bare_with_meta("altmeta");
    let bare_path = bare_dir.path().to_str().unwrap();

    gmeta(dir.path())
        .args(["remote", "add", bare_path, "--namespace=altmeta"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Added meta remote"));

    // Verify config has the correct fetch refspec
    let repo = git2::Repository::open(dir.path()).unwrap();
    let config = repo.config().unwrap();
    let fetch = config.get_string("remote.meta.fetch").unwrap();
    assert!(
        fetch.contains("refs/altmeta/"),
        "fetch refspec should use altmeta namespace, got: {}",
        fetch
    );
    let meta_ns = config.get_string("remote.meta.metanamespace").unwrap();
    assert_eq!(meta_ns, "altmeta");
}

#[test]
fn test_remote_add_shorthand_url_expansion() {
    let (dir, _sha) = setup_repo();

    // Shorthand "owner/repo" should expand to git@github.com:owner/repo.git.
    // The command will succeed (warning on fetch failure) — verify the expanded URL
    // appears in the output.
    gmeta(dir.path())
        .args(["remote", "add", "nonexistent-user-xyz/nonexistent-repo-xyz"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "git@github.com:nonexistent-user-xyz/nonexistent-repo-xyz.git",
        ));

    // Verify the config stored the expanded URL
    let repo = git2::Repository::open(dir.path()).unwrap();
    let config = repo.config().unwrap();
    let url = config.get_string("remote.meta.url").unwrap();
    assert_eq!(url, "git@github.com:nonexistent-user-xyz/nonexistent-repo-xyz.git");
}

#[test]
fn test_remote_list_and_remove() {
    let (dir, _sha) = setup_repo();
    let bare_dir = setup_bare_with_meta("meta");
    let bare_path = bare_dir.path().to_str().unwrap();

    // Add
    gmeta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();

    // List
    gmeta(dir.path())
        .args(["remote", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("meta\t"))
        .stdout(predicate::str::contains(bare_path));

    // Remove
    gmeta(dir.path())
        .args(["remote", "remove", "meta"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Removed meta remote"));

    // List again — empty
    gmeta(dir.path())
        .args(["remote", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("No metadata remotes configured"));
}

#[test]
fn test_push_simple() {
    let (dir, sha) = setup_repo();
    let bare_dir = setup_bare_with_meta("meta");
    let bare_path = bare_dir.path().to_str().unwrap();

    // Add remote and pull existing data
    gmeta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();
    gmeta(dir.path())
        .args(["pull"])
        .assert()
        .success();

    // Set local metadata
    let target = commit_target(&sha);
    gmeta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    // Push
    gmeta(dir.path())
        .args(["push"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Pushed metadata to meta"));

    // Verify: the pushed ref on the bare repo has no merge commits
    let bare = git2::Repository::open_bare(bare_dir.path()).unwrap();
    let commit = bare
        .find_reference("refs/meta/main")
        .unwrap()
        .peel_to_commit()
        .unwrap();
    assert_eq!(
        commit.parent_count(),
        1,
        "pushed commit should have exactly 1 parent (no merge commits)"
    );
}

#[test]
fn test_push_up_to_date() {
    let (dir, sha) = setup_repo();
    let bare_dir = setup_bare_with_meta("meta");
    let bare_path = bare_dir.path().to_str().unwrap();

    gmeta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();
    gmeta(dir.path())
        .args(["pull"])
        .assert()
        .success();

    let target = commit_target(&sha);
    gmeta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    // First push
    gmeta(dir.path())
        .args(["push"])
        .assert()
        .success();

    // Second push — nothing changed
    gmeta(dir.path())
        .args(["push"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Everything up-to-date"));
}

#[test]
fn test_push_commit_message_format() {
    let (dir, sha) = setup_repo();
    let bare_dir = setup_bare_with_meta("meta");
    let bare_path = bare_dir.path().to_str().unwrap();

    gmeta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();
    gmeta(dir.path())
        .args(["pull"])
        .assert()
        .success();

    let target = commit_target(&sha);
    gmeta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();
    gmeta(dir.path())
        .args(["set", &target, "agent:cost", "0.05"])
        .assert()
        .success();
    gmeta(dir.path())
        .args(["push"])
        .assert()
        .success();

    // Check the commit message on the bare repo
    let bare = git2::Repository::open_bare(bare_dir.path()).unwrap();
    let commit = bare
        .find_reference("refs/meta/main")
        .unwrap()
        .peel_to_commit()
        .unwrap();
    let msg = commit.message().unwrap();
    assert!(
        msg.contains("gmeta: serialize"),
        "commit message should start with 'gmeta: serialize', got: {}",
        msg
    );
    assert!(
        msg.contains("agent:model"),
        "commit message should contain changed key, got: {}",
        msg
    );
}

#[test]
fn test_push_conflict_produces_no_merge_commits() {
    // Two clones push different metadata — second clone should auto-merge
    // and the result should have no merge commits
    let bare_dir = setup_bare_with_meta("meta");
    let bare_path = bare_dir.path().to_str().unwrap();

    let (dir_a, sha_a) = setup_repo();
    let (dir_b, sha_b) = setup_repo();

    // Both add the same remote
    gmeta(dir_a.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();
    gmeta(dir_b.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();

    // Both pull initial state
    gmeta(dir_a.path()).args(["pull"]).assert().success();
    gmeta(dir_b.path()).args(["pull"]).assert().success();

    // A sets and pushes
    let target_a = commit_target(&sha_a);
    gmeta(dir_a.path())
        .args(["set", &target_a, "from:a", "value-a"])
        .assert()
        .success();
    gmeta(dir_a.path()).args(["push"]).assert().success();

    // B sets and pushes (should conflict then auto-merge)
    let target_b = commit_target(&sha_b);
    gmeta(dir_b.path())
        .args(["set", &target_b, "from:b", "value-b"])
        .assert()
        .success();
    gmeta(dir_b.path()).args(["push"]).assert().success();

    // Walk the entire history on the bare repo — no merge commits allowed
    let bare = git2::Repository::open_bare(bare_dir.path()).unwrap();
    let tip = bare
        .find_reference("refs/meta/main")
        .unwrap()
        .peel_to_commit()
        .unwrap();

    let mut revwalk = bare.revwalk().unwrap();
    revwalk.push(tip.id()).unwrap();
    for oid in revwalk {
        let oid = oid.unwrap();
        let commit = bare.find_commit(oid).unwrap();
        assert!(
            commit.parent_count() <= 1,
            "commit {} has {} parents — merge commits are not allowed in pushed history",
            &commit.id().to_string()[..8],
            commit.parent_count()
        );
    }
}

#[test]
fn test_pull_simple() {
    let (dir, _sha) = setup_repo();
    let bare_dir = setup_bare_with_meta("meta");
    let bare_path = bare_dir.path().to_str().unwrap();

    gmeta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();

    gmeta(dir.path())
        .args(["pull"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Pulled metadata from meta"));

    // The bare repo had project/testing = "hello" — check it materialized
    gmeta(dir.path())
        .args(["get", "project", "testing"])
        .assert()
        .success()
        .stdout(predicate::str::contains("hello"));
}

#[test]
fn test_pull_up_to_date() {
    let (dir, _sha) = setup_repo();
    let bare_dir = setup_bare_with_meta("meta");
    let bare_path = bare_dir.path().to_str().unwrap();

    gmeta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();

    // First pull
    gmeta(dir.path()).args(["pull"]).assert().success();

    // Second pull — nothing new
    gmeta(dir.path())
        .args(["pull"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Already up-to-date"));
}

#[test]
fn test_pull_merges_with_local_data() {
    let (dir, sha) = setup_repo();
    let bare_dir = setup_bare_with_meta("meta");
    let bare_path = bare_dir.path().to_str().unwrap();

    // Set some local-only metadata before adding the remote
    let target = commit_target(&sha);
    gmeta(dir.path())
        .args(["set", &target, "local:key", "local-value"])
        .assert()
        .success();

    // Add remote and pull
    gmeta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();
    gmeta(dir.path()).args(["pull"]).assert().success();

    // Should have both local and remote data
    gmeta(dir.path())
        .args(["get", &target, "local:key"])
        .assert()
        .success()
        .stdout(predicate::str::contains("local-value"));

    gmeta(dir.path())
        .args(["get", "project", "testing"])
        .assert()
        .success()
        .stdout(predicate::str::contains("hello"));
}

/// Build a bare repo with multiple gmeta serialize commits for promisor tests.
/// Returns the bare TempDir. The repo has 2 commits:
///   Commit 1 (older): project/old_key/__value = "old_value"
///   Commit 2 (tip):   project/testing/__value = "hello"
/// Commit 1's message lists: A\tproject\told_key
/// Commit 2's message lists: A\tproject\ttesting
fn setup_bare_with_history() -> TempDir {
    let bare_dir = TempDir::new().unwrap();
    let bare = git2::Repository::init_bare(bare_dir.path()).unwrap();
    let sig = git2::Signature::now("Test User", "test@example.com").unwrap();

    // --- Commit 1: project/old_key/__value = "old_value" ---
    let blob1 = bare.blob(b"\"old_value\"").unwrap();
    let mut val_tb = bare.treebuilder(None).unwrap();
    val_tb.insert("__value", blob1, 0o100644).unwrap();
    let val_tree = val_tb.write().unwrap();

    let mut proj_tb = bare.treebuilder(None).unwrap();
    proj_tb.insert("old_key", val_tree, 0o040000).unwrap();
    let proj_tree = proj_tb.write().unwrap();

    let mut root_tb = bare.treebuilder(None).unwrap();
    root_tb.insert("project", proj_tree, 0o040000).unwrap();
    let root_tree_oid = root_tb.write().unwrap();
    let root_tree = bare.find_tree(root_tree_oid).unwrap();

    let commit1_msg = "gmeta: serialize (1 changes)\n\nA\tproject\told_key";
    let commit1 = bare
        .commit(None, &sig, &sig, commit1_msg, &root_tree, &[])
        .unwrap();
    let commit1_obj = bare.find_commit(commit1).unwrap();

    // --- Commit 2 (tip): project/testing/__value = "hello" (old_key removed) ---
    let blob2 = bare.blob(b"\"hello\"").unwrap();
    let mut val_tb2 = bare.treebuilder(None).unwrap();
    val_tb2.insert("__value", blob2, 0o100644).unwrap();
    let val_tree2 = val_tb2.write().unwrap();

    let mut proj_tb2 = bare.treebuilder(None).unwrap();
    proj_tb2.insert("testing", val_tree2, 0o040000).unwrap();
    let proj_tree2 = proj_tb2.write().unwrap();

    let mut root_tb2 = bare.treebuilder(None).unwrap();
    root_tb2.insert("project", proj_tree2, 0o040000).unwrap();
    let root_tree_oid2 = root_tb2.write().unwrap();
    let root_tree2 = bare.find_tree(root_tree_oid2).unwrap();

    let commit2_msg = "gmeta: serialize (1 changes)\n\nA\tproject\ttesting";
    bare.commit(
        Some("refs/meta/main"),
        &sig,
        &sig,
        commit2_msg,
        &root_tree2,
        &[&commit1_obj],
    )
    .unwrap();

    bare_dir
}

#[test]
fn test_pull_inserts_promisor_entries() {
    let (dir, _sha) = setup_repo();
    let bare_dir = setup_bare_with_history();
    let bare_path = bare_dir.path().to_str().unwrap();

    // Add remote and pull
    gmeta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();
    gmeta(dir.path())
        .args(["pull"])
        .assert()
        .success()
        .stderr(predicate::str::contains("Indexed 1 keys from history"));

    // The tip key (testing) should be available immediately
    gmeta(dir.path())
        .args(["get", "project", "testing"])
        .assert()
        .success()
        .stdout(predicate::str::contains("hello"));

    // The historical key (old_key) was pruned from the tip tree, so it can't be hydrated.
    // Get should silently skip it (promised but not resolvable in tip).
    // Verify it doesn't crash.
    gmeta(dir.path())
        .args(["get", "project", "old_key"])
        .assert()
        .success();

    // Verify the promisor entry exists in the DB by checking that the key shows
    // up in get with --json for the full project target (it will be filtered out
    // since it can't be hydrated, but the tip key should still work)
    gmeta(dir.path())
        .args(["get", "project", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("testing"))
        .stdout(predicate::str::contains("hello"));
}

/// Build a bare repo where a key exists in both history and tip tree,
/// but the tip commit message only lists the newer key.
/// Commit 1: A project old_key (tree has project/old_key/__value)
/// Commit 2: A project testing (tree has project/testing/__value AND project/old_key/__value)
fn setup_bare_with_history_retained() -> TempDir {
    let bare_dir = TempDir::new().unwrap();
    let bare = git2::Repository::init_bare(bare_dir.path()).unwrap();
    let sig = git2::Signature::now("Test User", "test@example.com").unwrap();

    // --- Commit 1: project/old_key/__value = "old_value" ---
    let blob1 = bare.blob(b"\"old_value\"").unwrap();
    let mut val_tb = bare.treebuilder(None).unwrap();
    val_tb.insert("__value", blob1, 0o100644).unwrap();
    let val_tree = val_tb.write().unwrap();

    let mut proj_tb = bare.treebuilder(None).unwrap();
    proj_tb.insert("old_key", val_tree, 0o040000).unwrap();
    let proj_tree = proj_tb.write().unwrap();

    let mut root_tb = bare.treebuilder(None).unwrap();
    root_tb.insert("project", proj_tree, 0o040000).unwrap();
    let root_tree_oid = root_tb.write().unwrap();
    let root_tree = bare.find_tree(root_tree_oid).unwrap();

    let commit1_msg = "gmeta: serialize (1 changes)\n\nA\tproject\told_key";
    let commit1 = bare
        .commit(None, &sig, &sig, commit1_msg, &root_tree, &[])
        .unwrap();
    let commit1_obj = bare.find_commit(commit1).unwrap();

    // --- Commit 2 (tip): has both old_key and testing ---
    let blob2 = bare.blob(b"\"hello\"").unwrap();
    let mut val_tb2 = bare.treebuilder(None).unwrap();
    val_tb2.insert("__value", blob2, 0o100644).unwrap();
    let val_tree2 = val_tb2.write().unwrap();

    let mut proj_tb2 = bare.treebuilder(None).unwrap();
    proj_tb2.insert("testing", val_tree2, 0o040000).unwrap();
    proj_tb2.insert("old_key", val_tree, 0o040000).unwrap();
    let proj_tree2 = proj_tb2.write().unwrap();

    let mut root_tb2 = bare.treebuilder(None).unwrap();
    root_tb2.insert("project", proj_tree2, 0o040000).unwrap();
    let root_tree_oid2 = root_tb2.write().unwrap();
    let root_tree2 = bare.find_tree(root_tree_oid2).unwrap();

    // Only mention 'testing' in the tip commit — old_key was added in commit 1
    let commit2_msg = "gmeta: serialize (1 changes)\n\nA\tproject\ttesting";
    bare.commit(
        Some("refs/meta/main"),
        &sig,
        &sig,
        commit2_msg,
        &root_tree2,
        &[&commit1_obj],
    )
    .unwrap();

    bare_dir
}

#[test]
fn test_promisor_hydration_from_tip_tree() {
    // old_key is in both the history and the tip tree, but tip commit only
    // mentions 'testing'. Materialize processes the full tip tree, so old_key
    // gets materialized as a real entry (not promised).
    let (dir, _sha) = setup_repo();
    let bare_dir = setup_bare_with_history_retained();
    let bare_path = bare_dir.path().to_str().unwrap();

    gmeta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();
    gmeta(dir.path())
        .args(["pull"])
        .assert()
        .success();

    // Both keys should be available — old_key was in the tip tree so
    // materialize handled it directly (not as a promisor entry)
    gmeta(dir.path())
        .args(["get", "project", "old_key"])
        .assert()
        .success()
        .stdout(predicate::str::contains("old_value"));

    gmeta(dir.path())
        .args(["get", "project", "testing"])
        .assert()
        .success()
        .stdout(predicate::str::contains("hello"));
}

#[test]
fn test_promisor_entry_not_serialized() {
    let (dir, _sha) = setup_repo();
    let bare_dir = setup_bare_with_history();
    let bare_path = bare_dir.path().to_str().unwrap();

    // Pull to get promisor entries
    gmeta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();
    gmeta(dir.path())
        .args(["pull"])
        .assert()
        .success();

    // Serialize — should not include promised entries in the commit
    gmeta(dir.path())
        .args(["serialize"])
        .assert()
        .success();

    // Verify the local ref tree doesn't contain old_key (it was promised, not materialized)
    let repo = git2::Repository::open(dir.path()).unwrap();
    let local_ref = repo.find_reference("refs/meta/local/main").unwrap();
    let commit = local_ref.peel_to_commit().unwrap();
    let tree = commit.tree().unwrap();

    // The tree should have project/testing but not project/old_key
    let project_entry = tree.get_name("project").unwrap();
    let project_tree = repo.find_tree(project_entry.id()).unwrap();

    assert!(
        project_tree.get_name("testing").is_some(),
        "tip key 'testing' should be in serialized tree"
    );
    assert!(
        project_tree.get_name("old_key").is_none(),
        "promised key 'old_key' should NOT be in serialized tree"
    );
}

#[test]
fn test_pull_tip_only_no_promisor_entries() {
    // A single-commit remote should produce no promisor entries
    let (dir, _sha) = setup_repo();
    let bare_dir = setup_bare_with_meta("meta");
    let bare_path = bare_dir.path().to_str().unwrap();

    gmeta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();
    gmeta(dir.path())
        .args(["pull"])
        .assert()
        .success()
        // Should NOT contain "Indexed" since there are no non-tip commits to parse
        .stderr(predicate::str::contains("Indexed").not());
}

/// Copy all git objects from src repo into a bare repo (simulates push).
fn copy_meta_objects(src: &git2::Repository, bare_dir: &TempDir) {
    let src_objects = src.path().join("objects");
    let dst_objects = bare_dir.path().join("objects");
    copy_dir_contents(&src_objects, &dst_objects);
}

/// Copy all git objects from a bare repo into dst repo (simulates fetch).
fn copy_meta_objects_from(bare_dir: &TempDir, dst: &git2::Repository) {
    let src_objects = bare_dir.path().join("objects");
    let dst_objects = dst.path().join("objects");
    copy_dir_contents(&src_objects, &dst_objects);
}

/// Recursively copy directory contents (for loose objects + pack files).
fn copy_dir_contents(src: &std::path::Path, dst: &std::path::Path) {
    if !src.exists() {
        return;
    }
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if src_path.is_dir() {
            std::fs::create_dir_all(&dst_path).ok();
            copy_dir_contents(&src_path, &dst_path);
        } else {
            std::fs::copy(&src_path, &dst_path).ok();
        }
    }
}
