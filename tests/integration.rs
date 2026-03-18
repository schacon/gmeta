use assert_cmd::Command;
use predicates::prelude::*;
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
        .stdout(predicate::str::contains("refs/meta/local"));

    // Verify the ref exists and contains the right tree structure
    let repo = git2::Repository::open(dir.path()).unwrap();
    let reference = repo.find_reference("refs/meta/local").unwrap();
    let commit = reference.peel_to_commit().unwrap();
    let tree = commit.tree().unwrap();

    // Build the expected path from the full SHA
    let first2 = &sha[..2];
    let expected_path = format!("commit/{}/{}/k/agent/model/__value", first2, sha);

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
    let reference = repo.find_reference("refs/meta/local").unwrap();
    let commit = reference.peel_to_commit().unwrap();
    let tree = commit.tree().unwrap();

    let mut list_entries = Vec::new();
    tree.walk(git2::TreeWalkMode::PreOrder, |root, entry| {
        let full_path = format!("{}{}", root, entry.name().unwrap_or(""));
        if full_path.starts_with("branch/d6/sc-branch-1-deadbeef/k/agent/chat/__list/") {
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
        .stdout(predicate::str::contains("review:status"))
        .stdout(predicate::str::contains("approved"));
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
    let reference = repo.find_reference("refs/meta/local").unwrap();
    let commit = reference.peel_to_commit().unwrap();
    let tree = commit.tree().unwrap();

    let first2 = &sha[..2];
    let value_path = format!("commit/{}/{}/k/agent/model/__value", first2, sha);
    let tombstone_path = format!(
        "commit/{}/{}/__tombstones/k/agent/model/__deleted",
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
        .find_reference("refs/meta/local")
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
        .find_reference("refs/meta/local")
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id();

    // Simulate a fetched remote ref ahead of local.
    repo.reference("refs/meta/origin", second_oid, true, "test remote")
        .unwrap();
    // Move local ref back so materialize takes fast-forward path.
    repo.reference("refs/meta/local", first_oid, true, "rollback local")
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
        .find_reference("refs/meta/local")
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
        .find_reference("refs/meta/local")
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id();

    // Simulate fetched remote ahead of local, then rewind local ref.
    repo.reference("refs/meta/origin", second_oid, true, "test remote")
        .unwrap();
    repo.reference("refs/meta/local", first_oid, true, "rollback local")
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
    let reference = repo.find_reference("refs/meta/local").unwrap();
    let commit = reference.peel_to_commit().unwrap();
    let tree = commit.tree().unwrap();

    let mut entries = Vec::new();
    tree.walk(git2::TreeWalkMode::PreOrder, |root, entry| {
        let full_path = format!("{}{}", root, entry.name().unwrap_or(""));
        if full_path.starts_with("branch/d6/sc-branch-1-deadbeef/k/agent/chat/__list/")
            && entry.kind() == Some(git2::ObjectType::Blob)
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
        .stdout(predicate::str::contains("refs/notes/local"));

    // Verify the ref exists under the custom namespace
    let repo = git2::Repository::open(dir.path()).unwrap();
    assert!(repo.find_reference("refs/notes/local").is_ok());
    assert!(repo.find_reference("refs/meta/local").is_err());
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
    let a_local_ref = repo_a.find_reference("refs/meta/local").unwrap();
    let a_local_oid = a_local_ref.peel_to_commit().unwrap().id();
    copy_meta_objects(&repo_a, &bare_dir);
    let bare_repo = git2::Repository::open_bare(bare_dir.path()).unwrap();
    bare_repo
        .reference("refs/meta/local", a_local_oid, true, "push from A")
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
    let b_local_ref = repo_b.find_reference("refs/meta/local").unwrap();
    let b_local_oid = b_local_ref.peel_to_commit().unwrap().id();
    copy_meta_objects(&repo_b, &bare_dir);
    let bare_repo = git2::Repository::open_bare(bare_dir.path()).unwrap();
    bare_repo
        .reference("refs/meta/local", b_local_oid, true, "push from B")
        .unwrap();

    // === Step 3: User A pulls B's ref, overwrites a value locally, serializes ===
    // "Fetch": copy objects from bare to A, update refs/meta/origin
    copy_meta_objects_from(&bare_dir, &repo_a);
    let bare_repo = git2::Repository::open_bare(bare_dir.path()).unwrap();
    let bare_local = bare_repo
        .find_reference("refs/meta/local")
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
        .find_reference("refs/meta/local")
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id();
    copy_meta_objects(&repo_a, &bare_dir);
    git2::Repository::open_bare(bare_dir.path())
        .unwrap()
        .reference("refs/meta/local", a_oid, true, "push A")
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
        .find_reference("refs/meta/local")
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id();
    copy_meta_objects(&repo_b, &bare_dir);
    git2::Repository::open_bare(bare_dir.path())
        .unwrap()
        .reference("refs/meta/local", b_oid, true, "push B")
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
        .find_reference("refs/meta/local")
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id();
    copy_meta_objects(&repo_a, &bare_dir);
    git2::Repository::open_bare(bare_dir.path())
        .unwrap()
        .reference("refs/meta/local", a_oid_new, true, "push A new")
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
        .find_reference("refs/meta/local")
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
        .find_reference("refs/meta/local")
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id();
    repo.reference("refs/meta/origin", second_oid, true, "test remote")
        .unwrap();
    repo.reference("refs/meta/local", first_oid, true, "rollback local")
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
        .find_reference("refs/meta/local")
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
        .find_reference("refs/meta/local")
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
        .find_reference("refs/meta/local")
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id();
    repo.reference("refs/meta/local", base_oid, true, "rollback to base")
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
        .find_reference("refs/meta/local")
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
        .find_reference("refs/meta/local")
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
        .find_reference("refs/meta/local")
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
        .find_reference("refs/meta/local")
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id();

    // Simulate fetch: B -> bare -> A
    copy_meta_objects(&repo_b, &bare_dir);
    git2::Repository::open_bare(bare_dir.path())
        .unwrap()
        .reference("refs/meta/local", b_oid, true, "push B")
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
            "reason=no-common-ancestor-remote-wins",
        ))
        .stdout(predicate::str::contains("agent:model"));

    // Dry-run should not move local ref.
    let a_after_dry_run = repo_a
        .find_reference("refs/meta/local")
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id();
    assert_eq!(a_after_dry_run, a_oid);

    // Real materialize applies two-way merge where remote wins conflicts.
    gmeta(repo_a_dir.path())
        .args(["materialize"])
        .assert()
        .success()
        .stdout(predicate::str::contains("two-way merge"));

    // Conflict key should come from remote.
    gmeta(repo_a_dir.path())
        .args(["get", "project", "agent:model"])
        .assert()
        .success()
        .stdout(predicate::str::contains("remote"))
        .stdout(predicate::str::contains("local").not());

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
