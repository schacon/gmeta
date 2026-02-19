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
    let mut cmd = Command::cargo_bin("gmeta").unwrap();
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
        .args(["set", "-t", "list", &target, "items", r#"["hello","world"]"#])
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
    let last3 = &sha[sha.len() - 3..];
    let expected_path = format!("commit/{}/{}/{}/agent/model", first2, last3, sha);

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

    gmeta(dir.path())
        .args(["serialize"])
        .assert()
        .success();

    // Verify tree structure has list entries with timestamp-hash format
    let repo = git2::Repository::open(dir.path()).unwrap();
    let reference = repo.find_reference("refs/meta/local").unwrap();
    let commit = reference.peel_to_commit().unwrap();
    let tree = commit.tree().unwrap();

    let mut list_entries = Vec::new();
    tree.walk(git2::TreeWalkMode::PreOrder, |root, entry| {
        let full_path = format!("{}{}", root, entry.name().unwrap_or(""));
        if full_path.starts_with("branch/sc/eef/sc-branch-1-deadbeef/agent/chat/") {
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
