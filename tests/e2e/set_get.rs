use predicates::prelude::*;

use crate::harness::{self, commit_target, setup_repo};

#[test]
fn set_and_get_string() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    harness::gmeta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["get", &target])
        .assert()
        .success()
        .stdout(predicate::str::contains("agent:model"))
        .stdout(predicate::str::contains("claude-4.6"));
}

#[test]
fn set_and_get_with_partial_sha() {
    let (dir, sha) = setup_repo();
    let full_target = commit_target(&sha);
    let partial_target = commit_target(&sha[..8]);

    harness::gmeta(dir.path())
        .args(["set", &partial_target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["get", &full_target])
        .assert()
        .success()
        .stdout(predicate::str::contains("claude-4.6"));

    harness::gmeta(dir.path())
        .args(["get", &partial_target])
        .assert()
        .success()
        .stdout(predicate::str::contains("claude-4.6"));
}

#[test]
fn set_and_get_specific_key() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    harness::gmeta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["set", &target, "agent:provider", "anthropic"])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["get", &target, "agent:model"])
        .assert()
        .success()
        .stdout(predicate::str::contains("claude-4.6"))
        .stdout(predicate::str::contains("provider").not());
}

#[test]
fn set_and_get_json() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    harness::gmeta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["set", &target, "agent:provider", "anthropic"])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["get", "--json", &target])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"model\": \"claude-4.6\""))
        .stdout(predicate::str::contains("\"provider\": \"anthropic\""));
}

#[test]
fn json_with_authorship() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    harness::gmeta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["get", "--json", "--with-authorship", &target])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"value\": \"claude-4.6\""))
        .stdout(predicate::str::contains("\"author\": \"test@example.com\""))
        .stdout(predicate::str::contains("\"timestamp\""));
}

#[test]
fn partial_key_matching() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    harness::gmeta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["set", &target, "agent:provider", "anthropic"])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["set", &target, "other:key", "value"])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["get", &target, "agent"])
        .assert()
        .success()
        .stdout(predicate::str::contains("agent:model"))
        .stdout(predicate::str::contains("agent:provider"))
        .stdout(predicate::str::contains("other:key").not());
}

#[test]
fn partial_key_matching_commit_namespace_example() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    harness::gmeta(dir.path())
        .args(["set", &target, "agent:model", "claude-opus-4-6"])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["set", &target, "agent:provider", "anthropic"])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["set", &target, "agent:prompt", "Make me a sandwich"])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["set", &target, "agent:transcript", "..."])
        .assert()
        .success();

    harness::gmeta(dir.path())
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
fn rm_removes_value() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    harness::gmeta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["rm", &target, "agent:model"])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["get", &target, "agent:model"])
        .assert()
        .success()
        .stdout(predicate::str::is_empty());
}

#[test]
fn upsert_overwrites() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    harness::gmeta(dir.path())
        .args(["set", &target, "agent:model", "v1"])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["set", &target, "agent:model", "v2"])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["get", &target, "agent:model"])
        .assert()
        .success()
        .stdout(predicate::str::contains("v2"))
        .stdout(predicate::str::contains("v1").not());
}

#[test]
fn path_target() {
    let (dir, _sha) = setup_repo();

    harness::gmeta(dir.path())
        .args(["set", "path:src/main.rs", "review:status", "approved"])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["get", "path:src/main.rs"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "src/main.rs;review:status  approved",
        ));
}

#[test]
fn path_target_tree_lookup() {
    let (dir, _sha) = setup_repo();

    harness::gmeta(dir.path())
        .args(["set", "path:src/git", "owner", "schacon"])
        .assert()
        .success();
    harness::gmeta(dir.path())
        .args(["set", "path:src/observability", "owner", "caleb"])
        .assert()
        .success();
    harness::gmeta(dir.path())
        .args(["set", "path:src/metrics", "owner", "kiril"])
        .assert()
        .success();
    harness::gmeta(dir.path())
        .args(["set", "path:srcx/metrics", "owner", "nope"])
        .assert()
        .success();

    harness::gmeta(dir.path())
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
fn change_id_target() {
    let (dir, _sha) = setup_repo();

    harness::gmeta(dir.path())
        .args([
            "set",
            "change-id:550e8400-e29b-41d4-a716-446655440000",
            "status",
            "merged",
        ])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["get", "change-id:550e8400-e29b-41d4-a716-446655440000"])
        .assert()
        .success()
        .stdout(predicate::str::contains("status"))
        .stdout(predicate::str::contains("merged"));
}

#[test]
fn project_target() {
    let (dir, _sha) = setup_repo();

    harness::gmeta(dir.path())
        .args(["set", "project", "name", "my-project"])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["get", "project", "name"])
        .assert()
        .success()
        .stdout(predicate::str::contains("my-project"));
}

#[test]
fn invalid_target_type() {
    let (dir, _sha) = setup_repo();

    harness::gmeta(dir.path())
        .args(["set", "unknown:abc123", "key", "value"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("unknown target type"));
}

#[test]
fn target_value_too_short() {
    let (dir, _sha) = setup_repo();

    harness::gmeta(dir.path())
        .args(["set", "commit:ab", "key", "value"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("at least 3 characters"));
}

#[test]
fn custom_namespace() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    let repo = git2::Repository::open(dir.path()).unwrap();
    repo.config()
        .unwrap()
        .set_str("meta.namespace", "notes")
        .unwrap();
    drop(repo);

    harness::gmeta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["serialize"])
        .assert()
        .success()
        .stdout(predicate::str::contains("refs/notes/local/main"));

    let repo = git2::Repository::open(dir.path()).unwrap();
    assert!(repo.find_reference("refs/notes/local/main").is_ok());
    assert!(repo.find_reference("refs/meta/local/main").is_err());
}

#[test]
fn set_add_and_rm() {
    let (dir, _sha) = setup_repo();
    let target = "branch:sc-branch-1-deadbeef";

    harness::gmeta(dir.path())
        .args(["set:add", target, "reviewer", "alice@example.com"])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["set:add", target, "reviewer", "bob@example.com"])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["get", target, "reviewer"])
        .assert()
        .success()
        .stdout(predicate::str::contains("alice@example.com"))
        .stdout(predicate::str::contains("bob@example.com"));

    harness::gmeta(dir.path())
        .args(["set:rm", target, "reviewer", "alice@example.com"])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["get", target, "reviewer"])
        .assert()
        .success()
        .stdout(predicate::str::contains("bob@example.com"))
        .stdout(predicate::str::contains("alice@example.com").not());
}

#[test]
fn set_add_deduplicates_members() {
    let (dir, _sha) = setup_repo();
    let target = "branch:sc-branch-1-deadbeef";

    harness::gmeta(dir.path())
        .args(["set:add", target, "reviewer", "alice@example.com"])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["set:add", target, "reviewer", "alice@example.com"])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["get", target, "reviewer"])
        .assert()
        .success()
        .stdout(predicate::str::contains("alice@example.com").count(1));
}

#[test]
fn set_type_round_trips_and_serializes_members() {
    let (dir, _sha) = setup_repo();

    harness::gmeta(dir.path())
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

    harness::gmeta(dir.path())
        .args(["get", "branch:sc-branch-1-deadbeef", "reviewer"])
        .assert()
        .success()
        .stdout(predicate::str::contains("alice@example.com"))
        .stdout(predicate::str::contains("bob@example.com"));

    harness::gmeta(dir.path())
        .args(["serialize"])
        .assert()
        .success();

    let repo = git2::Repository::open(dir.path()).unwrap();
    let reference = repo.find_reference("refs/meta/local/main").unwrap();
    let commit = reference.peel_to_commit().unwrap();
    let tree = commit.tree().unwrap();
    let fanout = harness::target_fanout("sc-branch-1-deadbeef");

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
