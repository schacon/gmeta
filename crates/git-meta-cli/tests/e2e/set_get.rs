use gix::bstr::ByteSlice;
use gix::prelude::ObjectIdExt;
use predicates::prelude::*;

use crate::harness::{self, commit_target, open_repo, ref_to_commit_oid, setup_repo};

#[test]
fn set_and_get_string() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    harness::git_meta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    harness::git_meta(dir.path())
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

    harness::git_meta(dir.path())
        .args(["set", &partial_target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["get", &full_target])
        .assert()
        .success()
        .stdout(predicate::str::contains("claude-4.6"));

    harness::git_meta(dir.path())
        .args(["get", &partial_target])
        .assert()
        .success()
        .stdout(predicate::str::contains("claude-4.6"));
}

#[test]
fn set_and_get_specific_key() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    harness::git_meta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["set", &target, "agent:provider", "anthropic"])
        .assert()
        .success();

    harness::git_meta(dir.path())
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

    harness::git_meta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["set", &target, "agent:provider", "anthropic"])
        .assert()
        .success();

    harness::git_meta(dir.path())
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

    harness::git_meta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    harness::git_meta(dir.path())
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

    harness::git_meta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["set", &target, "agent:provider", "anthropic"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["set", &target, "other:key", "value"])
        .assert()
        .success();

    harness::git_meta(dir.path())
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

    harness::git_meta(dir.path())
        .args(["set", &target, "agent:model", "claude-opus-4-6"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["set", &target, "agent:provider", "anthropic"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["set", &target, "agent:prompt", "Make me a sandwich"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["set", &target, "agent:transcript", "..."])
        .assert()
        .success();

    harness::git_meta(dir.path())
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

    harness::git_meta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["rm", &target, "agent:model"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["get", &target, "agent:model"])
        .assert()
        .success()
        .stdout(predicate::str::is_empty());
}

#[test]
fn upsert_overwrites() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    harness::git_meta(dir.path())
        .args(["set", &target, "agent:model", "v1"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["set", &target, "agent:model", "v2"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["get", &target, "agent:model"])
        .assert()
        .success()
        .stdout(predicate::str::contains("v2"))
        .stdout(predicate::str::contains("v1").not());
}

#[test]
fn path_target() {
    let (dir, _sha) = setup_repo();

    harness::git_meta(dir.path())
        .args(["set", "path:src/main.rs", "review:status", "approved"])
        .assert()
        .success();

    harness::git_meta(dir.path())
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

    harness::git_meta(dir.path())
        .args(["set", "path:src/git", "owner", "schacon"])
        .assert()
        .success();
    harness::git_meta(dir.path())
        .args(["set", "path:src/observability", "owner", "caleb"])
        .assert()
        .success();
    harness::git_meta(dir.path())
        .args(["set", "path:src/metrics", "owner", "kiril"])
        .assert()
        .success();
    harness::git_meta(dir.path())
        .args(["set", "path:srcx/metrics", "owner", "nope"])
        .assert()
        .success();

    harness::git_meta(dir.path())
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

    harness::git_meta(dir.path())
        .args([
            "set",
            "change-id:550e8400-e29b-41d4-a716-446655440000",
            "status",
            "merged",
        ])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["get", "change-id:550e8400-e29b-41d4-a716-446655440000"])
        .assert()
        .success()
        .stdout(predicate::str::contains("status"))
        .stdout(predicate::str::contains("merged"));
}

#[test]
fn project_target() {
    let (dir, _sha) = setup_repo();

    harness::git_meta(dir.path())
        .args(["set", "project", "name", "my-project"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["get", "project", "name"])
        .assert()
        .success()
        .stdout(predicate::str::contains("my-project"));
}

#[test]
fn invalid_target_type() {
    let (dir, _sha) = setup_repo();

    harness::git_meta(dir.path())
        .args(["set", "unknown:abc123", "key", "value"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("unknown target type"));
}

#[test]
fn target_value_too_short() {
    let (dir, _sha) = setup_repo();

    harness::git_meta(dir.path())
        .args(["set", "commit:ab", "key", "value"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("at least 3 characters"));
}

#[test]
fn custom_namespace() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    git_config(dir.path(), "meta.namespace", "notes");

    harness::git_meta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["serialize"])
        .assert()
        .success()
        .stdout(predicate::str::contains("refs/notes/local/main"));

    let repo = open_repo(dir.path());
    assert!(repo.find_reference("refs/notes/local/main").is_ok());
    assert!(repo.find_reference("refs/meta/local/main").is_err());
}

#[test]
fn set_add_and_rm() {
    let (dir, _sha) = setup_repo();
    let target = "branch:sc-branch-1-deadbeef";

    harness::git_meta(dir.path())
        .args(["set:add", target, "reviewer", "alice@example.com"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["set:add", target, "reviewer", "bob@example.com"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["get", target, "reviewer"])
        .assert()
        .success()
        .stdout(predicate::str::contains("alice@example.com"))
        .stdout(predicate::str::contains("bob@example.com"));

    harness::git_meta(dir.path())
        .args(["set:rm", target, "reviewer", "alice@example.com"])
        .assert()
        .success();

    harness::git_meta(dir.path())
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

    harness::git_meta(dir.path())
        .args(["set:add", target, "reviewer", "alice@example.com"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["set:add", target, "reviewer", "alice@example.com"])
        .assert()
        .success();

    harness::git_meta(dir.path())
        .args(["get", target, "reviewer"])
        .assert()
        .success()
        .stdout(predicate::str::contains("alice@example.com").count(1));
}

#[test]
fn set_type_round_trips_and_serializes_members() {
    let (dir, _sha) = setup_repo();

    for member in ["alice@example.com", "bob@example.com", "alice@example.com"] {
        harness::git_meta(dir.path())
            .args(["set:add", "branch:sc-branch-1-deadbeef", "reviewer", member])
            .assert()
            .success();
    }

    harness::git_meta(dir.path())
        .args(["get", "branch:sc-branch-1-deadbeef", "reviewer"])
        .assert()
        .success()
        .stdout(predicate::str::contains("alice@example.com"))
        .stdout(predicate::str::contains("bob@example.com"));

    harness::git_meta(dir.path())
        .args(["serialize"])
        .assert()
        .success();

    let repo = open_repo(dir.path());
    let commit_oid = ref_to_commit_oid(&repo, "refs/meta/local/main");
    let commit_obj = commit_oid.attach(&repo).object().unwrap().into_commit();
    let tree = commit_obj.tree().unwrap();
    let fanout = harness::target_fanout("sc-branch-1-deadbeef");

    let set_prefix = format!("branch/{fanout}/sc-branch-1-deadbeef/reviewer/__set/");

    let mut set_members = Vec::new();
    let mut results = Vec::new();
    walk_tree(&repo, tree.id, "", &mut results);
    for (path, _) in &results {
        if path.starts_with(&set_prefix) {
            let tail = path.strip_prefix(&set_prefix).unwrap();
            if !tail.contains('/') {
                set_members.push(path.clone());
            }
        }
    }

    assert_eq!(set_members.len(), 2);
}

/// Recursively walk a tree, collecting `(path, blob_content)` pairs.
fn walk_tree(
    repo: &gix::Repository,
    tree_id: gix::ObjectId,
    prefix: &str,
    results: &mut Vec<(String, String)>,
) {
    let tree = tree_id.attach(repo).object().unwrap().into_tree();
    for entry in tree.iter() {
        let entry = entry.unwrap();
        let name = entry.filename().to_str().unwrap();
        let path = if prefix.is_empty() {
            name.to_string()
        } else {
            format!("{prefix}/{name}")
        };
        if entry.mode().is_tree() {
            walk_tree(repo, entry.object_id(), &path, results);
        } else {
            let blob = entry.object().unwrap();
            let content = std::str::from_utf8(blob.data.as_ref())
                .unwrap_or("")
                .to_string();
            results.push((path, content));
        }
    }
}

/// Set a git config value using the `git` subprocess.
fn git_config(repo_path: &std::path::Path, key: &str, value: &str) {
    let output = std::process::Command::new("git")
        .args(["-C", &repo_path.to_string_lossy(), "config", key, value])
        .output()
        .expect("should be able to run git config");
    assert!(output.status.success(), "git config {key} {value} failed");
}
