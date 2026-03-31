use predicates::prelude::*;

use crate::harness::{self, commit_target, setup_repo, target_fanout};

#[test]
fn serialize_creates_ref() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    harness::gmeta(dir.path())
        .args(["set", &target, "agent:model", "claude-4.6"])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["serialize"])
        .assert()
        .success()
        .stdout(predicate::str::contains("refs/meta/local/main"));

    let repo = git2::Repository::open(dir.path()).unwrap();
    let reference = repo.find_reference("refs/meta/local/main").unwrap();
    let commit = reference.peel_to_commit().unwrap();
    let tree = commit.tree().unwrap();

    let first2 = &sha[..2];
    let expected_path = format!("commit/{}/{}/agent/model/__value", first2, sha);

    let mut found = false;
    tree.walk(git2::TreeWalkMode::PreOrder, |root, entry| {
        let full_path = format!("{}{}", root, entry.name().unwrap_or(""));
        if full_path == expected_path {
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
fn serialize_path_target_uses_raw_segments_and_separator() {
    let (dir, _sha) = setup_repo();

    harness::gmeta(dir.path())
        .args(["set", "path:src/__generated/file.rs", "owner", "schacon"])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["serialize"])
        .assert()
        .success();

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
fn serialize_list_values() {
    let (dir, _sha) = setup_repo();

    harness::gmeta(dir.path())
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

    harness::gmeta(dir.path())
        .args(["serialize"])
        .assert()
        .success();

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
        )) && entry.kind() == Some(git2::ObjectType::Blob)
        {
            list_entries.push(full_path);
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
fn serialize_empty() {
    let (dir, _sha) = setup_repo();

    harness::gmeta(dir.path())
        .args(["serialize"])
        .assert()
        .success()
        .stdout(predicate::str::contains("no metadata to serialize"));
}

#[test]
fn serialize_list_uses_stored_timestamp() {
    let (dir, _sha) = setup_repo();

    harness::gmeta(dir.path())
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

    harness::gmeta(dir.path())
        .args(["serialize"])
        .assert()
        .success();

    let repo = git2::Repository::open(dir.path()).unwrap();
    let first_entries = collect_list_entry_names(&repo);
    assert_eq!(first_entries.len(), 2);

    harness::gmeta(dir.path())
        .args(["serialize"])
        .assert()
        .success();

    let repo = git2::Repository::open(dir.path()).unwrap();
    let second_entries = collect_list_entry_names(&repo);
    assert_eq!(second_entries.len(), 2);

    assert_eq!(
        first_entries, second_entries,
        "list entry names should be stable across serializations when data is unchanged"
    );
}

#[test]
fn serialize_rm_writes_tombstone_blob() {
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
        .args(["serialize"])
        .assert()
        .success();

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
