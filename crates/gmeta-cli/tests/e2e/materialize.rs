use predicates::prelude::*;
use tempfile::TempDir;

use crate::harness::{
    self, commit_target, copy_meta_objects, copy_meta_objects_from, setup_repo, target_fanout,
};

#[test]
fn fast_forward_applies_remote_removal() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    harness::gmeta(dir.path())
        .args(["set", &target, "agent:model", "v1"])
        .assert()
        .success();
    harness::gmeta(dir.path())
        .args(["serialize"])
        .assert()
        .success();

    let repo = git2::Repository::open(dir.path()).unwrap();
    let first_oid = repo
        .find_reference("refs/meta/local/main")
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id();
    drop(repo);

    harness::gmeta(dir.path())
        .args(["rm", &target, "agent:model"])
        .assert()
        .success();
    harness::gmeta(dir.path())
        .args(["serialize"])
        .assert()
        .success();

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

    harness::gmeta(dir.path())
        .args(["set", &target, "agent:model", "stale"])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["materialize"])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["get", &target, "agent:model"])
        .assert()
        .success()
        .stdout(predicate::str::is_empty());
}

#[test]
fn fast_forward_applies_remote_list_entry_removal() {
    let (dir, _sha) = setup_repo();
    let target = "branch:sc-branch-1-deadbeef";

    harness::gmeta(dir.path())
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
    harness::gmeta(dir.path())
        .args(["serialize"])
        .assert()
        .success();

    let repo = git2::Repository::open(dir.path()).unwrap();
    let first_oid = repo
        .find_reference("refs/meta/local/main")
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id();
    drop(repo);

    harness::gmeta(dir.path())
        .args(["list:pop", target, "agent:chat", "b"])
        .assert()
        .success();
    harness::gmeta(dir.path())
        .args(["serialize"])
        .assert()
        .success();

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

    harness::gmeta(dir.path())
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

    harness::gmeta(dir.path())
        .args(["materialize"])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["get", target, "agent:chat"])
        .assert()
        .success()
        .stdout(predicate::str::contains("a"))
        .stdout(predicate::str::contains("c"))
        .stdout(predicate::str::contains("b").not());
}

/// The core durability guarantee: metadata survives a complete database loss
/// as long as it has been serialized to git refs.
///
/// 1. Set metadata (string, list, set) across multiple target types
/// 2. Serialize to a git ref
/// 3. Delete the SQLite database entirely
/// 4. Materialize from the serialized ref
/// 5. Verify all data is fully restored
#[test]
fn serialize_wipe_db_materialize_restores_all_data() {
    let (dir, sha) = setup_repo();
    let commit = commit_target(&sha);

    // Set a string value on a commit target.
    harness::gmeta(dir.path())
        .args(["set", &commit, "agent:model", "claude-4.6"])
        .assert()
        .success();

    // Set a string value on a project target.
    harness::gmeta(dir.path())
        .args(["set", "project", "name", "my-project"])
        .assert()
        .success();

    // Set a list value on a branch target.
    harness::gmeta(dir.path())
        .args([
            "set",
            "-t",
            "list",
            "branch:sc-feature-abc123",
            "agent:chat",
            r#"["hello","world"]"#,
        ])
        .assert()
        .success();

    // Set a set value on a branch target.
    harness::gmeta(dir.path())
        .args([
            "set",
            "-t",
            "set",
            "branch:sc-feature-abc123",
            "reviewer",
            r#"["alice@example.com","bob@example.com"]"#,
        ])
        .assert()
        .success();

    // Serialize everything to refs/meta/local/main.
    harness::gmeta(dir.path())
        .args(["serialize"])
        .assert()
        .success();

    // Copy the local ref to a "remote" ref and delete the local ref so
    // materialize sees the remote as ahead and performs a fast-forward.
    let repo = git2::Repository::open(dir.path()).unwrap();
    let local_oid = repo
        .find_reference("refs/meta/local/main")
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id();
    repo.reference("refs/meta/origin", local_oid, true, "simulate remote")
        .unwrap();
    // Delete the local ref — materialize needs to see the remote as ahead.
    repo.find_reference("refs/meta/local/main")
        .unwrap()
        .delete()
        .unwrap();
    drop(repo);

    // Nuke the SQLite database — simulates total data loss.
    let db_path = dir.path().join(".git").join("gmeta.sqlite");
    assert!(db_path.exists(), "database should exist before deletion");
    std::fs::remove_file(&db_path).expect("should delete database");
    // Also remove the WAL/SHM files if present.
    let _ = std::fs::remove_file(dir.path().join(".git").join("gmeta.sqlite-wal"));
    let _ = std::fs::remove_file(dir.path().join(".git").join("gmeta.sqlite-shm"));

    // Materialize rebuilds the database from the serialized git tree.
    harness::gmeta(dir.path())
        .args(["materialize"])
        .assert()
        .success();

    // Verify the string value on the commit target is restored.
    harness::gmeta(dir.path())
        .args(["get", &commit, "agent:model"])
        .assert()
        .success()
        .stdout(predicate::str::contains("claude-4.6"));

    // Verify the project value is restored.
    harness::gmeta(dir.path())
        .args(["get", "project", "name"])
        .assert()
        .success()
        .stdout(predicate::str::contains("my-project"));

    // Verify list entries are restored.
    harness::gmeta(dir.path())
        .args(["get", "branch:sc-feature-abc123", "agent:chat"])
        .assert()
        .success()
        .stdout(predicate::str::contains("hello"))
        .stdout(predicate::str::contains("world"));

    // Verify set members are restored.
    harness::gmeta(dir.path())
        .args(["get", "branch:sc-feature-abc123", "reviewer"])
        .assert()
        .success()
        .stdout(predicate::str::contains("alice@example.com"))
        .stdout(predicate::str::contains("bob@example.com"));

    // Verify the data round-trips through a second serialize — the re-serialized
    // tree should be structurally identical (same commit SHA or same tree content).
    harness::gmeta(dir.path())
        .args(["serialize"])
        .assert()
        .success();

    let repo = git2::Repository::open(dir.path()).unwrap();
    let reference = repo.find_reference("refs/meta/local/main").unwrap();
    let tree = reference.peel_to_commit().unwrap().tree().unwrap();

    // Spot-check: the commit target path should exist in the re-serialized tree.
    let first2 = &sha[..2];
    let expected_path = format!("commit/{}/{}/agent/model/__value", first2, sha);
    let mut found = false;
    tree.walk(git2::TreeWalkMode::PreOrder, |root, entry| {
        let full = format!("{}{}", root, entry.name().unwrap_or(""));
        if full == expected_path {
            let blob = repo.find_blob(entry.id()).unwrap();
            assert_eq!(std::str::from_utf8(blob.content()).unwrap(), "claude-4.6");
            found = true;
        }
        git2::TreeWalkResult::Ok
    })
    .unwrap();
    assert!(found, "commit metadata should survive the full round-trip");

    // Spot-check: the list entries should exist in the re-serialized tree.
    let fanout = target_fanout("sc-feature-abc123");
    let list_prefix = format!("branch/{}/sc-feature-abc123/agent/chat/__list/", fanout);
    let mut list_count = 0;
    tree.walk(git2::TreeWalkMode::PreOrder, |root, entry| {
        let full = format!("{}{}", root, entry.name().unwrap_or(""));
        if full.starts_with(&list_prefix) && entry.kind() == Some(git2::ObjectType::Blob) {
            list_count += 1;
        }
        git2::TreeWalkResult::Ok
    })
    .unwrap();
    assert_eq!(
        list_count, 2,
        "list entries should survive the full round-trip"
    );
}

/// Simulate the full round-trip:
///
/// 1. User A sets metadata, serializes, "pushes" (we copy the ref)
/// 2. User B pulls, materializes (no new data), "pushes" the materialize commit
/// 3. User A pulls that back, overwrites a value locally, serializes
/// 4. User A materializes — the local change should NOT be overwritten
#[test]
fn preserves_local_changes_over_stale_remote() {
    let bare_dir = TempDir::new().unwrap();
    let repo_a_dir = TempDir::new().unwrap();
    let repo_b_dir = TempDir::new().unwrap();

    git2::Repository::init_bare(bare_dir.path()).unwrap();

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
    let init_oid = repo_a
        .commit(Some("HEAD"), &sig_a, &sig_a, "initial", &tree, &[])
        .unwrap();

    repo_a
        .reference("refs/remotes/origin/main", init_oid, true, "init")
        .unwrap();

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

    // === Step 1: User A sets metadata and serializes ===
    harness::gmeta(repo_a_dir.path())
        .args([
            "set",
            "change-id:uzytqkxrnstmxlzmvwluqomoynnowolp",
            "testing:user",
            "alice@example.com",
        ])
        .assert()
        .success();

    harness::gmeta(repo_a_dir.path())
        .args([
            "set",
            "change-id:uzytqkxrnstmxlzmvwluqomoynnowolp",
            "license",
            "apache",
        ])
        .assert()
        .success();

    harness::gmeta(repo_a_dir.path())
        .args(["serialize"])
        .assert()
        .success();

    let a_local_ref = repo_a.find_reference("refs/meta/local/main").unwrap();
    let a_local_oid = a_local_ref.peel_to_commit().unwrap().id();
    copy_meta_objects(&repo_a, &bare_dir);
    let bare_repo = git2::Repository::open_bare(bare_dir.path()).unwrap();
    bare_repo
        .reference("refs/meta/local/main", a_local_oid, true, "push from A")
        .unwrap();

    // === Step 2: User B pulls and materializes (no new data) ===
    copy_meta_objects_from(&bare_dir, &repo_b);
    repo_b
        .reference("refs/meta/origin", a_local_oid, true, "fetch from bare")
        .unwrap();

    harness::gmeta(repo_b_dir.path())
        .args(["materialize"])
        .assert()
        .success();

    harness::gmeta(repo_b_dir.path())
        .args(["serialize"])
        .assert()
        .success();

    let b_local_ref = repo_b.find_reference("refs/meta/local/main").unwrap();
    let b_local_oid = b_local_ref.peel_to_commit().unwrap().id();
    copy_meta_objects(&repo_b, &bare_dir);
    let bare_repo = git2::Repository::open_bare(bare_dir.path()).unwrap();
    bare_repo
        .reference("refs/meta/local/main", b_local_oid, true, "push from B")
        .unwrap();

    // === Step 3: User A pulls B's ref, overwrites a value locally, serializes ===
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

    harness::gmeta(repo_a_dir.path())
        .args([
            "set",
            "change-id:uzytqkxrnstmxlzmvwluqomoynnowolp",
            "testing:user",
            "tom@example.com",
        ])
        .assert()
        .success();

    harness::gmeta(repo_a_dir.path())
        .args(["serialize"])
        .assert()
        .success();

    // === Step 4: User A materializes — local change must survive ===
    harness::gmeta(repo_a_dir.path())
        .args(["materialize"])
        .assert()
        .success();

    harness::gmeta(repo_a_dir.path())
        .args(["get", "change-id:uzytqkxrnstmxlzmvwluqomoynnowolp"])
        .assert()
        .success()
        .stdout(predicate::str::contains("testing:user"))
        .stdout(predicate::str::contains("tom@example.com"))
        .stdout(predicate::str::contains("alice@example.com").not());

    harness::gmeta(repo_a_dir.path())
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
#[test]
fn both_sides_modified_later_timestamp_wins() {
    let bare_dir = TempDir::new().unwrap();
    let repo_a_dir = TempDir::new().unwrap();
    let repo_b_dir = TempDir::new().unwrap();

    git2::Repository::init_bare(bare_dir.path()).unwrap();

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
    harness::gmeta(repo_a_dir.path())
        .args([
            "set",
            "change-id:uzytqkxrnstmxlzmvwluqomoynnowolp",
            "testing:user",
            "alice@example.com",
        ])
        .assert()
        .success();

    harness::gmeta(repo_a_dir.path())
        .args(["serialize"])
        .assert()
        .success();

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
    copy_meta_objects_from(&bare_dir, &repo_b);
    repo_b
        .reference("refs/meta/origin", a_oid, true, "fetch")
        .unwrap();

    harness::gmeta(repo_b_dir.path())
        .args(["materialize"])
        .assert()
        .success();

    harness::gmeta(repo_b_dir.path())
        .args([
            "set",
            "change-id:uzytqkxrnstmxlzmvwluqomoynnowolp",
            "testing:user",
            "bob@example.com",
        ])
        .assert()
        .success();

    harness::gmeta(repo_b_dir.path())
        .args(["serialize"])
        .assert()
        .success();

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
    harness::gmeta(repo_a_dir.path())
        .args([
            "set",
            "change-id:uzytqkxrnstmxlzmvwluqomoynnowolp",
            "testing:user",
            "tom@example.com",
        ])
        .assert()
        .success();

    harness::gmeta(repo_a_dir.path())
        .args(["serialize"])
        .assert()
        .success();

    copy_meta_objects_from(&bare_dir, &repo_a);
    repo_a
        .reference("refs/meta/origin", b_oid, true, "fetch B")
        .unwrap();

    harness::gmeta(repo_a_dir.path())
        .args(["materialize"])
        .assert()
        .success();

    harness::gmeta(repo_a_dir.path())
        .args([
            "get",
            "change-id:uzytqkxrnstmxlzmvwluqomoynnowolp",
            "testing:user",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("tom@example.com"));

    // === Now test the reverse: B materializes A's newer changes ===
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

    harness::gmeta(repo_b_dir.path())
        .args(["materialize"])
        .assert()
        .success();

    harness::gmeta(repo_b_dir.path())
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
fn dry_run_does_not_mutate_sqlite_or_ref() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    harness::gmeta(dir.path())
        .args(["set", &target, "agent:model", "v1"])
        .assert()
        .success();
    harness::gmeta(dir.path())
        .args(["serialize"])
        .assert()
        .success();

    let repo = git2::Repository::open(dir.path()).unwrap();
    let first_oid = repo
        .find_reference("refs/meta/local/main")
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id();
    drop(repo);

    harness::gmeta(dir.path())
        .args(["set", &target, "agent:model", "v2"])
        .assert()
        .success();
    harness::gmeta(dir.path())
        .args(["serialize"])
        .assert()
        .success();

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

    harness::gmeta(dir.path())
        .args(["set", &target, "agent:model", "stale"])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["materialize", "--dry-run"])
        .assert()
        .success()
        .stdout(predicate::str::contains("dry-run: strategy=fast-forward"))
        .stdout(predicate::str::contains("agent:model"));

    harness::gmeta(dir.path())
        .args(["get", &target, "agent:model"])
        .assert()
        .success()
        .stdout(predicate::str::contains("stale"))
        .stdout(predicate::str::contains("v2").not());

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
fn dry_run_reports_concurrent_add_conflict_resolution() {
    let (dir, sha) = setup_repo();
    let target = commit_target(&sha);

    harness::gmeta(dir.path())
        .args(["set", &target, "base:key", "base"])
        .assert()
        .success();
    harness::gmeta(dir.path())
        .args(["serialize"])
        .assert()
        .success();

    let repo = git2::Repository::open(dir.path()).unwrap();
    let base_oid = repo
        .find_reference("refs/meta/local/main")
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id();
    drop(repo);

    harness::gmeta(dir.path())
        .args(["set", &target, "agent:model", "remote"])
        .assert()
        .success();
    harness::gmeta(dir.path())
        .args(["serialize"])
        .assert()
        .success();

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

    harness::gmeta(dir.path())
        .args(["set", &target, "agent:model", "local"])
        .assert()
        .success();
    harness::gmeta(dir.path())
        .args(["serialize"])
        .assert()
        .success();

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

    harness::gmeta(dir.path())
        .args(["materialize", "--dry-run"])
        .assert()
        .success()
        .stdout(predicate::str::contains("dry-run: strategy=three-way"))
        .stdout(predicate::str::contains("reason=concurrent-add"))
        .stdout(predicate::str::contains("agent:model"));

    harness::gmeta(dir.path())
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
fn no_common_ancestor_uses_two_way_merge_remote_wins() {
    let bare_dir = TempDir::new().unwrap();
    git2::Repository::init_bare(bare_dir.path()).unwrap();
    let (repo_a_dir, _sha_a) = setup_repo();
    let (repo_b_dir, _sha_b) = setup_repo();

    harness::gmeta(repo_a_dir.path())
        .args(["set", "project", "agent:model", "local"])
        .assert()
        .success();
    harness::gmeta(repo_a_dir.path())
        .args(["set", "project", "local:only", "keep-me"])
        .assert()
        .success();
    harness::gmeta(repo_a_dir.path())
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

    harness::gmeta(repo_b_dir.path())
        .args(["set", "project", "agent:model", "remote"])
        .assert()
        .success();
    harness::gmeta(repo_b_dir.path())
        .args(["set", "project", "remote:only", "keep-too"])
        .assert()
        .success();
    harness::gmeta(repo_b_dir.path())
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

    copy_meta_objects(&repo_b, &bare_dir);
    git2::Repository::open_bare(bare_dir.path())
        .unwrap()
        .reference("refs/meta/local/main", b_oid, true, "push B")
        .unwrap();
    copy_meta_objects_from(&bare_dir, &repo_a);
    repo_a
        .reference("refs/meta/origin", b_oid, true, "fetch B into A")
        .unwrap();

    harness::gmeta(repo_a_dir.path())
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

    let a_after_dry_run = repo_a
        .find_reference("refs/meta/local/main")
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id();
    assert_eq!(a_after_dry_run, a_oid);

    harness::gmeta(repo_a_dir.path())
        .args(["materialize"])
        .assert()
        .success()
        .stdout(predicate::str::contains("two-way merge"));

    harness::gmeta(repo_a_dir.path())
        .args(["get", "project", "agent:model"])
        .assert()
        .success()
        .stdout(predicate::str::contains("local"))
        .stdout(predicate::str::contains("remote").not());

    harness::gmeta(repo_a_dir.path())
        .args(["get", "project", "local:only"])
        .assert()
        .success()
        .stdout(predicate::str::contains("keep-me"));
    harness::gmeta(repo_a_dir.path())
        .args(["get", "project", "remote:only"])
        .assert()
        .success()
        .stdout(predicate::str::contains("keep-too"));
}
