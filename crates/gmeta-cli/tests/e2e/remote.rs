use predicates::prelude::*;
use tempfile::TempDir;

use crate::harness::{self, open_repo, setup_bare_with_meta, setup_repo};

#[test]
fn remote_add_no_meta_refs() {
    let (dir, _sha) = setup_repo();
    let bare_dir = TempDir::new().unwrap();
    {
        let _ = gix::init_bare(bare_dir.path()).unwrap();
        harness::open_repo(bare_dir.path())
    };

    let bare_path = bare_dir.path().to_str().unwrap();

    harness::gmeta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .failure()
        .stderr(predicate::str::contains("no metadata refs found"));
}

#[test]
fn remote_add_meta_refs_in_different_namespace() {
    let (dir, _sha) = setup_repo();
    let bare_dir = setup_bare_with_meta("altmeta");
    let bare_path = bare_dir.path().to_str().unwrap();

    harness::gmeta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .failure()
        .stderr(predicate::str::contains("refs/altmeta/main"))
        .stderr(predicate::str::contains("--namespace=altmeta"));
}

#[test]
fn remote_add_with_namespace_override() {
    let (dir, _sha) = setup_repo();
    let bare_dir = setup_bare_with_meta("altmeta");
    let bare_path = bare_dir.path().to_str().unwrap();

    harness::gmeta(dir.path())
        .args(["remote", "add", bare_path, "--namespace=altmeta"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Added meta remote"));

    let repo = open_repo(dir.path());
    let config = repo.config_snapshot();
    let fetch = config
        .string("remote.meta.fetch")
        .expect("fetch refspec should exist");
    let fetch_str = fetch.to_string();
    assert!(
        fetch_str.contains("refs/altmeta/"),
        "fetch refspec should use altmeta namespace, got: {}",
        fetch_str
    );
    let meta_ns = config
        .string("remote.meta.metanamespace")
        .expect("metanamespace should exist");
    assert_eq!(meta_ns.to_string(), "altmeta");
}

#[test]
fn remote_add_shorthand_url_expansion() {
    let (dir, _sha) = setup_repo();

    harness::gmeta(dir.path())
        .args(["remote", "add", "nonexistent-user-xyz/nonexistent-repo-xyz"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "git@github.com:nonexistent-user-xyz/nonexistent-repo-xyz.git",
        ));

    let repo = open_repo(dir.path());
    let config = repo.config_snapshot();
    let url = config
        .string("remote.meta.url")
        .expect("remote URL should exist");
    assert_eq!(
        url.to_string(),
        "git@github.com:nonexistent-user-xyz/nonexistent-repo-xyz.git"
    );
}

#[test]
fn remote_list_and_remove() {
    let (dir, _sha) = setup_repo();
    let bare_dir = setup_bare_with_meta("meta");
    let bare_path = bare_dir.path().to_str().unwrap();

    harness::gmeta(dir.path())
        .args(["remote", "add", bare_path])
        .assert()
        .success();

    harness::gmeta(dir.path())
        .args(["remote", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("meta\t"))
        .stdout(predicate::str::contains(bare_path));

    harness::gmeta(dir.path())
        .args(["remote", "remove", "meta"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Removed meta remote"));

    harness::gmeta(dir.path())
        .args(["remote", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("No metadata remotes configured"));
}
