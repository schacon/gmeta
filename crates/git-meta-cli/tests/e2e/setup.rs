use predicates::prelude::*;
use tempfile::TempDir;

use crate::harness::{self, open_repo, ref_to_commit_oid, setup_repo};

#[test]
fn setup_reads_dot_git_meta_and_initializes_remote() {
    let (dir, _sha) = setup_repo();
    let bare_dir = TempDir::new().unwrap();
    let _ = gix::init_bare(bare_dir.path()).unwrap();
    let bare_path = bare_dir.path().to_str().unwrap();

    std::fs::write(dir.path().join(".git-meta"), format!("{bare_path}\n")).unwrap();

    harness::git_meta(dir.path())
        .args(["setup"])
        .assert()
        .success()
        .stderr(predicate::str::contains(
            "Using metadata remote URL from",
        ))
        .stderr(predicate::str::contains(".git-meta"))
        .stdout(predicate::str::contains("Added meta remote"));

    let local = open_repo(dir.path());
    let local_tip = ref_to_commit_oid(&local, "refs/meta/local/main");
    let tracking_tip = ref_to_commit_oid(&local, "refs/meta/remotes/main");
    assert_eq!(local_tip, tracking_tip);

    let bare = open_repo(bare_dir.path());
    let bare_tip = ref_to_commit_oid(&bare, "refs/meta/main");
    assert_eq!(bare_tip, local_tip);
}

#[test]
fn setup_missing_dot_git_meta_bails_with_hint() {
    let (dir, _sha) = setup_repo();

    harness::git_meta(dir.path())
        .args(["setup"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("no .git-meta file found"))
        .stderr(predicate::str::contains("--init"));
}

#[test]
fn setup_empty_dot_git_meta_bails_with_hint() {
    let (dir, _sha) = setup_repo();
    std::fs::write(
        dir.path().join(".git-meta"),
        "# only a comment\n# another comment\n",
    )
    .unwrap();

    harness::git_meta(dir.path())
        .args(["setup"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "empty or contains no metadata remote URL",
        ));
}

#[test]
fn setup_ignores_comments_and_extra_lines() {
    let (dir, _sha) = setup_repo();
    let bare_dir = TempDir::new().unwrap();
    let _ = gix::init_bare(bare_dir.path()).unwrap();
    let bare_path = bare_dir.path().to_str().unwrap();

    let contents = format!(
        "# Pin the metadata remote for this project.\n\
         # Anyone running `git meta setup` here picks it up automatically.\n\
         \n\
         {bare_path}\n\
         \n\
         # Trailing notes are ignored.\n"
    );
    std::fs::write(dir.path().join(".git-meta"), contents).unwrap();

    harness::git_meta(dir.path())
        .args(["setup"])
        .assert()
        .success();

    let bare = open_repo(bare_dir.path());
    let _ = ref_to_commit_oid(&bare, "refs/meta/main");
}
