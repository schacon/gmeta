//! Test harness for gmeta end-to-end tests.
//!
//! Provides isolated, reproducible test environments inspired by GitButler's
//! `but-testsupport` crate. Key features:
//!
//! - **Shell script fixtures** via `gix-testtools` — readable, cacheable repo setup.
//! - **Environment isolation** — strips host git config, sets stable author/committer
//!   dates, and disables gpgsign so tests are reproducible across machines.

use assert_cmd::Command;
use gix::refs::transaction::PreviousValue;
use sha1::{Digest, Sha1};
use std::path::Path;
use tempfile::TempDir;
#[cfg(not(windows))]
const NULL_DEVICE: &str = "/dev/null";
#[cfg(windows)]
const NULL_DEVICE: &str = "NUL";

/// Environment variables to strip so host config doesn't leak into tests.
const ENV_VARS_TO_REMOVE: &[&str] = &[
    "GIT_DIR",
    "GIT_INDEX_FILE",
    "GIT_OBJECT_DIRECTORY",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_WORK_TREE",
    "GIT_COMMON_DIR",
    "GIT_ASKPASS",
    "SSH_ASKPASS",
    "GIT_EDITOR",
    "VISUAL",
    "EDITOR",
];

/// Apply environment isolation to an `assert_cmd::Command`.
///
/// Strips variables that could leak host state (GIT_DIR, GIT_EDITOR, etc.)
/// and sets stable, reproducible values for author, committer, config, and
/// signing. Ported from GitButler's `prepare_cmd_env` pattern.
fn isolate_cmd(cmd: &mut Command) {
    for var in ENV_VARS_TO_REMOVE {
        cmd.env_remove(var);
    }
    cmd.env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", NULL_DEVICE)
        .env("GIT_TERMINAL_PROMPT", "false")
        .env("GIT_AUTHOR_DATE", "2000-01-01 00:00:00 +0000")
        .env("GIT_AUTHOR_EMAIL", "test@example.com")
        .env("GIT_AUTHOR_NAME", "Test User")
        .env("GIT_COMMITTER_DATE", "2000-01-02 00:00:00 +0000")
        .env("GIT_COMMITTER_EMAIL", "test@example.com")
        .env("GIT_COMMITTER_NAME", "Test User")
        .env("GIT_CONFIG_COUNT", "3")
        .env("GIT_CONFIG_KEY_0", "commit.gpgsign")
        .env("GIT_CONFIG_VALUE_0", "false")
        .env("GIT_CONFIG_KEY_1", "tag.gpgsign")
        .env("GIT_CONFIG_VALUE_1", "false")
        .env("GIT_CONFIG_KEY_2", "init.defaultBranch")
        .env("GIT_CONFIG_VALUE_2", "main");
}
/// Build an isolated `gmeta` [`Command`] pointed at `dir`.
///
/// The command has full environment isolation applied so tests are reproducible.
pub fn gmeta(dir: &Path) -> Command {
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("git-meta");
    cmd.current_dir(dir);
    isolate_cmd(&mut cmd);
    cmd
}
/// Get a writable copy of the `tests/fixtures/{name}.sh` fixture.
///
/// Returns `(TempDir, initial_commit_sha)`. The `TempDir` owns the working
/// directory; dropping it cleans up.
pub fn writable_fixture(name: &str) -> (TempDir, String) {
    let tmp = gix_testtools::scripted_fixture_writable(name)
        .unwrap_or_else(|e| panic!("fixture '{name}' failed: {e}"));
    let sha = head_sha(tmp.path());
    (tmp, sha)
}

/// Get a writable copy of the `tests/fixtures/{name}.sh` fixture, passing
/// `args` to the script.
///
/// Returns the `TempDir` that owns the fixture directory. The directory
/// may contain a bare repo (no HEAD to extract).
pub fn writable_fixture_with_args(
    name: &str,
    args: impl IntoIterator<Item = impl Into<String>>,
) -> TempDir {
    gix_testtools::scripted_fixture_writable_with_args(
        name,
        args,
        gix_testtools::Creation::CopyFromReadOnly,
    )
    .unwrap_or_else(|e| panic!("fixture '{name}' (with args) failed: {e}"))
}

/// Extract the HEAD commit SHA from a git repository at `path`.
fn head_sha(path: &std::path::Path) -> String {
    let repo = gix::open_opts(path, test_open_opts()).expect("fixture should be a valid git repo");
    repo.head_id()
        .expect("fixture repo should have HEAD")
        .to_string()
}
/// Build a `commit:<sha>` target string.
pub fn commit_target(sha: &str) -> String {
    format!("commit:{sha}")
}

/// Compute the two-character fanout prefix for a value (first two hex chars of
/// its SHA-1 hash). Used to verify serialized tree paths.
pub fn target_fanout(value: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(value.as_bytes());
    let hash = format!("{:x}", hasher.finalize());
    hash[..2].to_string()
}
/// Create a fresh git repository in a new temp directory with user config set.
///
/// Returns `(TempDir, initial_commit_sha)`. Use this when a test needs a repo
/// that doesn't match any fixture (e.g. custom user email, specific file
/// content, or multi-repo scenarios).
pub fn setup_repo() -> (TempDir, String) {
    let dir = TempDir::new().expect("should be able to create temp dir");
    let _init = gix::init(dir.path()).expect("should be able to init repo");

    // Set user config via git subprocess (gix config mutation API is limited)
    git_config(dir.path(), "user.email", "test@example.com");
    git_config(dir.path(), "user.name", "Test User");

    // Reopen with test config overrides so committer info is available
    let repo = gix::open_opts(dir.path(), test_open_opts()).expect("should reopen repo");

    let sig = gix::actor::Signature {
        name: "Test User".into(),
        email: "test@example.com".into(),
        time: gix::date::Time::new(946684800, 0),
    };
    let tree_oid = repo
        .empty_tree()
        .edit()
        .expect("should create tree editor")
        .write()
        .expect("should write empty tree")
        .detach();
    let commit = gix::objs::Commit {
        message: "initial".into(),
        tree: tree_oid,
        author: sig.clone(),
        committer: sig,
        encoding: None,
        parents: Default::default(),
        extra_headers: Default::default(),
    };
    let commit_oid = repo
        .write_object(&commit)
        .expect("should create commit")
        .detach();
    repo.reference(
        "refs/heads/main",
        commit_oid,
        PreviousValue::Any,
        "initial commit",
    )
    .expect("should create main ref");
    // Point HEAD at refs/heads/main
    repo.reference("HEAD", commit_oid, PreviousValue::Any, "initial commit")
        .expect("should update HEAD");

    (dir, commit_oid.to_string())
}

/// Create a bare repo with a `refs/{ns}/main` ref containing gmeta tree
/// structure: `project/testing/__value = "hello"`.
///
/// Used as a remote for push/pull tests.
pub fn setup_bare_with_meta(ns: &str) -> TempDir {
    let bare_dir = TempDir::new().expect("should be able to create temp dir");
    let _init = gix::init_bare(bare_dir.path()).expect("should be able to init bare repo");
    let bare = gix::open_opts(bare_dir.path(), test_open_opts()).expect("should reopen bare repo");

    let sig = gix::actor::Signature {
        name: "Test User".into(),
        email: "test@example.com".into(),
        time: gix::date::Time::new(946684800, 0),
    };

    // Build tree: project/testing/__value (blob: "hello")
    let blob_oid = bare
        .write_blob(b"\"hello\"")
        .expect("should create blob")
        .detach();
    let mut editor = bare.empty_tree().edit().expect("should create tree editor");
    editor
        .upsert(
            "project/testing/__value",
            gix::objs::tree::EntryKind::Blob,
            blob_oid,
        )
        .expect("should insert project/testing/__value");
    let tree_oid = editor.write().expect("should write tree").detach();

    let ref_name = format!("refs/{ns}/main");
    let commit = gix::objs::Commit {
        message: "initial meta".into(),
        tree: tree_oid,
        author: sig.clone(),
        committer: sig,
        encoding: None,
        parents: Default::default(),
        extra_headers: Default::default(),
    };
    let commit_oid = bare
        .write_object(&commit)
        .expect("should create meta commit")
        .detach();
    bare.reference(
        ref_name.as_str(),
        commit_oid,
        PreviousValue::Any,
        "initial meta",
    )
    .expect("should create ref");

    bare_dir
}

/// Build a bare repo with multiple gmeta serialize commits for promisor tests.
///
/// The repo has 2 commits on `refs/meta/main`:
///   - Commit 1 (older): `project/old_key/__value = "old_value"`
///   - Commit 2 (tip):   `project/testing/__value = "hello"` (old_key removed)
pub fn setup_bare_with_history() -> TempDir {
    let bare_dir = TempDir::new().expect("should be able to create temp dir");
    let _init = gix::init_bare(bare_dir.path()).expect("should be able to init bare repo");
    let bare = gix::open_opts(bare_dir.path(), test_open_opts()).expect("should reopen bare repo");
    let sig = gix::actor::Signature {
        name: "Test User".into(),
        email: "test@example.com".into(),
        time: gix::date::Time::new(946684800, 0),
    };

    // --- Commit 1: project/old_key/__value = "old_value" ---
    let blob1 = bare
        .write_blob(b"\"old_value\"")
        .expect("should create blob")
        .detach();
    let mut editor1 = bare.empty_tree().edit().expect("should create tree editor");
    editor1
        .upsert(
            "project/old_key/__value",
            gix::objs::tree::EntryKind::Blob,
            blob1,
        )
        .expect("should insert old_key");
    let root_tree_oid1 = editor1.write().expect("should write tree").detach();

    let commit1_msg = "gmeta: serialize (1 changes)\n\nA\tproject\told_key";
    let commit1_obj = gix::objs::Commit {
        message: commit1_msg.into(),
        tree: root_tree_oid1,
        author: sig.clone(),
        committer: sig.clone(),
        encoding: None,
        parents: Default::default(),
        extra_headers: Default::default(),
    };
    let commit1_oid = bare
        .write_object(&commit1_obj)
        .expect("should create commit 1")
        .detach();

    // --- Commit 2 (tip): project/testing/__value = "hello" (old_key removed) ---
    let blob2 = bare
        .write_blob(b"\"hello\"")
        .expect("should create blob")
        .detach();
    let mut editor2 = bare.empty_tree().edit().expect("should create tree editor");
    editor2
        .upsert(
            "project/testing/__value",
            gix::objs::tree::EntryKind::Blob,
            blob2,
        )
        .expect("should insert testing");
    let root_tree_oid2 = editor2.write().expect("should write tree").detach();

    let commit2_msg = "gmeta: serialize (1 changes)\n\nA\tproject\ttesting";
    let commit2_obj = gix::objs::Commit {
        message: commit2_msg.into(),
        tree: root_tree_oid2,
        author: sig.clone(),
        committer: sig,
        encoding: None,
        parents: vec![commit1_oid].into(),
        extra_headers: Default::default(),
    };
    let commit2_oid = bare
        .write_object(&commit2_obj)
        .expect("should create commit 2")
        .detach();
    bare.reference(
        "refs/meta/main",
        commit2_oid,
        PreviousValue::Any,
        "commit 2",
    )
    .expect("should create ref");

    bare_dir
}

/// Build a bare repo where a key exists in both history and tip tree.
///
/// Like [`setup_bare_with_history`] but the tip commit retains `old_key` in
/// its tree alongside `testing`. The tip commit message only mentions `testing`.
pub fn setup_bare_with_history_retained() -> TempDir {
    let bare_dir = TempDir::new().expect("should be able to create temp dir");
    let _init = gix::init_bare(bare_dir.path()).expect("should be able to init bare repo");
    let bare = gix::open_opts(bare_dir.path(), test_open_opts()).expect("should reopen bare repo");
    let sig = gix::actor::Signature {
        name: "Test User".into(),
        email: "test@example.com".into(),
        time: gix::date::Time::new(946684800, 0),
    };

    // --- Commit 1: project/old_key/__value = "old_value" ---
    let blob1 = bare
        .write_blob(b"\"old_value\"")
        .expect("should create blob")
        .detach();
    let mut editor1 = bare.empty_tree().edit().expect("should create tree editor");
    editor1
        .upsert(
            "project/old_key/__value",
            gix::objs::tree::EntryKind::Blob,
            blob1,
        )
        .expect("should insert old_key");
    let root_tree_oid1 = editor1.write().expect("should write tree").detach();

    let commit1_msg = "gmeta: serialize (1 changes)\n\nA\tproject\told_key";
    let commit1_obj = gix::objs::Commit {
        message: commit1_msg.into(),
        tree: root_tree_oid1,
        author: sig.clone(),
        committer: sig.clone(),
        encoding: None,
        parents: Default::default(),
        extra_headers: Default::default(),
    };
    let commit1_oid = bare
        .write_object(&commit1_obj)
        .expect("should create commit 1")
        .detach();

    // --- Commit 2 (tip): both old_key and testing ---
    let blob2 = bare
        .write_blob(b"\"hello\"")
        .expect("should create blob")
        .detach();
    let mut editor2 = bare.empty_tree().edit().expect("should create tree editor");
    editor2
        .upsert(
            "project/old_key/__value",
            gix::objs::tree::EntryKind::Blob,
            blob1,
        )
        .expect("should insert old_key");
    editor2
        .upsert(
            "project/testing/__value",
            gix::objs::tree::EntryKind::Blob,
            blob2,
        )
        .expect("should insert testing");
    let root_tree_oid2 = editor2.write().expect("should write tree").detach();

    let commit2_msg = "gmeta: serialize (1 changes)\n\nA\tproject\ttesting";
    let commit2_obj = gix::objs::Commit {
        message: commit2_msg.into(),
        tree: root_tree_oid2,
        author: sig.clone(),
        committer: sig,
        encoding: None,
        parents: vec![commit1_oid].into(),
        extra_headers: Default::default(),
    };
    let commit2_oid = bare
        .write_object(&commit2_obj)
        .expect("should create commit 2")
        .detach();
    bare.reference(
        "refs/meta/main",
        commit2_oid,
        PreviousValue::Any,
        "commit 2",
    )
    .expect("should create ref");

    bare_dir
}
/// Copy all git objects from `src` repo into a bare repo at `bare_dir`.
///
/// Simulates a push by copying loose objects and pack files.
pub fn copy_meta_objects(src: &gix::Repository, bare_dir: &TempDir) {
    let src_objects = src.path().join("objects");
    let dst_objects = bare_dir.path().join("objects");
    copy_dir_contents(&src_objects, &dst_objects);
}

/// Copy all git objects from a bare repo at `bare_dir` into `dst` repo.
///
/// Simulates a fetch by copying loose objects and pack files.
pub fn copy_meta_objects_from(bare_dir: &TempDir, dst: &gix::Repository) {
    let src_objects = bare_dir.path().join("objects");
    let dst_objects = dst.path().join("objects");
    copy_dir_contents(&src_objects, &dst_objects);
}

/// Recursively copy directory contents (for loose objects + pack files).
fn copy_dir_contents(src: &std::path::Path, dst: &std::path::Path) {
    if !src.exists() {
        return;
    }
    for entry in std::fs::read_dir(src).expect("should be able to read dir") {
        let entry = entry.expect("should be a valid entry");
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

/// Set a git config value using the `git` subprocess.
///
/// Used in tests because gix's config mutation API is limited.
fn git_config(repo_path: &Path, key: &str, value: &str) {
    let output = std::process::Command::new("git")
        .args(["-C", &repo_path.to_string_lossy(), "config", key, value])
        .output()
        .expect("should be able to run git config");
    assert!(output.status.success(), "git config {key} {value} failed");
}

/// Open a gix repository at the given path with test-friendly config overrides.
///
/// Uses isolated config (no system/global) with stable author/committer identity,
/// so reference operations work even in CI where no global git config exists.
pub fn open_repo(path: &Path) -> gix::Repository {
    gix::open_opts(path, test_open_opts()).expect("should be able to open repo")
}

/// Returns gix open options with isolated config and test identity overrides.
///
/// Ensures committer/author info is available for reference operations
/// even when no global git config is present (e.g. in CI).
fn test_open_opts() -> gix::open::Options {
    gix::open::Options::isolated()
        .config_overrides(["user.name=Test User", "user.email=test@example.com"])
}

/// Resolve a reference to a commit OID (fully peeled).
pub fn ref_to_commit_oid(repo: &gix::Repository, ref_name: &str) -> gix::ObjectId {
    repo.find_reference(ref_name)
        .unwrap()
        .into_fully_peeled_id()
        .unwrap()
        .detach()
}
