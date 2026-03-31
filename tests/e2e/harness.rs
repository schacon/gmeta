//! Test harness for gmeta end-to-end tests.
//!
//! Provides isolated, reproducible test environments inspired by GitButler's
//! `but-testsupport` crate. Key features:
//!
//! - **Shell script fixtures** via `gix-testtools` — readable, cacheable repo setup.
//! - **Environment isolation** — strips host git config, sets stable author/committer
//!   dates, and disables gpgsign so tests are reproducible across machines.

use assert_cmd::Command;
use sha1::{Digest, Sha1};
use std::path::Path;
use tempfile::TempDir;

// ── Environment isolation ────────────────────────────────────────────────────

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

// ── Command helpers ──────────────────────────────────────────────────────────

/// Build an isolated `gmeta` [`Command`] pointed at `dir`.
///
/// The command has full environment isolation applied so tests are reproducible.
pub fn gmeta(dir: &Path) -> Command {
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("gmeta");
    cmd.current_dir(dir);
    isolate_cmd(&mut cmd);
    cmd
}

// ── Fixture helpers ──────────────────────────────────────────────────────────

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
    let repo = git2::Repository::open(path).expect("fixture should be a valid git repo");
    let oid = repo
        .head()
        .expect("fixture repo should have HEAD")
        .peel_to_commit()
        .expect("HEAD should point to a commit")
        .id();
    oid.to_string()
}

// ── Target helpers ───────────────────────────────────────────────────────────

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

// ── Repo setup helpers (for tests that need custom repo configurations) ──────

/// Create a fresh git repository in a new temp directory with user config set.
///
/// Returns `(TempDir, initial_commit_sha)`. Use this when a test needs a repo
/// that doesn't match any fixture (e.g. custom user email, specific file
/// content, or multi-repo scenarios).
pub fn setup_repo() -> (TempDir, String) {
    let dir = TempDir::new().expect("should be able to create temp dir");
    let repo = git2::Repository::init(dir.path()).expect("should be able to init repo");

    let mut config = repo.config().expect("should be able to get config");
    config
        .set_str("user.email", "test@example.com")
        .expect("should be able to set email");
    config
        .set_str("user.name", "Test User")
        .expect("should be able to set name");

    let sig =
        git2::Signature::now("Test User", "test@example.com").expect("should create signature");
    let tree_oid = repo
        .treebuilder(None)
        .expect("should create treebuilder")
        .write()
        .expect("should write tree");
    let tree = repo.find_tree(tree_oid).expect("should find tree");
    let commit_oid = repo
        .commit(Some("HEAD"), &sig, &sig, "initial", &tree, &[])
        .expect("should create commit");

    (dir, commit_oid.to_string())
}

/// Create a bare repo with a `refs/{ns}/main` ref containing gmeta tree
/// structure: `project/testing/__value = "hello"`.
///
/// Used as a remote for push/pull tests.
pub fn setup_bare_with_meta(ns: &str) -> TempDir {
    let bare_dir = TempDir::new().expect("should be able to create temp dir");
    let bare =
        git2::Repository::init_bare(bare_dir.path()).expect("should be able to init bare repo");

    let sig =
        git2::Signature::now("Test User", "test@example.com").expect("should create signature");
    let mut tb = bare
        .treebuilder(None)
        .expect("should create root treebuilder");

    // Build tree: project/testing/__value (blob: "hello")
    let blob_oid = bare.blob(b"\"hello\"").expect("should create blob");
    let mut sub_tb = bare
        .treebuilder(None)
        .expect("should create value treebuilder");
    sub_tb
        .insert("__value", blob_oid, 0o100644)
        .expect("should insert __value");
    let sub_tree_oid = sub_tb.write().expect("should write value tree");

    let mut project_tb = bare
        .treebuilder(None)
        .expect("should create project treebuilder");
    project_tb
        .insert("testing", sub_tree_oid, 0o040000)
        .expect("should insert testing subtree");
    let project_tree_oid = project_tb.write().expect("should write project tree");

    tb.insert("project", project_tree_oid, 0o040000)
        .expect("should insert project tree");
    let tree_oid = tb.write().expect("should write root tree");
    let tree = bare.find_tree(tree_oid).expect("should find root tree");

    let ref_name = format!("refs/{ns}/main");
    bare.commit(Some(&ref_name), &sig, &sig, "initial meta", &tree, &[])
        .expect("should create meta commit");

    bare_dir
}

/// Build a bare repo with multiple gmeta serialize commits for promisor tests.
///
/// The repo has 2 commits on `refs/meta/main`:
///   - Commit 1 (older): `project/old_key/__value = "old_value"`
///   - Commit 2 (tip):   `project/testing/__value = "hello"` (old_key removed)
pub fn setup_bare_with_history() -> TempDir {
    let bare_dir = TempDir::new().expect("should be able to create temp dir");
    let bare =
        git2::Repository::init_bare(bare_dir.path()).expect("should be able to init bare repo");
    let sig =
        git2::Signature::now("Test User", "test@example.com").expect("should create signature");

    // --- Commit 1: project/old_key/__value = "old_value" ---
    let blob1 = bare.blob(b"\"old_value\"").expect("should create blob");
    let mut val_tb = bare
        .treebuilder(None)
        .expect("should create value treebuilder");
    val_tb
        .insert("__value", blob1, 0o100644)
        .expect("should insert __value");
    let val_tree = val_tb.write().expect("should write value tree");

    let mut proj_tb = bare
        .treebuilder(None)
        .expect("should create project treebuilder");
    proj_tb
        .insert("old_key", val_tree, 0o040000)
        .expect("should insert old_key");
    let proj_tree = proj_tb.write().expect("should write project tree");

    let mut root_tb = bare
        .treebuilder(None)
        .expect("should create root treebuilder");
    root_tb
        .insert("project", proj_tree, 0o040000)
        .expect("should insert project tree");
    let root_tree_oid = root_tb.write().expect("should write root tree");
    let root_tree = bare.find_tree(root_tree_oid).expect("should find tree");

    let commit1_msg = "gmeta: serialize (1 changes)\n\nA\tproject\told_key";
    let commit1 = bare
        .commit(None, &sig, &sig, commit1_msg, &root_tree, &[])
        .expect("should create commit 1");
    let commit1_obj = bare.find_commit(commit1).expect("should find commit 1");

    // --- Commit 2 (tip): project/testing/__value = "hello" (old_key removed) ---
    let blob2 = bare.blob(b"\"hello\"").expect("should create blob");
    let mut val_tb2 = bare
        .treebuilder(None)
        .expect("should create value treebuilder");
    val_tb2
        .insert("__value", blob2, 0o100644)
        .expect("should insert __value");
    let val_tree2 = val_tb2.write().expect("should write value tree");

    let mut proj_tb2 = bare
        .treebuilder(None)
        .expect("should create project treebuilder");
    proj_tb2
        .insert("testing", val_tree2, 0o040000)
        .expect("should insert testing");
    let proj_tree2 = proj_tb2.write().expect("should write project tree");

    let mut root_tb2 = bare
        .treebuilder(None)
        .expect("should create root treebuilder");
    root_tb2
        .insert("project", proj_tree2, 0o040000)
        .expect("should insert project tree");
    let root_tree_oid2 = root_tb2.write().expect("should write root tree");
    let root_tree2 = bare.find_tree(root_tree_oid2).expect("should find tree");

    let commit2_msg = "gmeta: serialize (1 changes)\n\nA\tproject\ttesting";
    bare.commit(
        Some("refs/meta/main"),
        &sig,
        &sig,
        commit2_msg,
        &root_tree2,
        &[&commit1_obj],
    )
    .expect("should create commit 2");

    bare_dir
}

/// Build a bare repo where a key exists in both history and tip tree.
///
/// Like [`setup_bare_with_history`] but the tip commit retains `old_key` in
/// its tree alongside `testing`. The tip commit message only mentions `testing`.
pub fn setup_bare_with_history_retained() -> TempDir {
    let bare_dir = TempDir::new().expect("should be able to create temp dir");
    let bare =
        git2::Repository::init_bare(bare_dir.path()).expect("should be able to init bare repo");
    let sig =
        git2::Signature::now("Test User", "test@example.com").expect("should create signature");

    // --- Commit 1: project/old_key/__value = "old_value" ---
    let blob1 = bare.blob(b"\"old_value\"").expect("should create blob");
    let mut val_tb = bare
        .treebuilder(None)
        .expect("should create value treebuilder");
    val_tb
        .insert("__value", blob1, 0o100644)
        .expect("should insert __value");
    let val_tree = val_tb.write().expect("should write value tree");

    let mut proj_tb = bare
        .treebuilder(None)
        .expect("should create project treebuilder");
    proj_tb
        .insert("old_key", val_tree, 0o040000)
        .expect("should insert old_key");
    let proj_tree = proj_tb.write().expect("should write project tree");

    let mut root_tb = bare
        .treebuilder(None)
        .expect("should create root treebuilder");
    root_tb
        .insert("project", proj_tree, 0o040000)
        .expect("should insert project tree");
    let root_tree_oid = root_tb.write().expect("should write root tree");
    let root_tree = bare.find_tree(root_tree_oid).expect("should find tree");

    let commit1_msg = "gmeta: serialize (1 changes)\n\nA\tproject\told_key";
    let commit1 = bare
        .commit(None, &sig, &sig, commit1_msg, &root_tree, &[])
        .expect("should create commit 1");
    let commit1_obj = bare.find_commit(commit1).expect("should find commit 1");

    // --- Commit 2 (tip): both old_key and testing ---
    let blob2 = bare.blob(b"\"hello\"").expect("should create blob");
    let mut val_tb2 = bare
        .treebuilder(None)
        .expect("should create value treebuilder");
    val_tb2
        .insert("__value", blob2, 0o100644)
        .expect("should insert __value");
    let val_tree2 = val_tb2.write().expect("should write value tree");

    let mut proj_tb2 = bare
        .treebuilder(None)
        .expect("should create project treebuilder");
    proj_tb2
        .insert("old_key", val_tree, 0o040000)
        .expect("should insert old_key");
    proj_tb2
        .insert("testing", val_tree2, 0o040000)
        .expect("should insert testing");
    let proj_tree2 = proj_tb2.write().expect("should write project tree");

    let mut root_tb2 = bare
        .treebuilder(None)
        .expect("should create root treebuilder");
    root_tb2
        .insert("project", proj_tree2, 0o040000)
        .expect("should insert project tree");
    let root_tree_oid2 = root_tb2.write().expect("should write root tree");
    let root_tree2 = bare.find_tree(root_tree_oid2).expect("should find tree");

    let commit2_msg = "gmeta: serialize (1 changes)\n\nA\tproject\ttesting";
    bare.commit(
        Some("refs/meta/main"),
        &sig,
        &sig,
        commit2_msg,
        &root_tree2,
        &[&commit1_obj],
    )
    .expect("should create commit 2");

    bare_dir
}

// ── Object transfer helpers (simulate push/pull without network) ─────────────

/// Copy all git objects from `src` repo into a bare repo at `bare_dir`.
///
/// Simulates a push by copying loose objects and pack files.
pub fn copy_meta_objects(src: &git2::Repository, bare_dir: &TempDir) {
    let src_objects = src.path().join("objects");
    let dst_objects = bare_dir.path().join("objects");
    copy_dir_contents(&src_objects, &dst_objects);
}

/// Copy all git objects from a bare repo at `bare_dir` into `dst` repo.
///
/// Simulates a fetch by copying loose objects and pack files.
pub fn copy_meta_objects_from(bare_dir: &TempDir, dst: &git2::Repository) {
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
