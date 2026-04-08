#![allow(dead_code)]

use gmeta_core::Session;

/// Create an isolated git repository with an initial commit, returning
/// the temp directory (owns the lifetime) and the gix repository handle.
pub fn setup_repo() -> (tempfile::TempDir, gix::Repository) {
    let dir = tempfile::TempDir::new().unwrap();
    let _init = gix::init(dir.path()).unwrap();

    // Set user config via git subprocess (gix config mutation API is limited)
    let output = std::process::Command::new("git")
        .args([
            "-C",
            &dir.path().to_string_lossy(),
            "config",
            "user.email",
            "test@example.com",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let output = std::process::Command::new("git")
        .args([
            "-C",
            &dir.path().to_string_lossy(),
            "config",
            "user.name",
            "Test User",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());

    // Open with config overrides for reproducibility
    let repo = gix::open_opts(
        dir.path(),
        gix::open::Options::isolated()
            .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
    )
    .unwrap();

    // Create initial commit so HEAD exists
    let tree_oid = repo.empty_tree().edit().unwrap().write().unwrap().detach();
    let sig = gix::actor::Signature {
        name: "Test User".into(),
        email: "test@example.com".into(),
        time: gix::date::Time::new(946684800, 0),
    };
    let commit = gix::objs::Commit {
        message: "initial".into(),
        tree: tree_oid,
        author: sig.clone(),
        committer: sig,
        encoding: None,
        parents: Default::default(),
        extra_headers: Default::default(),
    };
    let commit_oid = repo.write_object(&commit).unwrap().detach();
    repo.reference(
        "refs/heads/main",
        commit_oid,
        gix::refs::transaction::PreviousValue::Any,
        "",
    )
    .unwrap();
    repo.reference(
        "HEAD",
        commit_oid,
        gix::refs::transaction::PreviousValue::Any,
        "",
    )
    .unwrap();

    (dir, repo)
}

/// Open a session from a repo with a fixed timestamp for determinism.
pub fn open_session(repo: gix::Repository) -> Session {
    Session::open(repo).unwrap().with_timestamp(1000)
}

/// Return the full 40-char commit SHA from the repo's HEAD.
pub fn head_sha(repo: &gix::Repository) -> String {
    repo.head_id().unwrap().to_string()
}

/// Recursively copy all files from one directory to another.
pub fn copy_dir_contents(src: &std::path::Path, dst: &std::path::Path) {
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

/// Copy objects from a source repo into a destination repo and
/// create a remote tracking ref (`refs/meta/origin`) pointing to `oid`.
pub fn inject_remote_ref(
    src_objects_dir: &std::path::Path,
    dst_dir: &std::path::Path,
    oid: gix::ObjectId,
) {
    let dst_objects = dst_dir.join(".git").join("objects");
    copy_dir_contents(src_objects_dir, &dst_objects);

    let dst_repo = gix::open_opts(
        dst_dir,
        gix::open::Options::isolated()
            .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
    )
    .unwrap();
    dst_repo
        .reference(
            "refs/meta/origin",
            oid,
            gix::refs::transaction::PreviousValue::Any,
            "simulated fetch",
        )
        .unwrap();
}

/// Open a second session on an existing repo directory with a
/// given timestamp. Useful after mutating refs externally.
pub fn reopen_session(dir: &std::path::Path, timestamp: i64) -> Session {
    let repo = gix::open_opts(
        dir,
        gix::open::Options::isolated()
            .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
    )
    .unwrap();
    Session::open(repo).unwrap().with_timestamp(timestamp)
}

/// Set up a three-way merge scenario: both repo A and repo C diverge from
/// a common base. Returns `(dir_a, dir_c, base_oid)` where the caller can
/// then modify A and C independently before materializing.
///
/// This function:
/// 1. Creates repo A with `base_fn` applied to the base state, then serializes
/// 2. Fast-forwards repo C from A's base (materialize with no local state)
/// 3. Returns the dirs and the base OID
pub fn setup_three_way_base(
    base_fn: impl FnOnce(&Session),
) -> (tempfile::TempDir, tempfile::TempDir, gix::ObjectId) {
    // Step 1: Create repo A and set up the base state
    let (dir_a, repo_a) = setup_repo();
    let session_a = Session::open(repo_a).unwrap().with_timestamp(1000);
    base_fn(&session_a);
    let _ = session_a.serialize().unwrap();

    // Step 2: Find A's local ref (the base commit)
    let repo_a_reopen = gix::open_opts(
        dir_a.path(),
        gix::open::Options::isolated()
            .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
    )
    .unwrap();
    let base_oid = repo_a_reopen
        .find_reference("refs/meta/local/main")
        .unwrap()
        .into_fully_peeled_id()
        .unwrap()
        .detach();

    // Step 3: Create repo C, fast-forward materialize A's base into it
    let (dir_c, _repo_c) = setup_repo();
    let src_objects = dir_a.path().join(".git").join("objects");
    let dst_objects = dir_c.path().join(".git").join("objects");
    copy_dir_contents(&src_objects, &dst_objects);

    // Set remote tracking ref in C pointing to A's base commit
    let repo_c_reopen = gix::open_opts(
        dir_c.path(),
        gix::open::Options::isolated()
            .config_overrides(["user.name=Test User", "user.email=test@example.com"]),
    )
    .unwrap();
    repo_c_reopen
        .reference(
            "refs/meta/origin",
            base_oid,
            gix::refs::transaction::PreviousValue::Any,
            "initial sync",
        )
        .unwrap();

    // Materialize into C to establish the common base
    let session_c = Session::open(repo_c_reopen).unwrap().with_timestamp(1500);
    let _ = session_c.materialize(None).unwrap();

    (dir_a, dir_c, base_oid)
}
